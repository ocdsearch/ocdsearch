import argparse
import asyncio
import logging
import json
from glob import glob
from uuid import uuid1
from elasticsearch_async import AsyncElasticsearch

logger = logging.getLogger(__name__)

index_body = {
    "settings": {
        "number_of_shards": 1
    },
    "mappings": {
        "tender": {
            "dynamic_templates": [
                {
                    "texts": {
                        "match_mapping_type": "string",
                        "match_pattern": "regex",
                        "match": "(title|description|details|name|locality|streetAddress)",
                        "mapping": { "type": "text" },
                    }
                },
                {
                    "keywords": {
                        "match_mapping_type": "string",
                        "mapping": { "type": "keyword", "ignore_above": 256 },
                    }
                }
            ],
            "_all": {
                "analyzer": "spanish",
            },
            "properties": {
                "status": { "type": "keyword" },
            }
        }
    }
}


async def index_tender(es, index_name, data):
    tender = data['tender']

    for key in ('awards', 'buyer', 'contracts', 'date', 'id'):
        if key not in tender and key in data:
            tender[key] = data[key]

    tender['tenderID'] = tender['id'] if 'id' in tender else str(uuid1())

    await es.index(index_name, doc_type='tender', body=tender)


async def async_main(es, args):
    exists = await es.indices.exists(args.index)

    if exists and args.delete:
        await es.indices.delete(args.index)
        exists = False

    if not exists:
        await es.indices.create(args.index, body=index_body)

    notjson = 0
    nottend = 0
    indexed = 0

    for name in glob("{}/*.json".format(args.path)):
        try:
            with open(name, encoding='utf-8') as fp:
                data = json.load(fp)
        except:
            logging.error("NOT_JSON {}".format(name))
            notjson += 1
            continue

        if 'tender' not in data:
            logging.warning("NOT_TENDER {}".format(name))
            nottend += 1
            continue

        try:
            logging.info("INDEX {}".format(name))
            await index_tender(es, args.index, data)
            indexed += 1
        except asyncio.CancelledError:
            logging.warning("CANCELLED")
            break

    logging.info("TOTAL {} indexed, {} not tender, {} not json"
                 .format(indexed, nottend, notjson))


def main():
    parser = argparse.ArgumentParser(description="OCDSearch indexer")
    parser.add_argument("index", default="ocds_tenders", help="index name")
    parser.add_argument("path", default=".", help="dir with json")
    parser.add_argument("-d", "--delete", action="store_true", help="delete index")
    parser.add_argument("-H", "--host", help="custom elastic host")
    args = parser.parse_args()
    host = args.host or "localhost"

    logging.basicConfig(format='%(levelname)s %(message)s', level=logging.INFO)

    client = AsyncElasticsearch([host], retry_on_timeout=True, timeout=30)
    client.loop = asyncio.get_event_loop()
    try:
        client.loop.run_until_complete(async_main(client, args))
    except KeyboardInterrupt:
        pass
    finally:
        client.loop.stop()
        client.transport.close()
