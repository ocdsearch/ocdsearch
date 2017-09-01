import argparse
from aiohttp import web
from elasticsearch_async import AsyncElasticsearch
from elasticsearch.exceptions import ElasticsearchException

default_index = 'tenders'


def match_query(field, text, operator=None, analyzer=None):
    query = {"query": text}
    if operator and text.find(" ") >= 0:
        query["operator"] = operator
    if analyzer:
        query["analyzer"] = analyzer
    return {"match": {field: query}}


def terms_query(field, values):
    if not isinstance(values, list):
        values = values.split(" ")
    if len(values) == 1:
        return {"term": {field: values[0]}}
    return {"terms": {field: values}}


def build_query(args):
    body = []

    query = args.pop('query', None)
    if query:
        body.append(match_query("_all", query))

    status = args.pop('status', [])
    if status:
        body.append(terms_query("status", status))

    tid = args.pop('tid', [])
    if tid:
        body.append(terms_query("tenderID", tid))

    if not body:
        body = {'query': {'match_all': {}}}
    elif len(body) == 1:
        body = {'query': body[0]}
    else:
        body = {'query': {'bool': {'must': body}}}

    return body


async def search(request):
    if request.method == "POST":
        args = await request.json()
    else:
        args = dict(request.query)
    start = args.pop('start', 0)
    index = args.pop('api', default_index)
    body = build_query(args)
    try:
        resp = await request.app['es'].search(index, body=body, from_=start)
    except ElasticsearchException as e:
        return web.json_response({'error': str(e)})
    if 'error' in resp or 'hits' not in resp:
        return web.json_response(resp)
    hits = resp['hits']
    data = {
        'query': body,
        'items': [i['_source'] for i in hits['hits']],
        'total': hits.get('total', 0),
        'start': start
    }
    return web.json_response(data)


async def status(request):
    index = request.query.get('api', default_index)
    data = await request.app['es'].indices.stats(index)
    index_total = data['indices'][index]['total']
    return web.json_response(index_total)


def main():
    parser = argparse.ArgumentParser(description="OCDSearch server")
    parser.add_argument("-s", "--path")
    parser.add_argument("-p", "--port", type=int)
    parser.add_argument("-H", "--host", help="custom elastic host")
    args = parser.parse_args()
    host = args.host or "localhost"

    app = web.Application()
    app.router.add_get('/', status)
    app.router.add_get('/search', search)
    app.router.add_post('/search', search)
    app['es'] = AsyncElasticsearch([host], retry_on_timeout=True, timeout=30)
    web.run_app(app, path=args.path, port=args.port)
    app['es'].transport.close()
