from setuptools import setup

setup(
    name='ocdsearch',
    version='0.1.dev0',
    description='OCDSearch Index and Search utilites',
    url='https://github.com/imaginal/ocdsearch',
    author='Volodymyr Flonts',
    author_email='flyonts@gmail.com',
    license='Apache License 2.0',
    packages=['ocdsearch'],
    install_requires=[
        'elasticsearch-async',
    ],
    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },
    entry_points={
        'console_scripts': [
            'ocds_index=ocds_index:main',
        ],
    },
)
