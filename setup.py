from setuptools import setup
import json


with open('metadata.json') as fp:
    metadata = json.load(fp)


setup(
    name='lexibank_diacl',
    description=metadata['title'],
    license=metadata.get('license', ''),
    url=metadata.get('url', ''),
    py_modules=['lexibank_diacl'],
    include_package_data=True,
    zip_safe=False,
    entry_points={
        'lexibank.dataset': [
            'diacl=lexibank_diacl:Dataset',
        ]
    },
    install_requires=[
        'pylexibank>=2.0',
    ],
    extras_require={
        'test': [
            'pytest-cldf',
        ],
    },
)
