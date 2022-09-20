from setuptools import setup, find_packages
from io import open
from os import path

import pathlib

VERSION = '0.2.3'

HERE = pathlib.Path(__file__).parent

README = (HERE / 'README.md').read_text()

with open(path.join(HERE, 'requirements.txt'), encoding='utf-8') as f:
    all_reqs = f.read().split('\n')

install_requires = [x.strip() for x in all_reqs if ('git+' not in x) and (
    not x.startswith('#')) and (not x.startswith('-'))]
dependency_links = [x.strip().replace('git+', '') for x in all_reqs if 'git+' not in x]

setup(
    name='kafka-connect-cli',
    description='A Simple CLI for Kafka connect REST API',
    version=VERSION,
    packages=find_packages(),
    install_requires=install_requires,
    python_requires='>=3.6',
    entry_points='''
            [console_scripts]
            kafka_connect=kafka_connect.__main__:main
        ''',
    author='Andrii Parfonov',
    keyword='kafka connect',
    long_description=README,
    long_description_content_type='text/markdown',
    license='MIT',
    url='https://github.com/andrew00x/kafka-connect-cli',
    download_url=f'https://github.com/andrew00x/kafka-connect-cli/archive/refs/tags/{VERSION}.tar.gz',
    dependency_links=dependency_links,
    author_email='andrew00x@gmail.com',
)
