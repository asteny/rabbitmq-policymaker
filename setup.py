# encoding: utf-8
from __future__ import absolute_import, print_function

from setuptools import setup, find_packages

__version__ = '0.0.1'
__author__ = 'Pavel Sofrony <pavel@sofrony.ru>'

setup(
    name='rabbitmq-policymaker',
    version=__version__,
    author=__author__,
    author_email='pavel@sofrony.ru',
    license="MIT",
    description="Policymaker for RabbitMQ",
    platforms="all",
    packages=find_packages(),
    install_requires=(
        'certifi==2019.11.28',
        'chardet==3.0.4',
        'colorlog==4.1.0',
        'ConfigArgParse==1.0',
        'fast-json==0.3.2',
        'idna==2.8',
        'multidict==4.7.4',
        'prettylog==0.3.0',
        'pyarlo==0.2.3',
        'pyrabbit2==1.0.7',
        'requests==2.22.0',
        'sseclient-py==1.7',
        'ujson==1.35',
        'urllib3==1.25.8',
        'yarl==1.4.2',
    ),
    entry_points={
        'console_scripts': [
            'rabbitmq-policymaker = rabbitmq-policymaker.main:main',
        ],
    },
    extras_require={
        ':python_version <= "3.7.3"': 'typing >= 3.5.2',
    },
)
