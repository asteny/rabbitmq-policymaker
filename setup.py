# encoding: utf-8
from __future__ import absolute_import, print_function

from setuptools import setup, find_packages

__version__ = "0.0.7"
__author__ = "Pavel Sofrony <pavel@sofrony.ru>"


def load_requirements(fname):
    """ load requirements from a pip requirements file """
    with open(fname) as f:
        line_iter = (line.strip() for line in f.readlines())
        return [line for line in line_iter if line and line[0] != "#"]


setup(
    name="rabbitmq-policymaker",
    version=__version__,
    author=__author__,
    author_email="pavel@sofrony.ru",
    license="MIT",
    description="Policymaker for RabbitMQ",
    platforms="all",
    packages=find_packages(),
    install_requires=[
        load_requirements("requirements.txt"),
        "pyrabbit2 @ https://github.com/asteny/pyrabbit2/archive/master.zip",
    ],
    extras_require={"develop": load_requirements("requirements.dev.txt")},
    python_requires=">=3.7",
    entry_points={
        "console_scripts": [
            "rabbitmq_policymaker = rabbitmq_policymaker.main:main"
        ]
    },
)
