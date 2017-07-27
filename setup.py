#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='cassandra_snapshot_backup',
    version='0.0.1',
    install_requires=['boto3'],
    find_packages=find_packages()
)
