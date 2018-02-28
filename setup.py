#!/usr/bin/env python

from pip.req import parse_requirements
from setuptools import setup, find_packages


try:
    with open('VERSION.txt', 'r') as v:
        version = v.read().strip()
except FileNotFoundError:
    version = '0.0.0.dev0'

with open('DESCRIPTION', 'r') as d:
    long_description = d.read()

# Requirements
install_reqs = parse_requirements('requirements.txt', session='dummy')
reqs = [str(ir.req) for ir in install_reqs]

setup(
    name='nyuki',
    description='Allowing the creation of independent unit to deal with stream processing while exposing an MQTT and REST API.',
    long_description=long_description,
    url='http://www.surycat.com',
    author='Optiflows R&D',
    author_email='rand@surycat.com',
    version=version,
    install_requires=reqs,
    packages=find_packages(exclude=['tests']),
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3 :: Only',
    ],
)
