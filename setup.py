#!/usr/bin/env python

from setuptools import setup, find_packages
try:  # for pip >= 10
    from pip._internal.req import parse_requirements
    from pip._internal.exceptions import InstallationError
except ImportError:  # for pip <= 9.0.3
    from pip.req import parse_requirements
    from pip.req import InstallationError

try:
    # Used to get requirements in production builds
    install_reqs = parse_requirements('requirements.txt', session='dummy')
    reqs = [str(ir.req) for ir in install_reqs]
except InstallationError:
    # Developers use Pipenv instead that also deals with requirements
    reqs = []

try:
    with open('VERSION.txt', 'r') as v:
        version = v.read().strip()
except FileNotFoundError:
    version = '0.0.0.dev0'

with open('DESCRIPTION', 'r') as d:
    long_description = d.read()

setup(
    name='nyuki',
    description='Base library to create microservices with MQTT and HTTP REST API.',
    long_description=long_description,
    url='http://www.enovacom.fr',
    author='Enovacom Surycat',
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
    ]
)
