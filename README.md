# Nyuki :bee:

[![Circle CI](https://img.shields.io/circleci/project/surycat/nyuki/master.svg)](https://circleci.com/gh/surycat/nyuki)
[![pypi version](http://img.shields.io/pypi/v/nyuki.svg)](https://pypi.python.org/pypi/nyuki)
[![python versions](https://img.shields.io/pypi/pyversions/nyuki.svg)](https://pypi.python.org/pypi/nyuki/)


## tl;dr
A lightweight Python library designed to implement agents (aka nyukis). It provides features that shall help developers with managing the following topics:

* Expose service features through a RESTful API
* Communication between nyukis (over HTTP or MQTT)
* Helpers for asyncio-based programming

This library has been written with a focus on reliability and developer-friendliness. Its design promotes single-threaded and asynchronous coding style through the extensive use of the [Python asyncio](https://docs.python.org/3/library/asyncio.html) event loop. A single loop is used to manage HTTP and MQTT-based communications as well as executing internal logic. Nyukis are suited for Agent-Oriented Programming and very useful to build distributed systems and scalable services.

## What is a nyuki?
A nyuki (a bee in Swahili) is an entity designed for real-time data processing (stream processing). Tying together several nyukis allows nearly unlimited use cases: from home automation (e.g. warm up my home when you're less than 5 miles away) to smart industries (lower down pressure and notify staff upon reaching a temperature threshold). This is up to the developer to write his own user story! Following that philosophy, nyukis are the nuts and bolts that helped design [Surycat](http://www.surycat.com).

Here is a list of core concepts tied to a nyuki:

* A nyuki runs as a standalone process
* A nyuki manages its own storage area (if it has data to store)
* A nyuki provides its own HTTP RESTful API
* A nyuki is connected to a bus for 1-to-many communication with other nyukis (currently using MQTT)

## Requirements
All you need is a Python interpreter. At the moment, only **Python 3.6** is supported.

## Getting started

Install the nyuki library:
```bash
pip install nyuki
```

Nyuki's paradigms are convenient for Docker-based environment. We recommend using one container per nyuki implementation.

Read more about the lib. in the [wiki](https://github.com/surycat/nyuki/wiki):
* [Examples](https://github.com/surycat/nyuki/wiki/Examples)
* [Configuration](https://github.com/surycat/nyuki/wiki/Configuration)
* [Features](https://github.com/surycat/nyuki/wiki/Features)

## Contributing

We always welcome great ideas. If you want to hack on the library, a [guide](CONTRIBUTING.md) is dedicated to it and describes the various steps involved.
