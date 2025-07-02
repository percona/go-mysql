# go-mysql

[![Build](https://github.com/percona/go-mysql/actions/workflows/go.yml/badge.svg)](https://github.com/percona/go-mysql/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/percona/go-mysql)](https://goreportcard.com/report/github.com/percona/go-mysql)
[![CLA assistant](https://cla-assistant.percona.com/readme/badge/percona/go-mysql)](https://cla-assistant.percona.com/percona/go-mysql)

This repo contains Go packages to help build tools for MySQL. For example, there's a [slow log parser](https://github.com/percona/go-mysql/tree/main/log/slow) and a [query fingerprinter](https://github.com/percona/go-mysql/tree/main/query). Feedback, pull requests, and bug reports are welcome.

## Docs

http://godoc.org/github.com/percona/go-mysql

## Overview

Package|Contains
-------|--------
[event](http://godoc.org/github.com/percona/go-mysql/event)|Aggregator and metric stats
[log](http://godoc.org/github.com/percona/go-mysql/log)|Event struct and log parser interface
[log/slow](http://godoc.org/github.com/percona/go-mysql/log/slow)|Slow log parser
[query](http://godoc.org/github.com/percona/go-mysql/query)|Fingerprinter and ID
test|Sample data

## Versioning

Packages are not versioned. Interfaces, data structures, and overall usage are subject to change without notice.

## Licensing

As of September 2019, percona/go-mysql has dropped AGPLv3 and is now licensed using the BSD 3-clause license.

Copyright (c) 2019, Percona LLC.
All rights reserved.

This source code is licensed under the BSD-style license found in the
LICENSE file in the root directory of this source tree.
