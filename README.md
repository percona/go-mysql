# go-mysql

This repo contains Go packages to help build tools for MySQL. For example, there's a [slow log parser](https://github.com/percona/go-mysql/tree/master/log/slow) and a [query fingerpinter](https://github.com/percona/go-mysql/tree/master/query). At [Percona](http://www.percona.com) we use these packages to build the agent for [Percona Cloud Tools](https://cloud.percona.com). We hope you find these packages useful. Feedback, pull requests, and bug reports are welcome.

## Overview

Package|Contains
-------|--------
event|Aggregator and metric statistics
log|Slow log parser
query|Fingerprinter and ID
test|Sample data

## Versioning

Packages are not versioned. Interfaces, data structures, and overall usage are subject to change without notice.

## Testing

To test these packages locally, you'll need:
* github.com/go-test/test
* gopkg.in/check.v1
