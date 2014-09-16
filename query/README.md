go-mysql/query
==============

This package contains functions to transform queries. This is used by [go-mysql/event](https://github.com/percona/go-mysql/event) to identify and group unique queries. Check out [percona-agent](https://github.com/percona/percona-agent) for a complete, real-world example.

This package is maintained by [Percona](http://www.percona.com/). Please create an account and report bugs at [jira.percona.com](http://jira.percona.com).

## Example

A command-line query fingerprint:

```go
package main

import (
	"fmt"
	"github.com/go-mysql/query"
	"os"
)

func main() {
	if len(os.Args) > 1 {
		fingerprint := query.Fingerprint(os.Args[1])
		fmt.Println(fingerprint)
	}
}
```

## Functions

### Fingerprint(query string) string

`Fingerprint` returns the canonical form of a query by replacing number, quoted strings, and value lists with `?`. It also normalizes white space, remove comments, and other transformations. Given

```sql
SELECT col1 FROM tbl2 WHERE id IN (1, 2, 3) OR name = 'Go' LIMIT 1
```

it returns

```sql
select col1 from tbl2 where id in(?+) or name = ? limit ?
```

Fingerprinting queries is usually done with regex. This works but it's slow (sometimes terribly slow if the regex are not carefully written to avoid backtracking). Also, regex make certain transformations very difficult or impossible, e.g. "123" in "table123" looks no different than in "col = 123" unless the regex is made more complicated (and slower). Moreover, Go regex does not support several look-around assertions like `(?=re)` and `(?!re)`. This makes matching even more difficult and complicated. For these reasons this fingerprint does not use regex. Even better: this fingerprint makes a single pass through the query, unlike several passes with several regex. The result is that, in informal benchmarks, this fingerprint is 3-5x faster than using regex.

### Id(fingerprint string) string

`Id` returns a 16-character long MD5 checksum of the given fingerprint. A fingerprint and an ID uniquely identify the same query (in most cases), but the ID is shorter and fixed-length which makes it easier to store, query, and use in URLs.
