package query_test

import (
	"fmt"

	"github.com/percona/go-mysql/query"
)

func ExampleFingerprint() {
	q := "SELECT c FROM t WHERE a=1 AND b='foo'\n" +
		"/* some comment */ ORDER BY c ASC LIMIT 1"
	f := query.Fingerprint(q)
	fmt.Println(f)
}
