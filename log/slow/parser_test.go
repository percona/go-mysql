/*
Copyright (c) 2019, Percona LLC.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package slow_test

import (
	l "log"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/percona/go-mysql/log"
	parser "github.com/percona/go-mysql/log/slow"
	"github.com/percona/go-mysql/test"
)

var (
	sample = test.RootDir() + "/test/slow-logs"
	opt    = log.Options{
		Debug:           false,
		DefaultLocation: time.UTC,
	}
)

func parseSlowLog(t *testing.T, filename string, o log.Options) []log.Event {
	file, err := os.Open(path.Join(sample, "/", filename))
	require.NoError(t, err)
	p := parser.NewSlowLogParser(file, o)
	require.NoError(t, err)
	got := []log.Event{}
	go p.Start()
	for e := range p.EventChan() {
		got = append(got, *e)
	}
	return got
}

// --------------------------------------------------------------------------

// No input, no events.
func TestParserEmptySlowLog(t *testing.T) {
	got := parseSlowLog(t, "empty.log", opt)
	expect := []log.Event{}
	assert.EqualValues(t, expect, got)
}

// slow001 is a most basic basic, normal slow log--nothing exotic.
func TestParserSlowLog001(t *testing.T) {
	got := parseSlowLog(t, "slow001.log", opt)
	expect := []log.Event{
		{
			Ts:        time.Date(2007, 10, 15, 21, 43, 52, 0, time.UTC),
			Admin:     false,
			Query:     `select sleep(2) from n`,
			User:      "root",
			Host:      "localhost",
			Db:        "test",
			Offset:    199,
			OffsetEnd: 358,
			TimeMetrics: map[string]float64{
				"Query_time": 2,
				"Lock_time":  0,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
		},
		{
			Ts:        time.Date(2007, 10, 15, 21, 45, 10, 0, time.UTC),
			Admin:     false,
			Query:     `select sleep(2) from test.n`,
			User:      "root",
			Host:      "localhost",
			Db:        "sakila",
			Offset:    358,
			OffsetEnd: 524,
			TimeMetrics: map[string]float64{
				"Query_time": 2,
				"Lock_time":  0,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
		},
	}
	assert.EqualValues(t, expect, got)
}

// slow002 is a basic slow log like slow001 but with more metrics, multi-line queries, etc.
func TestParseSlowLog002(t *testing.T) {
	got := parseSlowLog(t, "slow002.log", opt)
	expect := []log.Event{
		{
			Query:     "BEGIN",
			Ts:        time.Date(2007, 12, 18, 11, 48, 27, 0, time.UTC),
			Admin:     false,
			User:      "[SQL_SLAVE]",
			Host:      "",
			Offset:    0,
			OffsetEnd: 337,
			TimeMetrics: map[string]float64{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Thread_id":     10,
				"Rows_examined": 0,
				"Rows_sent":     0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Db: "db1",
			Query: `update db2.tuningdetail_21_265507 n
      inner join db1.gonzo a using(gonzo)
      set n.column1 = a.column1, n.word3 = a.word3`,
			Admin:     false,
			User:      "[SQL_SLAVE]",
			Host:      "",
			Offset:    337,
			OffsetEnd: 813,
			TimeMetrics: map[string]float64{
				"Query_time": 0.726052,
				"Lock_time":  0.000091,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Thread_id":     10,
				"Rows_examined": 62951,
				"Rows_sent":     0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         true,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `INSERT INTO db3.vendor11gonzo (makef, bizzle)
VALUES ('', 'Exact')`,
			Admin:     false,
			User:      "[SQL_SLAVE]",
			Host:      "",
			Offset:    813,
			OffsetEnd: 1332,
			TimeMetrics: map[string]float64{
				"InnoDB_queue_wait":    0.000000,
				"Lock_time":            0.000077,
				"InnoDB_rec_lock_wait": 0.000000,
				"Query_time":           0.000512,
				"InnoDB_IO_r_wait":     0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 24,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `UPDATE db4.vab3concept1upload
SET    vab3concept1id = '91848182522'
WHERE  vab3concept1upload='6994465'`,
			Admin:     false,
			User:      "[SQL_SLAVE]",
			Host:      "",
			Offset:    1332,
			OffsetEnd: 1862,
			TimeMetrics: map[string]float64{
				"Query_time":           0.033384,
				"InnoDB_IO_r_wait":     0.000000,
				"InnoDB_queue_wait":    0.000000,
				"Lock_time":            0.000028,
				"InnoDB_rec_lock_wait": 0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 11,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `INSERT INTO db1.conch (word3, vid83)
VALUES ('211', '18')`,
			Admin:     false,
			User:      "[SQL_SLAVE]",
			Host:      "",
			Offset:    1862,
			OffsetEnd: 2391,
			TimeMetrics: map[string]float64{
				"InnoDB_queue_wait":    0.000000,
				"Query_time":           0.000530,
				"InnoDB_IO_r_wait":     0.000000,
				"Lock_time":            0.000027,
				"InnoDB_rec_lock_wait": 0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 18,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `UPDATE foo.bar
SET    biz = '91848182522'`,
			Admin:     false,
			User:      "[SQL_SLAVE]",
			Host:      "",
			Offset:    2391,
			OffsetEnd: 2859,
			TimeMetrics: map[string]float64{
				"Lock_time":            0.000027,
				"InnoDB_rec_lock_wait": 0.000000,
				"InnoDB_queue_wait":    0.000000,
				"Query_time":           0.000530,
				"InnoDB_IO_r_wait":     0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 18,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `UPDATE bizzle.bat
SET    boop='bop: 899'
WHERE  fillze='899'`,
			Admin:     false,
			User:      "[SQL_SLAVE]",
			Host:      "",
			Offset:    2859,
			OffsetEnd: 3372,
			TimeMetrics: map[string]float64{
				"Query_time":           0.000530,
				"InnoDB_IO_r_wait":     0.000000,
				"InnoDB_queue_wait":    0.000000,
				"Lock_time":            0.000027,
				"InnoDB_rec_lock_wait": 0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 18,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
		{
			Query: `UPDATE foo.bar
SET    biz = '91848182522'`,
			Admin:     false,
			User:      "[SQL_SLAVE]",
			Host:      "",
			Offset:    3372,
			OffsetEnd: 3840,
			TimeMetrics: map[string]float64{
				"Query_time":           0.000530,
				"Lock_time":            0.000027,
				"InnoDB_rec_lock_wait": 0.000000,
				"InnoDB_queue_wait":    0.000000,
				"InnoDB_IO_r_wait":     0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_IO_r_bytes":     0,
				"Merge_passes":          0,
				"InnoDB_pages_distinct": 18,
				"Rows_sent":             0,
				"Thread_id":             10,
				"Rows_examined":         0,
				"InnoDB_IO_r_ops":       0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Full_scan":         false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Tmp_table_on_disk": false,
				"Tmp_table":         false,
				"QC_Hit":            false,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

// slow003 starts with a blank line.  I guess this once messed up SlowLogParser.pm?
func TestParserSlowLog003(t *testing.T) {
	got := parseSlowLog(t, "slow003.log", opt)
	expect := []log.Event{
		{
			Query:     "BEGIN",
			Admin:     false,
			Host:      "",
			Ts:        time.Date(2007, 12, 18, 11, 48, 27, 0, time.UTC),
			User:      "[SQL_SLAVE]",
			Offset:    1,
			OffsetEnd: 338,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float64{
				"Lock_time":  0.000000,
				"Query_time": 0.000012,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     10,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

// I don't know what's special about this slow004.
func TestParserSlowLog004(t *testing.T) {
	got := parseSlowLog(t, "slow004.log", opt)
	expect := []log.Event{
		{
			Query:       "select 12_13_foo from (select 12foo from 123_bar) as 123baz",
			Admin:       false,
			Host:        "localhost",
			Ts:          time.Date(2007, 10, 15, 21, 43, 52, 0, time.UTC),
			User:        "root",
			Offset:      199,
			OffsetEnd:   385,
			BoolMetrics: map[string]bool{},
			TimeMetrics: map[string]float64{
				"Lock_time":  0.000000,
				"Query_time": 2.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 0,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

// slow005 has a multi-line query with tabs in it.  A pathological case that
// would probably break the parser is a query like:
//
//	SELECT * FROM foo WHERE col = "Hello
//	# Query_time: 10
//	" LIMIT 1;
//
// There's no easy way to detect that "# Query_time" is part of the query and
// not part of the next event's header.
func TestParserSlowLog005(t *testing.T) {
	got := parseSlowLog(t, "slow005.log", opt)
	expect := []log.Event{
		{
			Query:     "foo\nbar\n\t\t\t0 AS counter\nbaz",
			Admin:     false,
			Host:      "",
			Ts:        time.Date(2007, 12, 18, 11, 48, 27, 0, time.UTC),
			User:      "[SQL_SLAVE]",
			Offset:    0,
			OffsetEnd: 359,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float64{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     10,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

// slow006 has the Schema: db metric _or_ use db; lines before the queries.
// Schema value should be used for log.Event.Db is no use db; line is present.
func TestParserSlowLog006(t *testing.T) {
	got := parseSlowLog(t, "slow006.log", opt)
	expect := []log.Event{
		{
			Query:     "SELECT col FROM foo_tbl",
			Db:        "foo",
			Admin:     false,
			Host:      "",
			Ts:        time.Date(2007, 12, 18, 11, 48, 27, 0, time.UTC),
			User:      "[SQL_SLAVE]",
			Offset:    0,
			OffsetEnd: 368,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float64{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     10,
			},
		},
		{
			Query:     "SELECT col FROM foo_tbl",
			Db:        "foo",
			Admin:     false,
			Host:      "",
			Ts:        time.Date(2007, 12, 18, 11, 48, 57, 0, time.UTC),
			User:      "[SQL_SLAVE]",
			Offset:    368,
			OffsetEnd: 736,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float64{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     10,
			},
		},
		{
			Query:     "SELECT col FROM bar_tbl",
			Db:        "bar",
			Admin:     false,
			Host:      "",
			Ts:        time.Date(2007, 12, 18, 11, 48, 57, 0, time.UTC),
			User:      "[SQL_SLAVE]",
			Offset:    736,
			OffsetEnd: 1100,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float64{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     20,
			},
		},
		{
			Query:     "SELECT col FROM bar_tbl",
			Db:        "bar",
			Admin:     false,
			Host:      "",
			Ts:        time.Date(2007, 12, 18, 11, 49, 5, 0, time.UTC),
			User:      "[SQL_SLAVE]",
			Offset:    1100,
			OffsetEnd: 1468,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float64{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     10,
			},
		},
		{
			Query:     "SELECT col FROM bar_tbl",
			Db:        "bar",
			Admin:     false,
			Host:      "",
			Ts:        time.Date(2007, 12, 18, 11, 49, 7, 0, time.UTC),
			User:      "[SQL_SLAVE]",
			Offset:    1468,
			OffsetEnd: 1832,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float64{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     20,
			},
		},
		{
			Query:     "SELECT col FROM foo_tbl",
			Db:        "foo",
			Admin:     false,
			Host:      "",
			Ts:        time.Date(2007, 12, 18, 11, 49, 30, 0, time.UTC),
			User:      "[SQL_SLAVE]",
			Offset:    1832,
			OffsetEnd: 2200,
			BoolMetrics: map[string]bool{
				"Filesort_on_disk":  false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
			},
			TimeMetrics: map[string]float64{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     30,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

// slow007 has Schema: db1 _and_ use db2;.  db2 should be used.
func TestParserSlowLog007(t *testing.T) {
	got := parseSlowLog(t, "slow007.log", opt)
	expect := []log.Event{
		{
			Query:       "SELECT fruit FROM trees",
			Db:          "db2",
			Admin:       false,
			Host:        "",
			Ts:          time.Date(2007, 12, 18, 11, 48, 27, 0, time.UTC),
			User:        "[SQL_SLAVE]",
			Offset:      0,
			OffsetEnd:   193,
			BoolMetrics: map[string]bool{},
			TimeMetrics: map[string]float64{
				"Query_time": 0.000012,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     3,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

// slow008 has 4 interesting things (which makes it a poor test case since we're
// testing many things at once):
//  1. an admin command, e.g.: # administrator command: Quit;
//  2. a SET NAMES query; SET <certain vars> are ignored
//  3. No Time metrics
//  4. IPs in the host metric, but we don't currently support these
func TestParserSlowLog008(t *testing.T) {
	got := parseSlowLog(t, "slow008.log", opt)
	expect := []log.Event{
		{
			Query:       "Quit",
			Db:          "db1",
			Admin:       true,
			Host:        "",
			User:        "meow",
			Offset:      0,
			OffsetEnd:   220,
			BoolMetrics: map[string]bool{},
			TimeMetrics: map[string]float64{
				"Query_time": 0.000002,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     5,
			},
		},
		{
			Query:       "SET NAMES utf8",
			Db:          "db",
			Admin:       false,
			Host:        "",
			User:        "meow",
			Offset:      220,
			OffsetEnd:   434,
			BoolMetrics: map[string]bool{},
			TimeMetrics: map[string]float64{
				"Query_time": 0.000899,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     6,
			},
		},
		{
			Query:       "SELECT MIN(id),MAX(id) FROM tbl",
			Db:          "db2",
			Admin:       false,
			Host:        "",
			User:        "meow",
			Offset:      434,
			OffsetEnd:   656,
			BoolMetrics: map[string]bool{},
			TimeMetrics: map[string]float64{
				"Query_time": 0.018799,
				"Lock_time":  0.009453,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     0,
				"Thread_id":     6,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

// Filter admin commands
func TestParserSlowLog009(t *testing.T) {
	opt := opt
	opt.FilterAdminCommand = map[string]bool{
		"Quit": true,
	}
	got := parseSlowLog(t, "slow009.log", opt)
	expect := []log.Event{
		{
			Query:     "Refresh",
			Db:        "",
			Admin:     true,
			Host:      "localhost",
			User:      "root",
			Offset:    196,
			OffsetEnd: 562,
			Ts:        time.Date(2009, 3, 11, 18, 11, 50, 0, time.UTC),
			TimeMetrics: map[string]float64{
				"Query_time": 0.017850,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Merge_passes":  0,
				"Rows_affected": 0,
				"Rows_examined": 0,
				"Rows_read":     0,
				"Rows_sent":     0,
				"Thread_id":     47,
			},
			BoolMetrics: map[string]bool{
				"QC_Hit":            false,
				"Full_scan":         false,
				"Full_join":         false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Filesort_on_disk":  false,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

// Rate limit
func TestParserSlowLog011(t *testing.T) {
	got := parseSlowLog(t, "slow011.log", opt)
	expect := []log.Event{
		{
			Offset:    0,
			OffsetEnd: 732,
			Query:     "SELECT foo FROM bar WHERE id=1",
			Db:        "maindb",
			Host:      "localhost",
			User:      "user1",
			Ts:        time.Date(2013, 11, 28, 1, 5, 31, 0, time.UTC),
			RateType:  "query",
			RateLimit: 2,
			TimeMetrics: map[string]float64{
				"Query_time":           0.000228,
				"Lock_time":            0.000114,
				"InnoDB_IO_r_wait":     0.000000,
				"InnoDB_rec_lock_wait": 0.000000,
				"InnoDB_queue_wait":    0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Rows_sent":             1,
				"Rows_examined":         1,
				"Rows_affected":         0,
				"Bytes_sent":            545,
				"Tmp_tables":            0,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Merge_passes":          0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_pages_distinct": 2,
			},
			BoolMetrics: map[string]bool{
				"QC_Hit":            false,
				"Full_scan":         false,
				"Full_join":         false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Filesort_on_disk":  false,
			},
		},
		{
			Offset:    732,
			OffsetEnd: 1440,
			Query:     "SELECT foo FROM bar WHERE id=2",
			Db:        "maindb",
			Host:      "localhost",
			User:      "user1",
			RateType:  "query",
			RateLimit: 2,
			TimeMetrics: map[string]float64{
				"Query_time":           0.000237,
				"Lock_time":            0.000122,
				"InnoDB_IO_r_wait":     0.000000,
				"InnoDB_rec_lock_wait": 0.000000,
				"InnoDB_queue_wait":    0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Rows_sent":             1,
				"Rows_examined":         1,
				"Rows_affected":         0,
				"Bytes_sent":            545,
				"Tmp_tables":            0,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Merge_passes":          0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_pages_distinct": 2,
			},
			BoolMetrics: map[string]bool{
				"QC_Hit":            false,
				"Full_scan":         false,
				"Full_join":         false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
				"Filesort":          false,
				"Filesort_on_disk":  false,
			},
		},
		{
			Offset:    1440,
			OffsetEnd: 2152,
			Query:     "INSERT INTO foo VALUES (NULL, 3)",
			Db:        "maindb",
			Host:      "localhost",
			User:      "user1",
			RateType:  "query",
			RateLimit: 2,
			TimeMetrics: map[string]float64{
				"Query_time":           0.000165,
				"Lock_time":            0.000048,
				"InnoDB_IO_r_wait":     0.000000,
				"InnoDB_rec_lock_wait": 0.000000,
				"InnoDB_queue_wait":    0.000000,
			},
			NumberMetrics: map[string]uint64{
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Rows_sent":             5,
				"Rows_examined":         10,
				"Rows_affected":         0,
				"Bytes_sent":            481,
				"Tmp_tables":            0,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Merge_passes":          0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_pages_distinct": 3,
			},
			BoolMetrics: map[string]bool{
				"QC_Hit":            false,
				"Full_scan":         false,
				"Full_join":         false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
				"Filesort":          true,
				"Filesort_on_disk":  false,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

func TestParserSlowLog012(t *testing.T) {
	got := parseSlowLog(t, "slow012.log", opt)
	expect := []log.Event{
		{
			Query:     "select * from mysql.user",
			Db:        "",
			Host:      "localhost",
			User:      "msandbox",
			Offset:    0,
			OffsetEnd: 185,
			TimeMetrics: map[string]float64{
				"Query_time": 0.000214,
				"Lock_time":  0.000086,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     2,
				"Rows_examined": 2,
			},
			BoolMetrics: map[string]bool{},
		},
		{
			Query:     "Quit",
			Admin:     true,
			Db:        "",
			Host:      "localhost",
			User:      "msandbox",
			Offset:    185,
			OffsetEnd: 375,
			TimeMetrics: map[string]float64{
				"Query_time": 0.000016,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     2,
				"Rows_examined": 2,
			},
			BoolMetrics: map[string]bool{},
		},
		{
			Query:     "SELECT @@max_allowed_packet",
			Db:        "dev_pct",
			Host:      "localhost",
			User:      "msandbox",
			Offset:    375,
			OffsetEnd: 609,
			Ts:        time.Date(2014, 4, 13, 19, 34, 13, 0, time.UTC),
			TimeMetrics: map[string]float64{
				"Query_time": 0.000127,
				"Lock_time":  0.000000,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
		},
	}
	assert.EqualValues(t, expect, got)
}

// Stack overflow bug due to meta lines.
func TestParserSlowLog013(t *testing.T) {
	got := parseSlowLog(t, "slow013.log", opt)
	expect := []log.Event{
		{
			Offset:    0,
			OffsetEnd: 353,
			Ts:        time.Date(2014, 2, 24, 22, 39, 34, 0, time.UTC),
			Query:     "select 950,q.* from qcm q INTO OUTFILE '/mnt/pct/exp/qcm_db950.txt'",
			User:      "root",
			Host:      "localhost",
			Db:        "db950",
			TimeMetrics: map[string]float64{
				"Query_time": 21.876617,
				"Lock_time":  0.002991,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":    14,
				"Killed":        0,
				"Last_errno":    0,
				"Rows_affected": 1605306,
				"Rows_examined": 1605306,
				"Rows_sent":     1605306,
			},
			BoolMetrics: map[string]bool{},
		},
		{
			Offset:    353,
			OffsetEnd: 6138,
			Ts:        time.Date(2014, 2, 24, 22, 39, 59, 0, time.UTC),
			Query:     "select 961,q.* from qcm q INTO OUTFILE '/mnt/pct/exp/qcm_db961.txt'",
			User:      "root",
			Host:      "localhost",
			Db:        "db961",
			TimeMetrics: map[string]float64{
				"Query_time": 20.304536,
				"Lock_time":  0.103324,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":    14,
				"Killed":        0,
				"Last_errno":    0,
				"Rows_affected": 1197472,
				"Rows_examined": 1197472,
				"Rows_sent":     1197472,
			},
			BoolMetrics: map[string]bool{},
		},
		{
			Offset:    6138,
			OffsetEnd: 6666,
			Ts:        time.Date(2014, 3, 11, 16, 7, 40, 0, time.UTC),
			Query:     "select count(*) into @discard from `information_schema`.`PARTITIONS`",
			User:      "debian-sys-maint",
			Host:      "localhost",
			Db:        "",
			TimeMetrics: map[string]float64{
				"Query_time": 94.38144,
				"Lock_time":  0.000174,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":    11,
				"Killed":        0,
				"Last_errno":    1146,
				"Rows_affected": 1,
				"Rows_examined": 17799,
				"Rows_sent":     0,
			},
			BoolMetrics: map[string]bool{},
		},
		{
			Offset:    6666,
			OffsetEnd: 7014,
			Ts:        time.Date(2014, 3, 12, 20, 28, 40, 0, time.UTC),
			Query:     "select 1,q.* from qcm q INTO OUTFILE '/mnt/pct/exp/qcm_db1.txt'",
			User:      "root",
			Host:      "localhost",
			Db:        "db1",
			TimeMetrics: map[string]float64{
				"Query_time": 407.540262,
				"Lock_time":  0.122377,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":    19,
				"Killed":        0,
				"Last_errno":    0,
				"Rows_affected": 34621308,
				"Rows_examined": 34621308,
				"Rows_sent":     34621308,
			},
			BoolMetrics: map[string]bool{},
		},
		{
			Offset:    7014,
			OffsetEnd: 7370,
			Ts:        time.Date(2014, 3, 12, 20, 29, 40, 0, time.UTC),
			Query:     "select 1006,q.* from qcm q INTO OUTFILE '/mnt/pct/exp/qcm_db1006.txt'",
			User:      "root",
			Host:      "localhost",
			Db:        "db1006",
			TimeMetrics: map[string]float64{
				"Query_time": 60.507698,
				"Lock_time":  0.002719,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":    14,
				"Killed":        0,
				"Last_errno":    0,
				"Rows_affected": 4937738,
				"Rows_examined": 4937738,
				"Rows_sent":     4937738,
			},
			BoolMetrics: map[string]bool{},
		},
	}
	assert.EqualValues(t, expect, got)
}

// Query line looks like header line.
func TestParserSlowLog014(t *testing.T) {
	got := parseSlowLog(t, "slow014.log", opt)
	expect := []log.Event{
		{
			Offset:    0,
			OffsetEnd: 690,
			Admin:     false,
			Query:     "SELECT * FROM cache\n WHERE `cacheid` IN ('id15965')",
			User:      "root",
			Host:      "localhost",
			Db:        "db1",
			TimeMetrics: map[string]float64{
				"InnoDB_IO_r_wait":     0,
				"InnoDB_queue_wait":    0,
				"InnoDB_rec_lock_wait": 0,
				"Lock_time":            4.7e-05,
				"Query_time":           0.000179,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":            2004,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_pages_distinct": 3,
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Merge_passes":          0,
				"Rows_affected":         0,
				"Rows_examined":         1,
				"Rows_read":             1,
				"Rows_sent":             1,
				"Thread_id":             103375137,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Tmp_tables":            0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
			},
		},
		{
			/**
			 * Here it is:
			 */
			Offset:    690,
			OffsetEnd: 2104,
			Admin:     false,
			Query:     "### Channels ###\n\u0009\u0009\u0009\u0009\u0009SELECT sourcetable, IF(f.lastcontent = 0, f.lastupdate, f.lastcontent) AS lastactivity,\n\u0009\u0009\u0009\u0009\u0009f.totalcount AS activity, type.class AS type,\n\u0009\u0009\u0009\u0009\u0009(f.nodeoptions \u0026 512) AS noUnsubscribe\n\u0009\u0009\u0009\u0009\u0009FROM node AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN contenttype AS type ON type.contenttypeid = f.contenttypeid \n\n\u0009\u0009\u0009\u0009\u0009INNER JOIN subscribed AS sd ON sd.did = f.nodeid AND sd.userid = 15965\n UNION  ALL \n\n\u0009\u0009\u0009\u0009\u0009### Users ###\n\u0009\u0009\u0009\u0009\u0009SELECT f.name AS title, f.userid AS keyval, 'user' AS sourcetable, IFNULL(f.lastpost, f.joindate) AS lastactivity,\n\u0009\u0009\u0009\u0009\u0009f.posts as activity, 'Member' AS type,\n\u0009\u0009\u0009\u0009\u00090 AS noUnsubscribe\n\u0009\u0009\u0009\u0009\u0009FROM user AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN userlist AS ul ON ul.relationid = f.userid AND ul.userid = 15965\n\u0009\u0009\u0009\u0009\u0009WHERE ul.type = 'f' AND ul.aq = 'yes'\n ORDER BY title ASC LIMIT 100",
			User:      "root",
			Host:      "localhost",
			Db:        "db1",
			TimeMetrics: map[string]float64{
				"InnoDB_IO_r_wait":     0,
				"InnoDB_queue_wait":    0,
				"InnoDB_rec_lock_wait": 0,
				"Lock_time":            0.000161,
				"Query_time":           0.000628,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":            323,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_pages_distinct": 3,
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Merge_passes":          0,
				"Rows_affected":         0,
				"Rows_examined":         0,
				"Rows_read":             0,
				"Rows_sent":             0,
				"Thread_id":             103375137,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Tmp_tables":            1,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          true,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         true,
				"QC_Hit":            false,
				"Tmp_table":         true,
				"Tmp_table_on_disk": false,
			},
		},
		{
			Offset:    2104,
			OffsetEnd: 3163,
			Query:     "SELECT COUNT(userfing.keyval) AS total\n\u0009\u0009\u0009FROM\n\u0009\u0009\u0009((### All Content ###\n\u0009\u0009\u0009\u0009\u0009SELECT f.nodeid AS keyval\n\u0009\u0009\u0009\u0009\u0009FROM node AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN subscribed AS sd ON sd.did = f.nodeid AND sd.userid = 15965) UNION ALL (\n\u0009\u0009\u0009\u0009\u0009### Users ###\n\u0009\u0009\u0009\u0009\u0009SELECT f.userid AS keyval\n\u0009\u0009\u0009\u0009\u0009FROM user AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN userlist AS ul ON ul.relationid = f.userid AND ul.userid = 15965\n\u0009\u0009\u0009\u0009\u0009WHERE ul.type = 'f' AND ul.aq = 'yes')\n) AS userfing",
			User:      "root",
			Host:      "localhost",
			Db:        "db1",
			TimeMetrics: map[string]float64{
				"InnoDB_IO_r_wait":     0,
				"InnoDB_queue_wait":    0,
				"InnoDB_rec_lock_wait": 0,
				"Lock_time":            0.000116,
				"Query_time":           0.00042,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":            60,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_pages_distinct": 3,
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Merge_passes":          0,
				"Rows_affected":         0,
				"Rows_examined":         0,
				"Rows_read":             0,
				"Rows_sent":             1,
				"Thread_id":             103375137,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Tmp_tables":            2,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         true,
				"QC_Hit":            false,
				"Tmp_table":         true,
				"Tmp_table_on_disk": false,
			},
		},
		{
			Offset:    3163,
			OffsetEnd: 4410,
			Query:     "SELECT u.userid, u.name AS name, u.usergroupid AS usergroupid, IFNULL(u.lastactivity, u.joindate) as lastactivity,\n\u0009\u0009\u0009\u0009IFNULL((SELECT userid FROM userlist AS ul2 WHERE ul2.userid = 15965 AND ul2.relationid = u.userid AND ul2.type = 'f' AND ul2.aq = 'yes'), 0) as isFollowing,\n\u0009\u0009\u0009\u0009IFNULL((SELECT userid FROM userlist AS ul2 WHERE ul2.userid = 15965 AND ul2.relationid = u.userid AND ul2.type = 'f' AND ul2.aq = 'pending'), 0) as isPending\nFROM user AS u\n\u0009\u0009\u0009\u0009INNER JOIN userlist AS ul ON (u.userid = ul.userid AND ul.relationid = 15965)\n\n\u0009\u0009\u0009WHERE ul.type = 'f' AND ul.aq = 'yes'\nORDER BY name ASC\nLIMIT 0, 100",
			User:      "root",
			Host:      "localhost",
			Db:        "db1",
			TimeMetrics: map[string]float64{
				"InnoDB_IO_r_wait":     0,
				"InnoDB_queue_wait":    0,
				"InnoDB_rec_lock_wait": 0,
				"Lock_time":            0.000144,
				"Query_time":           0.000457,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":            359,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_pages_distinct": 1,
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Merge_passes":          0,
				"Rows_affected":         0,
				"Rows_examined":         0,
				"Rows_read":             0,
				"Rows_sent":             0,
				"Thread_id":             103375137,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Tmp_tables":            1,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          true,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         true,
				"Tmp_table_on_disk": false,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

// Correct event offsets when parsing starts/resumes at an offset.
func TestParserSlowLog001StartOffset(t *testing.T) {
	opt := opt
	opt.StartOffset = 358
	// 358 is the first byte of the second (of 2) events.
	got := parseSlowLog(t, "slow001.log", opt)
	expect := []log.Event{
		{
			Ts:        time.Date(2007, 10, 15, 21, 45, 10, 0, time.UTC),
			Query:     `select sleep(2) from test.n`,
			User:      "root",
			Host:      "localhost",
			Db:        "sakila",
			Offset:    358,
			OffsetEnd: 524,
			TimeMetrics: map[string]float64{
				"Query_time": 2,
				"Lock_time":  0,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
		},
	}
	assert.EqualValues(t, expect, got)
}

// Line > bufio.MaxScanTokenSize = 64KiB
// https://jira.percona.com/browse/PCT-552
func TestParserSlowLog015(t *testing.T) {
	got := parseSlowLog(t, "slow015.log", log.Options{})
	assert.Len(t, got, 2)
}

// Start in header
func TestParseSlow016(t *testing.T) {
	got := parseSlowLog(t, "slow016.log", log.Options{Debug: false})
	expect := []log.Event{
		{
			Query:     `SHOW /*!50002 GLOBAL */ STATUS`,
			User:      "pt_agent",
			Host:      "localhost",
			Offset:    159,
			OffsetEnd: 413,
			TimeMetrics: map[string]float64{
				"Query_time": 0.003953,
				"Lock_time":  0.000059,
			},
			NumberMetrics: map[string]uint64{
				"Killed":        0,
				"Last_errno":    0,
				"Rows_sent":     571,
				"Rows_examined": 571,
				"Rows_affected": 0,
			},
			BoolMetrics: map[string]bool{},
		},
	}
	assert.EqualValues(t, expect, got)
}

// Start in query
func TestParseSlow017(t *testing.T) {
	got := parseSlowLog(t, "slow017.log", opt)
	expect := []log.Event{
		{
			Query:     `SHOW /*!50002 GLOBAL */ STATUS`,
			User:      "pt_agent",
			Host:      "localhost",
			Offset:    26,
			OffsetEnd: 280,
			TimeMetrics: map[string]float64{
				"Query_time": 0.003953,
				"Lock_time":  0.000059,
			},
			NumberMetrics: map[string]uint64{
				"Killed":        0,
				"Last_errno":    0,
				"Rows_sent":     571,
				"Rows_examined": 571,
				"Rows_affected": 0,
			},
			BoolMetrics: map[string]bool{},
		},
	}
	assert.EqualValues(t, expect, got)
}

func TestParseSlow019(t *testing.T) {
	got := parseSlowLog(t, "slow019.log", opt)
	expect := []log.Event{
		{
			Query:     `SELECT TABLE_SCHEMA, TABLE_NAME, ROWS_READ, ROWS_CHANGED, ROWS_CHANGED_X_INDEXES FROM INFORMATION_SCHEMA.TABLE_STATISTICS`,
			User:      "percona-agent",
			Host:      "localhost",
			Offset:    0,
			OffsetEnd: 641,
			TimeMetrics: map[string]float64{
				"Lock_time":  0.0001,
				"Query_time": 0.004599,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":      70092,
				"Killed":          0,
				"Last_errno":      0,
				"Merge_passes":    0,
				"Rows_affected":   0,
				"Rows_examined":   1473,
				"Rows_read":       1473,
				"Rows_sent":       1473,
				"Thread_id":       37911936,
				"Tmp_disk_tables": 0,
				"Tmp_table_sizes": 0,
				"Tmp_tables":      1,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         true,
				"QC_Hit":            false,
				"Tmp_table":         true,
				"Tmp_table_on_disk": false,
			},
		},
		{
			Query:     `SELECT cid, data, created, expire, serialized FROM cache_field WHERE cid IN ('field_info:bundle_extra:user:user')`,
			User:      "root",
			Host:      "localhost",
			Offset:    641,
			OffsetEnd: 1273,
			Db:        "cod7_plos15",
			TimeMetrics: map[string]float64{
				"Lock_time":  0,
				"Query_time": 2.2e-05,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":      1333,
				"Killed":          0,
				"Last_errno":      0,
				"Merge_passes":    0,
				"Rows_affected":   0,
				"Rows_examined":   0,
				"Rows_read":       0,
				"Rows_sent":       0,
				"Thread_id":       57434695,
				"Tmp_disk_tables": 0,
				"Tmp_table_sizes": 0,
				"Tmp_tables":      0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            true,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
			},
		},
		{
			Query:     "UPDATE captcha_sessions SET timestamp='1413583348', solution='1'\nWHERE  (csid = '28439')",
			User:      "root",
			Host:      "localhost",
			Offset:    1273,
			OffsetEnd: 2005,
			Db:        "cod7_plos15",
			TimeMetrics: map[string]float64{
				"InnoDB_IO_r_wait":     0,
				"InnoDB_queue_wait":    0,
				"InnoDB_rec_lock_wait": 0,
				"Lock_time":            7.8e-05,
				"Query_time":           0.005241,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":            52,
				"InnoDB_IO_r_bytes":     0,
				"InnoDB_IO_r_ops":       0,
				"InnoDB_pages_distinct": 8,
				"InnoDB_trx_id":         0,
				"Killed":                0,
				"Last_errno":            0,
				"Merge_passes":          0,
				"Rows_affected":         1,
				"Rows_examined":         1,
				"Rows_read":             1,
				"Rows_sent":             0,
				"Thread_id":             57434695,
				"Tmp_disk_tables":       0,
				"Tmp_table_sizes":       0,
				"Tmp_tables":            0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
			},
		},
		{
			Query:     `SELECT TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, ROWS_READ FROM INFORMATION_SCHEMA.INDEX_STATISTICS`,
			User:      "percona-agent",
			Host:      "localhost",
			Offset:    2005,
			OffsetEnd: 2621,
			TimeMetrics: map[string]float64{
				"Lock_time":  0.000115,
				"Query_time": 0.011565,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":      102084,
				"Killed":          0,
				"Last_errno":      0,
				"Merge_passes":    0,
				"Rows_affected":   0,
				"Rows_examined":   2146,
				"Rows_read":       2146,
				"Rows_sent":       2146,
				"Thread_id":       37911936,
				"Tmp_disk_tables": 0,
				"Tmp_table_sizes": 0,
				"Tmp_tables":      1,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         true,
				"QC_Hit":            false,
				"Tmp_table":         true,
				"Tmp_table_on_disk": false,
			},
		},
	}
	assert.EqualValues(t, expect, got)
}

// Test db is not inherited and multiple "use" commands.
func TestParseSlow023(t *testing.T) {
	got := parseSlowLog(t, "slow023.log", opt)
	expect := []log.Event{
		// Slice 0
		{
			Offset:    176,
			OffsetEnd: 418,
			Admin:     false,
			Query:     "SELECT field FROM table_a WHERE some_other_field = 'yahoo' LIMIT 1",
			User:      "bookblogs",
			Host:      "localhost",
			Db:        "dbnamea",
			TimeMetrics: map[string]float64{
				"Query_time": 0.321092,
				"Lock_time":  3.8e-05,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     0,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "",
			RateLimit:   0,
		},
		// Slice 1
		{
			Offset:    418,
			OffsetEnd: 595,
			Admin:     false,
			Query:     "SET NAMES utf8",
			User:      "bookblogs",
			Host:      "localhost",
			Db:        "",
			TimeMetrics: map[string]float64{
				"Lock_time":  0,
				"Query_time": 0.253052,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     0,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "",
			RateLimit:   0,
		},
		// Slice 2
		{
			Offset:    595,
			OffsetEnd: 794,
			Admin:     false,
			Query:     "SET GLOBAL slow_query_log=ON",
			User:      "percona-agent",
			Host:      "localhost",
			Db:        "",
			TimeMetrics: map[string]float64{
				"Query_time": 0.31645,
				"Lock_time":  0,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     0,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "",
			RateLimit:   0,
		},
		// Slice 3
		{
			Offset:    794,
			OffsetEnd: 982,
			Admin:     false,
			Query:     "SELECT @@SESSION.sql_mode",
			User:      "bookblogs",
			Host:      "localhost",
			Db:        "",
			TimeMetrics: map[string]float64{
				"Query_time": 3.7e-05,
				"Lock_time":  0,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     1,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "",
			RateLimit:   0,
		},
		// Slice 4
		{
			Offset:    982,
			OffsetEnd: 1218,
			Admin:     false,
			Query:     "SELECT field FROM table_b WHERE another_field = 'bazinga' AND site_id = 1",
			User:      "bookblogs",
			Host:      "localhost",
			Db:        "",
			TimeMetrics: map[string]float64{
				"Query_time": 0.000297,
				"Lock_time":  0.000141,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 1,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "", RateLimit: 0,
		},
		// Slice 5
		{
			Offset:    1218,
			OffsetEnd: 1388,
			Admin:     false,
			Query:     "use `dbnameb`",
			User:      "backup",
			Host:      "localhost",
			Db:        "dbnameb",
			TimeMetrics: map[string]float64{
				"Lock_time":  0,
				"Query_time": 0.000558,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     0,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "",
			RateLimit:   0,
		},
		// Slice 6
		{
			Offset:    1388,
			OffsetEnd: 1572,
			Admin:     false,
			Query:     "select @@collation_database",
			User:      "backup",
			Host:      "localhost",
			Db:        "",
			TimeMetrics: map[string]float64{
				"Query_time": 0.000204,
				"Lock_time":  0,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 0,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "",
			RateLimit:   0,
		},
		// Slice 7
		{
			Offset:    1572,
			OffsetEnd: 1817,
			Admin:     false,
			Query:     "SELECT another_field FROM table_c WHERE a_third_field = 'tiruriru' AND site_id = 1",
			User:      "bookblogs",
			Host:      "localhost",
			Db:        "",
			TimeMetrics: map[string]float64{
				"Query_time": 0.000164,
				"Lock_time":  5.9e-05,
			},
			NumberMetrics: map[string]uint64{
				"Rows_sent":     1,
				"Rows_examined": 1,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "",
			RateLimit:   0,
		},
	}
	assert.EqualValues(t, expect, got)
}

func TestParseSlow023A(t *testing.T) {
	filename := "slow023.log"
	o := log.Options{Debug: false}

	file, err := os.Open(path.Join(sample, "/", filename))
	if err != nil {
		l.Fatal(err)
	}
	p := parser.NewSlowLogParser(file, o)
	if err != nil {
		l.Fatal(err)
	}
	go p.Start()
	lastQuery := ""
	for e := range p.EventChan() {
		if e.Query == "" {
			t.Errorf("Empty query at offset: %d. Last valid query: %s\n", e.Offset, lastQuery)
		} else {
			lastQuery = e.Query
		}
	}
}

/*
Test header with invalid # Time or invalid # User lines
*/
func TestParseSlow024(t *testing.T) {
	got := parseSlowLog(t, "slow024.log", opt)
	expect := []log.Event{
		{
			Offset:    199,
			OffsetEnd: 361,
			Ts:        time.Date(2007, 10, 15, 21, 43, 52, 0, time.UTC),
			Admin:     false,
			Query:     "select sleep(1) from n",
			User:      "root",
			Host:      "localhost",
			Db:        "test",
			TimeMetrics: map[string]float64{
				"Lock_time":  0,
				"Query_time": 2,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     1,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "",
			RateLimit:   0,
		},
		{
			Offset:    361,
			OffsetEnd: 507,
			Admin:     false,
			Query:     "select sleep(2) from n",
			User:      "root",
			Host:      "localhost",
			Db:        "test",
			TimeMetrics: map[string]float64{
				"Lock_time":  0,
				"Query_time": 2,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     1,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "",
			RateLimit:   0,
		},
		{
			Offset:    507,
			OffsetEnd: 644,
			Ts:        time.Date(2007, 10, 15, 21, 43, 52, 0, time.UTC),
			Admin:     false,
			Query:     "select sleep(3) from n",
			User:      "",
			Host:      "",
			Db:        "test",
			TimeMetrics: map[string]float64{
				"Lock_time":  0,
				"Query_time": 2,
			},
			NumberMetrics: map[string]uint64{
				"Rows_examined": 0,
				"Rows_sent":     1,
			},
			BoolMetrics: map[string]bool{},
			RateType:    "",
			RateLimit:   0,
		},
	}
	assert.EqualValues(t, expect, got)
}

// https://jira.percona.com/browse/PMM-1834
func TestParseSlowMariaDBWithExplain(t *testing.T) {
	got := parseSlowLog(t, "mariadb102-with-explain.log", opt)
	expect := []log.Event{
		{
			Offset:    205,
			OffsetEnd: 630,
			Ts:        time.Date(2018, 2, 14, 16, 18, 7, 0, time.UTC),
			Admin:     false,
			Query:     "SELECT 1",
			User:      "root",
			Host:      "localhost",
			Db:        "",
			TimeMetrics: map[string]float64{
				"Lock_time":  0,
				"Query_time": 0.000277,
			},
			NumberMetrics: map[string]uint64{
				"Rows_affected": 0,
				"Rows_examined": 0,
				"Rows_sent":     1,
				"Thread_id":     8,
			},
			BoolMetrics: map[string]bool{
				"QC_hit": false,
			},
			RateType:  "",
			RateLimit: 0,
		},
	}
	assert.EqualValues(t, expect, got)
}

func TestParseSlow026(t *testing.T) {
	got := parseSlowLog(t, "slow026.log", opt)
	expect := []log.Event{
		{
			Offset:    0,
			OffsetEnd: 463,
			Ts:        time.Date(2017, 12, 13, 2, 41, 18, 673330000, time.UTC),
			Admin:     false,
			Query:     "select 1",
			User:      "test",
			Host:      "",
			Db:        "test",
			TimeMetrics: map[string]float64{
				"Lock_time":  0,
				"Query_time": 1.000249,
			},
			NumberMetrics: map[string]uint64{
				"Bytes_sent":      89,
				"Killed":          0,
				"Last_errno":      0,
				"Merge_passes":    0,
				"Rows_affected":   0,
				"Rows_examined":   0,
				"Rows_sent":       1,
				"Tmp_disk_tables": 0,
				"Tmp_table_sizes": 0,
				"Tmp_tables":      0,
			},
			BoolMetrics: map[string]bool{
				"Filesort":          false,
				"Filesort_on_disk":  false,
				"Full_join":         false,
				"Full_scan":         false,
				"QC_Hit":            false,
				"Tmp_table":         false,
				"Tmp_table_on_disk": false,
			},
			RateType:  "",
			RateLimit: 0,
		},
	}
	assert.EqualValues(t, expect, got)
}
