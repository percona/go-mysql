/*
	Copyright (c) 2014-2015, Percona LLC and/or its affiliates. All rights reserved.

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package event_test

import (
	"encoding/json"
	"io/ioutil"
	l "log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
	"github.com/percona/go-mysql/event"
	"github.com/percona/go-mysql/log"
	parser "github.com/percona/go-mysql/log/slow"
	"github.com/percona/go-mysql/query"
	"github.com/percona/go-mysql/test"
	"github.com/stretchr/testify/assert"
)

var (
	rootDir = test.RootDir()
	sample  = filepath.Join(rootDir, "test/slow-logs")
)

func aggregateSlowLog(input, output string, utcOffset time.Duration, examples bool) (string, string) {
	file, err := os.Open(filepath.Join(sample, input))
	if err != nil {
		l.Fatal(err)
	}
	opt := log.Options{}
	opt.DefaultLocation = time.UTC
	p := parser.NewSlowLogParser(file, opt)
	go p.Start()
	a := event.NewAggregator(examples, utcOffset, 10)
	for e := range p.EventChan() {
		f := query.Fingerprint(e.Query)
		id := query.Id(f)
		a.AddEvent(e, id, f)
	}
	got := a.Finalize()
	gotJson, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		l.Fatal(err)
	}

	resultOutputPath := filepath.Join("testdata", output)
	if *test.Update {
		if err := ioutil.WriteFile(resultOutputPath, gotJson, 0666); err != nil {
			l.Fatal(err)
		}
	}
	expectJson, err := ioutil.ReadFile(resultOutputPath)
	if err != nil {
		l.Fatal(err)
	}

	return string(gotJson), string(expectJson)
}

func zeroPercentiles(r *event.Result) {
	for _, metrics := range r.Global.Metrics.TimeMetrics {
		metrics.Med = event.Float64(0)
		metrics.P95 = event.Float64(0)
	}
	for _, metrics := range r.Global.Metrics.NumberMetrics {
		metrics.Med = event.Uint64(0)
		metrics.P95 = event.Uint64(0)
	}
	for _, class := range r.Class {
		for _, metrics := range class.Metrics.TimeMetrics {
			metrics.Med = event.Float64(0)
			metrics.P95 = event.Float64(0)
		}
		for _, metrics := range class.Metrics.NumberMetrics {
			metrics.Med = event.Uint64(0)
			metrics.P95 = event.Uint64(0)
		}
	}
}

func ordered(in map[string]*event.Class) (out []*event.Class) {
	var keys []string
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, in[k])
	}

	return out
}

// --------------------------------------------------------------------------

func TestSlow001(t *testing.T) {
	got, expect := aggregateSlowLog("slow001.log", "slow001.golden", 0, true)
	assert.JSONEq(t, expect, got)
}

func TestSlow001WithTzOffset(t *testing.T) {
	got, expect := aggregateSlowLog("slow001.log", "slow001.golden", -1*time.Hour, true)
	// Use the same files as TestSlow001NoExamples but with a tz=-1
	expect = strings.Replace(expect, "2007-10-15 21:43:52", "2007-10-15 20:43:52", 1)
	expect = strings.Replace(expect, "2007-10-15 21:45:10", "2007-10-15 20:45:10", 1)
	assert.JSONEq(t, expect, got)
}

func TestSlow001NoExamples(t *testing.T) {
	got, expect := aggregateSlowLog("slow001.log", "slow001-no-examples.golden", 0, false)
	assert.JSONEq(t, expect, got)
}

// Test p95 and median.
func TestSlow010(t *testing.T) {
	got, expect := aggregateSlowLog("slow010.log", "slow010.golden", 0, true)
	assert.JSONEq(t, expect, got)
}

func TestAddClassSlow001(t *testing.T) {
	expect, _ := aggregateSlowLog("slow001.log", "slow001.golden", 0, true)
	expectEventResult := event.Result{}
	if err := json.Unmarshal([]byte(expect), &expectEventResult); err != nil {
		t.Fatal(err)
	}
	zeroPercentiles(&expectEventResult)

	global := event.NewClass("", "", false)
	for _, class := range expectEventResult.Class {
		global.AddClass(class)
	}

	var emptyTime time.Time
	expectEventResult.Global.TsMin = emptyTime;
	expectEventResult.Global.TsMax = emptyTime;
	expectEventResult.Global.LastThreadID = 0;
	expectGlobalBytes, err := json.Marshal(expectEventResult.Global)
	if err != nil {
		t.Fatal(err)
	}

	gotGlobalBytes, err := json.Marshal(global)
	if err != nil {
		t.Fatal(err)
	}
	assert.JSONEq(t, string(expectGlobalBytes), string(gotGlobalBytes))
}

func TestAddClassSlow023(t *testing.T) {
	expect, _ := aggregateSlowLog("slow023.log", "slow023.golden", 0, true)
	expectEventResult := event.Result{}
	if err := json.Unmarshal([]byte(expect), &expectEventResult); err != nil {
		t.Fatal(err)
	}
	zeroPercentiles(&expectEventResult)

	global := event.NewClass("", "", false)
	for _, class := range ordered(expectEventResult.Class) {
		global.AddClass(class)
	}
	expectEventResult.Global.LastThreadID = 0;
	expectGlobalBytes, err := json.Marshal(expectEventResult.Global)
	if err != nil {
		t.Fatal(err)
	}
	gotGlobalBytes, err := json.Marshal(global)
	if err != nil {
		t.Fatal(err)
	}
	assert.JSONEq(t, string(expectGlobalBytes), string(gotGlobalBytes))
}

func TestSlow018(t *testing.T) {
	got, expect := aggregateSlowLog("slow018.log", "slow018.golden", 0, true)
	assert.JSONEq(t, expect, got)
}

// Tests for PCT-1006 & PCT-1085
func TestUseDb(t *testing.T) {
	// Test db is not inherited
	got, expect := aggregateSlowLog("slow020.log", "slow020.golden", 0, true)
	assert.JSONEq(t, expect, got)
	// Test "use" is not case sensitive
	got, expect = aggregateSlowLog("slow021.log", "slow021.golden", 0, true)
	assert.JSONEq(t, expect, got)
	// Test we are parsing db names in backticks
	got, expect = aggregateSlowLog("slow022.log", "slow022.golden", 0, true)
	assert.JSONEq(t, expect, got)
}

func TestOutlierSlow025(t *testing.T) {
	got, expect := aggregateSlowLog("slow025.log", "slow025.golden", 0, true)
	assert.JSONEq(t, expect, got)
}

func TestMariaDB102WithExplain(t *testing.T) {
	got, expect := aggregateSlowLog("mariadb102-with-explain.log", "mariadb102-with-explain.golden", 0, true)
	assert.JSONEq(t, expect, got)
}
