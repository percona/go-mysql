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

	"github.com/stretchr/testify/assert"

	"github.com/yehornaumenko/go-mysql/event"
	"github.com/yehornaumenko/go-mysql/log"
	parser "github.com/yehornaumenko/go-mysql/log/slow"
	"github.com/yehornaumenko/go-mysql/query"
	"github.com/yehornaumenko/go-mysql/test"
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
		a.AddEvent(e, id, e.User, e.Host, e.Db, e.Server, f)
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
		metrics.P99 = event.Float64(0)
	}
	for _, metrics := range r.Global.Metrics.NumberMetrics {
		metrics.P99 = event.Uint64(0)
	}
	for _, class := range r.Class {
		for _, metrics := range class.Metrics.TimeMetrics {
			metrics.P99 = event.Float64(0)
		}
		for _, metrics := range class.Metrics.NumberMetrics {
			metrics.P99 = event.Uint64(0)
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

// Test p99 and Cnt.
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

	global := event.NewClass("", "", "", "", "", "", false)
	for _, class := range expectEventResult.Class {
		global.AddClass(class)
	}
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

	global := event.NewClass("", "", "", "", "", "", false)
	for _, class := range ordered(expectEventResult.Class) {
		global.AddClass(class)
	}
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

func TestSlow026(t *testing.T) {
	got, expect := aggregateSlowLog("slow027.log", "slow027.golden", 0, true)
	assert.JSONEq(t, expect, got)
}
