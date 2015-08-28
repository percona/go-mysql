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
	"path"
	"testing"
	"time"

	. "github.com/go-test/test"
	"github.com/percona/go-mysql/event"
	log "github.com/percona/go-mysql/log"
	parser "github.com/percona/go-mysql/log/slow"
	"github.com/percona/go-mysql/query"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	sample   string
	result   string
	examples bool
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(t *C) {
	rootDir := RootDir()
	s.sample = path.Join(rootDir, "test/slow-logs")
	s.result = path.Join(rootDir, "test/results")
	s.examples = true
}

func (s *TestSuite) aggregateSlowLog(input, output string, utcOffset time.Duration) (got event.Result, expect event.Result) {
	bytes, err := ioutil.ReadFile(path.Join(s.result, "/", output))
	if err != nil {
		l.Fatal(err)
	}
	expect = event.Result{}
	if err := json.Unmarshal(bytes, &expect); err != nil {
		l.Fatal(err)
	}

	file, err := os.Open(path.Join(s.sample, "/", input))
	if err != nil {
		l.Fatal(err)
	}
	p := parser.NewSlowLogParser(file, log.Options{})
	if err != nil {
		l.Fatal(err)
	}
	go p.Start()
	a := event.NewAggregator(s.examples, utcOffset, 10)
	for e := range p.EventChan() {
		f := query.Fingerprint(e.Query)
		id := query.Id(f)
		a.AddEvent(e, id, f)
	}
	got = a.Finalize()
	return got, expect
}

func zeroPercentiles(r *event.Result) {
	for _, metrics := range r.Global.Metrics.TimeMetrics {
		metrics.Med = 0
		metrics.P95 = 0
	}
	for _, metrics := range r.Global.Metrics.NumberMetrics {
		metrics.Med = 0
		metrics.P95 = 0
	}
	for _, class := range r.Class {
		for _, metrics := range class.Metrics.TimeMetrics {
			metrics.Med = 0
			metrics.P95 = 0
		}
		for _, metrics := range class.Metrics.NumberMetrics {
			metrics.Med = 0
			metrics.P95 = 0
		}
	}
}

// --------------------------------------------------------------------------

func (s *TestSuite) TestSlow001(t *C) {
	got, expect := s.aggregateSlowLog("slow001.log", "slow001.json", 0)
	if same, diff := IsDeeply(got, expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

func (s *TestSuite) TestSlow001WithTzOffset(t *C) {
	got, expect := s.aggregateSlowLog("slow001.log", "slow001.json", -1*time.Hour)
	// Use the same files as TestSlow001NoExamples but with a tz=-1
	expect.Class["7F7D57ACDD8A346E"].Example.Ts = "2007-10-15 20:43:52"
	expect.Class["3A99CC42AEDCCFCD"].Example.Ts = "2007-10-15 20:45:10"
	if same, diff := IsDeeply(got, expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

func (s *TestSuite) TestSlow001NoExamples(t *C) {
	s.examples = false
	defer func() { s.examples = true }()
	got, expect := s.aggregateSlowLog("slow001.log", "slow001-no-examples.json", 0)
	if same, diff := IsDeeply(got, expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// Test p95 and median.
func (s *TestSuite) TestSlow010(t *C) {
	got, expect := s.aggregateSlowLog("slow010.log", "slow010.json", 0)
	if same, diff := IsDeeply(got, expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

func (s *TestSuite) TestAddClassSlow001(t *C) {
	expect, _ := s.aggregateSlowLog("slow001.log", "slow001.json", 0)
	zeroPercentiles(&expect)
	global := event.NewClass("", "", false)
	for _, class := range expect.Class {
		global.AddClass(class)
	}
	if same, diff := IsDeeply(global, expect.Global); !same {
		Dump(global)
		t.Error(diff)
	}
}

func (s *TestSuite) TestAddClassSlow023(t *C) {
	expect, _ := s.aggregateSlowLog("slow023.log", "slow018.json", 0)
	zeroPercentiles(&expect)
	global := event.NewClass("", "", false)
	for _, class := range expect.Class {
		global.AddClass(class)
	}
	if same, diff := IsDeeply(global, expect.Global); !same {
		Dump(global)
		t.Error(diff)
	}
}

func (s *TestSuite) TestSlow018(t *C) {
	got, expect := s.aggregateSlowLog("slow018.log", "slow018.json", 0)
	if same, diff := IsDeeply(got, expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

// Tests for PCT-1006 & PCT-1085
func (s *TestSuite) TestUseDb(t *C) {
	// Test db is not inherited
	got, expect := s.aggregateSlowLog("slow020.log", "slow020.json", 0)
	if same, diff := IsDeeply(got, expect); !same {
		Dump(got)
		t.Error(diff)
	}
	// Test "use" is not case sensitive
	got, expect = s.aggregateSlowLog("slow021.log", "slow021.json", 0)
	if same, diff := IsDeeply(got, expect); !same {
		Dump(got)
		t.Error(diff)
	}
	// Test we are parsing db names in backticks
	got, expect = s.aggregateSlowLog("slow022.log", "slow022.json", 0)
	if same, diff := IsDeeply(got, expect); !same {
		Dump(got)
		t.Error(diff)
	}
}

func (s *TestSuite) TestOutlierSlow025(t *C) {
	got, expect := s.aggregateSlowLog("slow025.log", "slow025.json", 0)
	if same, diff := IsDeeply(got, expect); !same {
		Dump(got)
		t.Error(diff)
	}
}
