/*
	Copyright (c) 2014, Percona LLC and/or its affiliates. All rights reserved.

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

package event

import (
	"github.com/percona/go-mysql/log"
	"time"
)

const (
	MAX_EXAMPLE_BYTES = 1024 * 10
)

// A QueryClass represents all events with the same fingerprint and class ID.
// This is only enforced by convention, so be careful not to mix events from
// different classes.
type QueryClass struct {
	Id           string   // 16-character hex checksum of fingerprint
	Fingerprint  string   // canonical form of query: values replaced with "?"
	Metrics      *Metrics // statistics for each metric, e.g. max Query_time
	TotalQueries uint64   // total number of queries in class
	Example      Example  `json:",omitempty"` // example query with max Query_time
	lastDb       string
	example      bool
}

// An Example is a real query and its database, timestamp, and Query_time.
// If the query is larger than MAX_EXAMPLE_BYTES, it is truncated and "..."
// is appended.
type Example struct {
	QueryTime float64 // Query_time
	Db        string  // Schema: <db> or USE <db>
	Query     string  // truncated to MAX_EXAMPLE_BYTES
	Ts        string  `json:",omitempty"` // in MySQL time zone
}

// NewQueryClass returns a new QueryClass for the class ID and fingerprint.
// If example is true, the query with the greatest Query_time is saved.
func NewQueryClass(classId string, fingerprint string, example bool) *QueryClass {
	class := &QueryClass{
		Id:           classId,
		Fingerprint:  fingerprint,
		Metrics:      NewMetrics(),
		TotalQueries: 0,
		example:      example,
	}
	return class
}

// AddEvent adds an event to the query class.
func (c *QueryClass) AddEvent(e *log.Event) {
	c.TotalQueries++
	c.Metrics.AddEvent(e)
	// Save last db seen for this query. This helps ensure the example query
	// has a db.
	if e.Db != "" {
		c.lastDb = e.Db
	}
	if c.example {
		if n, ok := e.TimeMetrics["Query_time"]; ok {
			if float64(n) > c.Example.QueryTime {
				c.Example.QueryTime = float64(n)
				if e.Db != "" {
					c.Example.Db = e.Db
				} else {
					c.Example.Db = c.lastDb
				}
				if len(e.Query) > MAX_EXAMPLE_BYTES {
					c.Example.Query = e.Query[0:MAX_EXAMPLE_BYTES-3] + "..."
				} else {
					c.Example.Query = e.Query
				}
				if e.Ts != "" {
					if t, err := time.Parse("060102 15:04:05", e.Ts); err != nil {
						c.Example.Ts = ""
					} else {
						c.Example.Ts = t.Format("2006-01-02 15:04:05")
					}
				} else {
					c.Example.Ts = ""
				}
			}
		}
	}
}

// Finalize calculates all metric statistics. Call this function when done
// adding events to the class.
func (c *QueryClass) Finalize() {
	c.Metrics.Finalize()
}
