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

type QueryClass struct {
	Id           string
	Fingerprint  string
	Metrics      *Metrics
	TotalQueries uint64
	Example      Example `json:",omitempty"`
	lastDb       string
	example      bool
}

type Example struct {
	QueryTime float64
	Db        string
	Query     string
	Ts        string `json:",omitempty"`
}

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
				c.Example.Query = e.Query
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

func (c *QueryClass) Finalize() {
	c.Metrics.Finalize()
}
