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

package event

import (
	"github.com/percona/go-mysql/log"
)

const (
	MAX_EXAMPLE_BYTES = 1024 * 10
)

// A Class represents all events with the same fingerprint and class ID.
// This is only enforced by convention, so be careful not to mix events from
// different classes.
type Class struct {
	Id            string   // 32-character hex checksum of fingerprint
	Fingerprint   string   // canonical form of query: values replaced with "?"
	Metrics       *Metrics // statistics for each metric, e.g. max Query_time
	TotalQueries  uint     // total number of queries in class
	UniqueQueries uint     // unique number of queries in class
	Example       *Example `json:",omitempty"` // sample query with max Query_time
	// --
	outliers uint
	lastDb   string
	sample   bool
}

// A Example is a real query and its database, timestamp, and Query_time.
// If the query is larger than MAX_EXAMPLE_BYTES, it is truncated and "..."
// is appended.
type Example struct {
	QueryTime float64 // Query_time
	Db        string  // Schema: <db> or USE <db>
	Query     string  // truncated to MAX_EXAMPLE_BYTES
	Ts        string  `json:",omitempty"` // in MySQL time zone
}

// NewClass returns a new Class for the class ID and fingerprint.
// If sample is true, the query with the greatest Query_time is saved.
func NewClass(id, fingerprint string, sample bool) *Class {
	class := &Class{
		Id:           id,
		Fingerprint:  fingerprint,
		Metrics:      NewMetrics(),
		TotalQueries: 0,
		Example:      &Example{},
		sample:       sample,
	}
	return class
}

// AddEvent adds an event to the query class.
func (c *Class) AddEvent(e *log.Event, outlier bool) {
	if outlier {
		c.outliers++
	} else {
		c.TotalQueries++
	}

	c.Metrics.AddEvent(e, outlier)

	// Save last db seen for this query. This helps ensure the sample query
	// has a db.
	if e.Db != "" {
		c.lastDb = e.Db
	}
	if c.sample {
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
				c.Example.Ts = e.Ts
			}
		}
	}
}

// AddClass adds a Class to the current class. This is used with Perfomance
// Schema which returns pre-aggregated classes instead of events.
func (c *Class) AddClass(newClass *Class) {
	c.UniqueQueries++
	c.TotalQueries += newClass.TotalQueries
	c.Example = nil

	for newMetric, newStats := range newClass.Metrics.TimeMetrics {
		stats, ok := c.Metrics.TimeMetrics[newMetric]
		if !ok {
			m := *newStats
			m.Med = 0
			m.P95 = 0
			c.Metrics.TimeMetrics[newMetric] = &m
		} else {
			stats.Sum += newStats.Sum
			stats.Avg = stats.Sum / float64(c.TotalQueries)
			if newStats.Min < stats.Min {
				stats.Min = newStats.Min
			}
			if newStats.Max > stats.Max {
				stats.Max = newStats.Max
			}
		}
	}

	for newMetric, newStats := range newClass.Metrics.NumberMetrics {
		stats, ok := c.Metrics.NumberMetrics[newMetric]
		if !ok {
			m := *newStats
			m.Med = 0
			m.P95 = 0
			c.Metrics.NumberMetrics[newMetric] = &m
		} else {
			stats.Sum += newStats.Sum
			stats.Avg = stats.Sum / uint64(c.TotalQueries)
			if newStats.Min < stats.Min {
				stats.Min = newStats.Min
			}
			if newStats.Max > stats.Max {
				stats.Max = newStats.Max
			}
		}
	}

	for newMetric, newStats := range newClass.Metrics.BoolMetrics {
		stats, ok := c.Metrics.BoolMetrics[newMetric]
		if !ok {
			m := *newStats
			c.Metrics.BoolMetrics[newMetric] = &m
		} else {
			stats.Sum += newStats.Sum
		}
	}
}

// Finalize calculates all metric statistics. Call this function when done
// adding events to the class.
func (c *Class) Finalize(rateLimit uint) {
	if rateLimit == 0 {
		rateLimit = 1
	}
	c.Metrics.Finalize(rateLimit)
	c.TotalQueries = (c.TotalQueries * rateLimit) + c.outliers
	if c.Example.QueryTime == 0 {
		c.Example = nil
	}
}
