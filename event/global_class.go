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
)

// A GlobalClass represents a set of events regardless of class.
type GlobalClass struct {
	TotalQueries  uint64   // total number of queries
	UniqueQueries uint64   // number of unique queries (classes)
	RateType      string   `json:",omitempty"` // Percona Server rate limit type
	RateLimit     uint     `json:",omitempty"` // Percona Server rate limit
	Metrics       *Metrics // metric statistics
}

// NewGlobalClass returns a new GlobalClass.
func NewGlobalClass() *GlobalClass {
	class := &GlobalClass{
		TotalQueries:  0,
		UniqueQueries: 0,
		Metrics:       NewMetrics(),
	}
	return class
}

// AddEvent adds an event to the global class.
func (c *GlobalClass) AddEvent(e *log.Event) error {
	var err error
	if e.RateType != "" {
		// Caller is responsible for making sure rate limits are not mixed,
		// e.g. first half of slow log is type=query limit=10 and the second
		// half is type=session limit=100. If rate limit changes, caller should
		// use different aggregators for each rate limit in order to know the
		// proper multipler to apply to each final result.
		c.RateType = e.RateType
		c.RateLimit = e.RateLimit
	}
	c.TotalQueries++
	c.Metrics.AddEvent(e)
	return err
}

// AddClass adds a QueryClass to the global class. This is used with Perfomance
// Schema which returns pre-aggregated classes instead of events.
func (c *GlobalClass) AddClass(class *QueryClass) {
	c.TotalQueries += class.TotalQueries
	c.UniqueQueries++

	for classMetric, classStats := range class.Metrics.TimeMetrics {
		globalStats, ok := c.Metrics.TimeMetrics[classMetric]
		if !ok {
			m := *classStats
			c.Metrics.TimeMetrics[classMetric] = &m
		} else {
			globalStats.Cnt += classStats.Cnt
			globalStats.Sum += classStats.Sum
			globalStats.Avg = (globalStats.Avg + classStats.Avg) / 2
			if classStats.Min < globalStats.Min {
				globalStats.Min = classStats.Min
			}
			if classStats.Max > globalStats.Max {
				globalStats.Max = classStats.Max
			}
		}
	}

	for classMetric, classStats := range class.Metrics.NumberMetrics {
		globalStats, ok := c.Metrics.NumberMetrics[classMetric]
		if !ok {
			m := *classStats
			c.Metrics.NumberMetrics[classMetric] = &m
		} else {
			globalStats.Cnt += classStats.Cnt
			globalStats.Sum += classStats.Sum
			globalStats.Avg = (globalStats.Avg + classStats.Avg) / 2
			if classStats.Min < globalStats.Min {
				globalStats.Min = classStats.Min
			}
			if classStats.Max > globalStats.Max {
				globalStats.Max = classStats.Max
			}
		}
	}

	for classMetric, classStats := range class.Metrics.BoolMetrics {
		globalStats, ok := c.Metrics.BoolMetrics[classMetric]
		if !ok {
			m := *classStats
			c.Metrics.BoolMetrics[classMetric] = &m
		} else {
			globalStats.Cnt += classStats.Cnt
			globalStats.True += classStats.True
		}
	}
}

// Finalize calculates all metric statistics given the unique number of queries.
// Call this function when done adding events or classes to the global class.
func (c *GlobalClass) Finalize(UniqueQueries uint64) {
	c.UniqueQueries = UniqueQueries
	c.Metrics.Finalize()
}
