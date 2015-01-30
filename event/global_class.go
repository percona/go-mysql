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

type GlobalClass struct {
	TotalQueries  uint64
	UniqueQueries uint64
	RateType      string `json:",omitempty"`
	RateLimit     uint   `json:",omitempty"`
	Metrics       *Metrics
}

func NewGlobalClass() *GlobalClass {
	class := &GlobalClass{
		TotalQueries:  0,
		UniqueQueries: 0,
		Metrics:       NewMetrics(),
	}
	return class
}

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

func (c *GlobalClass) Finalize(UniqueQueries uint64) {
	c.UniqueQueries = UniqueQueries
	c.Metrics.Finalize()
}
