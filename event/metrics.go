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
	"sort"
)

// Metrics encapsulate the metrics of an event like Query_time and Rows_sent.
type Metrics struct {
	TimeMetrics   map[string]*TimeStats   `json:",omitempty"`
	NumberMetrics map[string]*NumberStats `json:",omitempty"`
	BoolMetrics   map[string]*BoolStats   `json:",omitempty"`
}

// TimeStats are microsecond-based metrics like Query_time and Lock_time.
type TimeStats struct {
	vals  []float64 `json:"-"`
	Cnt   uint
	Sum   float64
	Min   float64
	Avg   float64
	Med   float64 // median
	Pct95 float64 // 95th percentile
	Max   float64
}

// NumberStats are integer-based metrics like Rows_sent and Merge_passes.
type NumberStats struct {
	vals  []uint64 `json:"-"`
	Cnt   uint
	Sum   uint64
	Min   uint64
	Avg   uint64
	Med   uint64 // median
	Pct95 uint64 // 95th percentile
	Max   uint64
}

// BoolStats are boolean-based metrics like QC_Hit and Filesort.
type BoolStats struct {
	Cnt  uint
	True uint // %True = True/Cnt, %False=(Cnt-True)/Cnt
}

// NewMetrics returns a pointer to an initialized Metrics structure.
func NewMetrics() *Metrics {
	m := &Metrics{
		TimeMetrics:   make(map[string]*TimeStats),
		NumberMetrics: make(map[string]*NumberStats),
		BoolMetrics:   make(map[string]*BoolStats),
	}
	return m
}

// AddEvent saves all the metrics of the event.
func (m *Metrics) AddEvent(e *log.Event) {
	for metric, val := range e.TimeMetrics {
		stats, seenMetric := m.TimeMetrics[metric]
		if !seenMetric {
			m.TimeMetrics[metric] = &TimeStats{
				vals: []float64{},
			}
			stats = m.TimeMetrics[metric]
		}
		stats.Cnt++
		stats.Sum += float64(val)
		stats.vals = append(stats.vals, float64(val))
	}

	for metric, val := range e.NumberMetrics {
		stats, seenMetric := m.NumberMetrics[metric]
		if !seenMetric {
			m.NumberMetrics[metric] = &NumberStats{
				vals: []uint64{},
			}
			stats = m.NumberMetrics[metric]
		}
		stats.Cnt++
		stats.Sum += val
		stats.vals = append(stats.vals, val)
	}

	for metric, val := range e.BoolMetrics {
		stats, seenMetric := m.BoolMetrics[metric]
		if seenMetric {
			stats.Cnt++
			if val {
				stats.True++
			}
		} else {
			stats := &BoolStats{
				Cnt: 1,
			}
			if val {
				stats.True++
			}
			m.BoolMetrics[metric] = stats
		}
	}
}

type byUint64 []uint64

func (a byUint64) Len() int      { return len(a) }
func (a byUint64) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byUint64) Less(i, j int) bool {
	return a[i] < a[j] // ascending order
}

// Finalize calculates the statistics of the added metrics. Call this function
// when done adding events.
func (m *Metrics) Finalize() {
	for _, s := range m.TimeMetrics {
		sort.Float64s(s.vals)

		s.Min = s.vals[0]
		s.Avg = s.Sum / float64(s.Cnt)
		s.Pct95 = s.vals[(95*s.Cnt)/100]
		s.Med = s.vals[(50*s.Cnt)/100] // median = 50th percentile
		s.Max = s.vals[s.Cnt-1]
	}

	for _, s := range m.NumberMetrics {
		sort.Sort(byUint64(s.vals))

		s.Min = s.vals[0]
		s.Avg = s.Sum / uint64(s.Cnt)
		s.Pct95 = s.vals[(95*s.Cnt)/100]
		s.Med = s.vals[(50*s.Cnt)/100] // median = 50th percentile
		s.Max = s.vals[s.Cnt-1]
	}
}
