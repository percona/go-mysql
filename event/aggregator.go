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

// A Result contains a global class and query classes with finalized metric
// statistics. The query classes are keyed on class ID.
type Result struct {
	Global *GlobalClass
	Class  map[string]*QueryClass // keyed on class ID
	Error  string
}

// An EventAggregator groups events into a global class and query classes by
// class ID. When there are no more events, a call to Finalize computes all
// metric statistics and returns a Result.
type EventAggregator struct {
	examples bool
	// --
	result *Result
}

// NewEventAggregator returns a new EventAggregator.
func NewEventAggregator(examples bool) *EventAggregator {
	result := &Result{
		Global: NewGlobalClass(),
		Class:  make(map[string]*QueryClass),
	}
	a := &EventAggregator{
		examples: examples,
		// --
		result: result,
	}
	return a
}

// AddEvent adds the event to the aggregator, automatically creating new query
// classes as needed.
func (a *EventAggregator) AddEvent(event *log.Event, id, fingerprint string) {

	// Add the event to the global class.
	a.result.Global.AddEvent(event)

	// Get the query class to which the event belongs.
	class, haveClass := a.result.Class[id]
	if !haveClass {
		class = NewQueryClass(id, fingerprint, a.examples)
		a.result.Class[id] = class
	}

	// Add the event to its query class.
	class.AddEvent(event)
}

// Finalize calculates all metric statistics and returns a Result.
// Call this function when done adding events to the aggregator.
func (a *EventAggregator) Finalize() *Result {
	for _, class := range a.result.Class {
		class.Finalize()
	}
	a.result.Global.Finalize(uint64(len(a.result.Class)))
	return a.result
}
