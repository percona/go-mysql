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

package log

type Event struct {
	Offset        uint64 // byte offset in log file, start of event
	Ts            string // if present in log file, often times not
	Admin         bool   // Query is admin command not SQL query
	Query         string // SQL query or admin command
	User          string
	Host          string
	Db            string
	TimeMetrics   map[string]float32 // *_time and *_wait metrics
	NumberMetrics map[string]uint64  // most metrics
	BoolMetrics   map[string]bool    // yes/no metrics
	// Percona Server
	RateType  string
	RateLimit byte
}

func NewEvent() *Event {
	event := new(Event)
	event.TimeMetrics = make(map[string]float32)
	event.NumberMetrics = make(map[string]uint64)
	event.BoolMetrics = make(map[string]bool)
	return event
}
