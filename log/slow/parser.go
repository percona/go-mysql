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

// Package slow implements a MySQL slow log parser.
package slow

import (
	"bufio"
	"fmt"
	"io"
	l "log"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/percona/go-mysql/log"
)

// Regular expressions to match important lines in slow log.
var timeRe = regexp.MustCompile(`Time: (\S+\s{1,2}\S+)`)
var userRe = regexp.MustCompile(`User@Host: ([^\[]+|\[[^[]+\]).*?@ (\S*) \[(.*)\]`)
var schema = regexp.MustCompile(`Schema: +(.*?) +Last_errno:`)
var headerRe = regexp.MustCompile(`^#\s+[A-Z]`)
var metricsRe = regexp.MustCompile(`(\w+): (\S+|\z)`)
var adminRe = regexp.MustCompile(`command: (.+)`)
var setRe = regexp.MustCompile(`^SET (?:last_insert_id|insert_id|timestamp)`)
var useRe = regexp.MustCompile(`^(?i)use `)

// A SlowLogParser parses a MySQL slow log. It implements the LogParser interface.
type SlowLogParser struct {
	file *os.File
	opt  log.Options
	// --
	stopChan    chan bool
	eventChan   chan *log.Event
	inHeader    bool
	inQuery     bool
	headerLines uint
	queryLines  uint64
	bytesRead   uint64
	lineOffset  uint64
	stopped     bool
	event       *log.Event
}

// NewSlowLogParser returns a new SlowLogParser that reads from the open file.
func NewSlowLogParser(file *os.File, opt log.Options) *SlowLogParser {
	p := &SlowLogParser{
		file: file,
		opt:  opt,
		// --
		stopChan:    make(chan bool, 1),
		eventChan:   make(chan *log.Event),
		inHeader:    false,
		inQuery:     false,
		headerLines: 0,
		queryLines:  0,
		lineOffset:  0,
		bytesRead:   opt.StartOffset,
		event:       log.NewEvent(),
	}
	return p
}

// EventChan returns the unbuffered event channel on which the caller can
// receive events.
func (p *SlowLogParser) EventChan() <-chan *log.Event {
	return p.eventChan
}

// Stop stops the parser before parsing the next event or while blocked on
// sending the current event to the event channel.
func (p *SlowLogParser) Stop() {
	if p.opt.Debug {
		l.Println("stopping")
	}
	p.stopChan <- true
	return
}

// Start starts the parser. Events are sent to the unbuffered event channel.
// Parsing stops on EOF, error, or call to Stop. The event channel is closed
// when parsing stops. The file is not closed.
func (p *SlowLogParser) Start() error {
	if p.opt.Debug {
		l.SetFlags(l.Ltime | l.Lmicroseconds)
		fmt.Println()
		l.Println("parsing " + p.file.Name())
	}

	// Seek to the offset, if any.
	// @todo error if start off > file size
	if p.opt.StartOffset > 0 {
		if _, err := p.file.Seek(int64(p.opt.StartOffset), os.SEEK_SET); err != nil {
			return err
		}
	}

	defer close(p.eventChan)

	r := bufio.NewReader(p.file)

SCANNER_LOOP:
	for !p.stopped {
		select {
		case <-p.stopChan:
			p.stopped = true
			break SCANNER_LOOP
		default:
		}

		line, err := r.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				return err
			}
			break SCANNER_LOOP
		}

		lineLen := uint64(len(line))
		p.bytesRead += lineLen
		p.lineOffset = p.bytesRead - lineLen
		if p.lineOffset != 0 {
			// @todo Need to get clear on why this is needed;
			// it does make the value correct; an off-by-one issue
			p.lineOffset += 1
		}

		if p.opt.Debug {
			fmt.Println()
			l.Printf("+%d line: %s", p.lineOffset, line)
		}

		// Filter out meta lines:
		//   /usr/local/bin/mysqld, Version: 5.6.15-62.0-tokudb-7.1.0-tokudb-log (binary). started with:
		//   Tcp port: 3306  Unix socket: /var/lib/mysql/mysql.sock
		//   Time                 Id Command    Argument
		if lineLen >= 20 && ((line[0] == '/' && line[lineLen-6:lineLen] == "with:\n") ||
			(line[0:5] == "Time ") ||
			(line[0:4] == "Tcp ") ||
			(line[0:4] == "TCP ")) {
			if p.opt.Debug {
				l.Println("meta")
			}
			continue
		}

		// PMM-1834: Filter out empty comments and MariaDB explain:
		if line == "#\n" || strings.HasPrefix(line, "# explain:") {
			continue
		}

		// Remove \n.
		line = line[0 : lineLen-1]

		if p.inHeader {
			p.parseHeader(line)
		} else if p.inQuery {
			p.parseQuery(line)
		} else if headerRe.MatchString(line) {
			p.inHeader = true
			p.inQuery = false
			p.parseHeader(line)
		}
	}

	if !p.stopped && p.queryLines > 0 {
		p.sendEvent(false, false)
	}

	if p.opt.Debug {
		l.Printf("\ndone")
	}
	return nil
}

// --------------------------------------------------------------------------

func (p *SlowLogParser) parseHeader(line string) {
	if p.opt.Debug {
		l.Println("header")
	}

	if !headerRe.MatchString(line) {
		p.inHeader = false
		p.inQuery = true
		p.parseQuery(line)
		return
	}

	if p.headerLines == 0 {
		p.event.Offset = p.lineOffset
	}
	p.headerLines++

	if strings.HasPrefix(line, "# Time") {
		if p.opt.Debug {
			l.Println("time")
		}
		m := timeRe.FindStringSubmatch(line)
		if len(m) < 2 {
			return
		}
		p.event.Ts = m[1]
		if userRe.MatchString(line) {
			if p.opt.Debug {
				l.Println("user (bad format)")
			}
			m := userRe.FindStringSubmatch(line)
			p.event.User = m[1]
			p.event.Host = m[2]
		}
	} else if strings.HasPrefix(line, "# User") {
		if p.opt.Debug {
			l.Println("user")
		}
		m := userRe.FindStringSubmatch(line)
		if len(m) < 3 {
			return
		}
		p.event.User = m[1]
		p.event.Host = m[2]
	} else if strings.HasPrefix(line, "# admin") {
		p.parseAdmin(line)
	} else {
		if p.opt.Debug {
			l.Println("metrics")
		}
		submatch := schema.FindStringSubmatch(line)
		if len(submatch) == 2 {
			p.event.Db = submatch[1]
		}

		m := metricsRe.FindAllStringSubmatch(line, -1)
		for _, smv := range m {
			// [String, Metric, Value], e.g. ["Query_time: 2", "Query_time", "2"]
			if strings.HasSuffix(smv[1], "_time") || strings.HasSuffix(smv[1], "_wait") {
				// microsecond value
				val, _ := strconv.ParseFloat(smv[2], 64)
				p.event.TimeMetrics[smv[1]] = val
			} else if smv[2] == "Yes" || smv[2] == "No" {
				// boolean value
				if smv[2] == "Yes" {
					p.event.BoolMetrics[smv[1]] = true
				} else {
					p.event.BoolMetrics[smv[1]] = false
				}
			} else if smv[1] == "Schema" {
				p.event.Db = smv[2]
			} else if smv[1] == "Log_slow_rate_type" {
				p.event.RateType = smv[2]
			} else if smv[1] == "Log_slow_rate_limit" {
				val, _ := strconv.ParseUint(smv[2], 10, 64)
				p.event.RateLimit = uint(val)
			} else {
				// integer value
				val, _ := strconv.ParseUint(smv[2], 10, 64)
				p.event.NumberMetrics[smv[1]] = val
			}
		}
	}
}

func (p *SlowLogParser) parseQuery(line string) {
	if p.opt.Debug {
		l.Println("query")
	}

	if strings.HasPrefix(line, "# admin") {
		p.parseAdmin(line)
		return
	} else if headerRe.MatchString(line) {
		if p.opt.Debug {
			l.Println("next event")
		}
		p.inHeader = true
		p.inQuery = false
		p.sendEvent(true, false)
		p.parseHeader(line)
		return
	}

	isUse := useRe.FindString(line)
	if p.queryLines == 0 && isUse != "" {
		if p.opt.Debug {
			l.Println("use db")
		}
		db := strings.TrimPrefix(line, isUse)
		db = strings.TrimRight(db, ";")
		db = strings.Trim(db, "`")
		p.event.Db = db
		// Set the 'use' as the query itself.
		// In case we are on a group of lines like in test 23, lines 6~8, the
		// query will be replaced by the real query "select field...."
		// In case we are on a group of lines like in test23, lines 27~28, the
		// query will be "use dbnameb" since the user executed a use command
		p.event.Query = line
	} else if setRe.MatchString(line) {
		if p.opt.Debug {
			l.Println("set var")
		}
		// @todo ignore or use these lines?
	} else {
		if p.opt.Debug {
			l.Println("query")
		}
		if p.queryLines > 0 {
			p.event.Query += "\n" + line
		} else {
			p.event.Query = line
		}
		p.queryLines++
	}
}

func (p *SlowLogParser) parseAdmin(line string) {
	if p.opt.Debug {
		l.Println("admin")
	}
	p.event.Admin = true
	m := adminRe.FindStringSubmatch(line)
	p.event.Query = m[1]
	p.event.Query = strings.TrimSuffix(p.event.Query, ";") // makes FilterAdminCommand work

	// admin commands should be the last line of the event.
	if filtered := p.opt.FilterAdminCommand[p.event.Query]; !filtered {
		if p.opt.Debug {
			l.Println("not filtered")
		}
		p.sendEvent(false, false)
	} else {
		p.inHeader = false
		p.inQuery = false
	}
}

func (p *SlowLogParser) sendEvent(inHeader bool, inQuery bool) {
	if p.opt.Debug {
		l.Println("send event")
	}

	// Make a new event and reset our metadata.
	defer func() {
		p.event = log.NewEvent()
		p.headerLines = 0
		p.queryLines = 0
		p.inHeader = inHeader
		p.inQuery = inQuery
	}()

	if _, ok := p.event.TimeMetrics["Query_time"]; !ok {
		if p.headerLines == 0 {
			l.Panicf("No Query_time in event at %d: %#v", p.lineOffset, p.event)
		}
		// Started parsing in header after Query_time.  Throw away event.
		return
	}

	// Clean up the event.
	p.event.Db = strings.TrimSuffix(p.event.Db, ";\n")
	p.event.Query = strings.TrimSuffix(p.event.Query, ";")

	// Send the event.  This will block.
	select {
	case p.eventChan <- p.event:
	case <-p.stopChan:
		p.stopped = true
	}
}
