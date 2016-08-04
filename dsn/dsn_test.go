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

package dsn

import (
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	sample   string
	result   string
	examples bool
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) TestParseDefaults(t *C) {
	input := "--user=root\n--password=rootpwd\n--port=3306\n--host=localhost\n--socket=/var/run/mysqld/mysqld.sock\n"
	dsn := ParseMySQLDefaults(input)
	t.Check(dsn.Username, Equals, "root")
	t.Check(dsn.Password, Equals, "rootpwd")
	t.Check(dsn.Hostname, Equals, "")
	t.Check(dsn.Port, Equals, "")
	t.Check(dsn.Socket, Equals, "/var/run/mysqld/mysqld.sock")

	input = "--user=root\n--password=rootpwd\n--port=3306\n--host=localhost"
	dsn = ParseMySQLDefaults(input)
	t.Check(dsn.Username, Equals, "root")
	t.Check(dsn.Password, Equals, "rootpwd")
	t.Check(dsn.Hostname, Equals, "localhost")
	t.Check(dsn.Port, Equals, "3306")
	t.Check(dsn.Socket, Equals, "")

}
