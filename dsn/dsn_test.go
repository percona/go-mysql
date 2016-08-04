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
	"os"
	"path"
	"testing"

	. "github.com/go-test/test"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
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

func (s *TestSuite) TestDefaults(t *C) {
	originalPath := os.Getenv("PATH")

	// Since we cannot install different versions of my_print_defaults, we are going
	// to use 2 shell scripts to mock the behavior of the original programs.
	// os.Exec (used in the Defaults func) search in the PATH for the program to run so,
	// let's change the path to point to our mock scripts.
	os.Setenv("PATH", path.Join(RootDir(), "test/scripts/my_print_defaults/5.5"))
	dsn, err := Defaults("a_fake_filename")
	t.Check(err, IsNil)
	t.Check(dsn.Username, Equals, "root5.5")
	t.Check(dsn.Password, Equals, "rootpwd")
	t.Check(dsn.Hostname, Equals, "")
	t.Check(dsn.Port, Equals, "")
	t.Check(dsn.Socket, Equals, "/var/run/mysqld/mysqld.sock")

	os.Setenv("PATH", path.Join(RootDir(), "test/scripts/my_print_defaults/5.6"))
	dsn, err = Defaults("a_fake_filename")
	t.Check(err, IsNil)
	t.Check(dsn.Username, Equals, "root5.6")
	t.Check(dsn.Password, Equals, "rootpwd")
	t.Check(dsn.Hostname, Equals, "")
	t.Check(dsn.Port, Equals, "")
	t.Check(dsn.Socket, Equals, "/var/run/mysqld/mysqld.sock")

	os.Setenv("PATH", originalPath)
}
