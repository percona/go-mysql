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
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/percona/go-mysql/test"
	"github.com/stretchr/testify/assert"
)

func TestParseDefaults(t *testing.T) {

	input := "--user=root\n--password=rootpwd\n--port=3306\n--host=localhost\n--socket=/var/run/mysqld/mysqld.sock\n"
	dsn := ParseMySQLDefaults(input)
	assert.Equal(t, "root", dsn.Username)
	assert.Equal(t, "rootpwd", dsn.Password)
	assert.Equal(t, "", dsn.Hostname)
	assert.Equal(t, "", dsn.Port)
	assert.Equal(t, "/var/run/mysqld/mysqld.sock", dsn.Socket)

	input = "--user=root\n--password=rootpwd\n--port=3306\n--host=localhost"
	dsn = ParseMySQLDefaults(input)
	assert.Equal(t, "root", dsn.Username)
	assert.Equal(t, "rootpwd", dsn.Password)
	assert.Equal(t, "localhost", dsn.Hostname)
	assert.Equal(t, "3306", dsn.Port)
	assert.Equal(t, "", dsn.Socket)

}

func TestDefaults(t *testing.T) {
	originalPath := os.Getenv("PATH")

	// Since we cannot install different versions of my_print_defaults, we are going
	// to use 2 shell scripts to mock the behavior of the original programs.
	// os.Exec (used in the Defaults func) search in the PATH for the program to run so,
	// let's change the path to point to our mock scripts.
	os.Setenv("PATH", path.Join(test.RootDir(), "test/scripts/my_print_defaults/5.5"))
	dsn, err := Defaults("a_fake_filename")
	assert.NoError(t, err)
	assert.Equal(t, "root5.5", dsn.Username)
	assert.Equal(t, "rootpwd", dsn.Password)
	assert.Equal(t, "", dsn.Hostname)
	assert.Equal(t, "", dsn.Port)
	assert.Equal(t, "/var/run/mysqld/mysqld.sock", dsn.Socket)

	os.Setenv("PATH", path.Join(test.RootDir(), "test/scripts/my_print_defaults/5.6"))
	dsn, err = Defaults("a_fake_filename")
	assert.NoError(t, err)
	assert.Equal(t, "root5.6", dsn.Username)
	assert.Equal(t, "rootpwd", dsn.Password)
	assert.Equal(t, "", dsn.Hostname)
	assert.Equal(t, "", dsn.Port)
	assert.Equal(t, "/var/run/mysqld/mysqld.sock", dsn.Socket)

	os.Setenv("PATH", originalPath)
}

func TestGetSocketFromProcessLists(t *testing.T) {
	var err error
	var socket string
	dsn := DSN{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	socket, err = GetSocket(ctx, dsn.String())
	assert.NoError(t, err)
	assert.NotEmpty(t, socket)

	// Each of below command may fail as result depends on OS.
	socket, err = GetSocketFromTCPConnection(context.TODO(), dsn.String())
	if err == nil {
		assert.NotEmpty(t, socket)
	}
	socket, err = GetSocketFromNetstat(context.TODO())
	if err == nil {
		assert.NotEmpty(t, socket)
	}
	socket, err = GetSocketFromProcessList(context.TODO())
	if err == nil {
		assert.NotEmpty(t, socket)
	}
}
