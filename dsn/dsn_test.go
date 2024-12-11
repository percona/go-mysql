/*
Copyright (c) 2019, Percona LLC.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package dsn

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/yehornaumenko/go-mysql/test"
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
