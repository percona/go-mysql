/*
   Copyright (c) 2016, Percona LLC and/or its affiliates. All rights reserved.

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
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"
)

type DSN struct {
	Username string
	Password string
	Hostname string
	Port     string
	Socket   string
	//
	DefaultsFile string
	Protocol     string
	//
	DefaultDb string
	Params    []string
}

const (
	ParseTimeParam    = "parseTime=true"
	TimezoneParam     = `time_zone='%2b00%3a00'`
	LocationParam     = "loc=UTC"
	OldPasswordsParam = "allowOldPasswords=true"
	HiddenPassword    = "***"
)

var (
	ErrNoSocket error = errors.New("cannot auto-detect MySQL socket")
)

func (dsn DSN) AutoDetect() (DSN, error) {
	defaults, err := Defaults(dsn.DefaultsFile)
	if err != nil {
		return dsn, err
	}

	if dsn.Username == "" {
		if defaults.Username != "" {
			dsn.Username = defaults.Username
		} else {
			dsn.Username = os.Getenv("USER")
			if dsn.Username == "" {
				dsn.Username = "root"
			}
		}
	}

	if dsn.Password == "" && defaults.Password != "" {
		dsn.Password = defaults.Password
	}

	if dsn.Hostname == "" {
		if defaults.Hostname != "" {
			dsn.Hostname = defaults.Hostname
		} else {
			dsn.Hostname = "localhost"
		}
	}

	if dsn.Port == "" {
		if defaults.Port != "" {
			dsn.Port = defaults.Port
		} else {
			dsn.Port = "3306"
		}
	}

	// MySQL magic: localhost means socket if socket isn't set and protocol isn't tcp.
	if dsn.Hostname == "localhost" && dsn.Socket == "" && dsn.Protocol != "tcp" {
		if defaults.Socket != "" {
			dsn.Socket = defaults.Socket
		} else {
			// Try to auto-detect MySQL socket from netstat output.
			out, err := exec.Command("netstat", "-anp").Output()
			if err != nil {
				return dsn, ErrNoSocket
			}
			socket := ParseSocketFromNetstat(string(out))
			if socket == "" {
				return dsn, ErrNoSocket
			}
			dsn.Socket = socket
		}
	}

	return dsn, nil
}

func Defaults(defaultsFile string) (DSN, error) {
	versionParams := [][]string{
		[]string{"-s", "client"},
		[]string{"client"},
	}
	if defaultsFile != "" {
		versionParams = [][]string{
			[]string{"--defaults-file=" + defaultsFile, "-s", "client"},
			[]string{"--defaults-file=" + defaultsFile, "client"},
		}
	}

	var err error
	var output []byte
	for _, params := range versionParams {
		cmd := exec.Command("my_print_defaults", params...)
		output, err = cmd.Output()
		if err == nil {
			break
		}
	}
	dsn := ParseMySQLDefaults(string(output))
	return dsn, nil
}

func (dsn DSN) String() string {
	dsnString := ""

	// Socket takes priority if set and protocol isn't tcp.
	if dsn.Socket != "" && dsn.Protocol != "tcp" {
		dsnString = fmt.Sprintf("%s:%s@unix(%s)",
			dsn.Username,
			dsn.Password,
			dsn.Socket,
		)
	} else {
		if dsn.Hostname == "" {
			dsn.Hostname = "localhost"
		}
		if dsn.Port == "" {
			dsn.Port = "3306"
		}
		dsnString = fmt.Sprintf("%s:%s@tcp(%s:%s)",
			dsn.Username,
			dsn.Password,
			dsn.Hostname,
			dsn.Port,
		)
	}

	dsnString += "/" + dsn.DefaultDb

	params := strings.Join(dsn.Params, "&")
	if params != "" {
		dsnString += "?" + params
	}

	return dsnString
}

func (dsn DSN) Verify() error {
	// Open connection to MySQL but...
	db, err := sql.Open("mysql", dsn.String())
	if err != nil {
		return err
	}
	defer db.Close()

	// ...try to use the connection for real.
	if err = db.Ping(); err != nil {
		return err
	}

	return nil
}

func HidePassword(dsn string) string {
	dsn = strings.TrimRight(strings.Split(dsn, "?")[0], "/")
	if strings.Index(dsn, "@") > 0 {
		dsnParts := strings.Split(dsn, "@")
		userPart := dsnParts[0]
		hostPart := ""
		if len(dsnParts) > 1 {
			hostPart = dsnParts[len(dsnParts)-1]
		}
		userPasswordParts := strings.Split(userPart, ":")
		dsn = fmt.Sprintf("%s:***@%s", userPasswordParts[0], hostPart)
	}
	return dsn
}

func ParseSocketFromNetstat(out string) string {
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "unix") && strings.Contains(line, "mysql") {
			fields := strings.Fields(line)
			socket := fields[len(fields)-1]
			if path.IsAbs(socket) {
				return socket
			}
		}
	}
	return ""
}

func ParseMySQLDefaults(output string) DSN {
	var re *regexp.Regexp
	var result [][]string // Result of FindAllStringSubmatch
	var dsn DSN

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		re = regexp.MustCompile("--user=(.*)")
		result = re.FindAllStringSubmatch(line, -1)
		if result != nil {
			dsn.Username = result[len(result)-1][1]
		}

		re = regexp.MustCompile("--password=(.*)")
		result = re.FindAllStringSubmatch(line, -1)
		if result != nil {
			dsn.Password = result[len(result)-1][1]
		}

		re = regexp.MustCompile("--socket=(.*)")
		result = re.FindAllStringSubmatch(line, -1)
		if result != nil {
			dsn.Socket = result[len(result)-1][1]
		}

		re = regexp.MustCompile("--host=(.*)")
		result = re.FindAllStringSubmatch(line, -1)
		if result != nil {
			dsn.Hostname = result[len(result)-1][1]
		}

		re = regexp.MustCompile("--port=(.*)")
		result = re.FindAllStringSubmatch(line, -1)
		if result != nil {
			dsn.Port = result[len(result)-1][1]
		}
	}
	if dsn.Socket != "" { // Cannot have socket & host
		dsn.Port = ""
		dsn.Hostname = ""
	}

	// Hostname always defaults to localhost.  If localhost means 127.0.0.1 or socket
	// is handled by mysql/DSN.DSN().
	if dsn.Hostname == "" && dsn.Socket == "" {
		dsn.Hostname = "localhost"
	}

	return dsn
}
