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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/shirou/gopsutil/process"
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
	// ErrNoSocket is returned when GetSocketFromProcessLists can't locate socket.
	ErrNoSocket = errors.New("cannot auto-detect MySQL socket")
)

func (dsn DSN) AutoDetect(ctx context.Context) (DSN, error) {
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
			socket, err := GetSocket(ctx, dsn.String())
			if err != nil {
				return dsn, err
			}
			dsn.Socket = socket
		}
	}

	return dsn, nil
}

func Defaults(defaultsFile string) (DSN, error) {
	versionParams := [][]string{
		{"-s", "client"},
		{"client"},
	}
	if defaultsFile != "" {
		versionParams = [][]string{
			{"--defaults-file=" + defaultsFile, "-s", "client"},
			{"--defaults-file=" + defaultsFile, "client"},
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

// GetSocketFromTCPConnection will try to get socket path by connecting to MySQL localhost TCP port.
// This is not reliable as TCP connections may be not allowed.
func GetSocketFromTCPConnection(ctx context.Context, dsn string) (socket string, err error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", ErrNoSocket
	}
	defer db.Close()

	err = db.QueryRowContext(ctx, "SELECT @@socket").Scan(socket)
	if err != nil {
		return "", ErrNoSocket
	}
	if !path.IsAbs(socket) {
		return "", ErrNoSocket
	}
	if socket != "" {
		return socket, nil
	}

	return "", ErrNoSocket
}

// GetSocketFromProcessList will loop through the list of PIDs until it finds a process
// named 'mysqld' and the it will try to get the socket by querying the open network
// connections for that process.
// Warning: this function returns the socket for the FIRST mysqld process it founds.
// If there are more than one MySQL instance, only the first one will be detected.
func GetSocketFromProcessList(ctx context.Context) (string, error) {
	pids, err := process.Pids()
	if err != nil {
		return "", errors.Wrap(err, "Cannot get the list of PIDs")
	}
	socketsMap := map[string]struct{}{}
	sockets := []string{}
	mysqldPIDs := []string{}
	for _, pid := range pids {
		proc, err := process.NewProcess(pid)
		if err != nil {
			continue
		}
		procName, err := proc.Name()
		if err != nil {
			continue
		}
		if procName != "mysqld" {
			continue
		}
		mysqlPID := fmt.Sprintf("%d", pid)
		mysqldPIDs = append(mysqldPIDs, mysqlPID)
		socketsFromPID, err := GetSocketsFromPID(ctx, mysqlPID)
		if err != nil {
			return "", errors.Wrapf(err, "Cannot get network connections for PID %d", pid)
		}
		for _, socket := range socketsFromPID {
			if strings.HasPrefix(socket, "->") {
				continue
			}
			if strings.HasSuffix(socket, "/mysqlx.sock") {
				continue
			}
			if _, exist := socketsMap[socket]; !exist {
				socketsMap[socket] = struct{}{}
				sockets = append(sockets, socket)
			}
		}
	}
	if len(sockets) > 1 {
		log.Printf("lsof: multiple sockets detected for pid(s) %v, choosing first one: %s\n", mysqldPIDs, strings.Join(sockets, ", "))
	}
	if len(sockets) > 0 {
		return sockets[0], nil
	}
	return "", ErrNoSocket
}

// GetSocketFromNetstat will loop through list of open sockets
// and try to find one matching `mysql` word.
// Warning: this function returns the socket for the FIRST entry it founds.
// If there are more sockets containing `mysql` word, only the first one will be detected.
func GetSocketFromNetstat(ctx context.Context) (string, error) {
	// Try to auto-detect MySQL socket from netstat output.
	out, err := exec.CommandContext(ctx, "netstat", "-anp").Output()
	if err != nil {
		return "", ErrNoSocket
	}

	socketsMap := map[string]struct{}{}
	sockets := []string{}
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if !strings.HasPrefix(line, "unix") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		socket := fields[len(fields)-1]
		if !path.IsAbs(socket) {
			continue
		}
		if strings.HasSuffix(socket, "/mysqlx.sock") {
			continue
		}
		if !strings.Contains(socket, "mysql") {
			continue
		}
		if _, exist := socketsMap[socket]; !exist {
			socketsMap[socket] = struct{}{}
			sockets = append(sockets, socket)
		}
	}
	if len(sockets) > 1 {
		log.Println("netstat: multiple sockets detected, choosing first one:", strings.Join(sockets, ", "))
	}
	if len(sockets) > 0 {
		return sockets[0], nil
	}
	return "", ErrNoSocket
}

// GetSocket tries to detect and return path to the MySQL socket.
func GetSocket(ctx context.Context, dsn string) (string, error) {
	var socket string
	var err error
	socket, err = GetSocketFromTCPConnection(ctx, dsn)
	if err != nil {
		socket, err = GetSocketFromProcessList(ctx)
		if err != nil {
			socket, err = GetSocketFromNetstat(ctx)
		}
	}
	return socket, err
}

// GetSocketsFromPID returns currently open UNIX domain socket files by process identifier (PID).
func GetSocketsFromPID(ctx context.Context, pid string) ([]string, error) {
	cmd := exec.CommandContext(
		ctx,
		"lsof",
		"-a",
		"-n",
		"-P",
		"-U",
		"-F",
		"n",
		"-p", pid,
	)
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return parseLsofForSockets(output), nil
}

// parseLsofForSockets parses `lsof -F n -p <pid>` output for open UNIX domain socket files.
func parseLsofForSockets(output []byte) (sockets []string) {
	socketsMap := map[string]struct{}{}
	lines := bytes.Split(output, []byte("\n"))
	for _, line := range lines {
		// `lsof -F n`
		// When the -F option is specified, lsof produces output that is suitable for processing by another program
		// - e.g, an awk or Perl script, or a C program.
		// n    file name, comment, Internet address
		if !bytes.HasPrefix(line, []byte("n")) {
			continue
		}
		line = bytes.TrimPrefix(line, []byte("n"))

		// lsof on trusty:                 `/var/run/mysqld/mysqld.sock`
		// lsof on xenial, artful, bionic: `/var/run/mysqld/mysqld.sock type=STREAM`
		line = bytes.TrimSuffix(line, []byte("type=STREAM"))
		line = bytes.TrimSpace(line)

		// Skip empty lines.
		if len(line) == 0 {
			continue
		}
		socket := string(line)

		// @Nailya had a case on xenial where `lsof` returned `ntype=DGRAM` and `ntype=STREAM` without any path.
		// I'm not sure what are those but we can try to avoid this by checking for absolute path.
		// # lsof -a -n -P -U -F n -p $(pgrep -x mysqld | tr \\n ,)
		// p952
		// f3
		// ntype=DGRAM
		// f18
		// ntype=STREAM
		// f19
		// ntype=STREAM
		// f22
		// n/var/run/mysqld/mysqld.sock type=STREAM
		// f24
		// n/var/run/mysqld/mysqlx.sock type=STREAM
		if !path.IsAbs(socket) {
			continue
		}

		if _, exist := socketsMap[socket]; !exist {
			socketsMap[socket] = struct{}{}
			sockets = append(sockets, socket)
		}
	}

	return sockets
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
