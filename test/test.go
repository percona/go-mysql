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

package test

import (
	"flag"
	"os"
	"path/filepath"
	"runtime"
)

var Update = flag.Bool("update", false, "update .golden files")

func RootDir() string {
	_, filename, _, _ := runtime.Caller(1)
	dir := filepath.Dir(filename)
	if fileExists(dir + "/.git") {
		return filepath.Clean(dir)
	}
	dir += "/"
	for i := 0; i < 10; i++ {
		dir = dir + "../"
		if fileExists(dir + ".git") {
			return filepath.Clean(dir)
		}
	}
	panic("Cannot find .git/")
}

func fileExists(file string) bool {
	if _, err := os.Stat(file); err == nil {
		return true
	}
	return false
}
