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

// +build ignore

// check-license checks that AGPL license header in all files matches header in this file.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
)

func getHeader() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("runtime.Caller(0) failed")
	}
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var header string
	s := bufio.NewScanner(f)
	for s.Scan() {
		if s.Text() == "" {
			break
		}
		header += s.Text() + "\n"
	}
	header += "\n"
	if err := s.Err(); err != nil {
		log.Fatal(err)
	}
	return header
}

var generatedHeader = regexp.MustCompile(`^// Code generated .* DO NOT EDIT\.`)

func checkHeader(path string, header string) bool {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	actual := make([]byte, len(header))
	_, err = io.ReadFull(f, actual)
	if err == io.ErrUnexpectedEOF {
		err = nil // some files are shorter than license header
	}
	if err != nil {
		log.Printf("%s - %s", path, err)
		return false
	}

	if generatedHeader.Match(actual) {
		return true
	}

	if header != string(actual) {
		log.Print(path)
		return false
	}
	return true
}

func main() {
	log.SetFlags(0)
	flag.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "Usage: go run .github/check-license.go")
		flag.CommandLine.PrintDefaults()
	}
	flag.Parse()

	header := getHeader()

	ok := true
	filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case ".git", "vendor":
				return filepath.SkipDir
			default:
				return nil
			}
		}

		if filepath.Ext(info.Name()) == ".go" {
			if !checkHeader(path, header) {
				ok = false
			}
		}
		return nil
	})

	if ok {
		os.Exit(0)
	}
	log.Print("Please update license header in those files.")
	os.Exit(1)
}
