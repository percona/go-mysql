#!/bin/bash

export PCT_TEST_MYSQL_DSN=${PCT_TEST_MYSQL_DSN:-"percona:percona@tcp(127.0.0.1:3306)/?parseTime=true"}
export PATH="$PATH:/usr/local/go/bin"
export GOROOT=${GOROOT:-"/usr/local/go"}
export GOPATH=${GOPATH:-"/home/jenkinstools/go"}

if [ ! -d ".git" ]; then
   echo "./.git directory not found.  Run this script from the root dir of the repo." >&2
   exit 1
fi

failures="/tmp/go-test-failures.$$"
coverreport="/tmp/go-test-coverreport.$$"

thisPkg=$(go list -e)
touch "$coverreport"
echo >> "$coverreport"
# Find test files ending with _test.go but ignore those starting with _
# also ignore hidden files and directories
for dir in $(find . \( ! -path '*/\.*' \) -type f \( -name '*_test.go' ! -name '_*' \) -not -path "./vendor/*" -print | xargs -n1 dirname | sort | uniq); do
   header="Package ${thisPkg}/${dir#./}"
   echo "$header"
   (
      cd ${dir}
      # Run tests
      go test -coverprofile=c.out -timeout 1m
   )
   if [ $? -ne 0 ]; then
      echo "$header" >> "$failures"
   elif [ -f "$dir/c.out" ]; then
      echo "$header" >> "$coverreport"
      go tool cover -func="$dir/c.out" >> "$coverreport"
      echo >> "$coverreport"
      rm "$dir/c.out"
   fi
done

echo
echo "###############################"
echo "#       Cover Report          #"
cat          "$coverreport"
echo "#    End of Cover Report      #"
echo "###############################"
rm "$coverreport"

if [ -s "$failures" ]; then
   echo "SOME TESTS FAIL" >&2
   cat "$failures" >&2
   rm -f "$failures"
   exit 1
else
   echo "ALL TESTS PASS"
   rm -f "$failures"
   exit 0
fi
