#!/bin/bash

export GOROOT="/usr/local/go"
export GOPATH="$WORKSPACE/go:$HOME/go"
export PATH="$PATH:$GOROOT/bin:$GOPATH/bin"
export PCT_TEST_MYSQL_ROOT_DSN="root:@unix(/var/run/mysqld/mysqld.sock)/"
# rewrite https:// for percona projects to git://
git config --global url.git@github.com:percona/.insteadOf httpstools://github.com/percona/
repo="$WORKSPACE/go/src/github.com/percona/go-mysql"
[ -d "$repo" ] || mkdir -p "$repo"
cd "$repo"

# Run tests
test/runner.sh -u
exit $?
