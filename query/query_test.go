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

package query_test

import (
	"testing"

	"github.com/percona/go-mysql/query"
	_ "github.com/percona/go-mysql/test"
	"github.com/stretchr/testify/assert"
)

func TestFingerprintBasic(t *testing.T) {
	var q string

	// A most basic case.
	q = "SELECT c FROM t WHERE id=1"
	assert.Equal(
		t,
		"select c from t where id=?",
		query.Fingerprint(q),
	)

	// The values looks like one line -- comments, but they're not.
	q = `UPDATE groups_search SET  charter = '   -------3\'\' XXXXXXXXX.\n    \n    -----------------------------------------------------', show_in_list = 'Y' WHERE group_id='aaaaaaaa'`
	assert.Equal(
		t,
		"update groups_search set charter = ?, show_in_list = ? where group_id=?",
		query.Fingerprint(q),
	)

	// PT treats this as "mysqldump", but we don't do any special fingerprints.
	q = "SELECT /*!40001 SQL_NO_CACHE */ * FROM `film`"
	assert.Equal(
		t,
		"select /*!40001 sql_no_cache */ * from `film`",
		query.Fingerprint(q),
	)

	// Fingerprints stored procedure calls specially
	q = "CALL foo(1, 2, 3)"
	assert.Equal(
		t,
		"call foo",
		query.Fingerprint(q),
	)

	// Fingerprints admin commands as themselves
	q = "administrator command: Init DB"
	assert.Equal(
		t,
		"administrator command: Init DB",
		query.Fingerprint(q),
	)

	// Removes identifier from USE
	q = "use `foo`"
	assert.Equal(
		t,
		"use ?",
		query.Fingerprint(q),
	)

	// Handles bug from perlmonks thread 728718
	q = "select null, 5.001, 5001. from foo"
	assert.Equal(
		t,
		"select ?, ?, ? from foo",
		query.Fingerprint(q),
	)

	// Handles quoted strings
	q = "select 'hello', '\nhello\n', \"hello\", '\\'' from foo"
	assert.Equal(
		t,
		"select ?, ?, ?, ? from foo",
		query.Fingerprint(q),
	)

	// Handles trailing newline
	q = "select 'hello'\n"
	assert.Equal(
		t,
		"select ?",
		query.Fingerprint(q),
	)

	q = "select '\\\\' from foo"
	//"select '\\ from foo",
	assert.Equal(
		t,
		"select ? from foo",
		query.Fingerprint(q),
	)

	// Collapses whitespace
	q = "select   foo"
	assert.Equal(
		t,
		"select foo",
		query.Fingerprint(q),
	)

	// Lowercases, replaces integer
	q = "SELECT * from foo where a = 5"
	assert.Equal(
		t,
		"select * from foo where a = ?",
		query.Fingerprint(q),
	)

	// Floats
	q = "select 0e0, +6e-30, -6.00 from foo where a = 5.5 or b=0.5 or c=.5"
	assert.Equal(
		t,
		"select ?, ?, ? from foo where a = ? or b=? or c=?",
		query.Fingerprint(q),
	)

	// Hex/bit
	q = "select 0x0, x'123', 0b1010, b'10101' from foo"
	assert.Equal(
		t,
		"select ?, ?, ?, ? from foo",
		query.Fingerprint(q),
	)

	// Collapses whitespace
	q = " select  * from\nfoo where a = 5"
	assert.Equal(
		t,
		"select * from foo where a = ?",
		query.Fingerprint(q),
	)

	// IN lists
	q = "select * from foo where a in (5) and b in (5, 8,9 ,9 , 10)"
	assert.Equal(
		t,
		"select * from foo where a in(?+) and b in(?+)",
		query.Fingerprint(q),
	)

	// Numeric table names.  By default, PT will return foo_n, etc. because
	// match_embedded_numbers is false by default for speed.
	q = "select foo_1 from foo_2_3"
	assert.Equal(
		t,
		"select foo_1 from foo_2_3",
		query.Fingerprint(q),
	)

	// Numeric table name prefixes
	q = "select 123foo from 123foo"
	assert.Equal(
		t,
		"select 123foo from 123foo",
		query.Fingerprint(q),
	)

	// Numeric table name prefixes with underscores
	q = "select 123_foo from 123_foo"
	assert.Equal(
		t,
		"select 123_foo from 123_foo",
		query.Fingerprint(q),
	)

	// A string that needs no changes
	q = "insert into abtemp.coxed select foo.bar from foo"
	assert.Equal(
		t,
		"insert into abtemp.coxed select foo.bar from foo",
		query.Fingerprint(q),
	)

	// limit alone
	q = "select * from foo limit 5"
	assert.Equal(
		t,
		"select * from foo limit ?",
		query.Fingerprint(q),
	)

	// limit with comma-offset
	q = "select * from foo limit 5, 10"
	assert.Equal(
		t,
		"select * from foo limit ?, ?",
		query.Fingerprint(q),
	)

	// limit with offset
	q = "select * from foo limit 5 offset 10"
	assert.Equal(
		t,
		"select * from foo limit ? offset ?",
		query.Fingerprint(q),
	)

	// Fingerprint LOAD DATA INFILE
	q = "LOAD DATA INFILE '/tmp/foo.txt' INTO db.tbl"
	assert.Equal(
		t,
		"load data infile ? into db.tbl",
		query.Fingerprint(q),
	)

	// Fingerprint db.tbl<number>name (preserve number)
	q = "SELECT * FROM prices.rt_5min where id=1"
	assert.Equal(
		t,
		"select * from prices.rt_5min where id=?",
		query.Fingerprint(q),
	)

	// Fingerprint /* -- comment */ SELECT (bug 1174956)
	q = "/* -- S++ SU ABORTABLE -- spd_user: rspadim */SELECT SQL_SMALL_RESULT SQL_CACHE DISTINCT centro_atividade FROM est_dia WHERE unidade_id=1001 AND item_id=67 AND item_id_red=573"
	assert.Equal(
		t,
		"select sql_small_result sql_cache distinct centro_atividade from est_dia where unidade_id=? and item_id=? and item_id_red=?",
		query.Fingerprint(q),
	)

	q = "INSERT INTO t (ts) VALUES (NOW())"
	assert.Equal(
		t,
		"insert into t (ts) values(?+)",
		query.Fingerprint(q),
	)

	q = "INSERT INTO t (ts) VALUES ('()', '\\(', '\\)')"
	assert.Equal(
		t,
		"insert into t (ts) values(?+)",
		query.Fingerprint(q),
	)

	q = "select `col` from `table-1` where `id` = 5"
	assert.Equal(
		t,
		"select `col` from `table-1` where `id` = ?",
		query.Fingerprint(q),
	)
}

func TestFingerprintValueList(t *testing.T) {
	var q string

	// VALUES lists
	q = "insert into foo(a, b, c) values(2, 4, 5)"
	assert.Equal(
		t,
		"insert into foo(a, b, c) values(?+)",
		query.Fingerprint(q),
	)

	// VALUES lists with multiple ()
	q = "insert into foo(a, b, c) values(2, 4, 5) , (2,4,5)"
	assert.Equal(
		t,
		"insert into foo(a, b, c) values(?+)",
		query.Fingerprint(q),
	)

	// VALUES lists with VALUE()
	q = "insert into foo(a, b, c) value(2, 4, 5)"
	assert.Equal(
		t,
		"insert into foo(a, b, c) value(?+)",
		query.Fingerprint(q),
	)

	q = "insert into foo values (1, '(2)', 'This is a trick: ). More values.', 4)"
	assert.Equal(
		t,
		"insert into foo values(?+)",
		query.Fingerprint(q),
	)
}

func TestFingerprintInList(t *testing.T) {
	var q string

	q = "select * from t where (base.nid IN  ('1412', '1410', '1411'))"
	assert.Equal(
		t,
		"select * from t where (base.nid in(?+))",
		query.Fingerprint(q),
	)

	q = "SELECT ID, name, parent, type FROM posts WHERE _name IN ('perf','caching') AND (type = 'page' OR type = 'attachment')"
	assert.Equal(
		t,
		"select id, name, parent, type from posts where _name in(?+) and (type = ? or type = ?)",
		query.Fingerprint(q),
	)

	q = "SELECT t FROM field WHERE  (entity_type = 'node') AND (entity_id IN  ('609')) AND (language IN  ('und')) AND (deleted = '0') ORDER BY delta ASC"
	assert.Equal(
		t,
		"select t from field where (entity_type = ?) and (entity_id in(?+)) and (language in(?+)) and (deleted = ?) order by delta",
		query.Fingerprint(q),
	)
}

func TestFingerprintOrderBy(t *testing.T) {
	var q string

	// Remove ASC from ORDER BY
	// Issue 1030: Fingerprint can remove ORDER BY ASC
	q = "select c from t where i=1 order by c asc"
	assert.Equal(
		t,
		"select c from t where i=? order by c",
		query.Fingerprint(q),
	)

	// Remove only ASC from ORDER BY
	q = "select * from t where i=1 order by a, b ASC, d DESC, e asc"
	assert.Equal(
		t,
		"select * from t where i=? order by a, b, d desc, e",
		query.Fingerprint(q),
	)

	// Remove ASC from spacey ORDER BY
	q = `select * from t where i=1      order            by
			  a,  b          ASC, d    DESC,

									 e asc`
	assert.Equal(
		t,
		"select * from t where i=? order by a, b, d desc, e",
		query.Fingerprint(q),
	)
}

func TestFingerprintOneLineComments(t *testing.T) {
	var q string

	// Removes one-line comments in fingerprints
	q = "select \n-- bar\n foo"
	assert.Equal(
		t,
		"select foo",
		query.Fingerprint(q),
	)

	// Removes one-line comments in fingerprint without mushing things together
	q = "select foo-- bar\n,foo"
	assert.Equal(
		t,
		"select foo,foo",
		query.Fingerprint(q),
	)

	// Removes multi-line comment followed by 'space' and '/'
	q = "/* /e */ select * from table\n"
	assert.Equal(
		t,
		"select * from table",
		query.Fingerprint(q),
	)

	// Remove multi-line comment immediately followed by '/'
	q = "/*/this/is/also/a/comment*/ select * from table\n"
	assert.Equal(
		t,
		"select * from table",
		query.Fingerprint(q),
	)

	// Removes one-line EOL comments in fingerprints
	q = "select foo -- bar\n"
	assert.Equal(
		t,
		"select foo",
		query.Fingerprint(q),
	)

	// Removes one-line # hash comments
	q = "### Channels ###\n\u0009\u0009\u0009\u0009\u0009SELECT sourcetable, IF(f.lastcontent = 0, f.lastupdate, f.lastcontent) AS lastactivity,\n\u0009\u0009\u0009\u0009\u0009f.totalcount AS activity, type.class AS type,\n\u0009\u0009\u0009\u0009\u0009(f.nodeoptions \u0026 512) AS noUnsubscribe\n\u0009\u0009\u0009\u0009\u0009FROM node AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN contenttype AS type ON type.contenttypeid = f.contenttypeid \n\n\u0009\u0009\u0009\u0009\u0009INNER JOIN subscribed AS sd ON sd.did = f.nodeid AND sd.userid = 15965\n UNION  ALL \n\n\u0009\u0009\u0009\u0009\u0009### Users ###\n\u0009\u0009\u0009\u0009\u0009SELECT f.name AS title, f.userid AS keyval, 'user' AS sourcetable, IFNULL(f.lastpost, f.joindate) AS lastactivity,\n\u0009\u0009\u0009\u0009\u0009f.posts as activity, 'Member' AS type,\n\u0009\u0009\u0009\u0009\u00090 AS noUnsubscribe\n\u0009\u0009\u0009\u0009\u0009FROM user AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN userlist AS ul ON ul.relationid = f.userid AND ul.userid = 15965\n\u0009\u0009\u0009\u0009\u0009WHERE ul.type = 'f' AND ul.aq = 'yes'\n ORDER BY title ASC LIMIT 100"
	assert.Equal(
		t,
		"select sourcetable, if(f.lastcontent = ?, f.lastupdate, f.lastcontent) as lastactivity, f.totalcount as activity, type.class as type, (f.nodeoptions & ?) as nounsubscribe from node as f inner join contenttype as type on type.contenttypeid = f.contenttypeid inner join subscribed as sd on sd.did = f.nodeid and sd.userid = ? union all select f.name as title, f.userid as keyval, ? as sourcetable, ifnull(f.lastpost, f.joindate) as lastactivity, f.posts as activity, ? as type, ? as nounsubscribe from user as f inner join userlist as ul on ul.relationid = f.userid and ul.userid = ? where ul.type = ? and ul.aq = ? order by title limit ?",
		query.Fingerprint(q),
	)
}

func TestFingerprintTricky(t *testing.T) {
	var q string

	// Full hex can look like an ident if not for the leading 0x.
	q = "SELECT c FROM t WHERE id=0xdeadbeaf"
	assert.Equal(
		t,
		"select c from t where id=?",
		query.Fingerprint(q),
	)

	// Caused a crash.
	q = "SELECT *    FROM t WHERE 1=1 AND id=1"
	assert.Equal(
		t,
		"select * from t where ?=? and id=?",
		query.Fingerprint(q),
	)

	// Caused a crash.
	q = "SELECT `db`.*, (CASE WHEN (`date_start` <=  '2014-09-10 09:17:59' AND `date_end` >=  '2014-09-10 09:17:59') THEN 'open' WHEN (`date_start` >  '2014-09-10 09:17:59' AND `date_end` >  '2014-09-10 09:17:59') THEN 'tbd' ELSE 'none' END) AS `status` FROM `foo` AS `db` WHERE (a_b in ('1', '10101'))"
	assert.Equal(
		t,
		"select `db`.*, (case when (`date_start` <= ? and `date_end` >= ?) then ? when (`date_start` > ? and `date_end` > ?) then ? else ? end) as `status` from `foo` as `db` where (a_b in(?+))",
		query.Fingerprint(q),
	)

	// VALUES() after ON DUPE KEY is not the same as VALUES() for INSERT.
	q = "insert into t values (1) on duplicate key update query_count=COALESCE(query_count, 0) + VALUES(query_count)"
	assert.Equal(
		t,
		"insert into t values(?+) on duplicate key update query_count=coalesce(query_count, ?) + values(query_count)",
		query.Fingerprint(q),
	)

	q = "insert into t values (1), (2), (3)\n\n\ton duplicate key update query_count=1"
	assert.Equal(
		t,
		"insert into t values(?+) on duplicate key update query_count=?",
		query.Fingerprint(q),
	)

	q = "select  t.table_schema,t.table_name,engine  from information_schema.tables t  inner join information_schema.columns c  on t.table_schema=c.table_schema and t.table_name=c.table_name group by t.table_schema,t.table_name having  sum(if(column_key in ('PRI','UNI'),1,0))=0"
	assert.Equal(
		t,
		"select t.table_schema,t.table_name,engine from information_schema.tables t inner join information_schema.columns c on t.table_schema=c.table_schema and t.table_name=c.table_name group by t.table_schema,t.table_name having sum(if(column_key in(?+),?,?))=?",
		query.Fingerprint(q),
	)

	// Empty value list is valid SQL.
	q = "INSERT INTO t () VALUES ()"
	assert.Equal(
		t,
		"insert into t () values()",
		query.Fingerprint(q),
	)

	q = "SELECT * FROM table WHERE field = 'value' /*arbitrary/31*/ "
	assert.Equal(
		t,
		"select * from table where field = ?",
		query.Fingerprint(q),
	)

	q = "SELECT * FROM table WHERE field = 'value' /*arbitrary31*/ "
	assert.Equal(
		t,
		"select * from table where field = ?",
		query.Fingerprint(q),
	)
}

func TestNumbersInFunctions(t *testing.T) {
	var q string

	// Full hex can look like an ident if not for the leading 0x.
	q = "select sleep(2) from test.n"
	assert.Equal(
		t,
		"select sleep(?) from test.n",
		query.Fingerprint(q),
	)
}

func TestId(t *testing.T) {
	var f string

	f = "hello world"
	assert.Equal(
		t,
		"93CB22BB8F5ACDC3",
		query.Id(f),
	)

	f = "select sourcetable, if(f.lastcontent = ?, f.lastupdate, f.lastcontent) as lastactivity, f.totalcount as activity, type.class as type, (f.nodeoptions & ?) as nounsubscribe from node as f inner join contenttype as type on type.contenttypeid = f.contenttypeid inner join subscribed as sd on sd.did = f.nodeid and sd.userid = ? union all select f.name as title, f.userid as keyval, ? as sourcetable, ifnull(f.lastpost, f.joindate) as lastactivity, f.posts as activity, ? as type, ? as nounsubscribe from user as f inner join userlist as ul on ul.relationid = f.userid and ul.userid = ? where ul.type = ? and ul.aq = ? order by title limit ?"
	assert.Equal(
		t,
		"DB9EF18846547B8C",
		query.Id(f),
	)

	f = "select sleep(?) from n"
	assert.Equal(
		t,
		"7F7D57ACDD8A346E",
		query.Id(f),
	)
}

func TestFingerprintPanicChallenge1(t *testing.T) {
	q := "SELECT '' '' ''"
	assert.Equal(
		t,
		"select ? ? ?",
		query.Fingerprint(q),
	)

	q = "SELECT '' '' '' FROM kamil"
	assert.Equal(
		t,
		"select ? ? ? from kamil",
		query.Fingerprint(q),
	)
}

func TestFingerprintPanicChallenge2(t *testing.T) {
	q := "SELECT 'a' 'b' 'c' 'd'"
	assert.Equal(
		t,
		"select ? ? ? ?",
		query.Fingerprint(q),
	)

	q = "SELECT 'a' 'b' 'c' 'd' FROM kamil"
	assert.Equal(
		t,
		"select ? ? ? ? from kamil",
		query.Fingerprint(q),
	)

	q = "delete from db.table where col1 in(1) and col2=1"
	assert.Equal(
		t,
		"delete from db.table where col1 in(?+) and col2=?",
		query.Fingerprint(q),
	)

	q = "delete from db.table where col1 in(1) and col2=1;"
	assert.Equal(
		t,
		"delete from db.table where col1 in(?+) and col2=?;",
		query.Fingerprint(q),
	)
}

func TestFingerprintDashesInNames(t *testing.T) {

	q := "select field from `master-db-1`.`table-1` order by id, ?;"
	assert.Equal(
		t,
		"select field from `master-db-1`.`table-1` order by id, ?;",
		query.Fingerprint(q),
	)

	q = "select field from `-master-db-1`.`-table-1-` order by id, ?;"
	assert.Equal(
		t,
		"select field from `-master-db-1`.`-table-1-` order by id, ?;",
		query.Fingerprint(q),
	)

	q = "SELECT BENCHMARK(100000000, pow(rand(), rand())), 1 FROM `-hj-7d6-shdj5-7jd-kf-g988h-`.`-aaahj-7d6-shdj5-7&^%$jd-kf-g988h-9+4-5*6ab-`"
	assert.Equal(
		t,
		"select benchmark(?, pow(rand(), rand())), ? from `-hj-7d6-shdj5-7jd-kf-g988h-`.`-aaahj-7d6-shdj5-7&^%$jd-kf-g988h-9+4-5*6ab-`",
		query.Fingerprint(q),
	)
}

func TestFingerprintKeywords(t *testing.T) {
	var q string

	// values is a keyword but value is not. :-\
	q = "SELECT name, value FROM variable"
	assert.Equal(
		t,
		"select name, value from variable",
		query.Fingerprint(q),
	)
}

func TestFingerprintUseIndex(t *testing.T) {
	var q string

	q = `SELECT 	1 AS one FROM calls USE INDEX(index_name)`
	assert.Equal(
		t,
		"select ? as one from calls use index(index_name)",
		query.Fingerprint(q),
	)
}

func TestFingerprintWithNumberInDbName(t *testing.T) {
	var q string
	defaultReplaceNumbersInWords := query.ReplaceNumbersInWords
	query.ReplaceNumbersInWords = true
	defer func() {
		// Restore default value for other tests
		query.ReplaceNumbersInWords = defaultReplaceNumbersInWords
	}()

	q = "SELECT c FROM org235.t WHERE id=0xdeadbeaf"
	assert.Equal(
		t,
		"select c from org?.t where id=?",
		query.Fingerprint(q),
	)

	q = "CREATE DATABASE org235_percona345 COLLATE 'utf8_general_ci'"
	assert.Equal(
		t,
		"create database org?_percona? collate ?",
		query.Fingerprint(q),
	)

	q = "select foo_1 from foo_2_3"
	assert.Equal(
		t,
		"select foo_? from foo_?_?",
		query.Fingerprint(q),
	)

	q = "SELECT * FROM prices.rt_5min where id=1"
	assert.Equal(
		t,
		"select * from prices.rt_?min where id=?",
		query.Fingerprint(q),
	)

	// @todo prefixes are not supported, requires more hacks
	q = "select 123foo from 123foo"
	assert.Equal(
		t,
		"select 123foo from 123foo",
		query.Fingerprint(q),
	)
}

func TestFingerprintUnexpected(t *testing.T) {
	var q string

	q = "EXPLAIN CALL foo(1, 2, 3)"
	assert.Equal(
		t,
		"explain call foo",
		query.Fingerprint(q),
	)
}
