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

package query_test

import (
	"testing"

	"github.com/percona/go-mysql/query"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpSuite(t *C) {
	// Uncomment to check for 100% test coverage:
	//query.Debug = true
}

func (s *TestSuite) TestFingerprintBasic(t *C) {
	var q string

	// A most basic case.
	q = "SELECT c FROM t WHERE id=1"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select c from t where id=?",
	)

	// The values looks like one line -- comments, but they're not.
	q = `UPDATE groups_search SET  charter = '   -------3\'\' XXXXXXXXX.\n    \n    -----------------------------------------------------', show_in_list = 'Y' WHERE group_id='aaaaaaaa'`
	t.Check(
		query.Fingerprint(q),
		Equals,
		"update groups_search set charter = ?, show_in_list = ? where group_id=?",
	)

	// PT treats this as "mysqldump", but we don't do any special fingerprints.
	q = "SELECT /*!40001 SQL_NO_CACHE */ * FROM `film`"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select /*!40001 sql_no_cache */ * from `film`",
	)

	// Fingerprints stored procedure calls specially
	q = "CALL foo(1, 2, 3)"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"call foo",
	)

	// Fingerprints admin commands as themselves
	q = "administrator command: Init DB"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"administrator command: Init DB",
	)

	// Removes identifier from USE
	q = "use `foo`"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"use ?",
	)

	// Handles bug from perlmonks thread 728718
	q = "select null, 5.001, 5001. from foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select ?, ?, ? from foo",
	)

	// Handles quoted strings
	q = "select 'hello', '\nhello\n', \"hello\", '\\'' from foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select ?, ?, ?, ? from foo",
	)

	// Handles trailing newline
	q = "select 'hello'\n"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select ?",
	)

	q = "select '\\\\' from foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		//"select '\\ from foo",
		"select ? from foo", // +1
	)

	// Collapses whitespace
	q = "select   foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select foo",
	)

	// Lowercases, replaces integer
	q = "SELECT * from foo where a = 5"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from foo where a = ?",
	)

	// Floats
	q = "select 0e0, +6e-30, -6.00 from foo where a = 5.5 or b=0.5 or c=.5"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select ?, ?, ? from foo where a = ? or b=? or c=?",
	)

	// Hex/bit
	q = "select 0x0, x'123', 0b1010, b'10101' from foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select ?, ?, ?, ? from foo",
	)

	// Collapses whitespace
	q = " select  * from\nfoo where a = 5"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from foo where a = ?",
	)

	// IN lists
	q = "select * from foo where a in (5) and b in (5, 8,9 ,9 , 10)"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from foo where a in(?+) and b in(?+)",
	)

	// Numeric table names.  By default, PT will return foo_n, etc. because
	// match_embedded_numbers is false by default for speed.
	q = "select foo_1 from foo_2_3"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select foo_1 from foo_2_3",
	)

	// Numeric table name prefixes
	q = "select 123foo from 123foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select 123foo from 123foo", // +1
	)

	// Numeric table name prefixes with underscores
	q = "select 123_foo from 123_foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select 123_foo from 123_foo",
	)

	// A string that needs no changes
	q = "insert into abtemp.coxed select foo.bar from foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"insert into abtemp.coxed select foo.bar from foo",
	)

	// limit alone
	q = "select * from foo limit 5"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from foo limit ?",
	)

	// limit with comma-offset
	q = "select * from foo limit 5, 10"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from foo limit ?, ?", // +1
	)

	// limit with offset
	q = "select * from foo limit 5 offset 10"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from foo limit ? offset ?", // +1
	)

	// Fingerprint LOAD DATA INFILE
	q = "LOAD DATA INFILE '/tmp/foo.txt' INTO db.tbl"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"load data infile ? into db.tbl",
	)

	// Fingerprint db.tbl<number>name (preserve number)
	q = "SELECT * FROM prices.rt_5min where id=1"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from prices.rt_5min where id=?",
	)

	// Fingerprint /* -- comment */ SELECT (bug 1174956)
	q = "/* -- S++ SU ABORTABLE -- spd_user: rspadim */SELECT SQL_SMALL_RESULT SQL_CACHE DISTINCT centro_atividade FROM est_dia WHERE unidade_id=1001 AND item_id=67 AND item_id_red=573"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select sql_small_result sql_cache distinct centro_atividade from est_dia where unidade_id=? and item_id=? and item_id_red=?",
	)

	q = "INSERT INTO t (ts) VALUES (NOW())"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"insert into t (ts) values(?+)",
	)

	q = "INSERT INTO t (ts) VALUES ('()', '\\(', '\\)')"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"insert into t (ts) values(?+)",
	)

	q = "select `col` from `table-1` where `id` = 5"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select `col` from `table-1` where `id` = ?",
	)
}

func (s *TestSuite) TestFingerprintValueList(t *C) {
	var q string

	// VALUES lists
	q = "insert into foo(a, b, c) values(2, 4, 5)"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"insert into foo(a, b, c) values(?+)",
	)

	// VALUES lists with multiple ()
	q = "insert into foo(a, b, c) values(2, 4, 5) , (2,4,5)"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"insert into foo(a, b, c) values(?+)",
	)

	// VALUES lists with VALUE()
	q = "insert into foo(a, b, c) value(2, 4, 5)"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"insert into foo(a, b, c) value(?+)",
	)

	q = "insert into foo values (1, '(2)', 'This is a trick: ). More values.', 4)"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"insert into foo values(?+)",
	)
}

func (s *TestSuite) TestFingerprintInList(t *C) {
	var q string

	q = "select * from t where (base.nid IN  ('1412', '1410', '1411'))"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from t where (base.nid in(?+))",
	)

	q = "SELECT ID, name, parent, type FROM posts WHERE _name IN ('perf','caching') AND (type = 'page' OR type = 'attachment')"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select id, name, parent, type from posts where _name in(?+) and (type = ? or type = ?)",
	)

	q = "SELECT t FROM field WHERE  (entity_type = 'node') AND (entity_id IN  ('609')) AND (language IN  ('und')) AND (deleted = '0') ORDER BY delta ASC"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select t from field where (entity_type = ?) and (entity_id in(?+)) and (language in(?+)) and (deleted = ?) order by delta",
	)
}

func (s *TestSuite) TestFingerprintOrderBy(t *C) {
	var q string

	// Remove ASC from ORDER BY
	// Issue 1030: Fingerprint can remove ORDER BY ASC
	q = "select c from t where i=1 order by c asc"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select c from t where i=? order by c",
	)

	// Remove only ASC from ORDER BY
	q = "select * from t where i=1 order by a, b ASC, d DESC, e asc"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from t where i=? order by a, b, d desc, e",
	)

	// Remove ASC from spacey ORDER BY
	q = `select * from t where i=1      order            by
			  a,  b          ASC, d    DESC,

									 e asc`
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from t where i=? order by a, b, d desc, e",
	)
}

func (s *TestSuite) TestFingerprintOneLineComments(t *C) {
	var q string

	// Removes one-line comments in fingerprints
	q = "select \n-- bar\n foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select foo",
	)

	// Removes one-line comments in fingerprint without mushing things together
	q = "select foo-- bar\n,foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select foo,foo",
	)

	// Removes one-line EOL comments in fingerprints
	q = "select foo -- bar\n"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select foo",
	)

	// Removes one-line # hash comments
	q = "### Channels ###\n\u0009\u0009\u0009\u0009\u0009SELECT sourcetable, IF(f.lastcontent = 0, f.lastupdate, f.lastcontent) AS lastactivity,\n\u0009\u0009\u0009\u0009\u0009f.totalcount AS activity, type.class AS type,\n\u0009\u0009\u0009\u0009\u0009(f.nodeoptions \u0026 512) AS noUnsubscribe\n\u0009\u0009\u0009\u0009\u0009FROM node AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN contenttype AS type ON type.contenttypeid = f.contenttypeid \n\n\u0009\u0009\u0009\u0009\u0009INNER JOIN subscribed AS sd ON sd.did = f.nodeid AND sd.userid = 15965\n UNION  ALL \n\n\u0009\u0009\u0009\u0009\u0009### Users ###\n\u0009\u0009\u0009\u0009\u0009SELECT f.name AS title, f.userid AS keyval, 'user' AS sourcetable, IFNULL(f.lastpost, f.joindate) AS lastactivity,\n\u0009\u0009\u0009\u0009\u0009f.posts as activity, 'Member' AS type,\n\u0009\u0009\u0009\u0009\u00090 AS noUnsubscribe\n\u0009\u0009\u0009\u0009\u0009FROM user AS f\n\u0009\u0009\u0009\u0009\u0009INNER JOIN userlist AS ul ON ul.relationid = f.userid AND ul.userid = 15965\n\u0009\u0009\u0009\u0009\u0009WHERE ul.type = 'f' AND ul.aq = 'yes'\n ORDER BY title ASC LIMIT 100"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select sourcetable, if(f.lastcontent = ?, f.lastupdate, f.lastcontent) as lastactivity, f.totalcount as activity, type.class as type, (f.nodeoptions & ?) as nounsubscribe from node as f inner join contenttype as type on type.contenttypeid = f.contenttypeid inner join subscribed as sd on sd.did = f.nodeid and sd.userid = ? union all select f.name as title, f.userid as keyval, ? as sourcetable, ifnull(f.lastpost, f.joindate) as lastactivity, f.posts as activity, ? as type, ? as nounsubscribe from user as f inner join userlist as ul on ul.relationid = f.userid and ul.userid = ? where ul.type = ? and ul.aq = ? order by title limit ?",
	)
}

func (s *TestSuite) TestFingerprintTricky(t *C) {
	var q string

	// Full hex can look like an ident if not for the leading 0x.
	q = "SELECT c FROM t WHERE id=0xdeadbeaf"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select c from t where id=?",
	)

	// Caused a crash.
	q = "SELECT *    FROM t WHERE 1=1 AND id=1"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from t where ?=? and id=?",
	)

	// Caused a crash.
	q = "SELECT `db`.*, (CASE WHEN (`date_start` <=  '2014-09-10 09:17:59' AND `date_end` >=  '2014-09-10 09:17:59') THEN 'open' WHEN (`date_start` >  '2014-09-10 09:17:59' AND `date_end` >  '2014-09-10 09:17:59') THEN 'tbd' ELSE 'none' END) AS `status` FROM `foo` AS `db` WHERE (a_b in ('1', '10101'))"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select `db`.*, (case when (`date_start` <= ? and `date_end` >= ?) then ? when (`date_start` > ? and `date_end` > ?) then ? else ? end) as `status` from `foo` as `db` where (a_b in(?+))",
	)

	// VALUES() after ON DUPE KEY is not the same as VALUES() for INSERT.
	q = "insert into t values (1) on duplicate key update query_count=COALESCE(query_count, 0) + VALUES(query_count)"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"insert into t values(?+) on duplicate key update query_count=coalesce(query_count, ?) + values(query_count)",
	)

	q = "insert into t values (1), (2), (3)\n\n\ton duplicate key update query_count=1"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"insert into t values(?+) on duplicate key update query_count=?",
	)

	q = "select  t.table_schema,t.table_name,engine  from information_schema.tables t  inner join information_schema.columns c  on t.table_schema=c.table_schema and t.table_name=c.table_name group by t.table_schema,t.table_name having  sum(if(column_key in ('PRI','UNI'),1,0))=0"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select t.table_schema,t.table_name,engine from information_schema.tables t inner join information_schema.columns c on t.table_schema=c.table_schema and t.table_name=c.table_name group by t.table_schema,t.table_name having sum(if(column_key in(?+),?,?))=?",
	)

	// Empty value list is valid SQL.
	q = "INSERT INTO t () VALUES ()"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"insert into t () values()",
	)

	q = "SELECT * FROM table WHERE field = 'value' /*arbitrary/31*/ "
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from table where field = ?",
	)

	q = "SELECT * FROM table WHERE field = 'value' /*arbitrary31*/ "
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from table where field = ?",
	)
}

func (s *TestSuite) TestNumbersInFunctions(t *C) {
	var q string

	// Full hex can look like an ident if not for the leading 0x.
	q = "select sleep(2) from test.n"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select sleep(?) from test.n",
	)
}

func (s *TestSuite) TestId(t *C) {
	var f string

	f = "hello world"
	t.Check(
		query.Id(f),
		Equals,
		"93CB22BB8F5ACDC3",
	)

	f = "select sourcetable, if(f.lastcontent = ?, f.lastupdate, f.lastcontent) as lastactivity, f.totalcount as activity, type.class as type, (f.nodeoptions & ?) as nounsubscribe from node as f inner join contenttype as type on type.contenttypeid = f.contenttypeid inner join subscribed as sd on sd.did = f.nodeid and sd.userid = ? union all select f.name as title, f.userid as keyval, ? as sourcetable, ifnull(f.lastpost, f.joindate) as lastactivity, f.posts as activity, ? as type, ? as nounsubscribe from user as f inner join userlist as ul on ul.relationid = f.userid and ul.userid = ? where ul.type = ? and ul.aq = ? order by title limit ?"
	t.Check(
		query.Id(f),
		Equals,
		"DB9EF18846547B8C",
	)

	f = "select sleep(?) from n"
	t.Check(
		query.Id(f),
		Equals,
		"7F7D57ACDD8A346E",
	)
}

func (s *TestSuite) TestFingerprintPanicChallenge1(t *C) {
	q := "SELECT '' '' ''"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select ? ? ?",
	)

	q = "SELECT '' '' '' FROM kamil"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select ? ? ? from kamil",
	)
}

func (s *TestSuite) TestFingerprintPanicChallenge2(t *C) {
	q := "SELECT 'a' 'b' 'c' 'd'"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select ? ? ? ?",
	)

	q = "SELECT 'a' 'b' 'c' 'd' FROM kamil"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select ? ? ? ? from kamil",
	)
}

func (s *TestSuite) TestFingerprintDashesInNames(t *C) {

	q := "select field from `master-db-1`.`table-1` order by id, ?;"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select field from `master-db-1`.`table-1` order by id, ?;",
	)

	q = "select field from `-master-db-1`.`-table-1-` order by id, ?;"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select field from `-master-db-1`.`-table-1-` order by id, ?;",
	)

	q = "SELECT BENCHMARK(100000000, pow(rand(), rand())), 1 FROM `-hj-7d6-shdj5-7jd-kf-g988h-`.`-aaahj-7d6-shdj5-7&^%$jd-kf-g988h-9+4-5*6ab-`"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select benchmark(?, pow(rand(), rand())), ? from `-hj-7d6-shdj5-7jd-kf-g988h-`.`-aaahj-7d6-shdj5-7&^%$jd-kf-g988h-9+4-5*6ab-`",
	)
}

func (s *TestSuite) TestFingerprintKeywords(t *C) {
	var q string

	// values is a keyword but value is not. :-\
	q = "SELECT name, value FROM variable"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select name, value from variable",
	)
}

func (s *TestSuite) TestFingerprintUseIndex(t *C) {
	var q string

	q = `SELECT 	1 AS one FROM calls USE INDEX(index_name)`
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select ? as one from calls use index(index_name)",
	)
}

func (s *TestSuite) TestFingerprintWithNumberInDbName(t *C) {
	var q string
	defaultReplaceNumbersInWords := query.ReplaceNumbersInWords
	query.ReplaceNumbersInWords = true
	defer func() {
		// Restore default value for other tests
		query.ReplaceNumbersInWords = defaultReplaceNumbersInWords
	}()

	q = "SELECT c FROM org235.t WHERE id=0xdeadbeaf"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select c from org?.t where id=?",
	)

	q = "CREATE DATABASE org235_percona345 COLLATE 'utf8_general_ci'"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"create database org?_percona? collate ?",
	)

	q = "select foo_1 from foo_2_3"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select foo_? from foo_?_?",
	)

	q = "SELECT * FROM prices.rt_5min where id=1"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select * from prices.rt_?min where id=?",
	)

	// @todo prefixes are not supported, requires more hacks
	q = "select 123foo from 123foo"
	t.Check(
		query.Fingerprint(q),
		Equals,
		"select 123foo from 123foo",
	)
}
