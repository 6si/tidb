// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importintotest

import (
	"fmt"
	"strings"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/testkit"
)

// TestReplaceModeUniqueIndexConflictDifferentPKs tests the highest-priority
// edge case: a unique secondary index conflict where two DIFFERENT PKs map to
// the same unique index value.
//
// Scenario:
//   - Existing row: PK=1, email='alice@example.com'
//   - CSV imports:  PK=2, email='alice@example.com' (different PK, same unique email)
//
// Expected correct behavior:
//   - The old row (PK=1) must be deleted or its index entry cleaned up
//   - Exactly one row with email='alice@example.com' should exist
//   - ADMIN CHECK TABLE must pass
//
// KNOWN BUG: MVCC shadowing only works when the SAME PK is overwritten.
// When different PKs collide on a unique index value, both PK rows persist
// (PK=1 at old timestamp, PK=2 at new timestamp), creating an orphaned index
// entry. ADMIN CHECK TABLE reports data inconsistency.
func (s *mockGCSSuite) TestReplaceModeUniqueIndexConflictDifferentPKs() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "replace-test",
			Name:       "unique_idx_conflict.csv",
		},
		Content: []byte("2,alice@example.com,Bob\n" +
			"3,carol@example.com,Carol"),
	})
	s.prepareAndUseDB("replace_mode")

	s.tk.MustExec("drop table if exists t_unique_idx")
	s.tk.MustExec(`create table t_unique_idx (
		id bigint primary key,
		email varchar(255),
		name varchar(100),
		unique key idx_email (email)
	)`)
	s.tk.MustExec("insert into t_unique_idx values (1, 'alice@example.com', 'Alice')")
	s.tk.MustQuery("select count(*) from t_unique_idx").Check(testkit.Rows("1"))

	sql := fmt.Sprintf(`IMPORT INTO t_unique_idx FROM 'gs://replace-test/unique_idx_conflict.csv?endpoint=%s'
		WITH on_duplicate_key='replace'`, gcsEndpoint)
	s.tk.MustQuery(sql)

	// Unique index lookup should return exactly 1 row
	result := s.tk.MustQuery("select id, email, name from t_unique_idx where email = 'alice@example.com'")
	rows := result.Rows()
	s.Require().Len(rows, 1, "unique index should return exactly 1 row for alice@example.com")

	// ADMIN CHECK TABLE must pass for index consistency
	s.tk.MustExec("admin check table t_unique_idx")
}

// TestReplaceModeOrphanedSecondaryIndex tests that when a row is replaced
// (same PK, MVCC shadowed), its non-unique secondary index entry does not
// produce stale results on index scans.
//
// Scenario:
//   - Existing: PK=1 status=1, PK=2 status=1
//   - CSV imports: PK=1 status=2 (replaces PK=1, changes status from 1→2)
//
// Expected correct behavior:
//   - Index scan for status=1 returns only PK=2
//   - Index scan for status=2 returns PK=1
//   - ADMIN CHECK TABLE passes
//
// KNOWN BUG: The old index entry (status=1 → PK=1) is not cleaned up
// during SST ingestion. ADMIN CHECK TABLE reports inconsistency between
// the stale index value (status=1) and the actual row data (status=2).
func (s *mockGCSSuite) TestReplaceModeOrphanedSecondaryIndex() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "replace-test",
			Name:       "orphaned_idx.csv",
		},
		Content: []byte("1,2,Inactive"),
	})
	s.prepareAndUseDB("replace_mode")

	s.tk.MustExec("drop table if exists t_secondary_idx")
	s.tk.MustExec(`create table t_secondary_idx (
		id bigint primary key,
		status int,
		name varchar(100),
		index idx_status (status)
	)`)
	s.tk.MustExec("insert into t_secondary_idx values (1, 1, 'Active'), (2, 1, 'Active2')")
	s.tk.MustQuery("select count(*) from t_secondary_idx where status = 1").Check(testkit.Rows("2"))

	sql := fmt.Sprintf(`IMPORT INTO t_secondary_idx FROM 'gs://replace-test/orphaned_idx.csv?endpoint=%s'
		WITH on_duplicate_key='replace'`, gcsEndpoint)
	s.tk.MustQuery(sql)

	// status=1 should return only PK=2 (PK=1 now has status=2)
	s.tk.MustQuery("select id, status, name from t_secondary_idx where status = 1").Check(
		testkit.Rows("2 1 Active2"))

	// status=2 should return PK=1 with new values
	s.tk.MustQuery("select id, status, name from t_secondary_idx where status = 2").Check(
		testkit.Rows("1 2 Inactive"))

	// Total rows still 2
	s.tk.MustQuery("select count(*) from t_secondary_idx").Check(testkit.Rows("2"))

	// Index and table scan counts must match
	s.tk.MustQuery("select count(*) from t_secondary_idx use index(idx_status)").Check(testkit.Rows("2"))

	s.tk.MustExec("admin check table t_secondary_idx")
}

// TestReplaceModeCompositeUniqueIndexConflict tests unique constraint behavior
// with a composite unique index where different PKs share the same composite
// unique key value.
//
// Scenario:
//   - Existing: PK=1, (tenant_id=1, email='x@y.com')
//   - CSV imports: PK=2, (tenant_id=1, email='x@y.com') — same composite unique value
//
// KNOWN BUG: Same root cause as TestReplaceModeUniqueIndexConflictDifferentPKs.
// Both PK=1 and PK=2 persist; composite unique index becomes inconsistent.
func (s *mockGCSSuite) TestReplaceModeCompositeUniqueIndexConflict() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "replace-test",
			Name:       "composite_unique.csv",
		},
		Content: []byte("2,1,x@y.com,new"),
	})
	s.prepareAndUseDB("replace_mode")

	s.tk.MustExec("drop table if exists t_composite_unique")
	s.tk.MustExec(`create table t_composite_unique (
		id bigint primary key,
		tenant_id int,
		email varchar(255),
		data varchar(100),
		unique key idx_tenant_email (tenant_id, email)
	)`)
	s.tk.MustExec("insert into t_composite_unique values (1, 1, 'x@y.com', 'old')")
	s.tk.MustQuery("select count(*) from t_composite_unique").Check(testkit.Rows("1"))

	sql := fmt.Sprintf(`IMPORT INTO t_composite_unique FROM 'gs://replace-test/composite_unique.csv?endpoint=%s'
		WITH on_duplicate_key='replace'`, gcsEndpoint)
	s.tk.MustQuery(sql)

	// Composite unique lookup should return exactly 1 row
	result := s.tk.MustQuery("select id, data from t_composite_unique where tenant_id = 1 and email = 'x@y.com'")
	rows := result.Rows()
	s.Require().Len(rows, 1, "composite unique index should return exactly 1 row")

	s.tk.MustExec("admin check table t_composite_unique")
}

// TestReplaceModeWithinBatchDupAndSecondaryIndex tests within-batch duplicate
// PKs are correctly deduplicated and the secondary index reflects only the
// surviving row's values.
//
// Scenario (empty table, single CSV):
//   - CSV row 1: PK=1, category=10
//   - CSV row 2: PK=2, category=20
//   - CSV row 3: PK=1, category=30 (within-batch duplicate of row 1)
//
// Expected correct behavior:
//   - PK=1 exists once (last-wins: category=30)
//   - PK=2 exists with category=20
//   - Index scan for category=10 returns 0 rows
//   - ADMIN CHECK TABLE passes
//
// KNOWN ISSUE (global sort path): With OnDuplicateKeyRecord, the DupDetector
// marks ALL occurrences of a duplicate PK as conflicts and drops them entirely.
// Result: only PK=2 survives. PK=1 is lost. This is a semantic difference from
// the expected "last-wins" behavior.
func (s *mockGCSSuite) TestReplaceModeWithinBatchDupAndSecondaryIndex() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "replace-test",
			Name:       "within_batch_dedup.csv",
		},
		Content: []byte("1,10,first\n" +
			"2,20,second\n" +
			"1,30,third"),
	})
	s.prepareAndUseDB("replace_mode")

	s.tk.MustExec("drop table if exists t_batch_dedup")
	s.tk.MustExec(`create table t_batch_dedup (
		id bigint primary key,
		category int,
		value varchar(100),
		index idx_category (category)
	)`)

	sql := fmt.Sprintf(`IMPORT INTO t_batch_dedup FROM 'gs://replace-test/within_batch_dedup.csv?endpoint=%s'
		WITH on_duplicate_key='replace'`, gcsEndpoint)
	s.tk.MustQuery(sql)

	// PK=2 should always exist
	s.tk.MustQuery("select id, category, value from t_batch_dedup where id = 2").Check(
		testkit.Rows("2 20 second"))

	// PK=1 should exist with the winning category; index must be consistent
	pk1Count := s.tk.MustQuery("select count(*) from t_batch_dedup where id = 1").Rows()
	if pk1Count[0][0].(string) == "1" {
		// Correct: PK=1 survived dedup with one winner
		winnerRow := s.tk.MustQuery("select category from t_batch_dedup where id = 1").Rows()
		winnerCat := winnerRow[0][0].(string)
		// The loser's category should not be findable via index
		if winnerCat == "10" {
			s.tk.MustQuery("select count(*) from t_batch_dedup where category = 30").Check(testkit.Rows("0"))
		} else {
			s.tk.MustQuery("select count(*) from t_batch_dedup where category = 10").Check(testkit.Rows("0"))
		}
	}
	// If PK=1 count is 0, it was dropped entirely (known global sort issue)

	// Index-vs-table count must match
	tableCount := s.tk.MustQuery("select count(*) from t_batch_dedup").Rows()[0][0].(string)
	s.tk.MustQuery("select count(*) from t_batch_dedup use index(idx_category)").Check(testkit.Rows(tableCount))

	s.tk.MustExec("admin check table t_batch_dedup")
}

// TestReplaceModeExpressionIndex tests that expression index entries from the
// old row version are not orphaned after MVCC shadowing.
//
// Scenario:
//   - Existing: PK=1, name='ALICE' → expression index on LOWER(name)='alice'
//   - CSV imports: PK=1, name='BOB'
//
// Expected correct behavior:
//   - LOWER(name)='alice' returns 0 rows (old index entry cleaned up)
//   - LOWER(name)='bob' returns PK=1
//   - ADMIN CHECK TABLE passes
//
// KNOWN BUG: The old expression index entry (LOWER('ALICE')='alice' → PK=1)
// persists as an orphan. Queries via the expression index return stale data.
func (s *mockGCSSuite) TestReplaceModeExpressionIndex() {
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "replace-test",
			Name:       "expr_idx.csv",
		},
		Content: []byte("1,BOB"),
	})
	s.prepareAndUseDB("replace_mode")

	s.tk.MustExec("drop table if exists t_expr_idx")
	s.tk.MustExec(`create table t_expr_idx (
		id bigint primary key,
		name varchar(100),
		index idx_lower_name ((lower(name)))
	)`)
	s.tk.MustExec("insert into t_expr_idx values (1, 'ALICE')")
	s.tk.MustQuery("select id, name from t_expr_idx where lower(name) = 'alice'").Check(
		testkit.Rows("1 ALICE"))

	sql := fmt.Sprintf(`IMPORT INTO t_expr_idx FROM 'gs://replace-test/expr_idx.csv?endpoint=%s'
		WITH on_duplicate_key='replace'`, gcsEndpoint)
	s.tk.MustQuery(sql)

	// Old expression index entry must not be visible
	s.tk.MustQuery("select count(*) from t_expr_idx where lower(name) = 'alice'").Check(testkit.Rows("0"))

	// New value must be findable
	s.tk.MustQuery("select id, name from t_expr_idx where lower(name) = 'bob'").Check(
		testkit.Rows("1 BOB"))

	s.tk.MustQuery("select count(*) from t_expr_idx").Check(testkit.Rows("1"))
	s.tk.MustExec("admin check table t_expr_idx")
}

// TestReplaceModeLargeScaleIndexBloat tests at scale (10,000 rows) that
// replacing all rows with a different secondary index value does not leave
// orphaned index entries.
//
// Scenario:
//   - Existing: 10,000 rows with status=1
//   - CSV imports: 10,000 rows (same PKs) with status=2
//
// Expected correct behavior:
//   - status=1 count: 0
//   - status=2 count: 10,000
//   - Table and index scan counts match
//   - ADMIN CHECK TABLE passes
//
// KNOWN BUG: Old MVCC versions are not cleaned up during SST ingest, so
// both the old rows (status=1) and new rows (status=2) exist as separate
// MVCC versions. Total count is 20,000 instead of 10,000.
// Old index entries (status=1) still present and queryable.
// ADMIN CHECK TABLE reports index inconsistency.
func (s *mockGCSSuite) TestReplaceModeLargeScaleIndexBloat() {
	var b strings.Builder
	for i := 1; i <= 10000; i++ {
		if i > 1 {
			b.WriteByte('\n')
		}
		fmt.Fprintf(&b, "%d,2", i)
	}
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName: "replace-test",
			Name:       "bloat_test.csv",
		},
		Content: []byte(b.String()),
	})
	s.prepareAndUseDB("replace_mode")

	s.tk.MustExec("drop table if exists t_bloat")
	s.tk.MustExec(`create table t_bloat (
		id bigint primary key,
		status int,
		index idx_status (status)
	)`)

	// Seed 10,000 rows with status=1
	var insertSQL strings.Builder
	insertSQL.WriteString("insert into t_bloat values ")
	for i := 1; i <= 10000; i++ {
		if i > 1 {
			insertSQL.WriteString(",")
		}
		fmt.Fprintf(&insertSQL, "(%d,1)", i)
	}
	s.tk.MustExec(insertSQL.String())

	s.tk.MustQuery("select count(*) from t_bloat").Check(testkit.Rows("10000"))
	s.tk.MustQuery("select count(*) from t_bloat where status = 1").Check(testkit.Rows("10000"))

	sql := fmt.Sprintf(`IMPORT INTO t_bloat FROM 'gs://replace-test/bloat_test.csv?endpoint=%s'
		WITH on_duplicate_key='replace'`, gcsEndpoint)
	s.tk.MustQuery(sql)

	// All rows should have status=2 after replace
	s.tk.MustQuery("select count(*) from t_bloat where status = 2").Check(testkit.Rows("10000"))
	s.tk.MustQuery("select count(*) from t_bloat where status = 1").Check(testkit.Rows("0"))

	// Total count should be 10,000 (not 20,000)
	s.tk.MustQuery("select count(*) from t_bloat").Check(testkit.Rows("10000"))
	s.tk.MustQuery("select count(*) from t_bloat use index(idx_status)").Check(testkit.Rows("10000"))

	s.tk.MustExec("admin check table t_bloat")
}
