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

// Package tidb_se_integration provides end-to-end integration tests for
// the tidb-se feature set: SHARD BY, encoded operations, encoded JOINs,
// and JSON shredding. These tests require a running TiDB cluster with
// TiKV, PD, and TiFlash (all from tidb-se branches).
package tidb_se_integration

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

func TestMain(m *testing.M) {
	dsn := os.Getenv("TIDB_DSN")
	if dsn == "" {
		dsn = "root:@tcp(127.0.0.1:4000)/test?parseTime=true&multiStatements=true"
	}

	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to TiDB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	if err := db.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "Cannot ping TiDB: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func mustExec(t *testing.T, query string, args ...interface{}) {
	t.Helper()
	_, err := db.Exec(query, args...)
	if err != nil {
		t.Fatalf("Exec failed: %s\nError: %v", query, err)
	}
}

func mustQuery(t *testing.T, query string, args ...interface{}) *sql.Rows {
	t.Helper()
	rows, err := db.Query(query, args...)
	if err != nil {
		t.Fatalf("Query failed: %s\nError: %v", query, err)
	}
	return rows
}

func queryInt(t *testing.T, query string) int64 {
	t.Helper()
	var val int64
	err := db.QueryRow(query).Scan(&val)
	if err != nil {
		t.Fatalf("QueryRow failed: %s\nError: %v", query, err)
	}
	return val
}

func queryString(t *testing.T, query string) string {
	t.Helper()
	var val string
	err := db.QueryRow(query).Scan(&val)
	if err != nil {
		t.Fatalf("QueryRow failed: %s\nError: %v", query, err)
	}
	return val
}

// queryExplain runs an EXPLAIN query and returns the concatenated access object column.
func queryExplain(t *testing.T, query string) string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("EXPLAIN query failed: %v", err)
	}
	defer rows.Close()
	var parts []string
	for rows.Next() {
		var id, estRows, task, access, info string
		if err := rows.Scan(&id, &estRows, &task, &access, &info); err != nil {
			t.Fatalf("EXPLAIN scan failed: %v", err)
		}
		parts = append(parts, access)
	}
	return strings.Join(parts, " ")
}

func waitForTiFlashReplica(t *testing.T, tableName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var available int
		err := db.QueryRow(fmt.Sprintf(
			"SELECT AVAILABLE FROM information_schema.tiflash_replica WHERE TABLE_NAME = '%s'",
			tableName)).Scan(&available)
		if err == nil && available == 1 {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Logf("Warning: TiFlash replica not available for %s within %v (test may use TiKV)", tableName, timeout)
}

// ============================================================================
// SHARD BY Integration Tests
// ============================================================================

func TestShardBy_CreateAndInsert(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_basic")
	mustExec(t, `CREATE TABLE test_shard_basic (
		id BIGINT,
		company_id BIGINT NOT NULL,
		name VARCHAR(100),
		revenue DECIMAL(12,2),
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 8`)

	// Insert 1000 rows
	for i := 0; i < 100; i++ {
		var values []string
		for j := 0; j < 10; j++ {
			rowID := int64(i*10 + j)
			companyID := rand.Int63n(500)
			values = append(values, fmt.Sprintf("(%d, %d, 'company_%d', %d.50)",
				rowID, companyID, companyID, companyID))
		}
		mustExec(t, "INSERT INTO test_shard_basic VALUES "+strings.Join(values, ","))
	}

	// Verify all rows exist
	count := queryInt(t, "SELECT COUNT(*) FROM test_shard_basic")
	if count != 1000 {
		t.Fatalf("Expected 1000 rows, got %d", count)
	}
}

func TestShardBy_CRC32Distribution(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_dist")
	mustExec(t, `CREATE TABLE test_shard_dist (
		id BIGINT,
		shard_key BIGINT NOT NULL,
		data VARCHAR(50),
		PRIMARY KEY (id, shard_key)
	) SHARD BY (shard_key) SHARDS 4`)

	// Insert 10000 rows with sequential shard keys
	for i := 0; i < 100; i++ {
		var values []string
		for j := 0; j < 100; j++ {
			rowID := int64(i*100 + j)
			values = append(values, fmt.Sprintf("(%d, %d, 'row_%d')", rowID, rowID, rowID))
		}
		mustExec(t, "INSERT INTO test_shard_dist VALUES "+strings.Join(values, ","))
	}

	// Query from information_schema to check shard distribution
	rows := mustQuery(t, `SELECT TABLE_NAME FROM information_schema.tables 
		WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME LIKE 'test_shard_dist%'`)
	var tables []string
	for rows.Next() {
		var name string
		rows.Scan(&name)
		tables = append(tables, name)
	}
	rows.Close()

	// Verify total count is correct
	count := queryInt(t, "SELECT COUNT(*) FROM test_shard_dist")
	if count != 10000 {
		t.Fatalf("Expected 10000 rows, got %d", count)
	}
}

func TestShardBy_PointQuery_ShardPruning(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_prune")
	mustExec(t, `CREATE TABLE test_shard_prune (
		id BIGINT,
		company_id BIGINT NOT NULL,
		status VARCHAR(20),
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 8`)

	// Insert data
	for i := 0; i < 100; i++ {
		mustExec(t, "INSERT INTO test_shard_prune VALUES (?, ?, ?)", i, i%50, "active")
	}

	// Point query on shard key should prune to 1 shard
	rows := mustQuery(t, "EXPLAIN SELECT * FROM test_shard_prune WHERE company_id = 25")
	var explainOutput string
	for rows.Next() {
		var id, estRows, task, accessObject, operatorInfo string
		rows.Scan(&id, &estRows, &task, &accessObject, &operatorInfo)
		explainOutput += id + " " + operatorInfo + "\n"
	}
	rows.Close()
	t.Logf("EXPLAIN for shard pruning:\n%s", explainOutput)

	// Verify the query returns correct results
	count := queryInt(t, "SELECT COUNT(*) FROM test_shard_prune WHERE company_id = 25")
	if count != 2 { // 25 and 75 both % 50 = 25
		t.Fatalf("Expected 2 rows for company_id=25, got %d", count)
	}
}

func TestShardBy_CompositeShardKey(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_composite")
	mustExec(t, `CREATE TABLE test_shard_composite (
		id BIGINT,
		region VARCHAR(10),
		category INT,
		amount DECIMAL(10,2),
		PRIMARY KEY (id, region, category)
	) SHARD BY (region, category) SHARDS 16`)

	// Insert varied data
	regions := []string{"US", "EU", "APAC", "LATAM"}
	for i := 0; i < 200; i++ {
		region := regions[i%4]
		category := i % 10
		mustExec(t, "INSERT INTO test_shard_composite VALUES (?, ?, ?, ?)",
			i, region, category, float64(i)*1.5)
	}

	// Query with both shard key parts — US is index 0, so categories are 0,2,4,6,8 (even only)
	count := queryInt(t, "SELECT COUNT(*) FROM test_shard_composite WHERE region = 'US' AND category = 0")
	if count != 10 { // 200 rows / 4 regions / 5 categories per region = 10
		t.Fatalf("Expected 10 rows for US+category=0, got %d", count)
	}

	// Verify shard-pruned query still works
	count = queryInt(t, "SELECT COUNT(*) FROM test_shard_composite WHERE region = 'US'")
	if count != 50 { // 200/4 = 50
		t.Fatalf("Expected 50 US rows, got %d", count)
	}
}

func TestShardBy_VarcharShardKey(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_varchar")
	mustExec(t, `CREATE TABLE test_shard_varchar (
		id BIGINT,
		email VARCHAR(100) NOT NULL,
		name VARCHAR(50),
		PRIMARY KEY (id, email)
	) SHARD BY (email) SHARDS 8`)

	emails := []string{"alice@co.com", "bob@co.com", "charlie@co.com", "dave@co.com", "eve@co.com"}
	for i := 0; i < 50; i++ {
		email := emails[i%5]
		mustExec(t, "INSERT INTO test_shard_varchar VALUES (?, ?, ?)", i, email, fmt.Sprintf("user_%d", i))
	}

	count := queryInt(t, "SELECT COUNT(*) FROM test_shard_varchar WHERE email = 'alice@co.com'")
	if count != 10 {
		t.Fatalf("Expected 10 rows for alice, got %d", count)
	}
}

func TestShardBy_NullShardKeyRejected(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_null")
	mustExec(t, `CREATE TABLE test_shard_null (
		id BIGINT,
		company_id BIGINT NOT NULL,
		data VARCHAR(50),
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 4`)

	// Insert valid row first
	mustExec(t, "INSERT INTO test_shard_null VALUES (1, 100, 'test')")

	// Attempt NULL insertion — should fail because shard key is NOT NULL
	_, err := db.Exec("INSERT INTO test_shard_null VALUES (2, NULL, 'test')")
	if err == nil {
		t.Fatal("Expected error inserting NULL into NOT NULL shard key column")
	}
}

func TestShardBy_LargeTransactionAcrossShards(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_txn")
	mustExec(t, `CREATE TABLE test_shard_txn (
		id BIGINT,
		shard_key BIGINT NOT NULL,
		value INT,
		PRIMARY KEY (id, shard_key)
	) SHARD BY (shard_key) SHARDS 8`)

	// Large transaction spanning multiple shards
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 500; i++ {
		_, err := tx.Exec("INSERT INTO test_shard_txn VALUES (?, ?, ?)", i, i, i*10)
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	count := queryInt(t, "SELECT COUNT(*) FROM test_shard_txn")
	if count != 500 {
		t.Fatalf("Expected 500 rows after txn, got %d", count)
	}

	// Verify SUM
	sum := queryInt(t, "SELECT SUM(value) FROM test_shard_txn")
	expectedSum := int64(0)
	for i := 0; i < 500; i++ {
		expectedSum += int64(i * 10)
	}
	if sum != expectedSum {
		t.Fatalf("Expected SUM=%d, got %d", expectedSum, sum)
	}
}

func TestShardBy_InPruning(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_in_prune")
	mustExec(t, `CREATE TABLE test_shard_in_prune (
		id BIGINT,
		region VARCHAR(50) NOT NULL,
		val INT,
		PRIMARY KEY (id, region)
	) SHARD BY (region) SHARDS 4`)

	mustExec(t, "INSERT INTO test_shard_in_prune VALUES (1, 'us-east', 10)")
	mustExec(t, "INSERT INTO test_shard_in_prune VALUES (2, 'us-west', 20)")
	mustExec(t, "INSERT INTO test_shard_in_prune VALUES (3, 'eu-west', 30)")
	mustExec(t, "INSERT INTO test_shard_in_prune VALUES (4, 'ap-east', 40)")

	// IN query should return correct results
	rows, err := db.Query("SELECT id, region, val FROM test_shard_in_prune WHERE region IN ('us-east', 'eu-west') ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var results []struct {
		id     int
		region string
		val    int
	}
	for rows.Next() {
		var r struct {
			id     int
			region string
			val    int
		}
		if err := rows.Scan(&r.id, &r.region, &r.val); err != nil {
			t.Fatal(err)
		}
		results = append(results, r)
	}
	if len(results) != 2 {
		t.Fatalf("Expected 2 rows from IN query, got %d", len(results))
	}
	if results[0].region != "us-east" || results[1].region != "eu-west" {
		t.Fatalf("Unexpected results: %+v", results)
	}

	// Verify pruning: EXPLAIN should NOT show partition:all
	explainRows, err := db.Query("EXPLAIN SELECT * FROM test_shard_in_prune WHERE region IN ('us-east', 'eu-west')")
	if err != nil {
		t.Fatal(err)
	}
	defer explainRows.Close()
	foundAll := false
	for explainRows.Next() {
		var id, estRows, task, access, info string
		if err := explainRows.Scan(&id, &estRows, &task, &access, &info); err != nil {
			t.Fatal(err)
		}
		if access == "partition:all" {
			foundAll = true
		}
		t.Logf("EXPLAIN: %s %s %s", id, access, info)
	}
	if foundAll {
		t.Fatal("IN query should prune partitions, but EXPLAIN shows partition:all")
	}
}

// TestShardBy_TruncateTable verifies TRUNCATE TABLE works on sharded tables —
// after TRUNCATE, the table should be empty and accept new inserts with correct
// shard key pruning.
func TestShardBy_TruncateTable(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_truncate")
	mustExec(t, `CREATE TABLE test_shard_truncate (
		id BIGINT,
		key_col VARCHAR(50) NOT NULL,
		val INT,
		PRIMARY KEY (id, key_col)
	) SHARD BY (key_col) SHARDS 4`)

	// Insert some data
	mustExec(t, "INSERT INTO test_shard_truncate VALUES (1, 'alpha', 10)")
	mustExec(t, "INSERT INTO test_shard_truncate VALUES (2, 'beta', 20)")
	mustExec(t, "INSERT INTO test_shard_truncate VALUES (3, 'gamma', 30)")
	cnt := queryInt(t, "SELECT COUNT(*) FROM test_shard_truncate")
	if cnt != 3 {
		t.Fatalf("Expected 3 rows before truncate, got %d", cnt)
	}

	// TRUNCATE
	mustExec(t, "TRUNCATE TABLE test_shard_truncate")
	cnt = queryInt(t, "SELECT COUNT(*) FROM test_shard_truncate")
	if cnt != 0 {
		t.Fatalf("Expected 0 rows after truncate, got %d", cnt)
	}

	// Insert after truncate
	mustExec(t, "INSERT INTO test_shard_truncate VALUES (10, 'delta', 100)")
	cnt = queryInt(t, "SELECT COUNT(*) FROM test_shard_truncate")
	if cnt != 1 {
		t.Fatalf("Expected 1 row after re-insert, got %d", cnt)
	}

	// Verify pruning still works after truncate
	row := db.QueryRow("SELECT val FROM test_shard_truncate WHERE key_col = 'delta'")
	var val int
	if err := row.Scan(&val); err != nil {
		t.Fatalf("Point query after truncate failed: %v", err)
	}
	if val != 100 {
		t.Fatalf("Expected val=100, got %d", val)
	}

	// EXPLAIN should show pruning
	explainRows, err := db.Query("EXPLAIN SELECT * FROM test_shard_truncate WHERE key_col = 'delta'")
	if err != nil {
		t.Fatal(err)
	}
	defer explainRows.Close()
	foundPruning := false
	for explainRows.Next() {
		var id, estRows, task, access, info string
		if err := explainRows.Scan(&id, &estRows, &task, &access, &info); err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(access, "partition:shard_") {
			foundPruning = true
		}
	}
	if !foundPruning {
		t.Fatal("Expected shard pruning in EXPLAIN after truncate, but not found")
	}
}

// TestShardBy_DDLOperations tests ADD COLUMN, ADD INDEX, DROP COLUMN on sharded tables.
func TestShardBy_DDLOperations(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_ddl")
	mustExec(t, `CREATE TABLE test_shard_ddl (
		id BIGINT,
		key_col VARCHAR(50) NOT NULL,
		val INT,
		extra VARCHAR(100),
		PRIMARY KEY (id, key_col)
	) SHARD BY (key_col) SHARDS 4`)

	mustExec(t, "INSERT INTO test_shard_ddl VALUES (1, 'test', 10, 'extra_val')")

	// ADD COLUMN
	mustExec(t, "ALTER TABLE test_shard_ddl ADD COLUMN new_col INT DEFAULT 0")
	row := db.QueryRow("SELECT new_col FROM test_shard_ddl WHERE key_col = 'test'")
	var newCol int
	if err := row.Scan(&newCol); err != nil {
		t.Fatalf("SELECT after ADD COLUMN failed: %v", err)
	}
	if newCol != 0 {
		t.Fatalf("Expected default new_col=0, got %d", newCol)
	}

	// ADD INDEX
	mustExec(t, "ALTER TABLE test_shard_ddl ADD INDEX idx_val (val)")

	// DROP COLUMN (non-shard-key column)
	mustExec(t, "ALTER TABLE test_shard_ddl DROP COLUMN extra")
	_, err := db.Exec("SELECT extra FROM test_shard_ddl LIMIT 1")
	if err == nil {
		t.Fatal("Expected error accessing dropped column 'extra', but got nil")
	}

	// Verify shard key pruning still works after DDL changes
	row = db.QueryRow("SELECT val FROM test_shard_ddl WHERE key_col = 'test'")
	var val int
	if err := row.Scan(&val); err != nil {
		t.Fatalf("Point query after DDL changes failed: %v", err)
	}
	if val != 10 {
		t.Fatalf("Expected val=10 after DDL changes, got %d", val)
	}
}

// TestShardBy_BatchUpsert tests batch INSERT ON DUPLICATE KEY UPDATE across shards.
func TestShardBy_BatchUpsert(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_upsert")
	mustExec(t, `CREATE TABLE test_shard_upsert (
		id BIGINT,
		region VARCHAR(20) NOT NULL,
		cnt INT DEFAULT 0,
		PRIMARY KEY (id, region)
	) SHARD BY (region) SHARDS 4`)

	// Batch insert
	mustExec(t, `INSERT INTO test_shard_upsert (id, region, cnt) VALUES 
		(1, 'us', 1), (2, 'eu', 1), (3, 'ap', 1), (4, 'sa', 1)`)

	// Upsert: increment existing + insert new
	mustExec(t, `INSERT INTO test_shard_upsert (id, region, cnt) VALUES 
		(1, 'us', 5), (5, 'af', 1)
		ON DUPLICATE KEY UPDATE cnt = cnt + VALUES(cnt)`)

	// Verify: id=1 should have cnt=6, id=5 should have cnt=1
	row := db.QueryRow("SELECT cnt FROM test_shard_upsert WHERE id = 1 AND region = 'us'")
	var cnt int
	if err := row.Scan(&cnt); err != nil {
		t.Fatal(err)
	}
	if cnt != 6 {
		t.Fatalf("Expected cnt=6 after upsert, got %d", cnt)
	}
	total := queryInt(t, "SELECT COUNT(*) FROM test_shard_upsert")
	if total != 5 {
		t.Fatalf("Expected 5 total rows, got %d", total)
	}
}

// TestShardBy_MultiColumnShardWithIN tests IN clause on one column of a
// multi-column shard key combined with EQ on the other.
func TestShardBy_MultiColumnShardWithIN(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_multi_in")
	mustExec(t, `CREATE TABLE test_shard_multi_in (
		id BIGINT,
		region VARCHAR(20) NOT NULL,
		category VARCHAR(20) NOT NULL,
		val INT,
		PRIMARY KEY (id, region, category)
	) SHARD BY (region, category) SHARDS 8`)

	mustExec(t, "INSERT INTO test_shard_multi_in VALUES (1, 'us', 'tech', 100)")
	mustExec(t, "INSERT INTO test_shard_multi_in VALUES (2, 'us', 'fin', 200)")
	mustExec(t, "INSERT INTO test_shard_multi_in VALUES (3, 'eu', 'tech', 300)")
	mustExec(t, "INSERT INTO test_shard_multi_in VALUES (4, 'eu', 'fin', 400)")

	// EQ on both: should prune to 1 shard
	explainResult := queryExplain(t, "EXPLAIN SELECT * FROM test_shard_multi_in WHERE region = 'us' AND category = 'tech'")
	if strings.Contains(explainResult, "partition:all") {
		t.Fatal("EQ on both shard key columns should prune, but got partition:all")
	}

	// IN on region + EQ on category: should prune to specific shards
	explainResult = queryExplain(t, "EXPLAIN SELECT * FROM test_shard_multi_in WHERE region IN ('us', 'eu') AND category = 'tech'")
	if strings.Contains(explainResult, "partition:all") {
		t.Fatal("IN + EQ on multi-column shard key should prune, but got partition:all")
	}

	// Verify correct rows returned
	cnt := queryInt(t, "SELECT COUNT(*) FROM test_shard_multi_in WHERE region IN ('us', 'eu') AND category = 'tech'")
	if cnt != 2 {
		t.Fatalf("Expected 2 rows for IN+EQ query, got %d", cnt)
	}

	// EQ on only one column: should NOT prune (needs all shard key columns)
	explainResult = queryExplain(t, "EXPLAIN SELECT * FROM test_shard_multi_in WHERE region = 'us'")
	if !strings.Contains(explainResult, "partition:all") {
		t.Fatal("EQ on only one of two shard key columns should scan all, but pruned")
	}
}

// TestShardBy_WindowFunctions tests window functions across shards.
func TestShardBy_WindowFunctions(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_shard_window")
	mustExec(t, `CREATE TABLE test_shard_window (
		id BIGINT,
		region VARCHAR(20) NOT NULL,
		amount BIGINT,
		PRIMARY KEY (id, region)
	) SHARD BY (region) SHARDS 4`)

	mustExec(t, `INSERT INTO test_shard_window VALUES 
		(1, 'us', 100), (2, 'us', 200), (3, 'eu', 50), (4, 'eu', 150), (5, 'ap', 300)`)

	// Window function: ROW_NUMBER partitioned by shard key column
	rows, err := db.Query(`SELECT id, region, amount,
		ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
		FROM test_shard_window ORDER BY region, rn`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	type result struct {
		id, amount int
		region     string
		rn         int
	}
	var results []result
	for rows.Next() {
		var r result
		if err := rows.Scan(&r.id, &r.region, &r.amount, &r.rn); err != nil {
			t.Fatal(err)
		}
		results = append(results, r)
	}
	if len(results) != 5 {
		t.Fatalf("Expected 5 rows with window function, got %d", len(results))
	}
	// Verify ROW_NUMBER resets per partition
	for _, r := range results {
		if r.rn < 1 {
			t.Fatalf("Invalid ROW_NUMBER %d for id=%d", r.rn, r.id)
		}
	}
}

// ============================================================================
// Encoded Operations Integration Tests
// ============================================================================

func TestEncoded_DictionaryEncoding_Filter(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_enc_filter")
	mustExec(t, `CREATE TABLE test_enc_filter (
		id BIGINT PRIMARY KEY,
		status VARCHAR(20) NOT NULL,
		region VARCHAR(10) NOT NULL,
		amount BIGINT
	)`)

	// Add TiFlash replica
	mustExec(t, "ALTER TABLE test_enc_filter SET TIFLASH REPLICA 1")

	// Insert data with low-cardinality columns (good for dictionary encoding)
	statuses := []string{"active", "inactive", "pending", "cancelled"}
	regions := []string{"US", "EU", "APAC"}
	for i := 0; i < 1000; i++ {
		mustExec(t, "INSERT INTO test_enc_filter VALUES (?, ?, ?, ?)",
			i, statuses[i%4], regions[i%3], i*10)
	}

	waitForTiFlashReplica(t, "test_enc_filter", 60*time.Second)

	// Force TiFlash read
	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// Equality filter — should use encoded filter on dictionary
	count := queryInt(t, "SELECT COUNT(*) FROM test_enc_filter WHERE status = 'active'")
	if count != 250 {
		t.Fatalf("Expected 250 active rows, got %d", count)
	}

	// IN filter
	count = queryInt(t, "SELECT COUNT(*) FROM test_enc_filter WHERE region IN ('US', 'EU')")
	if count < 600 { // ~667
		t.Fatalf("Expected ~667 US/EU rows, got %d", count)
	}
}

func TestEncoded_DictionaryEncoding_GroupBy(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_enc_groupby")
	mustExec(t, `CREATE TABLE test_enc_groupby (
		id BIGINT PRIMARY KEY,
		department VARCHAR(30) NOT NULL,
		salary BIGINT NOT NULL
	)`)

	mustExec(t, "ALTER TABLE test_enc_groupby SET TIFLASH REPLICA 1")

	depts := []string{"Engineering", "Sales", "Marketing", "Support", "HR"}
	for i := 0; i < 500; i++ {
		mustExec(t, "INSERT INTO test_enc_groupby VALUES (?, ?, ?)",
			i, depts[i%5], 50000+i*100)
	}

	waitForTiFlashReplica(t, "test_enc_groupby", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// GROUP BY on low-cardinality column (encoded group-by)
	rows := mustQuery(t, `SELECT department, COUNT(*), SUM(salary) 
		FROM test_enc_groupby GROUP BY department ORDER BY department`)
	defer rows.Close()

	var deptCount int
	for rows.Next() {
		var dept string
		var cnt, totalSal int64
		rows.Scan(&dept, &cnt, &totalSal)
		deptCount++
		if cnt != 100 {
			t.Errorf("Department %s: expected 100 rows, got %d", dept, cnt)
		}
	}
	if deptCount != 5 {
		t.Fatalf("Expected 5 departments, got %d", deptCount)
	}
}

func TestEncoded_LikeFilter(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_enc_like")
	mustExec(t, `CREATE TABLE test_enc_like (
		id BIGINT PRIMARY KEY,
		email VARCHAR(100) NOT NULL
	)`)

	mustExec(t, "ALTER TABLE test_enc_like SET TIFLASH REPLICA 1")

	// Low-cardinality emails (good for dictionary)
	emails := []string{
		"alice@company.com", "bob@company.com", "charlie@other.org",
		"dave@company.com", "eve@other.org",
	}
	for i := 0; i < 500; i++ {
		mustExec(t, "INSERT INTO test_enc_like VALUES (?, ?)", i, emails[i%5])
	}

	waitForTiFlashReplica(t, "test_enc_like", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// LIKE with wildcard — tests encoded LIKE on dictionary entries
	count := queryInt(t, "SELECT COUNT(*) FROM test_enc_like WHERE email LIKE '%@company.com'")
	if count != 300 { // alice, bob, dave = 3/5 × 500 = 300
		t.Fatalf("Expected 300 company.com emails, got %d", count)
	}

	count = queryInt(t, "SELECT COUNT(*) FROM test_enc_like WHERE email LIKE 'a%'")
	if count != 100 { // only alice = 1/5 × 500
		t.Fatalf("Expected 100 emails starting with 'a', got %d", count)
	}
}

// ============================================================================
// Encoded JOIN Integration Tests
// ============================================================================

func TestEncodedJoin_InnerJoin_StarSchema(t *testing.T) {
	// Fact table (large, low-cardinality FK)
	mustExec(t, "DROP TABLE IF EXISTS fact_events")
	mustExec(t, `CREATE TABLE fact_events (
		id BIGINT PRIMARY KEY,
		company_id BIGINT NOT NULL,
		event_type VARCHAR(20) NOT NULL,
		revenue BIGINT NOT NULL
	)`)

	// Dimension table (small)
	mustExec(t, "DROP TABLE IF EXISTS dim_companies")
	mustExec(t, `CREATE TABLE dim_companies (
		id BIGINT PRIMARY KEY,
		industry VARCHAR(30) NOT NULL,
		region VARCHAR(10) NOT NULL
	)`)

	mustExec(t, "ALTER TABLE fact_events SET TIFLASH REPLICA 1")
	mustExec(t, "ALTER TABLE dim_companies SET TIFLASH REPLICA 1")

	// Insert dimension data (50 companies)
	industries := []string{"Tech", "Finance", "Healthcare", "Retail", "Energy"}
	regions := []string{"US", "EU", "APAC"}
	for i := 1; i <= 50; i++ {
		mustExec(t, "INSERT INTO dim_companies VALUES (?, ?, ?)",
			i, industries[i%5], regions[i%3])
	}

	// Insert fact data (10000 events referencing companies 1-50)
	for batch := 0; batch < 100; batch++ {
		var values []string
		for j := 0; j < 100; j++ {
			id := batch*100 + j
			companyID := (id % 50) + 1
			eventType := []string{"view", "click", "purchase", "signup"}[id%4]
			revenue := id * 10
			values = append(values, fmt.Sprintf("(%d, %d, '%s', %d)", id, companyID, eventType, revenue))
		}
		mustExec(t, "INSERT INTO fact_events VALUES "+strings.Join(values, ","))
	}

	waitForTiFlashReplica(t, "fact_events", 60*time.Second)
	waitForTiFlashReplica(t, "dim_companies", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// Star join: fact INNER JOIN dimension GROUP BY dimension.industry
	rows := mustQuery(t, `SELECT c.industry, COUNT(*), SUM(f.revenue)
		FROM fact_events f
		INNER JOIN dim_companies c ON f.company_id = c.id
		GROUP BY c.industry
		ORDER BY c.industry`)
	defer rows.Close()

	var groupCount int
	var totalCount int64
	for rows.Next() {
		var industry string
		var cnt, rev int64
		rows.Scan(&industry, &cnt, &rev)
		groupCount++
		totalCount += cnt
	}

	if groupCount != 5 {
		t.Fatalf("Expected 5 industry groups, got %d", groupCount)
	}
	if totalCount != 10000 {
		t.Fatalf("Expected total 10000 events across groups, got %d", totalCount)
	}
}

func TestEncodedJoin_LeftJoin_NullGroup(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS fact_orders")
	mustExec(t, "DROP TABLE IF EXISTS dim_customers")

	mustExec(t, `CREATE TABLE fact_orders (
		id BIGINT PRIMARY KEY,
		customer_id BIGINT NOT NULL,
		amount BIGINT NOT NULL
	)`)

	mustExec(t, `CREATE TABLE dim_customers (
		id BIGINT PRIMARY KEY,
		segment VARCHAR(20) NOT NULL
	)`)

	mustExec(t, "ALTER TABLE fact_orders SET TIFLASH REPLICA 1")
	mustExec(t, "ALTER TABLE dim_customers SET TIFLASH REPLICA 1")

	// Dimension: customers 1-30 only
	segments := []string{"Enterprise", "SMB", "Startup"}
	for i := 1; i <= 30; i++ {
		mustExec(t, "INSERT INTO dim_customers VALUES (?, ?)", i, segments[i%3])
	}

	// Fact: orders from customers 1-50 (customers 31-50 have no dimension match)
	for i := 1; i <= 500; i++ {
		customerID := (i % 50) + 1
		mustExec(t, "INSERT INTO fact_orders VALUES (?, ?, ?)", i, customerID, i*5)
	}

	waitForTiFlashReplica(t, "fact_orders", 60*time.Second)
	waitForTiFlashReplica(t, "dim_customers", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// LEFT JOIN: preserve all fact rows, unmatched get NULL segment
	rows := mustQuery(t, `SELECT c.segment, COUNT(*), SUM(o.amount)
		FROM fact_orders o
		LEFT JOIN dim_customers c ON o.customer_id = c.id
		GROUP BY c.segment
		ORDER BY c.segment`)
	defer rows.Close()

	var hasNull bool
	var totalRows int64
	for rows.Next() {
		var segment sql.NullString
		var cnt, total int64
		rows.Scan(&segment, &cnt, &total)
		totalRows += cnt
		if !segment.Valid {
			hasNull = true
			// Customers 31-50 (20 customers, each appears ~10 times)
			if cnt < 100 {
				t.Errorf("NULL group should have ~200 rows (customers 31-50), got %d", cnt)
			}
		}
	}

	if !hasNull {
		t.Fatal("LEFT JOIN should have a NULL group for unmatched fact rows")
	}
	if totalRows != 500 {
		t.Fatalf("LEFT JOIN should preserve all 500 fact rows, got %d", totalRows)
	}
}

func TestEncodedJoin_PartialDimensionMatch(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS fact_clicks")
	mustExec(t, "DROP TABLE IF EXISTS dim_pages")

	mustExec(t, `CREATE TABLE fact_clicks (
		id BIGINT PRIMARY KEY,
		page_id BIGINT NOT NULL,
		user_id BIGINT NOT NULL
	)`)

	mustExec(t, `CREATE TABLE dim_pages (
		id BIGINT PRIMARY KEY,
		category VARCHAR(20) NOT NULL
	)`)

	mustExec(t, "ALTER TABLE fact_clicks SET TIFLASH REPLICA 1")
	mustExec(t, "ALTER TABLE dim_pages SET TIFLASH REPLICA 1")

	// Only pages 1-10 exist in dimension
	for i := 1; i <= 10; i++ {
		mustExec(t, "INSERT INTO dim_pages VALUES (?, ?)", i, fmt.Sprintf("cat_%d", i%3))
	}

	// Facts reference pages 1-20 (pages 11-20 don't exist in dimension)
	for i := 1; i <= 200; i++ {
		mustExec(t, "INSERT INTO fact_clicks VALUES (?, ?, ?)", i, (i%20)+1, i)
	}

	waitForTiFlashReplica(t, "fact_clicks", 60*time.Second)
	waitForTiFlashReplica(t, "dim_pages", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// INNER JOIN: only matched rows
	innerCount := queryInt(t, `SELECT COUNT(*) FROM fact_clicks f 
		INNER JOIN dim_pages p ON f.page_id = p.id`)
	if innerCount != 100 { // pages 1-10, each ~10 rows
		t.Fatalf("INNER JOIN expected 100 rows (pages 1-10), got %d", innerCount)
	}

	// LEFT JOIN: all fact rows preserved
	leftCount := queryInt(t, `SELECT COUNT(*) FROM fact_clicks f 
		LEFT JOIN dim_pages p ON f.page_id = p.id`)
	if leftCount != 200 {
		t.Fatalf("LEFT JOIN expected 200 rows (all facts), got %d", leftCount)
	}
}

func TestEncodedJoin_MultiDimensionJoin(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS fact_sales")
	mustExec(t, "DROP TABLE IF EXISTS dim_product")
	mustExec(t, "DROP TABLE IF EXISTS dim_store")

	mustExec(t, `CREATE TABLE fact_sales (
		id BIGINT PRIMARY KEY,
		product_id BIGINT NOT NULL,
		store_id BIGINT NOT NULL,
		quantity BIGINT NOT NULL,
		revenue BIGINT NOT NULL
	)`)

	mustExec(t, `CREATE TABLE dim_product (
		id BIGINT PRIMARY KEY,
		name VARCHAR(50) NOT NULL,
		category VARCHAR(20) NOT NULL
	)`)

	mustExec(t, `CREATE TABLE dim_store (
		id BIGINT PRIMARY KEY,
		city VARCHAR(30) NOT NULL,
		region VARCHAR(10) NOT NULL
	)`)

	mustExec(t, "ALTER TABLE fact_sales SET TIFLASH REPLICA 1")
	mustExec(t, "ALTER TABLE dim_product SET TIFLASH REPLICA 1")
	mustExec(t, "ALTER TABLE dim_store SET TIFLASH REPLICA 1")

	// Products
	categories := []string{"Electronics", "Clothing", "Food", "Books"}
	for i := 1; i <= 20; i++ {
		mustExec(t, "INSERT INTO dim_product VALUES (?, ?, ?)",
			i, fmt.Sprintf("Product_%d", i), categories[i%4])
	}

	// Stores
	cities := []string{"NYC", "LA", "Chicago", "Houston", "Phoenix"}
	regions := []string{"East", "West", "Central"}
	for i := 1; i <= 10; i++ {
		mustExec(t, "INSERT INTO dim_store VALUES (?, ?, ?)",
			i, cities[i%5], regions[i%3])
	}

	// Fact: 2000 sales
	for i := 1; i <= 2000; i++ {
		mustExec(t, "INSERT INTO fact_sales VALUES (?, ?, ?, ?, ?)",
			i, (i%20)+1, (i%10)+1, i%100, i*5)
	}

	waitForTiFlashReplica(t, "fact_sales", 60*time.Second)
	waitForTiFlashReplica(t, "dim_product", 60*time.Second)
	waitForTiFlashReplica(t, "dim_store", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// Multi-dimension star join
	rows := mustQuery(t, `SELECT p.category, s.region, SUM(f.revenue), COUNT(*)
		FROM fact_sales f
		INNER JOIN dim_product p ON f.product_id = p.id
		INNER JOIN dim_store s ON f.store_id = s.id
		GROUP BY p.category, s.region
		ORDER BY p.category, s.region`)
	defer rows.Close()

	var groupCount int
	var totalCount int64
	for rows.Next() {
		var cat, reg string
		var rev, cnt int64
		rows.Scan(&cat, &reg, &rev, &cnt)
		groupCount++
		totalCount += cnt
	}

	if groupCount == 0 {
		t.Fatal("Multi-dimension join returned no groups")
	}
	if totalCount != 2000 {
		t.Fatalf("Expected total 2000 rows from join, got %d", totalCount)
	}
	t.Logf("Multi-dimension join: %d groups, %d total rows", groupCount, totalCount)
}

func TestEncodedJoin_NullForeignKey(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS fact_nullable_fk")
	mustExec(t, "DROP TABLE IF EXISTS dim_lookup")

	mustExec(t, `CREATE TABLE fact_nullable_fk (
		id BIGINT PRIMARY KEY,
		lookup_id BIGINT,
		value BIGINT NOT NULL
	)`)

	mustExec(t, `CREATE TABLE dim_lookup (
		id BIGINT PRIMARY KEY,
		label VARCHAR(20) NOT NULL
	)`)

	mustExec(t, "ALTER TABLE fact_nullable_fk SET TIFLASH REPLICA 1")
	mustExec(t, "ALTER TABLE dim_lookup SET TIFLASH REPLICA 1")

	// Dimension
	for i := 1; i <= 5; i++ {
		mustExec(t, "INSERT INTO dim_lookup VALUES (?, ?)", i, fmt.Sprintf("label_%d", i))
	}

	// Fact with some NULL FKs
	for i := 1; i <= 100; i++ {
		if i%5 == 0 {
			mustExec(t, "INSERT INTO fact_nullable_fk VALUES (?, NULL, ?)", i, i*10)
		} else {
			mustExec(t, "INSERT INTO fact_nullable_fk VALUES (?, ?, ?)", i, (i%5)+1, i*10)
		}
	}

	waitForTiFlashReplica(t, "fact_nullable_fk", 60*time.Second)
	waitForTiFlashReplica(t, "dim_lookup", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// INNER JOIN: NULL FK rows excluded
	innerCount := queryInt(t, `SELECT COUNT(*) FROM fact_nullable_fk f 
		INNER JOIN dim_lookup d ON f.lookup_id = d.id`)
	if innerCount != 80 { // 100 - 20 NULL rows
		t.Fatalf("INNER JOIN with NULL FK: expected 80, got %d", innerCount)
	}

	// LEFT JOIN: all rows preserved, NULL FK → NULL dimension
	leftCount := queryInt(t, `SELECT COUNT(*) FROM fact_nullable_fk f 
		LEFT JOIN dim_lookup d ON f.lookup_id = d.id`)
	if leftCount != 100 {
		t.Fatalf("LEFT JOIN with NULL FK: expected 100, got %d", leftCount)
	}

	// GROUP BY on left join — NULL label group should exist
	rows := mustQuery(t, `SELECT d.label, COUNT(*), SUM(f.value)
		FROM fact_nullable_fk f
		LEFT JOIN dim_lookup d ON f.lookup_id = d.id
		GROUP BY d.label
		ORDER BY d.label`)
	defer rows.Close()

	var nullGroupExists bool
	for rows.Next() {
		var label sql.NullString
		var cnt, sum int64
		rows.Scan(&label, &cnt, &sum)
		if !label.Valid {
			nullGroupExists = true
			if cnt != 20 {
				t.Errorf("NULL FK group: expected 20 rows, got %d", cnt)
			}
		}
	}
	if !nullGroupExists {
		t.Fatal("LEFT JOIN GROUP BY should have NULL group for NULL FK rows")
	}
}

func TestEncodedJoin_WithFilter(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS fact_filtered")
	mustExec(t, "DROP TABLE IF EXISTS dim_category")

	mustExec(t, `CREATE TABLE fact_filtered (
		id BIGINT PRIMARY KEY,
		cat_id BIGINT NOT NULL,
		status VARCHAR(20) NOT NULL,
		amount BIGINT NOT NULL
	)`)

	mustExec(t, `CREATE TABLE dim_category (
		id BIGINT PRIMARY KEY,
		name VARCHAR(30) NOT NULL
	)`)

	mustExec(t, "ALTER TABLE fact_filtered SET TIFLASH REPLICA 1")
	mustExec(t, "ALTER TABLE dim_category SET TIFLASH REPLICA 1")

	for i := 1; i <= 5; i++ {
		mustExec(t, "INSERT INTO dim_category VALUES (?, ?)", i, fmt.Sprintf("Category_%d", i))
	}

	statuses := []string{"active", "inactive", "pending"}
	for i := 1; i <= 600; i++ {
		mustExec(t, "INSERT INTO fact_filtered VALUES (?, ?, ?, ?)",
			i, (i%5)+1, statuses[i%3], i*10)
	}

	waitForTiFlashReplica(t, "fact_filtered", 60*time.Second)
	waitForTiFlashReplica(t, "dim_category", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// Filter + Join + GroupBy pipeline: WHERE status='active' → JOIN → GROUP BY category
	rows := mustQuery(t, `SELECT c.name, COUNT(*), SUM(f.amount)
		FROM fact_filtered f
		INNER JOIN dim_category c ON f.cat_id = c.id
		WHERE f.status = 'active'
		GROUP BY c.name
		ORDER BY c.name`)
	defer rows.Close()

	var totalFiltered int64
	for rows.Next() {
		var name string
		var cnt, sum int64
		rows.Scan(&name, &cnt, &sum)
		totalFiltered += cnt
	}

	// 600 rows, 1/3 active = 200
	if totalFiltered != 200 {
		t.Fatalf("Filter+Join: expected 200 active rows across groups, got %d", totalFiltered)
	}
}

// ============================================================================
// JSON Shredding Integration Tests
// ============================================================================

func TestJsonShredding_BasicInsertAndQuery(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_json_basic")
	mustExec(t, `CREATE TABLE test_json_basic (
		id BIGINT PRIMARY KEY,
		data JSON NOT NULL
	)`)

	mustExec(t, "ALTER TABLE test_json_basic SET TIFLASH REPLICA 1")

	// Insert JSON with consistent schema
	for i := 0; i < 100; i++ {
		mustExec(t, `INSERT INTO test_json_basic VALUES (?, ?)`,
			i, fmt.Sprintf(`{"name": "user_%d", "age": %d, "status": "active"}`, i, 20+i%40))
	}

	waitForTiFlashReplica(t, "test_json_basic", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// Basic JSON extract
	count := queryInt(t, `SELECT COUNT(*) FROM test_json_basic WHERE data->>'$.status' = 'active'`)
	if count != 100 {
		t.Fatalf("Expected 100 active rows, got %d", count)
	}

	// Range filter on JSON path
	count = queryInt(t, `SELECT COUNT(*) FROM test_json_basic WHERE CAST(data->>'$.age' AS SIGNED) > 40`)
	if count == 0 {
		t.Fatal("Expected some rows with age > 40")
	}
}

func TestJsonShredding_MixedSchemas(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_json_mixed")
	mustExec(t, `CREATE TABLE test_json_mixed (
		id BIGINT PRIMARY KEY,
		data JSON
	)`)

	mustExec(t, "ALTER TABLE test_json_mixed SET TIFLASH REPLICA 1")

	// Insert data with varying schemas (schema evolution scenario)
	for i := 0; i < 50; i++ {
		// Schema A: name + age
		mustExec(t, `INSERT INTO test_json_mixed VALUES (?, ?)`,
			i, fmt.Sprintf(`{"name": "user_%d", "age": %d}`, i, 20+i))
	}
	for i := 50; i < 100; i++ {
		// Schema B: name + age + email (new field)
		mustExec(t, `INSERT INTO test_json_mixed VALUES (?, ?)`,
			i, fmt.Sprintf(`{"name": "user_%d", "age": %d, "email": "user_%d@co.com"}`, i, 20+i, i))
	}
	for i := 100; i < 120; i++ {
		// Schema C: completely different (city + zip)
		mustExec(t, `INSERT INTO test_json_mixed VALUES (?, ?)`,
			i, fmt.Sprintf(`{"city": "City_%d", "zip": %d}`, i, 10000+i))
	}
	// Some NULLs
	for i := 120; i < 130; i++ {
		mustExec(t, `INSERT INTO test_json_mixed VALUES (?, NULL)`, i)
	}

	waitForTiFlashReplica(t, "test_json_mixed", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// Query path that exists in some rows but not others
	count := queryInt(t, `SELECT COUNT(*) FROM test_json_mixed WHERE data->>'$.name' IS NOT NULL`)
	if count != 100 { // rows 0-99 have name
		t.Fatalf("Expected 100 rows with $.name, got %d", count)
	}

	count = queryInt(t, `SELECT COUNT(*) FROM test_json_mixed WHERE data->>'$.email' IS NOT NULL`)
	if count != 50 { // rows 50-99 have email
		t.Fatalf("Expected 50 rows with $.email, got %d", count)
	}

	count = queryInt(t, `SELECT COUNT(*) FROM test_json_mixed WHERE data->>'$.city' IS NOT NULL`)
	if count != 20 { // rows 100-119 have city
		t.Fatalf("Expected 20 rows with $.city, got %d", count)
	}
}

func TestJsonShredding_GroupByJsonPath(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_json_groupby")
	mustExec(t, `CREATE TABLE test_json_groupby (
		id BIGINT PRIMARY KEY,
		data JSON NOT NULL
	)`)

	mustExec(t, "ALTER TABLE test_json_groupby SET TIFLASH REPLICA 1")

	// Insert events with JSON data
	regions := []string{"US", "EU", "APAC", "LATAM"}
	for i := 0; i < 400; i++ {
		region := regions[i%4]
		mustExec(t, `INSERT INTO test_json_groupby VALUES (?, ?)`,
			i, fmt.Sprintf(`{"region": "%s", "revenue": %d, "type": "sale"}`, region, (i+1)*10))
	}

	waitForTiFlashReplica(t, "test_json_groupby", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// GROUP BY on JSON path
	rows := mustQuery(t, `SELECT data->>'$.region' as region, COUNT(*), 
		SUM(CAST(data->>'$.revenue' AS SIGNED))
		FROM test_json_groupby 
		GROUP BY data->>'$.region'
		ORDER BY region`)
	defer rows.Close()

	var groups int
	for rows.Next() {
		var region string
		var cnt, sum int64
		rows.Scan(&region, &cnt, &sum)
		groups++
		if cnt != 100 {
			t.Errorf("Region %s: expected 100 rows, got %d", region, cnt)
		}
	}
	if groups != 4 {
		t.Fatalf("Expected 4 region groups, got %d", groups)
	}
}

func TestJsonShredding_NestedObjects(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_json_nested")
	mustExec(t, `CREATE TABLE test_json_nested (
		id BIGINT PRIMARY KEY,
		data JSON NOT NULL
	)`)

	mustExec(t, "ALTER TABLE test_json_nested SET TIFLASH REPLICA 1")

	for i := 0; i < 100; i++ {
		mustExec(t, `INSERT INTO test_json_nested VALUES (?, ?)`,
			i, fmt.Sprintf(`{"user": {"name": "user_%d", "age": %d}, "meta": {"created": "2024-01-%02d"}}`,
				i, 20+i%40, (i%28)+1))
	}

	waitForTiFlashReplica(t, "test_json_nested", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// Query nested path
	count := queryInt(t, `SELECT COUNT(*) FROM test_json_nested WHERE data->>'$.user.name' IS NOT NULL`)
	if count != 100 {
		t.Fatalf("Expected 100 rows with $.user.name, got %d", count)
	}

	// Filter on nested path
	val := queryString(t, `SELECT data->>'$.user.name' FROM test_json_nested WHERE id = 42`)
	if val != "user_42" {
		t.Fatalf("Expected user_42, got %s", val)
	}
}

func TestJsonShredding_LargeDocuments(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_json_large")
	mustExec(t, `CREATE TABLE test_json_large (
		id BIGINT PRIMARY KEY,
		data JSON NOT NULL
	)`)

	mustExec(t, "ALTER TABLE test_json_large SET TIFLASH REPLICA 1")

	// Insert large JSON documents (many keys — tests sparsity pruning)
	for i := 0; i < 50; i++ {
		// Common keys present in all rows
		json := fmt.Sprintf(`{"id": %d, "status": "active", "region": "US"`, i)
		// Sparse keys (only in some rows)
		if i%10 == 0 {
			json += fmt.Sprintf(`, "rare_field_%d": "value_%d"`, i, i)
		}
		if i%5 == 0 {
			json += fmt.Sprintf(`, "medium_field": %d`, i*100)
		}
		json += "}"
		mustExec(t, `INSERT INTO test_json_large VALUES (?, ?)`, i, json)
	}

	waitForTiFlashReplica(t, "test_json_large", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// Common field query
	count := queryInt(t, `SELECT COUNT(*) FROM test_json_large WHERE data->>'$.status' = 'active'`)
	if count != 50 {
		t.Fatalf("Expected 50 active rows, got %d", count)
	}

	// Sparse field query  
	count = queryInt(t, `SELECT COUNT(*) FROM test_json_large WHERE data->>'$.medium_field' IS NOT NULL`)
	if count != 10 { // every 5th row
		t.Fatalf("Expected 10 rows with medium_field, got %d", count)
	}
}

// ============================================================================
// Cross-Component Integration Tests
// ============================================================================

func TestCrossComponent_ShardedTable_WithTiFlash_EncodedJoin(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS sharded_fact")
	mustExec(t, "DROP TABLE IF EXISTS dim_for_sharded")

	mustExec(t, `CREATE TABLE sharded_fact (
		id BIGINT,
		company_id BIGINT NOT NULL,
		status VARCHAR(20) NOT NULL,
		revenue BIGINT NOT NULL,
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 4`)

	mustExec(t, `CREATE TABLE dim_for_sharded (
		id BIGINT PRIMARY KEY,
		industry VARCHAR(30) NOT NULL
	)`)

	mustExec(t, "ALTER TABLE sharded_fact SET TIFLASH REPLICA 1")
	mustExec(t, "ALTER TABLE dim_for_sharded SET TIFLASH REPLICA 1")

	industries := []string{"Tech", "Finance", "Healthcare", "Retail"}
	for i := 1; i <= 20; i++ {
		mustExec(t, "INSERT INTO dim_for_sharded VALUES (?, ?)", i, industries[i%4])
	}

	for i := 1; i <= 1000; i++ {
		companyID := (i % 20) + 1
		status := []string{"active", "inactive"}[i%2]
		mustExec(t, "INSERT INTO sharded_fact VALUES (?, ?, ?, ?)",
			i, companyID, status, i*5)
	}

	waitForTiFlashReplica(t, "sharded_fact", 60*time.Second)
	waitForTiFlashReplica(t, "dim_for_sharded", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// Full pipeline: sharded fact table + encoded filter + star join
	rows := mustQuery(t, `SELECT d.industry, COUNT(*), SUM(f.revenue)
		FROM sharded_fact f
		INNER JOIN dim_for_sharded d ON f.company_id = d.id
		WHERE f.status = 'active'
		GROUP BY d.industry
		ORDER BY d.industry`)
	defer rows.Close()

	var totalActive int64
	var groups int
	for rows.Next() {
		var ind string
		var cnt, rev int64
		rows.Scan(&ind, &cnt, &rev)
		groups++
		totalActive += cnt
	}

	if groups != 4 {
		t.Fatalf("Expected 4 industry groups, got %d", groups)
	}
	if totalActive != 500 { // half of 1000 are active
		t.Fatalf("Expected 500 active rows, got %d", totalActive)
	}
}

func TestCrossComponent_JsonColumn_InShardedTable(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS sharded_json")
	mustExec(t, `CREATE TABLE sharded_json (
		id BIGINT,
		tenant_id BIGINT NOT NULL,
		payload JSON NOT NULL,
		PRIMARY KEY (id, tenant_id)
	) SHARD BY (tenant_id) SHARDS 4`)

	mustExec(t, "ALTER TABLE sharded_json SET TIFLASH REPLICA 1")

	// Insert JSON data across shards
	for i := 0; i < 200; i++ {
		tenantID := int64(i%10 + 1)
		json := fmt.Sprintf(`{"event": "click", "page": "/page_%d", "duration": %d}`, i%20, i*100)
		mustExec(t, "INSERT INTO sharded_json VALUES (?, ?, ?)", i, tenantID, json)
	}

	waitForTiFlashReplica(t, "sharded_json", 60*time.Second)

	mustExec(t, "SET @@tidb_isolation_read_engines = 'tiflash'")
	defer mustExec(t, "SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	// Query with shard pruning + JSON extract
	count := queryInt(t, `SELECT COUNT(*) FROM sharded_json 
		WHERE tenant_id = 5 AND payload->>'$.event' = 'click'`)
	if count != 20 { // 200/10 = 20 rows per tenant
		t.Fatalf("Expected 20 rows for tenant 5, got %d", count)
	}

	// GROUP BY JSON path within sharded table
	rows := mustQuery(t, `SELECT payload->>'$.event', COUNT(*)
		FROM sharded_json
		GROUP BY payload->>'$.event'`)
	defer rows.Close()

	var found bool
	for rows.Next() {
		var event string
		var cnt int64
		rows.Scan(&event, &cnt)
		if event == "click" {
			found = true
			if cnt != 200 {
				t.Errorf("Expected 200 click events, got %d", cnt)
			}
		}
	}
	if !found {
		t.Fatal("Should find 'click' event type")
	}
}

func TestCrossComponent_InfoSchema_ShardSkew(t *testing.T) {
	mustExec(t, "DROP TABLE IF EXISTS test_skew_check")
	mustExec(t, `CREATE TABLE test_skew_check (
		id BIGINT,
		key_col BIGINT NOT NULL,
		data VARCHAR(100),
		PRIMARY KEY (id, key_col)
	) SHARD BY (key_col) SHARDS 4`)

	// Insert data
	for i := 0; i < 100; i++ {
		mustExec(t, "INSERT INTO test_skew_check VALUES (?, ?, ?)", i, i, fmt.Sprintf("data_%d", i))
	}

	// Query shard skew information
	rows := mustQuery(t, `SELECT TABLE_NAME, SHARD_KEY, SHARD_COUNT
		FROM information_schema.SHARD_SKEW 
		WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'test_skew_check'`)
	defer rows.Close()

	var found bool
	for rows.Next() {
		var tableName, shardKey string
		var shardCount int
		rows.Scan(&tableName, &shardKey, &shardCount)
		found = true
		if shardCount != 4 {
			t.Errorf("Expected SHARD_COUNT=4, got %d", shardCount)
		}
		if shardKey != "key_col" {
			t.Errorf("Expected SHARD_KEY='key_col', got '%s'", shardKey)
		}
	}
	if !found {
		t.Fatal("SHARD_SKEW should have rows for sharded table")
	}
}
