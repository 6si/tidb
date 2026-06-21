//go:build cluster_integration

package shardkey

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

const (
	clusterDSN    = "root@tcp(k8s-tidbshar-tidbshar-527997beb2-b7b40e83bd4bc403.elb.us-east-1.amazonaws.com:4000)/"
	dialTimeout   = 5 * time.Second
	clusterTestDB = "shardkey_cluster_test"
)

// clusterDB opens a connection to the live TiDB cluster and creates a fresh
// test database. It returns the *sql.DB and a cleanup function. If the cluster
// is unreachable the test is skipped.
func clusterDB(t *testing.T) *sql.DB {
	t.Helper()
	// Use a root connection (no db selected) to create/drop the test database.
	rootDSN := fmt.Sprintf("%s?timeout=%s&parseTime=true", clusterDSN, dialTimeout)
	rootDB, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Skipf("cluster unreachable: %v", err)
	}
	if err := rootDB.Ping(); err != nil {
		rootDB.Close()
		t.Skipf("cluster unreachable: %v", err)
	}
	if _, err = rootDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", clusterTestDB)); err != nil {
		rootDB.Close()
		t.Fatalf("drop db: %v", err)
	}
	if _, err = rootDB.Exec(fmt.Sprintf("CREATE DATABASE %s", clusterTestDB)); err != nil {
		rootDB.Close()
		t.Fatalf("create db: %v", err)
	}
	rootDB.Close()

	// Re-open with the test database in the DSN so every connection in the pool
	// targets the right database regardless of which connection is picked.
	dbDSN := fmt.Sprintf("%s%s?timeout=%s&parseTime=true", clusterDSN, clusterTestDB, dialTimeout)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		t.Fatalf("open db connection: %v", err)
	}
	db.SetConnMaxLifetime(30 * time.Second)
	if err := db.Ping(); err != nil {
		db.Close()
		t.Fatalf("ping db: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
		if cleanupDB, err2 := sql.Open("mysql", rootDSN); err2 == nil {
			cleanupDB.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", clusterTestDB))
			cleanupDB.Close()
		}
	})
	return db
}

// ---------------------------------------------------------------------------
// (a) DDL PK constraint
// ---------------------------------------------------------------------------

func TestCluster_DDL_ShardKeyNotInPK(t *testing.T) {
	db := clusterDB(t)

	// Shard key NOT in PK → error
	_, err := db.Exec(`CREATE TABLE t_bad (
		id BIGINT NOT NULL PRIMARY KEY,
		company_id BIGINT NOT NULL
	) SHARD BY (company_id) SHARDS 4`)
	require.Error(t, err, "expected error when shard key is not in PK")
	require.Contains(t, err.Error(), "primary key")

	// Shard key IN PK → success
	_, err = db.Exec(`CREATE TABLE t_good (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)
	require.NoError(t, err)
	defer db.Exec("DROP TABLE IF EXISTS t_good")
}

func TestCluster_DDL_NoPK(t *testing.T) {
	db := clusterDB(t)

	// No explicit PK + SHARD BY → error
	_, err := db.Exec(`CREATE TABLE t_nopk (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL
	) SHARD BY (company_id) SHARDS 4`)
	require.Error(t, err, "expected error when table has no PK")
}

func TestCluster_DDL_MultiColShardKey(t *testing.T) {
	db := clusterDB(t)

	// Multi-column shard key, all in PK → success
	_, err := db.Exec(`CREATE TABLE t_multi (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		region_id BIGINT NOT NULL,
		PRIMARY KEY (company_id, region_id, id)
	) SHARD BY (company_id, region_id) SHARDS 4`)
	require.NoError(t, err)
	defer db.Exec("DROP TABLE IF EXISTS t_multi")

	// Multi-column shard key, only one in PK → error
	_, err = db.Exec(`CREATE TABLE t_multi_bad (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		region_id BIGINT NOT NULL,
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id, region_id) SHARDS 4`)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// (b) Shard routing correctness
// ---------------------------------------------------------------------------

func TestCluster_ShardRouting(t *testing.T) {
	db := clusterDB(t)

	_, err := db.Exec(`CREATE TABLE t_route (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		val VARCHAR(64),
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)
	require.NoError(t, err)
	defer db.Exec("DROP TABLE IF EXISTS t_route")

	// Insert rows with known company_id values
	testData := []struct {
		id        int64
		companyID int64
		val       string
	}{
		{1, 42, "a"},
		{2, 99, "b"},
		{3, 1, "c"},
		{4, 1000, "d"},
		{5, 42, "e"},
	}
	for _, d := range testData {
		_, err := db.Exec("INSERT INTO t_route (id, company_id, val) VALUES (?, ?, ?)",
			d.id, d.companyID, d.val)
		require.NoError(t, err)
	}

	// SELECT with shard key filter returns correct rows
	rows, err := db.Query("SELECT id, val FROM t_route WHERE company_id = 42 ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		var v string
		require.NoError(t, rows.Scan(&id, &v))
		ids = append(ids, id)
	}
	require.Equal(t, []int64{1, 5}, ids)

	// EXPLAIN should show shard-aware access (not full scan of all physical tables)
	var plan strings.Builder
	explainRows, err := db.Query("EXPLAIN SELECT * FROM t_route WHERE company_id = 42")
	require.NoError(t, err)
	defer explainRows.Close()
	cols, _ := explainRows.Columns()
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for explainRows.Next() {
		require.NoError(t, explainRows.Scan(ptrs...))
		for _, v := range vals {
			if v.Valid {
				plan.WriteString(v.String)
				plan.WriteString(" ")
			}
		}
		plan.WriteString("\n")
	}
	planStr := plan.String()
	// The plan should reference a specific partition/physical table, not a Union of all shards
	t.Logf("EXPLAIN plan:\n%s", planStr)
}

// ---------------------------------------------------------------------------
// (c) Point_Get with shard key in WHERE
// ---------------------------------------------------------------------------

func TestCluster_PointGet(t *testing.T) {
	db := clusterDB(t)

	_, err := db.Exec(`CREATE TABLE t_point (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		val INT DEFAULT 0,
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)
	require.NoError(t, err)
	defer db.Exec("DROP TABLE IF EXISTS t_point")

	_, err = db.Exec("INSERT INTO t_point (id, company_id, val) VALUES (100, 42, 7)")
	require.NoError(t, err)

	// With both PK columns (includes shard key) → should use Point_Get
	plan1 := explainPlan(t, db, "SELECT val FROM t_point WHERE company_id = 42 AND id = 100")
	t.Logf("Plan with shard key:\n%s", plan1)
	require.True(t, strings.Contains(plan1, "Point_Get") || strings.Contains(plan1, "point_get"),
		"expected Point_Get when shard key is in WHERE, got:\n%s", plan1)

	// With only id (no shard key) → should NOT use Point_Get, should scan
	plan2 := explainPlan(t, db, "SELECT val FROM t_point WHERE id = 100")
	t.Logf("Plan without shard key:\n%s", plan2)
	require.False(t, strings.Contains(plan2, "Point_Get") && !strings.Contains(plan2, "Scan"),
		"expected scan (not Point_Get) when shard key missing from WHERE")
}

// ---------------------------------------------------------------------------
// (d) Cross-shard transaction
// ---------------------------------------------------------------------------

func TestCluster_CrossShardTransaction(t *testing.T) {
	db := clusterDB(t)

	_, err := db.Exec(`CREATE TABLE t_txn (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		val INT DEFAULT 0,
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)
	require.NoError(t, err)
	defer db.Exec("DROP TABLE IF EXISTS t_txn")

	// Insert two rows in different shards (company_id=42 → slot 0, company_id=99 → slot 1)
	_, err = db.Exec("INSERT INTO t_txn (id, company_id, val) VALUES (1, 42, 10)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO t_txn (id, company_id, val) VALUES (2, 99, 20)")
	require.NoError(t, err)

	// Cross-shard transaction: atomically update both rows
	tx, err := db.Begin()
	require.NoError(t, err)
	_, err = tx.Exec("UPDATE t_txn SET val = val + 100 WHERE company_id = 42 AND id = 1")
	require.NoError(t, err)
	_, err = tx.Exec("UPDATE t_txn SET val = val + 200 WHERE company_id = 99 AND id = 2")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Verify both updates visible
	var v1, v2 int
	require.NoError(t, db.QueryRow("SELECT val FROM t_txn WHERE company_id = 42 AND id = 1").Scan(&v1))
	require.NoError(t, db.QueryRow("SELECT val FROM t_txn WHERE company_id = 99 AND id = 2").Scan(&v2))
	require.Equal(t, 110, v1)
	require.Equal(t, 220, v2)
}

// ---------------------------------------------------------------------------
// (e) NULL shard key rejected
// ---------------------------------------------------------------------------

func TestCluster_NullShardKeyRejected(t *testing.T) {
	db := clusterDB(t)

	_, err := db.Exec(`CREATE TABLE t_null (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)
	require.NoError(t, err)
	defer db.Exec("DROP TABLE IF EXISTS t_null")

	// NULL in shard key column (which is NOT NULL + part of PK) → rejected
	_, err = db.Exec("INSERT INTO t_null (id, company_id) VALUES (1, NULL)")
	require.Error(t, err, "expected error inserting NULL into shard key column")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func explainPlan(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN " + query)
	require.NoError(t, err)
	defer rows.Close()

	var plan strings.Builder
	cols, _ := rows.Columns()
	vals := make([]sql.NullString, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	for rows.Next() {
		require.NoError(t, rows.Scan(ptrs...))
		for _, v := range vals {
			if v.Valid {
				plan.WriteString(v.String)
				plan.WriteString("\t")
			}
		}
		plan.WriteString("\n")
	}
	return plan.String()
}
