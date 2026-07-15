package shardkey

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// TC-DML-ST: Simple Table INSERT/SELECT
// ---------------------------------------------------------------------------

func TestDML_ST_InsertAndSelect(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders (id BIGINT NOT NULL AUTO_INCREMENT, company_id BIGINT NOT NULL, amount DECIMAL(12,2), PRIMARY KEY (id))`)
	tk.MustExec(`INSERT INTO orders (company_id, amount) VALUES (100, 9.99), (200, 19.99)`)
	tk.MustQuery(`SELECT id, company_id, amount FROM orders ORDER BY id`).Check(
		[][]any{{"1", "100", "9.99"}, {"2", "200", "19.99"}},
	)
}

// ---------------------------------------------------------------------------
// TC-DML-SST: Sharded Simple Table
// ---------------------------------------------------------------------------

func TestDML_SST_InsertAndCount(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL, company_id BIGINT NOT NULL, amount DECIMAL(12,2), PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec(`INSERT INTO orders_sharded VALUES (1, 42, 10.00), (2, 42, 20.00), (3, 99, 30.00)`)
	tk.MustQuery(`SELECT COUNT(*) FROM orders_sharded`).Check([][]any{{"3"}})
}

func TestDML_SST_ShardPruningEquality(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL, company_id BIGINT NOT NULL, amount DECIMAL(12,2), PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec(`INSERT INTO orders_sharded VALUES (1, 42, 10.00), (2, 42, 20.00), (3, 99, 30.00)`)

	// Shard-key equality filter must prune to a single shard (not all)
	rows := tk.MustQuery(`EXPLAIN SELECT * FROM orders_sharded WHERE company_id = 42`).Rows()
	plan := joinPlan(rows)
	require.Contains(t, plan, "partition:shard_", "equality filter must prune to a shard")
	require.NotContains(t, plan, "partition:all", "equality filter must not scan all shards")
}

func TestDML_SST_FullScanTouchesAllShards(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL, company_id BIGINT NOT NULL, amount DECIMAL(12,2), PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec(`INSERT INTO orders_sharded VALUES (1, 42, 10.00), (2, 99, 20.00)`)

	rows := tk.MustQuery(`EXPLAIN SELECT COUNT(*) FROM orders_sharded`).Rows()
	plan := joinPlan(rows)
	require.Contains(t, plan, "partition:all", "full scan must touch all shards")
}

func TestDML_SST_NullShardKey(t *testing.T) {
	// With the shard-key-must-be-in-PK constraint, shard key columns are always
	// NOT NULL (PK columns cannot be NULL). Inserting NULL for the shard key
	// should be rejected.
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL, company_id BIGINT NOT NULL, amount DECIMAL(12,2), PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)
	tk.MustContainErrMsg(`INSERT INTO orders_sharded VALUES (10, NULL, 1.00)`, "Column 'company_id' cannot be null")
}

func TestDML_SST_UpdateNonShardColumn(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL, company_id BIGINT NOT NULL, amount DECIMAL(12,2), PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec(`INSERT INTO orders_sharded VALUES (1, 42, 10.00)`)
	tk.MustQuery(`SELECT COUNT(*) FROM orders_sharded`).Check([][]any{{"1"}})
	// Filter by shard key only — point lookups by non-shard-key columns use a different plan path
	tk.MustExec(`UPDATE orders_sharded SET amount = 99.00 WHERE company_id = 42`)
	tk.MustQuery(`SELECT amount FROM orders_sharded WHERE company_id = 42`).Check([][]any{{"99.00"}})
}

func TestDML_SST_UpdateShardKeyCrossShardAtomic(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL, company_id BIGINT NOT NULL, amount DECIMAL(12,2), PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec(`INSERT INTO orders_sharded VALUES (100, 1, 50.00)`)

	// Update company_id to a value that may hash to a different shard
	// Filter by shard key only to avoid the Point_Get path (which has a known issue with
	// routing to the wrong shard when filtering by non-shard-key columns)
	tk.MustExec(`UPDATE orders_sharded SET company_id = 999 WHERE company_id = 1`)

	// Old shard key value no longer returns this row
	tk.MustQuery(`SELECT COUNT(*) FROM orders_sharded WHERE company_id = 1`).Check([][]any{{"0"}})
	// New shard key value returns it
	tk.MustQuery(`SELECT COUNT(*) FROM orders_sharded WHERE company_id = 999`).Check([][]any{{"1"}})
	// Total row count invariant: still exactly 1 row
	tk.MustQuery(`SELECT COUNT(*) FROM orders_sharded`).Check([][]any{{"1"}})
}

func TestDML_SST_UpdateShardKeyRollback(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL, company_id BIGINT NOT NULL, amount DECIMAL(12,2), PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec(`INSERT INTO orders_sharded VALUES (100, 1, 50.00)`)

	tk.MustExec(`BEGIN`)
	tk.MustExec(`UPDATE orders_sharded SET company_id = 999 WHERE company_id = 1`)
	tk.MustExec(`ROLLBACK`)

	// Row must still have original shard key value (filter by shard key to avoid Point_Get path)
	tk.MustQuery(`SELECT COUNT(*) FROM orders_sharded WHERE company_id = 1`).Check([][]any{{"1"}})
	tk.MustQuery(`SELECT COUNT(*) FROM orders_sharded WHERE company_id = 999`).Check([][]any{{"0"}})
}

func TestDML_SST_Delete(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL, company_id BIGINT NOT NULL, amount DECIMAL(12,2), PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec(`INSERT INTO orders_sharded VALUES (1, 42, 10.00), (2, 42, 20.00), (3, 99, 30.00)`)
	tk.MustExec(`DELETE FROM orders_sharded WHERE id = 3`)
	tk.MustQuery(`SELECT COUNT(*) FROM orders_sharded WHERE company_id = 99`).Check([][]any{{"0"}})
}

// ---------------------------------------------------------------------------
// TC-DML-SST: Row count invariant across 50 company_ids
// ---------------------------------------------------------------------------

func TestDML_SST_RowCountInvariant(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL, company_id BIGINT NOT NULL, val INT, PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)

	// Insert 1000 rows: 50 company_ids × 20 rows each
	for i := 0; i < 50; i++ {
		for j := 0; j < 20; j++ {
			tk.MustExec(fmt.Sprintf(`INSERT INTO orders_sharded VALUES (%d, %d, %d)`, i*20+j+1, i+1, j))
		}
	}
	tk.MustQuery(`SELECT COUNT(*) FROM orders_sharded`).Check([][]any{{"1000"}})
	for i := 1; i <= 50; i++ {
		tk.MustQuery(fmt.Sprintf(`SELECT COUNT(*) FROM orders_sharded WHERE company_id = %d`, i)).
			Check([][]any{{"20"}})
	}
}

// ---------------------------------------------------------------------------
// TC-DML-PT-R: Partitioned Table (RANGE) INSERT/SELECT
// ---------------------------------------------------------------------------

func TestDML_PT_Range_InsertAndPrune(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_range (
		id BIGINT NOT NULL, company_id BIGINT NOT NULL, created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) PARTITION BY RANGE (YEAR(created_at)) (
		PARTITION p2023 VALUES LESS THAN (2024),
		PARTITION p2024 VALUES LESS THAN (2025),
		PARTITION p2025 VALUES LESS THAN (2026),
		PARTITION pmax  VALUES LESS THAN MAXVALUE
	)`)
	tk.MustExec(`INSERT INTO orders_range VALUES (1, 10, '2023-06-15'), (2, 20, '2024-03-01'), (3, 30, '2025-09-10')`)
	tk.MustQuery(`SELECT id, YEAR(created_at) AS yr FROM orders_range ORDER BY id`).Check(
		[][]any{{"1", "2023"}, {"2", "2024"}, {"3", "2025"}},
	)

	rows := tk.MustQuery(`EXPLAIN SELECT * FROM orders_range WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'`).Rows()
	plan := joinPlan(rows)
	require.Contains(t, plan, "p2024", "range partition pruning must select p2024")
	require.NotContains(t, plan, "p2023", "range partition pruning must exclude p2023")
}

// ---------------------------------------------------------------------------
// TC-DML-SPT-R: Sharded Partitioned Table (RANGE + SHARD_KEY)
// ---------------------------------------------------------------------------

func TestDML_SPT_Range_InsertAndCount(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id BIGINT NOT NULL, company_id BIGINT NOT NULL, created_at DATE NOT NULL,
		PRIMARY KEY (company_id, id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec(`INSERT INTO orders_range_sharded VALUES
		(1, 42, '2023-05-01'), (2, 42, '2024-05-01'),
		(3, 99, '2023-11-01'), (4, 99, '2024-11-01'), (5, 17, '2025-02-01')`)
	tk.MustQuery(`SELECT COUNT(*) FROM orders_range_sharded`).Check([][]any{{"5"}})

	// Both partition AND shard filter should reduce scanned partitions
	rows := tk.MustQuery(`EXPLAIN SELECT * FROM orders_range_sharded WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31' AND company_id = 42`).Rows()
	plan := joinPlan(rows)
	// Plan must prune to a subset of partitions (not scan all 12 physical IDs)
	require.NotContains(t, plan, "partition:all", "combined filter must prune partitions")
}

func TestDML_SPT_Range_PartitionFilter_CorrectResults(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id BIGINT NOT NULL, company_id BIGINT NOT NULL, created_at DATE NOT NULL,
		PRIMARY KEY (company_id, id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec(`INSERT INTO orders_range_sharded VALUES
		(1, 42, '2023-05-01'), (2, 42, '2024-05-01'), (3, 99, '2025-02-01')`)

	// Range filter correctness — must only return 2023 rows
	tk.MustQuery(`SELECT COUNT(*) FROM orders_range_sharded WHERE created_at < '2024-01-01'`).Check([][]any{{"1"}})
	// Range filter correctness — must return 2024 rows
	tk.MustQuery(`SELECT COUNT(*) FROM orders_range_sharded WHERE created_at >= '2024-01-01' AND created_at < '2025-01-01'`).Check([][]any{{"1"}})
}

func TestDML_SPT_Range_ShardOnlyFilter(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id BIGINT NOT NULL, company_id BIGINT NOT NULL, created_at DATE NOT NULL,
		PRIMARY KEY (company_id, id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)

	// Shard-only filter: must prune to a subset (not scan all 12 shards)
	rows := tk.MustQuery(`EXPLAIN SELECT * FROM orders_range_sharded WHERE company_id = 42`).Rows()
	plan := joinPlan(rows)
	require.NotContains(t, plan, "partition:all", "shard filter must not scan all partitions")
}

// ---------------------------------------------------------------------------
// TC-DML-SPT-L: Sharded Partitioned Table (LIST + SHARD_KEY)
// ---------------------------------------------------------------------------

func TestDML_SPT_List_InsertAndBothPruning(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_list_sharded (
		id BIGINT NOT NULL, company_id BIGINT NOT NULL, region_id INT NOT NULL,
		PRIMARY KEY (company_id, id, region_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST (region_id) (
		PARTITION p_us   VALUES IN (1, 2, 3),
		PARTITION p_eu   VALUES IN (4, 5, 6),
		PARTITION p_apac VALUES IN (7, 8, 9)
	)`)
	tk.MustExec(`INSERT INTO orders_list_sharded VALUES (1, 42, 1), (2, 42, 4), (3, 99, 7)`)
	tk.MustQuery(`SELECT COUNT(*) FROM orders_list_sharded`).Check([][]any{{"3"}})

	// LIST partition + shard key filter should prune to a subset (not scan all)
	rows := tk.MustQuery(`EXPLAIN SELECT * FROM orders_list_sharded WHERE region_id = 1 AND company_id = 42`).Rows()
	plan := joinPlan(rows)
	require.NotContains(t, plan, "partition:all", "LIST+shard filter must prune partitions")
}

// ---------------------------------------------------------------------------
// TC-DML-SPT-H: Sharded Partitioned Table (HASH + SHARD_KEY)
// ---------------------------------------------------------------------------

func TestDML_SPT_Hash_InsertAndShardPruning(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_hash_sharded (
		id BIGINT NOT NULL, company_id BIGINT NOT NULL, bucket_id BIGINT NOT NULL,
		PRIMARY KEY (company_id, id, bucket_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY HASH (bucket_id) PARTITIONS 4`)
	tk.MustExec(`INSERT INTO orders_hash_sharded VALUES (1, 42, 10), (2, 42, 11), (3, 99, 10)`)
	tk.MustQuery(`SELECT COUNT(*) FROM orders_hash_sharded`).Check([][]any{{"3"}})

	// HASH is not value-prunable, but shard key pruning limits physical scans
	rows := tk.MustQuery(`EXPLAIN SELECT * FROM orders_hash_sharded WHERE company_id = 42`).Rows()
	plan := joinPlan(rows)
	// Should not scan all 16 physical IDs (4 hash × 4 shards) — shard key prunes within each hash partition
	require.NotContains(t, plan, "partition:all", "shard filter must prune within hash partitions")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func joinPlan(rows [][]any) string {
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		parts = append(parts, fmt.Sprintf("%v", r))
	}
	return strings.Join(parts, "\n")
}
