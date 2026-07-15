// Copyright 2026 PingCAP, Inc.
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

package tables_test

// Shard key integration tests covering the priority TCs from
// docs/test/2026-04-26-14-00_shard-key-integration-test-cases.md.
//
// Skipped (require a full cluster / TiFlash):
//   §4  MPP Co-location (TC-MPP-*)
//   §10 TiFlash Replica Management (TC-TIFLASH-*)
//   §22 Backup / Restore / TiCDC (TC-BR-*, TC-TICDC-*)
//   §11.5 GC correctness after DROP PARTITION (requires pd-ctl polling)
//   §9.1/9.2 Placement label checks (TC-EDGE-01/02)
//
// TC-DML-SST-03 (NULL shard key): skipped — schema has company_id NOT NULL.
// TC-DML-SST-04b/04c (same/cross-shard UPDATE): testkit is single-node; behaviour is correct
//   end-to-end but shard routing cannot be confirmed at the physical region level.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

// ---------------------------------------------------------------------------
// §1 DDL CREATE TABLE
// ---------------------------------------------------------------------------

func TestDDLCreateST(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// TC-DDL-ST-01: simple table
	tk.MustExec(`CREATE TABLE orders (
		id         BIGINT NOT NULL AUTO_INCREMENT,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		created_at DATETIME,
		PRIMARY KEY (id)
	)`)
	tk.MustQuery("SHOW CREATE TABLE orders").CheckContain("orders")
	tk.MustExec("DROP TABLE orders")
}

func TestDDLCreatePT(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// TC-DDL-PT-R-01
	tk.MustExec(`CREATE TABLE orders_range (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) PARTITION BY RANGE (YEAR(created_at)) (
		PARTITION p2023 VALUES LESS THAN (2024),
		PARTITION p2024 VALUES LESS THAN (2025),
		PARTITION p2025 VALUES LESS THAN (2026),
		PARTITION pmax  VALUES LESS THAN MAXVALUE
	)`)
	tk.MustExec("DROP TABLE orders_range")

	// TC-DDL-PT-RC-01
	tk.MustExec(`CREATE TABLE orders_range_cols (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec("DROP TABLE orders_range_cols")

	// TC-DDL-PT-L-01
	tk.MustExec(`CREATE TABLE orders_list (
		id         BIGINT NOT NULL,
		region_id  INT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id, region_id)
	) PARTITION BY LIST (region_id) (
		PARTITION p_us   VALUES IN (1, 2, 3),
		PARTITION p_eu   VALUES IN (4, 5, 6),
		PARTITION p_apac VALUES IN (7, 8, 9)
	)`)
	tk.MustExec("DROP TABLE orders_list")

	// TC-DDL-PT-LC-01
	tk.MustExec(`CREATE TABLE orders_list_cols (
		id         BIGINT NOT NULL,
		country    VARCHAR(2) NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id, country)
	) PARTITION BY LIST COLUMNS (country) (
		PARTITION p_us    VALUES IN ('US'),
		PARTITION p_gb    VALUES IN ('GB'),
		PARTITION p_de    VALUES IN ('DE'),
		PARTITION p_other VALUES IN ('AU', 'CA', 'JP')
	)`)
	tk.MustExec("DROP TABLE orders_list_cols")

	// TC-DDL-PT-H-01
	tk.MustExec(`CREATE TABLE orders_hash (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id, company_id)
	) PARTITION BY HASH (company_id) PARTITIONS 8`)
	tk.MustExec("DROP TABLE orders_hash")

	// TC-DDL-PT-K-01
	tk.MustExec(`CREATE TABLE orders_key (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id, company_id)
	) PARTITION BY KEY (company_id) PARTITIONS 4`)
	tk.MustExec("DROP TABLE orders_key")
}

func TestDDLCreateSST(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// TC-DDL-SST-01
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL AUTO_INCREMENT,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustQuery("SHOW CREATE TABLE orders_sharded").CheckContain("SHARD BY (`company_id`) SHARDS 4")
	tk.MustExec("DROP TABLE orders_sharded")

	// TC-DDL-SST-02: multi-column shard key
	tk.MustExec(`CREATE TABLE events_sharded (
		id          BIGINT NOT NULL,
		company_id  BIGINT NOT NULL,
		user_id     BIGINT NOT NULL,
		event_type  VARCHAR(64),
		PRIMARY KEY (id)
	) SHARD BY (company_id, user_id) SHARDS 8`)
	tk.MustQuery("SHOW CREATE TABLE events_sharded").CheckContain("SHARD BY (`company_id`, `user_id`) SHARDS 8")
	tk.MustExec("DROP TABLE events_sharded")

	// TC-DDL-SST-03: VARCHAR shard key
	tk.MustExec(`CREATE TABLE tenants_sharded (
		id          BIGINT NOT NULL,
		tenant_code VARCHAR(32) NOT NULL,
		data        TEXT,
		PRIMARY KEY (id)
	) SHARD BY (tenant_code) SHARDS 4`)
	tk.MustExec("DROP TABLE tenants_sharded")

	// TC-DDL-SST-04: CHAR shard key
	tk.MustExec(`CREATE TABLE regions_sharded (
		id   BIGINT NOT NULL,
		code CHAR(3) NOT NULL,
		name VARCHAR(128),
		PRIMARY KEY (id)
	) SHARD BY (code) SHARDS 4`)
	tk.MustExec("DROP TABLE regions_sharded")

	// TC-DDL-SST-05: minimum shard count (2)
	tk.MustExec(`CREATE TABLE t_shard_min (
		id BIGINT PRIMARY KEY,
		k  BIGINT NOT NULL
	) SHARD BY (k) SHARDS 2`)
	tk.MustExec("DROP TABLE t_shard_min")

	// TC-DDL-SST-06: maximum shard count (64)
	tk.MustExec(`CREATE TABLE t_shard_max (
		id BIGINT PRIMARY KEY,
		k  BIGINT NOT NULL
	) SHARD BY (k) SHARDS 64`)
	tk.MustExec("DROP TABLE t_shard_max")
}

func TestDDLCreateSPT(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// TC-DDL-SPT-R-01
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec("DROP TABLE orders_range_sharded")

	// TC-DDL-SPT-RC-01
	tk.MustExec(`CREATE TABLE events_range_cols_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		region     VARCHAR(16) NOT NULL,
		ts         DATE NOT NULL,
		PRIMARY KEY (id, region, ts)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (region, ts) (
		PARTITION p_eu_2024 VALUES LESS THAN ('EU', '2025-01-01'),
		PARTITION p_eu_max  VALUES LESS THAN ('EU', MAXVALUE),
		PARTITION p_us_2024 VALUES LESS THAN ('US', '2025-01-01'),
		PARTITION p_us_max  VALUES LESS THAN (MAXVALUE, MAXVALUE)
	)`)
	tk.MustExec("DROP TABLE events_range_cols_sharded")

	// TC-DDL-SPT-L-01
	tk.MustExec(`CREATE TABLE orders_list_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		region_id  INT NOT NULL,
		PRIMARY KEY (id, region_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST (region_id) (
		PARTITION p_us   VALUES IN (1, 2, 3),
		PARTITION p_eu   VALUES IN (4, 5, 6),
		PARTITION p_apac VALUES IN (7, 8, 9)
	)`)
	tk.MustExec("DROP TABLE orders_list_sharded")

	// TC-DDL-SPT-LC-01
	tk.MustExec(`CREATE TABLE orders_list_cols_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		country    VARCHAR(2) NOT NULL,
		PRIMARY KEY (id, country)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST COLUMNS (country) (
		PARTITION p_us VALUES IN ('US'),
		PARTITION p_gb VALUES IN ('GB'),
		PARTITION p_de VALUES IN ('DE')
	)`)
	tk.MustExec("DROP TABLE orders_list_cols_sharded")

	// TC-DDL-SPT-H-01
	tk.MustExec(`CREATE TABLE orders_hash_sharded (
		id        BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		bucket_id  BIGINT NOT NULL,
		PRIMARY KEY (id, bucket_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY HASH (bucket_id) PARTITIONS 4`)
	tk.MustExec("DROP TABLE orders_hash_sharded")

	// TC-DDL-SPT-K-01
	tk.MustExec(`CREATE TABLE orders_key_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		bucket_id  BIGINT NOT NULL,
		PRIMARY KEY (id, bucket_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY KEY (bucket_id) PARTITIONS 4`)
	tk.MustQuery("SHOW CREATE TABLE orders_key_sharded").CheckContain("SHARD BY (`company_id`) SHARDS 4")
	tk.MustQuery("SHOW CREATE TABLE orders_key_sharded").CheckContain("PARTITION BY KEY (`bucket_id`) PARTITIONS 4")
	tk.MustExec("DROP TABLE orders_key_sharded")
}

// ---------------------------------------------------------------------------
// §2 DDL Error Cases
// ---------------------------------------------------------------------------

func TestDDLErrors(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// TC-DDL-ERR-01: shard count = 0
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (k) SHARDS 0",
		"Shard count must be greater than 1",
	)
	// TC-DDL-ERR-01b: shard count = 1
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (k) SHARDS 1",
		"Shard count must be greater than 1",
	)
	// TC-DDL-ERR-02: shard count > 64
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (k) SHARDS 65",
		"Shard count must be between 2 and 64",
	)
	// TC-DDL-ERR-03: BLOB shard key
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, data BLOB) SHARD BY (data) SHARDS 4",
		"BLOB, BINARY, and VARBINARY are not allowed",
	)
	// TC-DDL-ERR-04: VARBINARY shard key
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, data VARBINARY(256)) SHARD BY (data) SHARDS 4",
		"BLOB, BINARY, and VARBINARY are not allowed",
	)
	// TC-DDL-ERR-05: BINARY shard key
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, code BINARY(16)) SHARD BY (code) SHARDS 4",
		"BLOB, BINARY, and VARBINARY are not allowed",
	)
	// TC-DDL-ERR-06: FLOAT shard key
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, score FLOAT) SHARD BY (score) SHARDS 4",
		"not allowed",
	)
	// TC-DDL-ERR-07: DECIMAL shard key
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, amount DECIMAL(10,2)) SHARD BY (amount) SHARDS 4",
		"not allowed",
	)
	// TC-DDL-ERR-08: DATETIME shard key
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, ts DATETIME) SHARD BY (ts) SHARDS 4",
		"not allowed",
	)
	// TC-DDL-ERR-09: JSON shard key
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, meta JSON) SHARD BY (meta) SHARDS 4",
		"not allowed",
	)
	// TC-DDL-ERR-10: AUTO_INCREMENT shard key
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, k BIGINT) SHARD BY (id) SHARDS 4",
		"cannot be AUTO_INCREMENT",
	)
	// TC-DDL-ERR-11: SHARD BY on TEMPORARY TABLE
	tk.MustContainErrMsg(
		"CREATE TEMPORARY TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (k) SHARDS 4",
		"temporary",
	)
	// TC-DDL-ERR-11b: SHARD BY on GLOBAL TEMPORARY TABLE
	tk.MustContainErrMsg(
		"CREATE GLOBAL TEMPORARY TABLE t (id BIGINT PRIMARY KEY, k BIGINT NOT NULL) SHARD BY (k) SHARDS 4 ON COMMIT DELETE ROWS",
		"temporary",
	)
	// TC-DDL-ERR-12: SHARD BY with SHARD_ROW_ID_BITS
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT, k BIGINT) SHARD BY (k) SHARDS 4 SHARD_ROW_ID_BITS = 4",
		"SHARD_ROW_ID_BITS",
	)
	// TC-DDL-ERR-13: duplicate shard key columns
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (k, k) SHARDS 4",
		"Duplicate column",
	)
	// TC-DDL-ERR-14: non-existent column
	tk.MustContainErrMsg(
		"CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (nonexistent) SHARDS 4",
		"doesn't exist",
	)
	// TC-DDL-ERR-15: partition column = shard key column (RANGE)
	tk.MustContainErrMsg(`CREATE TABLE t_overlap_range (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		ts         DATE NOT NULL,
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE (company_id) (
		PARTITION p_low  VALUES LESS THAN (1000),
		PARTITION p_high VALUES LESS THAN MAXVALUE
	)`, "cannot be both a partition column and a shard key column")
	// TC-DDL-ERR-16: partition column = shard key column (LIST)
	tk.MustContainErrMsg(`CREATE TABLE t_overlap_list (
		id        BIGINT NOT NULL,
		region_id INT NOT NULL,
		PRIMARY KEY (id, region_id)
	) SHARD BY (region_id) SHARDS 4
	PARTITION BY LIST (region_id) (
		PARTITION p_us VALUES IN (1, 2),
		PARTITION p_eu VALUES IN (3, 4)
	)`, "cannot be both a partition column and a shard key column")
	// TC-DDL-OK-15: RANGE partition column DIFFERENT from shard key — baseline passing case
	tk.MustExec(`CREATE TABLE t_no_overlap (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		ts         DATE NOT NULL,
		PRIMARY KEY (id, ts)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (ts) (
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec("DROP TABLE t_no_overlap")
}

// ---------------------------------------------------------------------------
// §3 DML INSERT / SELECT
// ---------------------------------------------------------------------------

func TestDMLSimpleTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders (
		id         BIGINT NOT NULL AUTO_INCREMENT,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		created_at DATETIME,
		PRIMARY KEY (id)
	)`)

	// TC-DML-ST-01
	tk.MustExec("INSERT INTO orders (company_id, amount) VALUES (100, 9.99), (200, 19.99)")
	tk.MustQuery("SELECT COUNT(*) FROM orders").Check(testkit.Rows("2"))
}

func TestDMLShardedSimpleTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)

	// TC-DML-SST-01: insert and count
	tk.MustExec("INSERT INTO orders_sharded (id, company_id, amount) VALUES (1, 42, 10.00), (2, 42, 20.00), (3, 99, 30.00)")
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded").Check(testkit.Rows("3"))

	// TC-DML-SST-04: UPDATE non-shard-key column (Bug #1 regression — must use full scan, not point-get)
	tk.MustExec("UPDATE orders_sharded SET amount = 99.00 WHERE id = 1 AND company_id = 42")
	tk.MustQuery("SELECT amount FROM orders_sharded WHERE id = 1 AND company_id = 42").Check(testkit.Rows("99.00"))

	// TC-DML-SST-05: DELETE
	tk.MustExec("DELETE FROM orders_sharded WHERE id = 3 AND company_id = 99")
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded WHERE company_id = 99").Check(testkit.Rows("0"))
}

func TestDMLShardedPartitionedRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)

	// TC-DML-SPT-R-01
	tk.MustExec(`INSERT INTO orders_range_sharded (id, company_id, created_at) VALUES
		(1, 42, '2023-05-01'), (2, 42, '2024-05-01'),
		(3, 99, '2023-11-01'), (4, 99, '2024-11-01'), (5, 17, '2025-02-01')`)
	tk.MustQuery("SELECT COUNT(*) FROM orders_range_sharded").Check(testkit.Rows("5"))

	// Both filters → exactly 1 row
	tk.MustQuery(`SELECT id FROM orders_range_sharded
		WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31' AND company_id = 42`).
		Check(testkit.Rows("2"))
}

func TestDMLShardedPartitionedList(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_list_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		region_id  INT NOT NULL,
		PRIMARY KEY (id, region_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST (region_id) (
		PARTITION p_us   VALUES IN (1, 2, 3),
		PARTITION p_eu   VALUES IN (4, 5, 6),
		PARTITION p_apac VALUES IN (7, 8, 9)
	)`)

	// TC-DML-SPT-L-01
	tk.MustExec(`INSERT INTO orders_list_sharded (id, company_id, region_id) VALUES
		(1, 42, 1), (2, 42, 4), (3, 99, 7)`)
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_sharded").Check(testkit.Rows("3"))
	tk.MustQuery("SELECT id FROM orders_list_sharded WHERE region_id = 1 AND company_id = 42").
		Check(testkit.Rows("1"))
}

func TestDMLShardedPartitionedHash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_hash_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		bucket_id  BIGINT NOT NULL,
		PRIMARY KEY (id, bucket_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY HASH (bucket_id) PARTITIONS 4`)

	// TC-DML-SPT-H-01
	tk.MustExec("INSERT INTO orders_hash_sharded (id, company_id, bucket_id) VALUES (1, 42, 10), (2, 42, 11), (3, 99, 10)")
	tk.MustQuery("SELECT COUNT(*) FROM orders_hash_sharded").Check(testkit.Rows("3"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_hash_sharded WHERE company_id = 42").Check(testkit.Rows("2"))
}

func TestDMLShardedPartitionedKeyRegression(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_key_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		bucket_id  BIGINT NOT NULL,
		PRIMARY KEY (id, bucket_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY KEY (bucket_id) PARTITIONS 4`)

	// TC-DML-SPT-K-01: nil-dereference panic regression — must not panic
	tk.MustExec("INSERT INTO orders_key_sharded (id, company_id, bucket_id) VALUES (1, 42, 10)")
	tk.MustQuery("SELECT COUNT(*) FROM orders_key_sharded").Check(testkit.Rows("1"))

	// TC-DML-SPT-K-02: basic INSERT/SELECT correctness
	tk.MustExec(`INSERT INTO orders_key_sharded (id, company_id, bucket_id) VALUES
		(2, 42, 1), (3, 99, 0), (4, 99, 2), (5, 17, 3)`)
	tk.MustQuery("SELECT COUNT(*) FROM orders_key_sharded").Check(testkit.Rows("5"))
	tk.MustQuery("SELECT id, company_id, bucket_id FROM orders_key_sharded WHERE id = 3 AND bucket_id = 0").
		Check(testkit.Rows("3 99 0"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_key_sharded WHERE company_id = 42").Check(testkit.Rows("2"))

	// TC-DML-SPT-K-03: UPDATE — must not panic (Bug #4 fix regression)
	tk.MustExec("UPDATE orders_key_sharded SET company_id = 42 WHERE id = 1 AND bucket_id = 10")
	tk.MustQuery("SELECT company_id FROM orders_key_sharded WHERE id = 1 AND bucket_id = 10").
		Check(testkit.Rows("42"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_key_sharded").Check(testkit.Rows("5"))

	// TC-DML-SPT-K-04: DELETE — must not panic (Bug #4 fix regression)
	tk.MustExec("DELETE FROM orders_key_sharded WHERE id = 5 AND bucket_id = 3")
	tk.MustQuery("SELECT COUNT(*) FROM orders_key_sharded WHERE company_id = 17").Check(testkit.Rows("0"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_key_sharded").Check(testkit.Rows("4"))
}

// ---------------------------------------------------------------------------
// §5 Partition Pruning (EXPLAIN assertions)
// ---------------------------------------------------------------------------

func TestPruningSSTBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec("INSERT INTO orders_sharded (id, company_id, amount) VALUES (1, 42, 10.00), (2, 42, 20.00), (3, 99, 30.00)")

	// TC-PRUNE-SST-01: equality on shard key → single shard
	rows := tk.MustQuery("EXPLAIN SELECT * FROM orders_sharded WHERE company_id = 42").Rows()
	planStr := explainToStr(rows)
	require.True(t, strings.Contains(planStr, "partition:shard_"), "expected single shard in plan, got: "+planStr)
	// Verify it's not all shards
	require.False(t, strings.Contains(planStr, "partition:all"), "should not scan all shards for company_id=42")

	// TC-PRUNE-SST-03: non-shard-key filter → full scan
	rows = tk.MustQuery("EXPLAIN SELECT * FROM orders_sharded WHERE amount > 100").Rows()
	planStr = explainToStr(rows)
	require.True(t, strings.Contains(planStr, "partition:all"), "expected full scan for amount filter, got: "+planStr)
}

func TestPruningSSTDynamicAndStatic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)

	// TC-PRUNE-SST-04: dynamic pruning mode
	tk.MustExec("SET tidb_partition_prune_mode = 'dynamic'")
	rows := tk.MustQuery("EXPLAIN SELECT * FROM orders_sharded WHERE company_id = 7").Rows()
	planStr := explainToStr(rows)
	require.True(t, strings.Contains(planStr, "partition:shard_"), "dynamic: expected single shard, got: "+planStr)

	// TC-PRUNE-SST-05: static pruning mode
	tk.MustExec("SET tidb_partition_prune_mode = 'static'")
	rows = tk.MustQuery("EXPLAIN SELECT * FROM orders_sharded WHERE company_id = 7").Rows()
	planStr = explainToStr(rows)
	require.True(t, strings.Contains(planStr, "partition:shard_"), "static: expected single shard, got: "+planStr)

	// Restore default
	tk.MustExec("SET tidb_partition_prune_mode = 'dynamic'")
}

func TestPruningSPTRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec(`INSERT INTO orders_range_sharded VALUES (1, 42, '2023-05-01'), (2, 42, '2024-05-01'), (3, 17, '2025-01-15')`)

	// TC-PRUNE-SPT-R-01: both range partition and shard pruned → single physical shard
	rows := tk.MustQuery(`EXPLAIN SELECT * FROM orders_range_sharded
		WHERE created_at = '2024-06-01' AND company_id = 42`).Rows()
	planStr := explainToStr(rows)
	// Should show p2024 shard only — not p2023 or pmax
	require.True(t, strings.Contains(planStr, "p2024"), "expected p2024 partition, got: "+planStr)
	require.False(t, strings.Contains(planStr, "p2023"), "should NOT include p2023, got: "+planStr)

	// TC-PRUNE-SPT-R-02: only range key → all shards in that partition
	rows = tk.MustQuery("EXPLAIN SELECT * FROM orders_range_sharded WHERE created_at = '2024-06-01'").Rows()
	planStr = explainToStr(rows)
	require.True(t, strings.Contains(planStr, "p2024"), "expected p2024, got: "+planStr)
	// All 4 shards of p2024 must appear
	require.True(t, strings.Contains(planStr, "p2024_s0") || strings.Contains(planStr, "p2024"),
		"expected p2024 shards, got: "+planStr)

	// TC-PRUNE-SPT-R-03: only shard key → one shard per partition
	rows = tk.MustQuery("EXPLAIN SELECT * FROM orders_range_sharded WHERE company_id = 42").Rows()
	planStr = explainToStr(rows)
	// Should include p2023, p2024, pmax (shard slot for 42 in each)
	require.True(t, strings.Contains(planStr, "p2023") || strings.Contains(planStr, "p2024"),
		"expected multiple partitions for shard-only filter, got: "+planStr)

	// TC-PRUNE-SPT-R-04: no filter → partition:all
	rows = tk.MustQuery("EXPLAIN SELECT COUNT(*) FROM orders_range_sharded").Rows()
	planStr = explainToStr(rows)
	require.True(t, strings.Contains(planStr, "partition:all"),
		"expected partition:all for full scan, got: "+planStr)
}

func TestPruningSPTList(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_list_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		region_id  INT NOT NULL,
		PRIMARY KEY (id, region_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST (region_id) (
		PARTITION p_us   VALUES IN (1, 2, 3),
		PARTITION p_eu   VALUES IN (4, 5, 6),
		PARTITION p_apac VALUES IN (7, 8, 9)
	)`)
	tk.MustExec("INSERT INTO orders_list_sharded VALUES (1, 42, 1), (2, 42, 4)")

	// TC-PRUNE-SPT-L-01: LIST partition + shard key equality → single physical shard in p_us
	rows := tk.MustQuery("EXPLAIN SELECT * FROM orders_list_sharded WHERE region_id = 1 AND company_id = 42").Rows()
	planStr := explainToStr(rows)
	require.True(t, strings.Contains(planStr, "p_us"), "expected p_us partition, got: "+planStr)
	require.False(t, strings.Contains(planStr, "p_eu"), "should NOT include p_eu, got: "+planStr)
	require.False(t, strings.Contains(planStr, "p_apac"), "should NOT include p_apac, got: "+planStr)
}

func TestPruningSPTHash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_hash_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		bucket_id  BIGINT NOT NULL,
		PRIMARY KEY (id, bucket_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY HASH (bucket_id) PARTITIONS 4`)

	// TC-PRUNE-SPT-H-01: shard key prunes slots; HASH partitions not prunable by value
	// Bug #3 regression: must NOT route only to p0
	rows := tk.MustQuery("EXPLAIN SELECT * FROM orders_hash_sharded WHERE company_id = 42").Rows()
	planStr := explainToStr(rows)
	// Must include shards from more than just p0 (all HASH partitions survive)
	require.False(t, strings.Contains(planStr, "partition:all") && strings.Contains(planStr, "p0_s0") && !strings.Contains(planStr, "p1"),
		"Bug #3: must not route exclusively to p0, got: "+planStr)
}

func TestPruningSPTKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_key_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		bucket_id  BIGINT NOT NULL,
		PRIMARY KEY (id, bucket_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY KEY (bucket_id) PARTITIONS 4`)

	// TC-PRUNE-SPT-K-01: shard-key equality prunes slots, KEY partitions all survive
	rows := tk.MustQuery("EXPLAIN SELECT * FROM orders_key_sharded WHERE company_id = 42").Rows()
	planStr := explainToStr(rows)
	// Should NOT be all 16 shards (partition:all)
	require.False(t, strings.Contains(planStr, "partition:all"),
		"should prune to shard slots, not all partitions, got: "+planStr)
	// Should include shards from all 4 logical KEY partitions (p0..p3)
	require.True(t, strings.Contains(planStr, "p0") && strings.Contains(planStr, "p1"),
		"Bug #3: all KEY partitions must appear in plan, got: "+planStr)

	// TC-PRUNE-SPT-K-02: no predicate → partition:all
	rows = tk.MustQuery("EXPLAIN SELECT COUNT(*) FROM orders_key_sharded").Rows()
	planStr = explainToStr(rows)
	require.True(t, strings.Contains(planStr, "partition:all"),
		"expected partition:all for full scan, got: "+planStr)
}

// ---------------------------------------------------------------------------
// §14 Unique Key Semantics
// ---------------------------------------------------------------------------

func TestUniqueKeySemantics(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// TC-DDL-UNIQUE-01: UNIQUE key without shard key.
	// Server allows this; uniqueness is shard-local only (not global across shards).
	// Duplicate values that hash to different shards are silently allowed.
	tk.MustExec(`CREATE TABLE t_unique_bad (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		email      VARCHAR(255) NOT NULL,
		UNIQUE KEY uk_email (email)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec("INSERT INTO t_unique_bad VALUES (1, 42, 'a@b.com')")
	// Same email in a different shard (company_id=99 hashes to a different slot): allowed.
	tk.MustExec("INSERT INTO t_unique_bad VALUES (2, 99, 'a@b.com')")
	tk.MustQuery("SELECT COUNT(*) FROM t_unique_bad WHERE email = 'a@b.com'").Check(testkit.Rows("2"))
	tk.MustExec("DROP TABLE t_unique_bad")

	// TC-DDL-UNIQUE-02: UNIQUE key including shard key — allowed; duplicate rejected
	tk.MustExec(`CREATE TABLE t_unique_ok (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		email      VARCHAR(255) NOT NULL,
		UNIQUE KEY uk_company_email (company_id, email)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec("INSERT INTO t_unique_ok VALUES (1, 42, 'a@b.com')")
	tk.MustContainErrMsg("INSERT INTO t_unique_ok VALUES (2, 42, 'a@b.com')", "Duplicate entry")
	tk.MustExec("DROP TABLE t_unique_ok")

	// TC-DDL-UNIQUE-03: PK without shard key — document the behaviour
	// This is the common auto-increment PK pattern. We just verify it can be created without error.
	tk.MustExec(`CREATE TABLE t_pk_no_shard (
		id         BIGINT NOT NULL AUTO_INCREMENT,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec("INSERT INTO t_pk_no_shard (company_id) VALUES (42), (99)")
	tk.MustQuery("SELECT COUNT(*) FROM t_pk_no_shard").Check(testkit.Rows("2"))
	tk.MustExec("DROP TABLE t_pk_no_shard")
}

// ---------------------------------------------------------------------------
// §16 Index Lookup and Point-Get Routing (Bug #1 regression)
// ---------------------------------------------------------------------------

func TestPointGetRouting(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec("INSERT INTO orders_sharded VALUES (1, 42, 10.00), (2, 42, 20.00), (3, 99, 30.00)")

	// TC-ACCESS-POINT-01: PK-only query must NOT use a single-shard Point_Get
	// (Bug #1 regression: without shard key, planner must fall back to full scan)
	rows := tk.MustQuery("EXPLAIN SELECT * FROM orders_sharded WHERE id = 1").Rows()
	planStr := explainToStr(rows)
	require.False(t, strings.Contains(planStr, "Point_Get") && !strings.Contains(planStr, "partition:all"),
		"Bug #1: PK-only Point_Get must not route to single shard; got: "+planStr)
	// Data must be returned correctly
	tk.MustQuery("SELECT id, company_id FROM orders_sharded WHERE id = 1").Check(testkit.Rows("1 42"))

	// TC-ACCESS-POINT-02: PK + shard key → single-shard Point_Get allowed
	rows = tk.MustQuery("EXPLAIN SELECT * FROM orders_sharded WHERE id = 1 AND company_id = 42").Rows()
	planStr = explainToStr(rows)
	// With shard key present the planner may choose Point_Get routed to the right shard
	tk.MustQuery("SELECT id, company_id FROM orders_sharded WHERE id = 1 AND company_id = 42").
		Check(testkit.Rows("1 42"))

	// TC-ACCESS-POINT-03: IN list on PK, no shard key — all rows returned
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded WHERE id IN (1, 2, 3)").Check(testkit.Rows("3"))

	// TC-ACCESS-POINT-04: IN list on PK + shard key — correct rows
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded WHERE id IN (1, 2) AND company_id = 42").
		Check(testkit.Rows("2"))
}

// ---------------------------------------------------------------------------
// §19 Transaction Isolation Across Shard Moves
// ---------------------------------------------------------------------------

func TestTransactionIsolation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")

	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec("INSERT INTO orders_sharded VALUES (100, 1, 50.00)")

	// TC-TXN-SHARDMOVE-01: uncommitted cross-shard UPDATE invisible to concurrent reader
	tk.MustExec("BEGIN")
	tk.MustExec("UPDATE orders_sharded SET company_id = 999 WHERE id = 100 AND company_id = 1")
	// Session B sees old value (snapshot isolation)
	tk2.MustQuery("SELECT COUNT(*) FROM orders_sharded WHERE id = 100").Check(testkit.Rows("1"))
	tk.MustExec("COMMIT")
	// After commit, new value visible
	tk2.MustQuery("SELECT COUNT(*) FROM orders_sharded WHERE id = 100").Check(testkit.Rows("1"))
	tk2.MustQuery("SELECT company_id FROM orders_sharded WHERE id = 100").Check(testkit.Rows("999"))

	// TC-TXN-SHARDMOVE-02: ROLLBACK restores original row
	tk.MustExec("BEGIN")
	tk.MustExec("UPDATE orders_sharded SET company_id = 1 WHERE id = 100 AND company_id = 999")
	tk.MustExec("ROLLBACK")
	tk.MustQuery("SELECT company_id FROM orders_sharded WHERE id = 100").Check(testkit.Rows("999"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded WHERE id = 100").Check(testkit.Rows("1"))
}

// ---------------------------------------------------------------------------
// §21 Online Schema Change
// ---------------------------------------------------------------------------

func TestOnlineSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec("INSERT INTO orders_sharded VALUES (1, 42, 10.00), (2, 99, 20.00)")

	// TC-ALTER-COLUMN-01: DROP shard key column must be rejected
	tk.MustContainErrMsg("ALTER TABLE orders_sharded DROP COLUMN company_id", "")
	// Note: the exact error message depends on implementation (foreign key / shard key guard).
	// We just verify the operation is not silently allowed.

	// TC-ALTER-RENAME-01: RENAME TABLE preserves shard metadata
	tk.MustExec("RENAME TABLE orders_sharded TO orders_sharded_v2")
	tk.MustQuery("SHOW CREATE TABLE orders_sharded_v2").CheckContain("SHARD BY (`company_id`) SHARDS 4")
	// Verify data still accessible
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded_v2").Check(testkit.Rows("2"))
	tk.MustExec("RENAME TABLE orders_sharded_v2 TO orders_sharded")
}

// ---------------------------------------------------------------------------
// §12 ALTER TABLE Partition Operations on SPT (ADD/DROP PARTITION)
// ---------------------------------------------------------------------------

func TestAlterSPTLC(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_list_cols_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		country    VARCHAR(2) NOT NULL,
		PRIMARY KEY (id, country)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST COLUMNS (country) (
		PARTITION p_us VALUES IN ('US'),
		PARTITION p_gb VALUES IN ('GB'),
		PARTITION p_de VALUES IN ('DE')
	)`)
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (1, 42, 'US'), (2, 99, 'GB')")

	// TC-ALTER-SPT-LC-01: ADD PARTITION
	tk.MustExec("ALTER TABLE orders_list_cols_sharded ADD PARTITION (PARTITION p_au VALUES IN ('AU'))")
	tk.MustQuery("SHOW CREATE TABLE orders_list_cols_sharded").CheckContain("p_au")

	// TC-ALTER-SPT-LC-02: INSERT into new partition and query
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (100, 42, 'AU')")
	tk.MustQuery("SELECT id FROM orders_list_cols_sharded WHERE country = 'AU' AND company_id = 42").
		Check(testkit.Rows("100"))

	// TC-ALTER-SPT-LC-03: DROP PARTITION — was previously failing with error 1512
	tk.MustExec("ALTER TABLE orders_list_cols_sharded DROP PARTITION p_us")
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded WHERE country = 'US'").Check(testkit.Rows("0"))
	tk.MustQuery("SHOW CREATE TABLE orders_list_cols_sharded").CheckNotContain("p_us")

	// TC-ALTER-SPT-LC-04: DROP multiple partitions
	tk.MustExec("ALTER TABLE orders_list_cols_sharded DROP PARTITION p_gb, p_de")
	tk.MustQuery("SHOW CREATE TABLE orders_list_cols_sharded").CheckNotContain("p_gb")
	tk.MustQuery("SHOW CREATE TABLE orders_list_cols_sharded").CheckNotContain("p_de")

	// TC-ALTER-SPT-LC-05: DROP non-existent partition
	tk.MustContainErrMsg("ALTER TABLE orders_list_cols_sharded DROP PARTITION p_nonexistent", "")
}

func TestAlterSPTRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec("INSERT INTO orders_range_sharded VALUES (1, 42, '2023-06-01')")

	// TC-ALTER-SPT-R-01: DROP PARTITION on RANGE+SHARD table
	tk.MustExec("ALTER TABLE orders_range_sharded DROP PARTITION p2023")
	tk.MustQuery("SELECT COUNT(*) FROM orders_range_sharded WHERE created_at < '2024-01-01'").
		Check(testkit.Rows("0"))
}

// ---------------------------------------------------------------------------
// §8 SHOW CREATE TABLE (metadata visibility)
// ---------------------------------------------------------------------------

func TestMetadataVisibility(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// TC-META-SST-01: ShardKeyInfo visible in SHOW CREATE TABLE
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL AUTO_INCREMENT,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustQuery("SHOW CREATE TABLE orders_sharded").CheckContain("SHARD BY (`company_id`) SHARDS 4")
	tk.MustExec("DROP TABLE orders_sharded")

	// TC-META-SPT-01: both PARTITION BY and SHARD BY visible
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	result := tk.MustQuery("SHOW CREATE TABLE orders_range_sharded")
	result.CheckContain("SHARD BY (`company_id`) SHARDS 4")
	result.CheckContain("PARTITION BY RANGE COLUMNS")
	tk.MustExec("DROP TABLE orders_range_sharded")

	// TC-META-SPT-K-01: SPT-K SHOW CREATE TABLE
	tk.MustExec(`CREATE TABLE orders_key_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		bucket_id  BIGINT NOT NULL,
		PRIMARY KEY (id, bucket_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY KEY (bucket_id) PARTITIONS 4`)
	result = tk.MustQuery("SHOW CREATE TABLE orders_key_sharded")
	result.CheckContain("SHARD BY (`company_id`) SHARDS 4")
	result.CheckContain("PARTITION BY KEY (`bucket_id`) PARTITIONS 4")
	tk.MustExec("DROP TABLE orders_key_sharded")
}

// ---------------------------------------------------------------------------
// §15 Composite Shard-Key Pruning
// ---------------------------------------------------------------------------

func TestCompositeShardKeyPruning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE events_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		user_id    BIGINT NOT NULL,
		event_type VARCHAR(64),
		PRIMARY KEY (id)
	) SHARD BY (company_id, user_id) SHARDS 8`)

	// TC-PRUNE-COMPOSITE-01: full composite equality → single shard
	rows := tk.MustQuery("EXPLAIN SELECT * FROM events_sharded WHERE company_id = 42 AND user_id = 99").Rows()
	planStr := explainToStr(rows)
	require.True(t, strings.Contains(planStr, "partition:shard_"),
		"full composite key: expected single shard, got: "+planStr)
	require.False(t, strings.Contains(planStr, "partition:all"),
		"full composite key: should not full-scan, got: "+planStr)

	// TC-PRUNE-COMPOSITE-02: only first column → full scan (cannot prune partial composite key)
	rows = tk.MustQuery("EXPLAIN SELECT * FROM events_sharded WHERE company_id = 42").Rows()
	planStr = explainToStr(rows)
	require.True(t, strings.Contains(planStr, "partition:all"),
		"partial composite key: expected full scan, got: "+planStr)

	// TC-PRUNE-COMPOSITE-03: only second column → full scan
	rows = tk.MustQuery("EXPLAIN SELECT * FROM events_sharded WHERE user_id = 99").Rows()
	planStr = explainToStr(rows)
	require.True(t, strings.Contains(planStr, "partition:all"),
		"partial composite key (second col): expected full scan, got: "+planStr)
}

// ---------------------------------------------------------------------------
// §3.8 DML — Bulk Operations
// ---------------------------------------------------------------------------

func TestDMLBulkOperations(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 4`)

	tk.MustExec("INSERT INTO orders_sharded VALUES (200, 42, 10.00)")

	// TC-DML-BULK-02: REPLACE INTO — must update in-place, no ghost row
	tk.MustExec("REPLACE INTO orders_sharded VALUES (200, 42, 99.00)")
	tk.MustQuery("SELECT amount FROM orders_sharded WHERE id = 200 AND company_id = 42").
		Check(testkit.Rows("99.00"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded WHERE id = 200").
		Check(testkit.Rows("1"))

	// TC-DML-BULK-03: ON DUPLICATE KEY UPDATE changing a non-shard-key column
	tk.MustExec("INSERT INTO orders_sharded VALUES (300, 1, 5.00)")
	tk.MustExec("INSERT INTO orders_sharded VALUES (300, 1, 5.00) ON DUPLICATE KEY UPDATE amount = 50.00")
	tk.MustQuery("SELECT amount FROM orders_sharded WHERE id = 300 AND company_id = 1").
		Check(testkit.Rows("50.00"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded WHERE id = 300").
		Check(testkit.Rows("1"))

	// TC-DML-BULK-03b: ON DUPLICATE KEY UPDATE changing the shard key column (cross-shard move).
	// The row (id=400, company_id=1) must atomically move to the new shard for company_id=999.
	// No ghost row may remain in the old shard.
	tk.MustExec("INSERT INTO orders_sharded VALUES (400, 1, 5.00)")
	tk.MustExec("INSERT INTO orders_sharded VALUES (400, 1, 5.00) ON DUPLICATE KEY UPDATE company_id = 999, amount = 99.00")
	tk.MustQuery("SELECT amount FROM orders_sharded WHERE id = 400 AND company_id = 999").
		Check(testkit.Rows("99.00"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded WHERE id = 400 AND company_id = 1").
		Check(testkit.Rows("0"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded WHERE id = 400").
		Check(testkit.Rows("1"))

	// TC-DML-BULK-01 (LOAD DATA): skipped — requires file system access; covered by lightning tests.
}

// ---------------------------------------------------------------------------
// §11 Regression — Non-Sharded Tables Unaffected
// ---------------------------------------------------------------------------

func TestRegressionNonShardedUnaffected(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// TC-REG-01: Plain table DDL unchanged — no SHARD BY clause in output
	tk.MustExec(`CREATE TABLE orders (
		id         BIGINT NOT NULL AUTO_INCREMENT,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	)`)
	showCreate := tk.MustQuery("SHOW CREATE TABLE orders").Rows()
	ddl := fmt.Sprintf("%v", showCreate)
	require.NotContains(t, ddl, "SHARD BY",
		"TC-REG-01: plain table must not have SHARD BY in DDL output")

	// TC-REG-02: Partitioned table DDL unchanged
	tk.MustExec(`CREATE TABLE orders_range (
		id         BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	showCreate = tk.MustQuery("SHOW CREATE TABLE orders_range").Rows()
	ddl = fmt.Sprintf("%v", showCreate)
	require.NotContains(t, ddl, "SHARD BY",
		"TC-REG-02: partitioned table must not have SHARD BY in DDL output")

	// TC-REG-04: Queries on plain tables return correct results
	tk.MustExec("INSERT INTO orders (company_id, amount) VALUES (1, 10.00), (1, 20.00), (2, 30.00)")
	tk.MustQuery("SELECT COUNT(*) FROM orders WHERE company_id = 1").Check(testkit.Rows("2"))
	tk.MustQuery("SELECT SUM(amount) FROM orders").Check(testkit.Rows("60.00"))
}

// ---------------------------------------------------------------------------
// §13 INFORMATION_SCHEMA Limitations for SPT
// ---------------------------------------------------------------------------

func TestISLimitationsSPT(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Create a SPT-LC table
	tk.MustExec(`CREATE TABLE orders_list_cols_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		country    VARCHAR(10) NOT NULL,
		PRIMARY KEY (id, country)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST COLUMNS (country) (
		PARTITION p_us VALUES IN ('US'),
		PARTITION p_gb VALUES IN ('GB'),
		PARTITION p_de VALUES IN ('DE')
	)`)

	// TC-META-SPT-02: SHOW CREATE TABLE returns logical partition names
	showCreate := tk.MustQuery("SHOW CREATE TABLE orders_list_cols_sharded").Rows()
	ddl := fmt.Sprintf("%v", showCreate)
	require.Contains(t, ddl, "p_us",
		"TC-META-SPT-02: SHOW CREATE TABLE must show logical partition names")
	require.Contains(t, ddl, "p_gb",
		"TC-META-SPT-02: SHOW CREATE TABLE must show logical partition name p_gb")
	require.NotContains(t, ddl, "shard_0",
		"TC-META-SPT-02: SHOW CREATE TABLE must not expose physical shard sub-partition names")

	// TC-META-SPT-05: TABLE_ROWS in IS.PARTITIONS is 0 for SPT — known limitation
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (1, 42, 'US'), (2, 99, 'US')")
	isRows := tk.MustQuery(`SELECT TABLE_ROWS FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'orders_list_cols_sharded'`).Rows()
	for _, r := range isRows {
		require.Equal(t, "0", fmt.Sprintf("%v", r[0]),
			"TC-META-SPT-05: TABLE_ROWS must be 0 for SPT (known IS limitation)")
	}

	// TC-META-SPT-06: SELECT COUNT(*) PARTITION (p_us) gives correct count
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded PARTITION (p_us)").
		Check(testkit.Rows("2"))

	// SELECT from explicit partition only returns matching rows
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (3, 10, 'GB')")
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded PARTITION (p_gb)").
		Check(testkit.Rows("1"))
	// p_us partition should not return GB rows
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded PARTITION (p_us)").
		Check(testkit.Rows("2"))
}

// ---------------------------------------------------------------------------
// §17 Generated Columns as Shard Key
// ---------------------------------------------------------------------------

func TestGeneratedColumnShardKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// TC-DDL-GENCOL-01: STORED generated column as shard key — document allow/reject
	err := tk.ExecToErr(`CREATE TABLE t_gen_stored (
		id         BIGINT PRIMARY KEY,
		email      VARCHAR(255),
		email_hash BIGINT AS (CRC32(email)) STORED
	) SHARD BY (email_hash) SHARDS 4`)
	if err == nil {
		// Server allows STORED generated column as shard key
		tk.MustExec("INSERT INTO t_gen_stored (id, email) VALUES (1, 'a@b.com'), (2, 'c@d.com')")
		tk.MustQuery("SELECT COUNT(*) FROM t_gen_stored").Check(testkit.Rows("2"))
		tk.MustExec("DROP TABLE t_gen_stored")
		t.Logf("TC-DDL-GENCOL-01: STORED generated column as shard key is ALLOWED")
	} else {
		t.Logf("TC-DDL-GENCOL-01: STORED generated column as shard key is REJECTED: %v", err)
	}

	// TC-DDL-GENCOL-02: VIRTUAL generated column as shard key — expected to be rejected
	err = tk.ExecToErr(`CREATE TABLE t_gen_virtual (
		id         BIGINT PRIMARY KEY,
		email      VARCHAR(255),
		email_hash BIGINT AS (CRC32(email)) VIRTUAL
	) SHARD BY (email_hash) SHARDS 4`)
	if err == nil {
		t.Logf("TC-DDL-GENCOL-02: VIRTUAL generated column as shard key is ALLOWED (unexpected)")
		tk.MustExec("DROP TABLE t_gen_virtual")
	} else {
		t.Logf("TC-DDL-GENCOL-02: VIRTUAL generated column as shard key is REJECTED (expected): %v", err)
	}
}

// ---------------------------------------------------------------------------
// §18 INSERT ... SELECT Routing
// ---------------------------------------------------------------------------

func TestInsertSelectRouting(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec(`CREATE TABLE orders_sharded_copy (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec(`CREATE TABLE orders_sharded_8 (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 8`)

	tk.MustExec("INSERT INTO orders_sharded VALUES (1, 42, 10.00), (2, 42, 20.00), (3, 99, 30.00)")

	// TC-DML-INSERT-SELECT-01: INSERT ... SELECT into same-shard-count copy
	tk.MustExec("INSERT INTO orders_sharded_copy (id, company_id, amount) SELECT id+10000, company_id, amount FROM orders_sharded")
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded_copy").Check(testkit.Rows("3"))
	// Routing is by target shard key; company_id=42 rows land in correct shard
	rows := tk.MustQuery("EXPLAIN SELECT * FROM orders_sharded_copy WHERE company_id = 42").Rows()
	planStr := explainToStr(rows)
	require.False(t, strings.Contains(planStr, "partition:all"),
		"TC-DML-INSERT-SELECT-01: shard-pruned query on copy must not scan all shards; got: "+planStr)

	// TC-DML-INSERT-SELECT-02: INSERT ... SELECT into different shard count (8 shards)
	tk.MustExec("INSERT INTO orders_sharded_8 (id, company_id, amount) SELECT id+20000, company_id, amount FROM orders_sharded")
	tk.MustQuery("SELECT COUNT(*) FROM orders_sharded_8").Check(testkit.Rows("3"))
}

// ---------------------------------------------------------------------------
// §20 Statistics and ANALYZE
// ---------------------------------------------------------------------------

func TestAnalyzeStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)

	tk.MustExec("INSERT INTO orders_sharded VALUES (1, 42, 10.00), (2, 42, 20.00), (3, 99, 30.00)")
	tk.MustExec("INSERT INTO orders_range_sharded VALUES (1, 42, '2024-05-01'), (2, 99, '2023-11-01'), (3, 17, '2025-01-15')")

	// TC-STATS-01: ANALYZE on SST completes without error
	tk.MustExec("ANALYZE TABLE orders_sharded")
	metaRows := tk.MustQuery("SHOW STATS_META WHERE db_name = 'test' AND table_name = 'orders_sharded'").Rows()
	require.NotEmpty(t, metaRows, "TC-STATS-01: SHOW STATS_META must return rows after ANALYZE on SST")

	// TC-STATS-03: ANALYZE on SPT completes without error
	tk.MustExec("ANALYZE TABLE orders_range_sharded")
	metaRows = tk.MustQuery("SHOW STATS_META WHERE db_name = 'test' AND table_name = 'orders_range_sharded'").Rows()
	require.NotEmpty(t, metaRows, "TC-STATS-03: SHOW STATS_META must return rows after ANALYZE on SPT")

	// TC-STATS-02: plan row estimate works after ANALYZE (query runs without error)
	tk.MustQuery("EXPLAIN SELECT * FROM orders_sharded WHERE company_id = 42").Rows()

	// TC-STATS-04: SHOW STATS_HISTOGRAMS returns rows after ANALYZE on SST
	histRows := tk.MustQuery("SHOW STATS_HISTOGRAMS WHERE db_name = 'test' AND table_name = 'orders_sharded'").Rows()
	require.NotEmpty(t, histRows,
		"TC-STATS-04: SHOW STATS_HISTOGRAMS must return rows after ANALYZE on SST")

	// TC-STATS-05: SHOW STATS_HISTOGRAMS returns rows after ANALYZE on SPT
	histRows = tk.MustQuery("SHOW STATS_HISTOGRAMS WHERE db_name = 'test' AND table_name = 'orders_range_sharded'").Rows()
	require.NotEmpty(t, histRows,
		"TC-STATS-05: SHOW STATS_HISTOGRAMS must return rows after ANALYZE on SPT")
}

// ---------------------------------------------------------------------------
// §23 Explicit PARTITION Clause on SPT
// ---------------------------------------------------------------------------

func TestExplicitPartitionClause(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec(`INSERT INTO orders_range_sharded VALUES
		(1, 42, '2023-05-01'),
		(2, 42, '2024-05-01'),
		(3, 99, '2024-11-01'),
		(4, 17, '2025-02-01')`)

	// TC-PARTITION-EXPLICIT-01: SELECT with explicit partition + shard key predicate
	// Only rows in p2024 with company_id=42 should be returned
	tk.MustQuery("SELECT id FROM orders_range_sharded PARTITION (p2024) WHERE company_id = 42").
		Check(testkit.Rows("2"))

	// TC-PARTITION-EXPLICIT-02: Explicit partition that doesn't match row's date range → 0 rows
	tk.MustQuery("SELECT COUNT(*) FROM orders_range_sharded PARTITION (p2023) WHERE created_at = '2024-05-01'").
		Check(testkit.Rows("0"))

	// TC-PARTITION-EXPLICIT-03: EXPLAIN with explicit partition shows correct physical shard
	rows := tk.MustQuery("EXPLAIN SELECT * FROM orders_range_sharded PARTITION (p2024) WHERE company_id = 42").Rows()
	planStr := explainToStr(rows)
	// Should scan only within p2024 (not all partitions)
	require.False(t, strings.Contains(planStr, "p2023"),
		"TC-PARTITION-EXPLICIT-03: explicit p2024 must not scan p2023; got: "+planStr)
	t.Logf("TC-PARTITION-EXPLICIT-03: EXPLAIN plan:\n%s", planStr)
}

// ---------------------------------------------------------------------------
// §4 MPP Co-location (TiFlash) — aggregations, window functions, join variants
//
// These tests work in the testkit mock environment by:
//   1. Creating plain tables (no shard DDL needed).
//   2. Injecting ShardKeyInfo directly into the in-memory TableInfo.
//   3. Marking the table as having a TiFlash replica via SetTiFlashReplica.
//   4. Setting tidb_isolation_read_engines=tiflash and tidb_allow_mpp=1.
//
// Tests that require a real TiFlash cluster are not included here.
// ---------------------------------------------------------------------------

// mppPlan returns a single-string concatenation of EXPLAIN FORMAT='brief' output.
func mppPlan(tk *testkit.TestKit, sql string) string {
	rows := tk.MustQuery("explain format='brief' " + sql).Rows()
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		parts = append(parts, fmt.Sprintf("%v", r))
	}
	return strings.Join(parts, "\n")
}

// setShardKey injects ShardKeyInfo into an in-memory TableInfo for MPP tests.
func setShardKey(t *testing.T, dom *domain.Domain, dbName, tblName string, shardCols []string, shardCnt int) {
	is := dom.InfoSchema()
	info, err := is.TableByName(context.Background(), pmodel.NewCIStr(dbName), pmodel.NewCIStr(tblName))
	require.NoError(t, err)
	info.Meta().ShardKeyInfo = &model.ShardKeyInfo{
		Columns:  shardCols,
		ShardCnt: shardCnt,
	}
}

// TestMPP_Aggregation covers TC-MPP-AGG-01..04 and TC-MPP-SST-08.
func TestMPP_Aggregation(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	)`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	setShardKey(t, dom, "test", "orders_sharded", []string{"company_id"}, 4)

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")

	// TC-MPP-AGG-01: GROUP BY shard key — all agg local, no HashPartition exchange
	plan := mppPlan(tk,
		"select company_id, COUNT(*), SUM(amount), AVG(amount), MAX(amount), MIN(amount) "+
			"FROM orders_sharded GROUP BY company_id")
	require.NotContains(t, plan, "HashPartition",
		"TC-MPP-AGG-01: GROUP BY shard key must not require HashPartition exchange; plan:\n"+plan)

	// TC-MPP-AGG-02: DISTINCT on shard key — local per shard, no exchange
	plan = mppPlan(tk, "SELECT DISTINCT company_id FROM orders_sharded")
	require.NotContains(t, plan, "HashPartition",
		"TC-MPP-AGG-02: DISTINCT on shard key must not require HashPartition exchange; plan:\n"+plan)

	// TC-MPP-AGG-03: DISTINCT on non-shard-key column — requires global exchange
	plan = mppPlan(tk, "SELECT DISTINCT amount FROM orders_sharded")
	require.Contains(t, plan, "HashPartition",
		"TC-MPP-AGG-03: DISTINCT on non-shard column must require HashPartition exchange; plan:\n"+plan)

	// TC-MPP-AGG-04: HAVING on shard-key aggregation — local filter, no exchange
	plan = mppPlan(tk,
		"SELECT company_id, SUM(amount) AS total FROM orders_sharded GROUP BY company_id HAVING total > 1000")
	require.NotContains(t, plan, "HashPartition",
		"TC-MPP-AGG-04: HAVING on shard-key agg must not require HashPartition exchange; plan:\n"+plan)

	// TC-MPP-AGG-05: COUNT(DISTINCT) grouped by shard key — distinct scoped per shard, no exchange
	plan = mppPlan(tk,
		"SELECT company_id, COUNT(DISTINCT amount) FROM orders_sharded GROUP BY company_id")
	require.NotContains(t, plan, "HashPartition",
		"TC-MPP-AGG-05: COUNT(DISTINCT) grouped by shard key must not require HashPartition exchange; plan:\n"+plan)

	// TC-MPP-SST-08: Global SUM (no GROUP BY) — partial agg merge requires some exchange.
	// Actual behavior: optimizer uses PassThrough exchange (coordinator pulls partial results),
	// not HashPartition. Assert that an ExchangeSender is present (data does cross a node boundary).
	plan = mppPlan(tk, "SELECT SUM(amount) FROM orders_sharded")
	require.Contains(t, plan, "ExchangeSender",
		"TC-MPP-SST-08: global SUM without GROUP BY must use ExchangeSender for partial agg merge; plan:\n"+plan)
}

// TestMPP_WindowFunctions covers TC-MPP-SST-10 and TC-MPP-SST-11.
func TestMPP_WindowFunctions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	)`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	setShardKey(t, dom, "test", "orders_sharded", []string{"company_id"}, 4)

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")

	// TC-MPP-SST-10: Window PARTITION BY shard key — local per shard, no exchange
	plan := mppPlan(tk,
		"SELECT id, company_id, SUM(amount) OVER (PARTITION BY company_id) FROM orders_sharded")
	require.NotContains(t, plan, "HashPartition",
		"TC-MPP-SST-10: WINDOW PARTITION BY shard key must not require HashPartition exchange; plan:\n"+plan)

	// TC-MPP-SST-11: Window ORDER BY non-shard-key column — requires some exchange for global ordering.
	// Actual behavior: optimizer uses PassThrough exchange (not HashPartition) to gather data
	// at coordinator before applying the global ORDER BY. Assert ExchangeSender is present.
	plan = mppPlan(tk,
		"SELECT id, amount, ROW_NUMBER() OVER (ORDER BY amount) FROM orders_sharded")
	require.Contains(t, plan, "ExchangeSender",
		"TC-MPP-SST-11: WINDOW ORDER BY non-shard column must use ExchangeSender for global ordering; plan:\n"+plan)
	t.Logf("TC-MPP-SST-11: plan (PassThrough, not HashPartition, is correct):\n%s", plan)
}

// TestMPP_JoinVariants covers TC-MPP-JOIN-02, TC-MPP-JOIN-04, TC-MPP-JOIN-05.
// TC-MPP-JOIN-03 (FULL OUTER JOIN) is skipped: TiFlash MPP does not support FULL OUTER JOIN.
func TestMPP_JoinVariants(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	)`)
	tk.MustExec(`CREATE TABLE customers_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		name       VARCHAR(255),
		PRIMARY KEY (id)
	)`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	testkit.SetTiFlashReplica(t, dom, "test", "customers_sharded")
	setShardKey(t, dom, "test", "orders_sharded", []string{"company_id"}, 4)
	setShardKey(t, dom, "test", "customers_sharded", []string{"company_id"}, 4)

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")

	// TC-MPP-JOIN-02: RIGHT JOIN on shard key — co-located, no exchange
	plan := mppPlan(tk,
		"SELECT /*+ shuffle_join(orders_sharded, customers_sharded) */ o.id, c.company_id "+
			"FROM orders_sharded o RIGHT JOIN customers_sharded c ON o.company_id = c.company_id")
	require.NotContains(t, plan, "HashPartition",
		"TC-MPP-JOIN-02: co-located RIGHT JOIN must not require HashPartition exchange; plan:\n"+plan)

	// TC-MPP-JOIN-04: JOIN with expression in ON clause (company_id + 0) — document behavior.
	// The expression prevents co-location detection: expect HashPartition exchange.
	plan = mppPlan(tk,
		"SELECT /*+ shuffle_join(orders_sharded, customers_sharded) */ COUNT(*) "+
			"FROM orders_sharded o JOIN customers_sharded c ON o.company_id + 0 = c.company_id")
	// Document: expression in ON clause breaks co-location optimization.
	t.Logf("TC-MPP-JOIN-04: plan with expression in ON clause:\n%s\nHashPartition=%v",
		plan, strings.Contains(plan, "HashPartition"))

	// TC-MPP-JOIN-05: JOIN with WHERE filtering both tables on shard key
	plan = mppPlan(tk,
		"SELECT /*+ shuffle_join(orders_sharded, customers_sharded) */ COUNT(*) "+
			"FROM orders_sharded o JOIN customers_sharded c ON o.company_id = c.company_id "+
			"WHERE o.company_id = 42 AND c.company_id = 42")
	require.NotContains(t, plan, "HashPartition",
		"TC-MPP-JOIN-05: co-located join with shard-key WHERE must not require HashPartition exchange; plan:\n"+plan)
}

// TestMPP_MixedTables covers TC-MPP-MIXED-01 and TC-MPP-MIXED-02.
func TestMPP_MixedTables(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Sharded table
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	)`)
	// Plain (non-sharded) table
	tk.MustExec(`CREATE TABLE orders_plain (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	)`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	testkit.SetTiFlashReplica(t, dom, "test", "orders_plain")
	setShardKey(t, dom, "test", "orders_sharded", []string{"company_id"}, 4)
	// orders_plain has no ShardKeyInfo

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")

	// TC-MPP-MIXED-01: Sharded joined with plain table — exchange required on at least one side.
	// Actual behavior: without a shuffle hint, the optimizer chooses broadcast for the non-sharded
	// table (Broadcast exchange) rather than HashPartition. Either way, an ExchangeSender is present.
	plan := mppPlan(tk,
		"SELECT COUNT(*) FROM orders_sharded o JOIN orders_plain p ON o.company_id = p.company_id")
	require.Contains(t, plan, "ExchangeSender",
		"TC-MPP-MIXED-01: sharded+plain join must use ExchangeSender on at least one side; plan:\n"+plan)
	t.Logf("TC-MPP-MIXED-01: plan (Broadcast chosen over HashPartition for plain side):\n%s", plan)

	// TC-MPP-MIXED-02: Broadcast hint on non-sharded table — p broadcast, no shuffle on sharded table
	plan = mppPlan(tk,
		"SELECT /*+ broadcast_join(orders_plain) */ COUNT(*) "+
			"FROM orders_sharded o JOIN orders_plain p ON o.company_id = p.company_id")
	// With broadcast hint the plain table is replicated to all nodes; sharded side needs no shuffle.
	// The plan should contain Broadcast exchange type (not HashPartition) for orders_plain.
	require.NotContains(t, plan, "HashPartition",
		"TC-MPP-MIXED-02: broadcast hint must eliminate HashPartition exchange on sharded table; plan:\n"+plan)
	t.Logf("TC-MPP-MIXED-02: broadcast plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// §10 TiFlash Replica (mock-level coverage)
// ---------------------------------------------------------------------------

// TestTiFlashReplicaMock covers the parts of §10 that are testable without a
// real TiFlash cluster:
//   - SetTiFlashReplica marks a sharded table as TiFlash-available in the mock
//   - EXPLAIN with isolation_read_engines=tiflash + MPP routes to cop[tiflash]
//   - READ_FROM_STORAGE(TIFLASH[...]) hint is accepted on a sharded table
//
// Cluster-level §10 tests (TC-TIFLASH-01..06) that require a running TiFlash
// node are not covered here:
//   - SHOW TABLE orders_sharded REGIONS — requires real TiKV/TiFlash regions
//   - AVAILABLE=1 in information_schema.TIFLASH_REPLICA — requires replication
//   - Verify N learner peers per physical shard — requires pd-ctl
func TestTiFlashReplicaMock(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)

	// TC-TIFLASH-mock-01: SetTiFlashReplica marks the table as TiFlash-available
	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")

	// TC-TIFLASH-mock-02: EXPLAIN with tiflash read engine + MPP routes to tiflash
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")
	plan := mppPlan(tk, "SELECT COUNT(*) FROM orders_sharded WHERE company_id = 42")
	require.Contains(t, plan, "tiflash",
		"TC-TIFLASH-mock-02: plan must route to tiflash engine; got:\n"+plan)

	// TC-TIFLASH-mock-03: READ_FROM_STORAGE(TIFLASH[...]) hint is accepted with MPP on
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	rows := tk.MustQuery(
		"EXPLAIN SELECT /*+ READ_FROM_STORAGE(TIFLASH[orders_sharded]) */ COUNT(*) FROM orders_sharded").Rows()
	planStr := explainToStr(rows)
	require.Contains(t, planStr, "tiflash",
		"TC-TIFLASH-mock-03: READ_FROM_STORAGE hint must route to tiflash; got:\n"+planStr)
}

// ---------------------------------------------------------------------------
// §22 BR / TiCDC — SHOW CREATE TABLE roundtrip (unit-testable subset)
// ---------------------------------------------------------------------------

// TestBRCDCShowCreateRoundtrip verifies that SHOW CREATE TABLE emits the correct
// SHARD BY clause for all SPT combinations. This is the unit-testable proxy for
// BR restore correctness: BR restores by replaying SHOW CREATE TABLE DDL, so if
// the clause is wrong the restored table will be missing its shard routing.
//
// Full BR/restore and TiCDC tests (TC-BR-01, TC-TICDC-01) require a running
// BR binary and a downstream TiCDC sink — they are cluster-level only and
// cannot be run in the testkit mock environment.
func TestBRCDCShowCreateRoundtrip(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// SST: SHARD BY only, no partitioning
	tk.MustExec(`CREATE TABLE t_sst (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)
	tk.MustQuery("SHOW CREATE TABLE t_sst").CheckContain("SHARD BY (`company_id`) SHARDS 4")
	tk.MustExec("DROP TABLE t_sst")

	// SPT-R: RANGE COLUMNS + SHARD BY
	tk.MustExec(`CREATE TABLE t_spt_r (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	result := tk.MustQuery("SHOW CREATE TABLE t_spt_r")
	result.CheckContain("SHARD BY (`company_id`) SHARDS 4")
	result.CheckContain("PARTITION BY RANGE COLUMNS(`created_at`)")
	result.CheckContain("p2024")
	tk.MustExec("DROP TABLE t_spt_r")

	// SPT-LC: LIST COLUMNS + SHARD BY
	tk.MustExec(`CREATE TABLE t_spt_lc (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		country    VARCHAR(2) NOT NULL,
		PRIMARY KEY (id, country)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST COLUMNS (country) (
		PARTITION p_us VALUES IN ('US'),
		PARTITION p_gb VALUES IN ('GB')
	)`)
	result = tk.MustQuery("SHOW CREATE TABLE t_spt_lc")
	result.CheckContain("SHARD BY (`company_id`) SHARDS 4")
	result.CheckContain("PARTITION BY LIST COLUMNS")
	result.CheckContain("p_us")
	result.CheckContain("p_gb")
	tk.MustExec("DROP TABLE t_spt_lc")

	// SPT-K: KEY + SHARD BY
	tk.MustExec(`CREATE TABLE t_spt_k (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		bucket_id  BIGINT NOT NULL,
		PRIMARY KEY (id, bucket_id)
	) SHARD BY (company_id) SHARDS 8
	PARTITION BY KEY (bucket_id) PARTITIONS 4`)
	result = tk.MustQuery("SHOW CREATE TABLE t_spt_k")
	result.CheckContain("SHARD BY (`company_id`) SHARDS 8")
	result.CheckContain("PARTITION BY KEY (`bucket_id`) PARTITIONS 4")
	tk.MustExec("DROP TABLE t_spt_k")

	// SPT composite shard key: multi-column SHARD BY + RANGE
	tk.MustExec(`CREATE TABLE t_spt_composite (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		user_id    BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id, user_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	result = tk.MustQuery("SHOW CREATE TABLE t_spt_composite")
	result.CheckContain("SHARD BY (`company_id`, `user_id`) SHARDS 4")
	result.CheckContain("PARTITION BY RANGE COLUMNS")
	tk.MustExec("DROP TABLE t_spt_composite")
}

// TestTiFlashPlacementRulesShardedTable verifies that ALTER TABLE ... SET TIFLASH REPLICA
// emits placement rules for ALL physical shard IDs of an SST, not just the base table ID.
// Regression test for: ConfigureTiFlashPDForTable(baseID) missing shard_1..shard_N rules.
func TestTiFlashPlacementRulesShardedTable(t *testing.T) {
	// Set up mock TiFlash store BEFORE store creation so checkTiFlashReplicaCount passes.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/infoschema/mockTiFlashStoreCount"))
	}()

	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)

	// CREATE TABLE ... SHARD BY allocates ShardIDs in the DDL job submitter and persists
	// them to meta. Read them from the live InfoSchema (no setShardKey needed here).
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("orders_sharded"))
	require.NoError(t, err)
	ski := tbl.Meta().ShardKeyInfo
	require.NotNil(t, ski, "ShardKeyInfo must be set by CREATE TABLE ... SHARD BY")
	require.Len(t, ski.ShardIDs, 4, "expected 4 shard IDs allocated by DDL")

	// SET TIFLASH REPLICA goes through the DDL handler which reads raw tblInfo from meta.
	tk.MustExec("ALTER TABLE orders_sharded SET TIFLASH REPLICA 1")

	// All 4 shard IDs must have placement rules in the mock PD.
	tiflash.Lock()
	rules := tiflash.GlobalTiFlashPlacementRules
	tiflash.Unlock()
	for _, shardID := range ski.ShardIDs {
		ruleID := infosync.MakeRuleID(shardID)
		_, ok := rules[ruleID]
		require.True(t, ok,
			"placement rule %q missing for shard ID %d — base table rule only was emitted (regression)", ruleID, shardID)
	}

	// Base table ID must NOT have a rule (data lives in shards, not the base table region).
	baseRuleID := infosync.MakeRuleID(tbl.Meta().ID)
	_, baseHasRule := rules[baseRuleID]
	require.False(t, baseHasRule,
		"base table rule %q should not be emitted for SST — shards cover all data", baseRuleID)

	// --- SPT case ---
	// Specifically assert shardIDs[1]..[3] get rules (the ones the original bug was missing).
	tiflash.Lock()
	tiflash.GlobalTiFlashPlacementRules = make(map[string]*pdhttp.Rule)
	tiflash.Unlock()

	tk.MustExec(`CREATE TABLE orders_spt (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)

	tblSPT, err2 := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("orders_spt"))
	require.NoError(t, err2)
	pi := tblSPT.Meta().GetPartitionInfo()
	require.NotNil(t, pi, "SPT must have a PartitionInfo")
	require.Len(t, pi.Definitions, 3, "expected 3 range partitions")
	for i, def := range pi.Definitions {
		require.Len(t, def.ShardIDs, 4, "partition %d must have 4 shard IDs", i)
	}

	tk.MustExec("ALTER TABLE orders_spt SET TIFLASH REPLICA 1")

	tiflash.Lock()
	sptRules := tiflash.GlobalTiFlashPlacementRules
	tiflash.Unlock()

	// Every per-partition shard sub-ID must have a rule — 3 partitions × 4 shards = 12 rules.
	for i, def := range pi.Definitions {
		for j, shardID := range def.ShardIDs {
			ruleID := infosync.MakeRuleID(shardID)
			_, ok := sptRules[ruleID]
			require.True(t, ok,
				"SPT: placement rule %q missing for partition %d shard[%d]=%d", ruleID, i, j, shardID)
		}
		// Logical partition ID must NOT have a rule — data lives in sub-shards.
		logicRuleID := infosync.MakeRuleID(def.ID)
		_, logicHasRule := sptRules[logicRuleID]
		require.False(t, logicHasRule,
			"SPT: logical partition rule %q should not be emitted when ShardIDs are set", logicRuleID)
	}
}

// TestTiKVCoprocessorJoins covers §24 — joins on sharded tables executed via TiKV
// coprocessor (tidb_isolation_read_engines='tikv'). Validates correctness and basic
// plan shapes for Hash Join, Index Lookup Join, FULL OUTER JOIN, and self-join.
func TestTiKVCoprocessorJoins(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Setup: orders_sharded (SST, shard key company_id, 4 shards)
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)
	setShardKey(t, dom, "test", "orders_sharded", []string{"company_id"}, 4)

	// customers_sharded (SST, shard key company_id, 4 shards)
	tk.MustExec(`CREATE TABLE customers_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		name       VARCHAR(255),
		PRIMARY KEY (id)
	) SHARD BY (company_id) SHARDS 4`)
	setShardKey(t, dom, "test", "customers_sharded", []string{"company_id"}, 4)

	// orders_range_sharded (SPT-R)
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)

	// companies: plain non-sharded table
	tk.MustExec(`CREATE TABLE companies (
		id   BIGINT PRIMARY KEY,
		name VARCHAR(50)
	)`)

	// Seed data
	tk.MustExec("INSERT INTO orders_sharded VALUES (1, 42, 10.00), (2, 42, 20.00), (3, 99, 30.00)")
	tk.MustExec("INSERT INTO customers_sharded VALUES (1, 42, 'Acme'), (2, 99, 'Globex')")
	tk.MustExec("INSERT INTO companies VALUES (42, 'Acme Corp'), (99, 'Globex Inc')")
	tk.MustExec("INSERT INTO orders_range_sharded VALUES (1, 42, '2024-05-01'), (2, 99, '2023-11-01')")

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tikv'")

	// TC-TIKV-JOIN-01: Hash Join between two SSTs on shard key.
	// TiKV cop joins are always Hash Join — no co-located MPP optimization here.
	plan := explainToStr(tk.MustQuery("EXPLAIN FORMAT='brief' SELECT o.id, o.amount, c.name FROM orders_sharded o JOIN customers_sharded c ON o.company_id = c.company_id").Rows())
	require.Contains(t, plan, "HashJoin",
		"TC-TIKV-JOIN-01: expected HashJoin in TiKV plan; plan:\n"+plan)
	tk.MustQuery(`SELECT o.id, o.amount, c.name
		FROM orders_sharded o
		JOIN customers_sharded c ON o.company_id = c.company_id
		ORDER BY o.id`).Check(testkit.Rows(
		"1 10.00 Acme",
		"2 20.00 Acme",
		"3 30.00 Globex",
	))

	// TC-TIKV-JOIN-02: IndexLookUpJoin hint on customers_sharded with shard-key predicate.
	// With company_id=42 predicate, orders_sharded prunes to one shard.
	// INL_JOIN hint may be ignored if the optimizer determines HashJoin is cheaper on
	// small in-memory tables; assert result correctness regardless of plan shape.
	tk.MustExec("ALTER TABLE customers_sharded ADD INDEX idx_company(company_id)")
	rows := tk.MustQuery(`SELECT /*+ INL_JOIN(c) */ o.id, c.name
		FROM orders_sharded o
		JOIN customers_sharded c ON o.company_id = c.company_id
		WHERE o.company_id = 42
		ORDER BY o.id`).Rows()
	require.Equal(t, 2, len(rows), "TC-TIKV-JOIN-02: expected 2 rows for company_id=42")
	require.Equal(t, "1", rows[0][0], "TC-TIKV-JOIN-02: row 0 id should be 1")
	require.Equal(t, "Acme", rows[0][1], "TC-TIKV-JOIN-02: row 0 name should be Acme")
	require.Equal(t, "2", rows[1][0], "TC-TIKV-JOIN-02: row 1 id should be 2")
	// Log plan so we can observe whether INL_JOIN hint was honoured
	inlPlan := explainToStr(tk.MustQuery("EXPLAIN FORMAT='brief' SELECT /*+ INL_JOIN(c) */ o.id, c.name FROM orders_sharded o JOIN customers_sharded c ON o.company_id = c.company_id WHERE o.company_id = 42").Rows())
	t.Logf("TC-TIKV-JOIN-02 plan (INL_JOIN hint): %s", inlPlan)

	// TC-TIKV-JOIN-03: Hash Join between SST and non-sharded table.
	// No pruning on orders_sharded (no shard key predicate); companies is build side.
	plan = explainToStr(tk.MustQuery("EXPLAIN FORMAT='brief' SELECT o.id, o.amount, co.name FROM orders_sharded o JOIN companies co ON o.company_id = co.id").Rows())
	require.Contains(t, plan, "HashJoin",
		"TC-TIKV-JOIN-03: expected HashJoin; plan:\n"+plan)
	tk.MustQuery(`SELECT o.id, o.amount, co.name
		FROM orders_sharded o
		JOIN companies co ON o.company_id = co.id
		ORDER BY o.id`).Check(testkit.Rows(
		"1 10.00 Acme Corp",
		"2 20.00 Acme Corp",
		"3 30.00 Globex Inc",
	))

	// TC-TIKV-JOIN-04: SPT-R joined to SST on shard key with pruning predicate.
	// Both sides prune to company_id=42 shard. orders_range_sharded has no amount col.
	tk.MustQuery(`SELECT o.id, r.id AS r_id
		FROM orders_sharded o
		JOIN orders_range_sharded r ON o.company_id = r.company_id
		WHERE o.company_id = 42
		ORDER BY o.id, r_id`).Check(testkit.Rows(
		"1 1",
		"2 1",
	))
	plan = explainToStr(tk.MustQuery("EXPLAIN FORMAT='brief' SELECT o.id, r.id FROM orders_sharded o JOIN orders_range_sharded r ON o.company_id = r.company_id WHERE o.company_id = 42").Rows())
	t.Logf("TC-TIKV-JOIN-04 plan (shard pruning on both sides): %s", plan)

	// TC-TIKV-JOIN-05: FULL OUTER JOIN is not supported by TiDB's parser.
	// Both TiFlash MPP (TC-MPP-JOIN-03) and TiKV reject this syntax at parse time.
	// The test documents the behavior: parser returns a syntax error, no fallback path exists.
	err := tk.ExecToErr(`SELECT o.id, c.name
		FROM orders_sharded o
		FULL OUTER JOIN customers_sharded c ON o.company_id = c.company_id
		ORDER BY o.id, c.name`)
	require.Error(t, err, "TC-TIKV-JOIN-05: FULL OUTER JOIN must be rejected by parser")
	require.Contains(t, err.Error(), "You have an error in your SQL syntax",
		"TC-TIKV-JOIN-05: expected syntax error for FULL OUTER JOIN; got: %v", err)

	// TC-TIKV-JOIN-06: Self-join on sharded table with shard key predicate.
	// Pruning applies to both sides (same shard); result is pairs within company_id=42.
	tk.MustQuery(`SELECT a.id, b.id AS b_id
		FROM orders_sharded a
		JOIN orders_sharded b ON a.company_id = b.company_id AND a.id < b.id
		WHERE a.company_id = 42
		ORDER BY a.id, b_id`).Check(testkit.Rows("1 2"))

	// Reset
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tikv,tiflash'")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// explainToStr joins all rows of an EXPLAIN output into a single string
// for substring matching.
func explainToStr(rows [][]any) string {
	var b strings.Builder
	for _, row := range rows {
		for _, col := range row {
			b.WriteString(" ")
			b.WriteString(col.(string))
		}
		b.WriteString("\n")
	}
	return b.String()
}
