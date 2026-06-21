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

package shardkey

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// INFORMATION_SCHEMA.SHARD_SKEW tests
// ---------------------------------------------------------------------------

func TestShardSkew_BasicQuery(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE skew1 (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		val INT,
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)

	// SHARD_SKEW should show 4 rows (one per shard) for this table.
	rows := tk.MustQuery(`SELECT TABLE_SCHEMA, TABLE_NAME, SHARD_KEY, SHARD_COUNT, SHARD_ID
		FROM information_schema.SHARD_SKEW
		WHERE TABLE_NAME = 'skew1'
		ORDER BY SHARD_ID`).Rows()
	require.Len(t, rows, 4)
	for i, row := range rows {
		require.Equal(t, "test", row[0])               // TABLE_SCHEMA
		require.Equal(t, "skew1", row[1])              // TABLE_NAME
		require.Equal(t, "company_id", row[2])         // SHARD_KEY
		require.Equal(t, "4", row[3])                  // SHARD_COUNT
		require.Equal(t, fmt.Sprintf("%d", i), row[4]) // SHARD_ID
	}
}

func TestShardSkew_WithData(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE skew2 (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		status VARCHAR(32),
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)

	// Insert rows that will be distributed across shards via CRC32.
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf(`INSERT INTO skew2 VALUES (%d, %d, 'active')`, i, i))
	}

	// All columns should be populated.
	rows := tk.MustQuery(`SELECT SHARD_ID, ROW_COUNT, MIN_ROW_COUNT, MAX_ROW_COUNT, AVG_ROW_COUNT, SKEW_RATIO
		FROM information_schema.SHARD_SKEW
		WHERE TABLE_NAME = 'skew2'
		ORDER BY SHARD_ID`).Rows()
	require.Len(t, rows, 4)
}

func TestShardSkew_SkewRatio(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE skew3 (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)

	// SKEW_RATIO should be present (≥ 1.0 when there's data, or 1.0 when empty).
	rows := tk.MustQuery(`SELECT SKEW_RATIO
		FROM information_schema.SHARD_SKEW
		WHERE TABLE_NAME = 'skew3'
		LIMIT 1`).Rows()
	require.Len(t, rows, 1)
	// With no data, skew ratio should be 1.0 (all empty).
	require.Equal(t, "1", rows[0][0])
}

func TestShardSkew_PhysicalTableIDs(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE skew4 (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 2`)

	// Each shard should have a distinct PHYSICAL_TABLE_ID.
	rows := tk.MustQuery(`SELECT PHYSICAL_TABLE_ID
		FROM information_schema.SHARD_SKEW
		WHERE TABLE_NAME = 'skew4'
		ORDER BY SHARD_ID`).Rows()
	require.Len(t, rows, 2)
	require.NotEqual(t, rows[0][0], rows[1][0])
}

func TestShardSkew_CompositeShardKey(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE skew5 (
		id BIGINT NOT NULL,
		region_id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (region_id, company_id, id)
	) SHARD BY (region_id, company_id) SHARDS 8`)

	rows := tk.MustQuery(`SELECT SHARD_KEY, SHARD_COUNT
		FROM information_schema.SHARD_SKEW
		WHERE TABLE_NAME = 'skew5'
		LIMIT 1`).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "region_id,company_id", rows[0][0])
	require.Equal(t, "8", rows[0][1])
}

func TestShardSkew_NonShardedTableNotShown(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE noskew (id BIGINT PRIMARY KEY, val INT)`)

	rows := tk.MustQuery(`SELECT * FROM information_schema.SHARD_SKEW WHERE TABLE_NAME = 'noskew'`).Rows()
	require.Len(t, rows, 0)
}

func TestShardSkew_MultipleShardedTables(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE ms1 (
		id BIGINT NOT NULL,
		cid BIGINT NOT NULL,
		PRIMARY KEY (cid, id)
	) SHARD BY (cid) SHARDS 2`)
	tk.MustExec(`CREATE TABLE ms2 (
		id BIGINT NOT NULL,
		cid BIGINT NOT NULL,
		PRIMARY KEY (cid, id)
	) SHARD BY (cid) SHARDS 4`)

	rows := tk.MustQuery(`SELECT TABLE_NAME, COUNT(*) AS shard_cnt
		FROM information_schema.SHARD_SKEW
		WHERE TABLE_NAME IN ('ms1', 'ms2')
		GROUP BY TABLE_NAME
		ORDER BY TABLE_NAME`).Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "ms1", rows[0][0])
	require.Equal(t, "2", rows[0][1])
	require.Equal(t, "ms2", rows[1][0])
	require.Equal(t, "4", rows[1][1])
}
