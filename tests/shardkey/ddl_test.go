package shardkey

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	metamodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// DDL helpers
// ---------------------------------------------------------------------------

func setup(t *testing.T) (*testkit.TestKit, *domain.Domain) {
	t.Helper()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	return tk, domain.GetDomain(tk.Session())
}

func tableInfo(t *testing.T, dom *domain.Domain, tblName string) *metamodel.TableInfo {
	t.Helper()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(tblName))
	require.NoError(t, err)
	return tbl.Meta()
}

// ---------------------------------------------------------------------------
// TC-DDL-ST: Simple Table (no shard key)
// ---------------------------------------------------------------------------

func TestDDL_ST_BasicCreate(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders (
		id         BIGINT NOT NULL AUTO_INCREMENT,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		created_at DATETIME,
		PRIMARY KEY (id)
	)`)
	mi := tableInfo(t, dom, "orders")
	require.Nil(t, mi.ShardKeyInfo, "plain table must have nil ShardKeyInfo")
	require.Nil(t, mi.Partition, "plain table must have nil Partition")
}

func TestDDL_ST_ShowCreateNoShardClause(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders (id BIGINT PRIMARY KEY, company_id BIGINT)`)
	row := tk.MustQuery("SHOW CREATE TABLE orders").Rows()[0][1].(string)
	require.NotContains(t, row, "SHARD_KEY", "plain table SHOW CREATE must not contain SHARD_KEY")
}

// ---------------------------------------------------------------------------
// TC-DDL-PT: Partitioned Table (no shard key)
// ---------------------------------------------------------------------------

func TestDDL_PT_Range(t *testing.T) {
	tk, dom := setup(t)
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
	mi := tableInfo(t, dom, "orders_range")
	require.Nil(t, mi.ShardKeyInfo, "range-partitioned table must have nil ShardKeyInfo")
	require.Len(t, mi.Partition.Definitions, 4)
}

func TestDDL_PT_RangeColumns(t *testing.T) {
	tk, dom := setup(t)
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
	mi := tableInfo(t, dom, "orders_range_cols")
	require.Nil(t, mi.ShardKeyInfo)
	require.Len(t, mi.Partition.Definitions, 3)
}

func TestDDL_PT_List(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders_list (
		id        BIGINT NOT NULL,
		region_id INT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id, region_id)
	) PARTITION BY LIST (region_id) (
		PARTITION p_us   VALUES IN (1, 2, 3),
		PARTITION p_eu   VALUES IN (4, 5, 6),
		PARTITION p_apac VALUES IN (7, 8, 9)
	)`)
	mi := tableInfo(t, dom, "orders_list")
	require.Nil(t, mi.ShardKeyInfo)
	require.Len(t, mi.Partition.Definitions, 3)
}

func TestDDL_PT_ListColumns(t *testing.T) {
	tk, dom := setup(t)
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
	mi := tableInfo(t, dom, "orders_list_cols")
	require.Nil(t, mi.ShardKeyInfo)
	require.Len(t, mi.Partition.Definitions, 4)
}

func TestDDL_PT_Hash(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders_hash (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id, company_id)
	) PARTITION BY HASH (company_id) PARTITIONS 8`)
	mi := tableInfo(t, dom, "orders_hash")
	require.Nil(t, mi.ShardKeyInfo)
	require.Len(t, mi.Partition.Definitions, 8)
}

func TestDDL_PT_Key(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders_key (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id, company_id)
	) PARTITION BY KEY (company_id) PARTITIONS 4`)
	mi := tableInfo(t, dom, "orders_key")
	require.Nil(t, mi.ShardKeyInfo)
	require.Len(t, mi.Partition.Definitions, 4)
}

// ---------------------------------------------------------------------------
// TC-DDL-SST: Sharded Simple Table
// ---------------------------------------------------------------------------

func TestDDL_SST_BasicCreate(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (
		id         BIGINT NOT NULL AUTO_INCREMENT,
		company_id BIGINT NOT NULL,
		amount     DECIMAL(12,2),
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)
	mi := tableInfo(t, dom, "orders_sharded")
	require.NotNil(t, mi.ShardKeyInfo)
	require.Equal(t, []string{"company_id"}, mi.ShardKeyInfo.Columns)
	require.Equal(t, 4, mi.ShardKeyInfo.ShardCnt)
	// Shards are represented as synthetic partitions
	require.NotNil(t, mi.Partition)
	require.Len(t, mi.Partition.Definitions, 4)
	// Each partition definition must have a unique ID
	ids := make(map[int64]bool)
	for _, def := range mi.Partition.Definitions {
		require.NotZero(t, def.ID)
		require.False(t, ids[def.ID], "duplicate shard ID %d", def.ID)
		ids[def.ID] = true
	}
}

func TestDDL_SST_MultiColumnShardKey(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE events_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		user_id    BIGINT NOT NULL,
		event_type VARCHAR(64),
		PRIMARY KEY (company_id, user_id, id)
	) SHARD BY (company_id, user_id) SHARDS 8`)
	mi := tableInfo(t, dom, "events_sharded")
	require.NotNil(t, mi.ShardKeyInfo)
	require.Equal(t, []string{"company_id", "user_id"}, mi.ShardKeyInfo.Columns)
	require.Equal(t, 8, mi.ShardKeyInfo.ShardCnt)
	require.Len(t, mi.Partition.Definitions, 8)
}

func TestDDL_SST_VarcharShardKey(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE tenants_sharded (
		id          BIGINT NOT NULL,
		tenant_code VARCHAR(32) NOT NULL,
		data        TEXT,
		PRIMARY KEY (tenant_code, id)
	) SHARD BY (tenant_code) SHARDS 4`)
}

func TestDDL_SST_CharShardKey(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE regions_sharded (
		id   BIGINT NOT NULL,
		code CHAR(3) NOT NULL,
		name VARCHAR(128),
		PRIMARY KEY (code, id)
	) SHARD BY (code) SHARDS 4`)
}

func TestDDL_SST_MinShards(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t_shard_min (id BIGINT NOT NULL, k BIGINT NOT NULL, PRIMARY KEY (k, id)) SHARD BY (k) SHARDS 2`)
	mi := tableInfo(t, dom, "t_shard_min")
	require.Equal(t, 2, mi.ShardKeyInfo.ShardCnt)
	require.Len(t, mi.Partition.Definitions, 2)
}

func TestDDL_SST_MaxShards(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t_shard_max (id BIGINT NOT NULL, k BIGINT NOT NULL, PRIMARY KEY (k, id)) SHARD BY (k) SHARDS 64`)
	mi := tableInfo(t, dom, "t_shard_max")
	require.Equal(t, 64, mi.ShardKeyInfo.ShardCnt)
	require.Len(t, mi.Partition.Definitions, 64)
	// All 64 shard IDs are unique
	ids := make(map[int64]bool, 64)
	for _, def := range mi.Partition.Definitions {
		require.False(t, ids[def.ID])
		ids[def.ID] = true
	}
}

func TestDDL_SST_ShardNamesAreSequential(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t_names (id BIGINT NOT NULL, k BIGINT NOT NULL, PRIMARY KEY (k, id)) SHARD BY (k) SHARDS 4`)
	mi := tableInfo(t, dom, "t_names")
	for i, def := range mi.Partition.Definitions {
		require.Equal(t, pmodel.NewCIStr("shard_"+strings.TrimPrefix(def.Name.L, "shard_")), def.Name)
		_ = i
	}
	require.Equal(t, "shard_0", mi.Partition.Definitions[0].Name.L)
	require.Equal(t, "shard_3", mi.Partition.Definitions[3].Name.L)
}

func TestDDL_SST_ShardIDsDoNotCollideWithTableID(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t_ids (id BIGINT NOT NULL, k BIGINT NOT NULL, PRIMARY KEY (k, id)) SHARD BY (k) SHARDS 4`)
	mi := tableInfo(t, dom, "t_ids")
	for _, def := range mi.Partition.Definitions {
		require.NotEqual(t, mi.ID, def.ID, "shard ID must not equal table ID")
	}
}

// ---------------------------------------------------------------------------
// TC-DDL-SPT: Sharded Partitioned Table
// ---------------------------------------------------------------------------

func TestDDL_SPT_Range(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (company_id, id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	mi := tableInfo(t, dom, "orders_range_sharded")
	require.NotNil(t, mi.ShardKeyInfo)
	require.Equal(t, []string{"company_id"}, mi.ShardKeyInfo.Columns)
	require.Equal(t, 4, mi.ShardKeyInfo.ShardCnt)
	// 3 named partitions; each has 4 ShardIDs
	require.Len(t, mi.Partition.Definitions, 3)
	uniqueIDs := make(map[int64]bool, 12)
	for _, def := range mi.Partition.Definitions {
		require.Len(t, def.ShardIDs, 4)
		for _, sid := range def.ShardIDs {
			require.NotZero(t, sid)
			require.False(t, uniqueIDs[sid], "duplicate shard physical ID")
			uniqueIDs[sid] = true
		}
	}
}

func TestDDL_SPT_List(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders_list_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		region_id  INT NOT NULL,
		PRIMARY KEY (company_id, id, region_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST (region_id) (
		PARTITION p_us   VALUES IN (1, 2, 3),
		PARTITION p_eu   VALUES IN (4, 5, 6),
		PARTITION p_apac VALUES IN (7, 8, 9)
	)`)
	mi := tableInfo(t, dom, "orders_list_sharded")
	require.NotNil(t, mi.ShardKeyInfo)
	// 3 named partitions; each has 4 ShardIDs
	require.Len(t, mi.Partition.Definitions, 3)
	for _, def := range mi.Partition.Definitions {
		require.Len(t, def.ShardIDs, 4)
	}
}

func TestDDL_SPT_ListColumns(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders_list_cols_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		country    VARCHAR(2) NOT NULL,
		PRIMARY KEY (company_id, id, country)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST COLUMNS (country) (
		PARTITION p_us VALUES IN ('US'),
		PARTITION p_gb VALUES IN ('GB'),
		PARTITION p_de VALUES IN ('DE')
	)`)
	mi := tableInfo(t, dom, "orders_list_cols_sharded")
	require.NotNil(t, mi.ShardKeyInfo)
	// 3 named partitions; each has 4 ShardIDs
	require.Len(t, mi.Partition.Definitions, 3)
	for _, def := range mi.Partition.Definitions {
		require.Len(t, def.ShardIDs, 4)
	}
}

func TestDDL_SPT_Hash(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders_hash_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		bucket_id  BIGINT NOT NULL,
		PRIMARY KEY (company_id, id, bucket_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY HASH (bucket_id) PARTITIONS 4`)
	mi := tableInfo(t, dom, "orders_hash_sharded")
	require.NotNil(t, mi.ShardKeyInfo)
	// 4 HASH partitions; each has 4 ShardIDs
	require.Len(t, mi.Partition.Definitions, 4)
	for _, def := range mi.Partition.Definitions {
		require.Len(t, def.ShardIDs, 4)
	}
}

func TestDDL_SPT_Key(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders_key_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		bucket_id  BIGINT NOT NULL,
		PRIMARY KEY (company_id, id, bucket_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY KEY (bucket_id) PARTITIONS 4`)
	mi := tableInfo(t, dom, "orders_key_sharded")
	require.NotNil(t, mi.ShardKeyInfo)
	// 4 KEY partitions; each has 4 ShardIDs
	require.Len(t, mi.Partition.Definitions, 4)
	for _, def := range mi.Partition.Definitions {
		require.Len(t, def.ShardIDs, 4)
	}
}

// ---------------------------------------------------------------------------
// TC-DDL-ERR: Error / rejection cases
// ---------------------------------------------------------------------------

func TestDDL_ERR_ShardCountZero(t *testing.T) {
	// parser rejects <= 1, so 0 is caught at parse time
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (k) SHARDS 0`,
		"Shard count must be greater than 1",
	)
}

func TestDDL_ERR_ShardCountOne(t *testing.T) {
	// TC-DDL-ERR-01b
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (k) SHARDS 1`,
		"Shard count must be greater than 1",
	)
}

func TestDDL_ERR_ShardCountExceedsMax(t *testing.T) {
	// TC-DDL-ERR-02
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (k) SHARDS 65`,
		"Shard count must be between 2 and 64",
	)
}

func TestDDL_ERR_BlobShardKey(t *testing.T) {
	// TC-DDL-ERR-03
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, data BLOB) SHARD BY (data) SHARDS 4`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_VarbinaryShardKey(t *testing.T) {
	// TC-DDL-ERR-04
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, data VARBINARY(256)) SHARD BY (data) SHARDS 4`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_BinaryShardKey(t *testing.T) {
	// TC-DDL-ERR-05
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, code BINARY(16)) SHARD BY (code) SHARDS 4`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_FloatShardKey(t *testing.T) {
	// TC-DDL-ERR-06
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, score FLOAT) SHARD BY (score) SHARDS 4`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_DecimalShardKey(t *testing.T) {
	// TC-DDL-ERR-07
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, amount DECIMAL(10,2)) SHARD BY (amount) SHARDS 4`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_DatetimeShardKey(t *testing.T) {
	// TC-DDL-ERR-08
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, ts DATETIME) SHARD BY (ts) SHARDS 4`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_JSONShardKey(t *testing.T) {
	// TC-DDL-ERR-09
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, meta JSON) SHARD BY (meta) SHARDS 4`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_AutoIncrementShardKey(t *testing.T) {
	// TC-DDL-ERR-10
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, k BIGINT) SHARD BY (id) SHARDS 4`,
		dbterror.ErrShardKeyAutoIncrement,
	)
}

func TestDDL_ERR_TemporaryTable(t *testing.T) {
	// TC-DDL-ERR-11
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TEMPORARY TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (k) SHARDS 4`,
		"SHARD BY on temporary tables",
	)
}

func TestDDL_ERR_ShardRowIDBits(t *testing.T) {
	// TC-DDL-ERR-12
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT, k BIGINT) SHARD BY (k) SHARDS 4 SHARD_ROW_ID_BITS = 4`,
		"SHARD BY together with SHARD_ROW_ID_BITS",
	)
}

func TestDDL_ERR_DuplicateShardKeyColumn(t *testing.T) {
	// TC-DDL-ERR-13
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (k, k) SHARDS 4`,
		"k",
	)
}

func TestDDL_ERR_NonExistentColumn(t *testing.T) {
	// TC-DDL-ERR-14
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD BY (nonexistent) SHARDS 4`,
		"nonexistent",
	)
}

func TestDDL_ERR_PartitionColumnOverlapRange(t *testing.T) {
	// TC-DDL-ERR-15: partition column also used as shard key — must be rejected
	tk, _ := setup(t)
	tk.MustContainErrMsg(`CREATE TABLE t_overlap_range (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE (company_id) (
		PARTITION p_low  VALUES LESS THAN (1000),
		PARTITION p_high VALUES LESS THAN MAXVALUE
	)`,
		"company_id",
	)
}

func TestDDL_ERR_PartitionColumnOverlapList(t *testing.T) {
	// TC-DDL-ERR-16
	tk, _ := setup(t)
	tk.MustContainErrMsg(`CREATE TABLE t_overlap_list (
		id        BIGINT NOT NULL,
		region_id INT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id, region_id)
	) SHARD BY (region_id) SHARDS 4
	PARTITION BY LIST (region_id) (
		PARTITION p_us VALUES IN (1, 2),
		PARTITION p_eu VALUES IN (3, 4)
	)`,
		"region_id",
	)
}

func TestDDL_OK_DifferentPartitionAndShardColumns(t *testing.T) {
	// TC-DDL-OK-15: partition col != shard col — should succeed
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t_no_overlap (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		ts         DATE NOT NULL,
		PRIMARY KEY (company_id, id, ts)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (ts) (
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	mi := tableInfo(t, dom, "t_no_overlap")
	require.NotNil(t, mi.ShardKeyInfo)
	// 2 named partitions; each has 4 ShardIDs
	require.Len(t, mi.Partition.Definitions, 2)
	for _, def := range mi.Partition.Definitions {
		require.Len(t, def.ShardIDs, 4)
	}
}

// ---------------------------------------------------------------------------
// TC-DDL-ERR: Alter table — parse error (no ALTER SHARD_KEY syntax)
// ---------------------------------------------------------------------------

func TestDDL_ALTER_NoAlterShardSyntax(t *testing.T) {
	// TC-ALTER-01, TC-ALTER-02, TC-ALTER-03 — all should fail at parse level
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL AUTO_INCREMENT, company_id BIGINT NOT NULL, PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)
	tk.MustContainErrMsg(`ALTER TABLE orders_sharded MODIFY SHARDS INTO 8 SHARDS`, "")
	tk.MustContainErrMsg(`ALTER TABLE orders_sharded DROP SHARD_KEY`, "")
	tk.MustContainErrMsg(`ALTER TABLE orders ADD SHARD BY (company_id) SHARDS 4`, "")
}

// ---------------------------------------------------------------------------
// TC-EDGE: Allowed integer types
// ---------------------------------------------------------------------------

func TestDDL_EDGE_AllowedIntegerTypes(t *testing.T) {
	tk, _ := setup(t)
	cases := []struct {
		name    string
		colType string
	}{
		{"tinyint", "TINYINT NOT NULL"},
		{"smallint", "SMALLINT NOT NULL"},
		{"mediumint", "MEDIUMINT NOT NULL"},
		{"int_type", "INT NOT NULL"},
		{"bigint_unsigned", "BIGINT UNSIGNED NOT NULL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tk.MustExec("DROP TABLE IF EXISTS t_type_test")
			tk.MustExec("CREATE TABLE t_type_test (id BIGINT NOT NULL, k " + tc.colType + ", PRIMARY KEY (k, id)) SHARD BY (k) SHARDS 4")
		})
	}
}

func TestDDL_EDGE_TextShardKey(t *testing.T) {
	// TC-EDGE-11: TEXT columns cannot be part of the PK, so TEXT shard key
	// is now rejected by the shard-key-must-be-in-PK constraint.
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t_text (id BIGINT PRIMARY KEY, k TEXT NOT NULL) SHARD BY (k) SHARDS 4`,
		dbterror.ErrShardKeyNotInPrimaryKey,
	)
}

// ---------------------------------------------------------------------------
// TC-REG: Non-sharded tables unaffected
// ---------------------------------------------------------------------------

func TestDDL_REG_PlainTableUnchanged(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders (id BIGINT PRIMARY KEY, company_id BIGINT)`)
	mi := tableInfo(t, dom, "orders")
	require.Nil(t, mi.ShardKeyInfo)
	require.Nil(t, mi.Partition)
}

func TestDDL_REG_PartitionedTableUnchanged(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE orders_range (
		id BIGINT NOT NULL, ts DATE NOT NULL, PRIMARY KEY (id, ts)
	) PARTITION BY RANGE COLUMNS (ts) (
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	mi := tableInfo(t, dom, "orders_range")
	require.Nil(t, mi.ShardKeyInfo)
	require.Len(t, mi.Partition.Definitions, 2)
}

// ---------------------------------------------------------------------------
// TC-ALTER-SPT: ADD / DROP PARTITION on sharded+partitioned tables
// (Covers the bug found by Lightning: DROP PARTITION on SHARD BY + LIST COLUMNS
// tables was incorrectly rejected with error 1512.)
// ---------------------------------------------------------------------------

func setupSPTListCols(t *testing.T) *testkit.TestKit {
	t.Helper()
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_list_cols_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		country    VARCHAR(2) NOT NULL,
		PRIMARY KEY (company_id, id, country)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST COLUMNS (country) (
		PARTITION p_us VALUES IN ('US'),
		PARTITION p_gb VALUES IN ('GB'),
		PARTITION p_de VALUES IN ('DE')
	)`)
	return tk
}

func TestDDL_ALTER_SPT_LC_AddPartition(t *testing.T) {
	// TC-ALTER-SPT-LC-01
	tk := setupSPTListCols(t)
	tk.MustExec(`ALTER TABLE orders_list_cols_sharded ADD PARTITION (
		PARTITION p_au VALUES IN ('AU')
	)`)
	row := tk.MustQuery("SHOW CREATE TABLE orders_list_cols_sharded").Rows()[0][1].(string)
	require.Contains(t, row, "p_au")
}

func TestDDL_ALTER_SPT_LC_AddPartitionPhysicalIDs(t *testing.T) {
	// TC-ALTER-SPT-LC-01: new partition gets exactly ShardCnt ShardIDs
	tk := setupSPTListCols(t)
	tk.MustExec(`ALTER TABLE orders_list_cols_sharded ADD PARTITION (
		PARTITION p_au VALUES IN ('AU')
	)`)
	// Reload via tableInfo to check shardIDs
	store := testkit.CreateMockStore(t)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	// Re-create in this store to get dom access
	tk2.MustExec(`CREATE TABLE orders_list_cols_sharded (
		id BIGINT NOT NULL, company_id BIGINT NOT NULL, country VARCHAR(2) NOT NULL,
		PRIMARY KEY (company_id, id, country)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY LIST COLUMNS (country) (
		PARTITION p_us VALUES IN ('US'), PARTITION p_au VALUES IN ('AU')
	)`)
	mi := tableInfo(t, domain.GetDomain(tk2.Session()), "orders_list_cols_sharded")
	for _, def := range mi.Partition.Definitions {
		require.Len(t, def.ShardIDs, 4, "partition %s must have 4 ShardIDs after ADD PARTITION", def.Name.L)
	}
}

func TestDDL_ALTER_SPT_LC_DropPartition(t *testing.T) {
	// TC-ALTER-SPT-LC-03: DROP PARTITION on LIST COLUMNS + SHARD_KEY must succeed
	// (previously failed with error 1512: DROP PARTITION can only be used on RANGE/LIST partitions)
	tk := setupSPTListCols(t)
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (1, 42, 'US'), (2, 99, 'GB')")
	tk.MustExec("ALTER TABLE orders_list_cols_sharded DROP PARTITION p_us")
	// Partition gone from SHOW CREATE TABLE
	row := tk.MustQuery("SHOW CREATE TABLE orders_list_cols_sharded").Rows()[0][1].(string)
	require.NotContains(t, row, "p_us")
	// Data is gone
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded WHERE country = 'US'").Check(
		testkit.Rows("0"),
	)
	// Other partitions unaffected
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded WHERE country = 'GB'").Check(
		testkit.Rows("1"),
	)
}

func TestDDL_ALTER_SPT_LC_DropMultiplePartitions(t *testing.T) {
	// TC-ALTER-SPT-LC-04: DROP multiple partitions in one statement
	tk := setupSPTListCols(t)
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (1, 42, 'US'), (2, 99, 'GB'), (3, 7, 'DE')")
	tk.MustExec("ALTER TABLE orders_list_cols_sharded DROP PARTITION p_gb, p_de")
	row := tk.MustQuery("SHOW CREATE TABLE orders_list_cols_sharded").Rows()[0][1].(string)
	require.NotContains(t, row, "p_gb")
	require.NotContains(t, row, "p_de")
	require.Contains(t, row, "p_us")
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded").Check(testkit.Rows("1"))
}

func TestDDL_ALTER_SPT_LC_DropNonExistentPartition(t *testing.T) {
	// TC-ALTER-SPT-LC-05: DROP non-existent partition must error
	tk := setupSPTListCols(t)
	tk.MustContainErrMsg(
		"ALTER TABLE orders_list_cols_sharded DROP PARTITION p_nonexistent",
		"Error in list of partitions to DROP",
	)
}

func TestDDL_ALTER_SPT_R_DropPartition(t *testing.T) {
	// TC-ALTER-SPT-R-01: DROP PARTITION on RANGE COLUMNS + SHARD_KEY
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_range_sharded (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		created_at DATE NOT NULL,
		PRIMARY KEY (company_id, id, created_at)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (created_at) (
		PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
		PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
		PARTITION pmax  VALUES LESS THAN (MAXVALUE)
	)`)
	tk.MustExec("INSERT INTO orders_range_sharded VALUES (1, 42, '2023-06-01'), (2, 99, '2024-03-01')")
	tk.MustExec("ALTER TABLE orders_range_sharded DROP PARTITION p2023")
	row := tk.MustQuery("SHOW CREATE TABLE orders_range_sharded").Rows()[0][1].(string)
	require.NotContains(t, row, "p2023")
	tk.MustQuery("SELECT COUNT(*) FROM orders_range_sharded WHERE created_at < '2024-01-01'").Check(
		testkit.Rows("0"),
	)
	tk.MustQuery("SELECT COUNT(*) FROM orders_range_sharded").Check(testkit.Rows("1"))
}

func TestDDL_ALTER_SPT_LC_AddThenDrop(t *testing.T) {
	// TC-ALTER-SPT-LC-01 + TC-ALTER-SPT-LC-03 combined: ADD then DROP roundtrip
	tk := setupSPTListCols(t)
	tk.MustExec("ALTER TABLE orders_list_cols_sharded ADD PARTITION (PARTITION p_au VALUES IN ('AU'))")
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (10, 1, 'AU')")
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded WHERE country = 'AU'").Check(testkit.Rows("1"))
	tk.MustExec("ALTER TABLE orders_list_cols_sharded DROP PARTITION p_au")
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded WHERE country = 'AU'").Check(testkit.Rows("0"))
}

// ---------------------------------------------------------------------------
// TC-META-SPT: information_schema.PARTITIONS limitations for SPT
// (Covers the bug found by Lightning: IS.PARTITIONS shows only physical shard
// sub-partitions with PARTITION_METHOD=NONE and TABLE_ROWS=0 for SPT tables.
// Logical partition names must be obtained via SHOW CREATE TABLE.)
// ---------------------------------------------------------------------------

func TestMeta_SPT_ShowCreateTableHasLogicalPartitionNames(t *testing.T) {
	// TC-META-SPT-02: SHOW CREATE TABLE returns logical names, not shard_N names
	tk := setupSPTListCols(t)
	row := tk.MustQuery("SHOW CREATE TABLE orders_list_cols_sharded").Rows()[0][1].(string)
	require.Contains(t, row, "p_us")
	require.Contains(t, row, "p_gb")
	require.Contains(t, row, "p_de")
	require.NotContains(t, row, "shard_0")
}

func TestMeta_SPT_InfoSchemaPartitionsDoesNotShowLogicalNames(t *testing.T) {
	// TC-META-SPT-03: In unistore (test env), IS.PARTITIONS returns logical partition names for
	// SHARD BY + LIST COLUMNS tables. On a real TiKV cluster, physical shard sub-partitions are
	// shown instead (cluster behavior differs from unistore). This test verifies unistore behavior.
	tk := setupSPTListCols(t)
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (1, 42, 'US'), (2, 99, 'GB')")
	rows := tk.MustQuery(`
		SELECT PARTITION_NAME
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'orders_list_cols_sharded'
		ORDER BY PARTITION_NAME
	`).Rows()
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		names = append(names, r[0].(string))
	}
	// Unistore shows logical partition names (p_de, p_gb, p_us)
	require.Contains(t, names, "p_us")
	require.Contains(t, names, "p_gb")
	require.Contains(t, names, "p_de")
}

func TestMeta_PT_InfoSchemaPartitionsShowsLogicalNames(t *testing.T) {
	// TC-META-SPT-04: For non-sharded PT, IS.PARTITIONS works correctly (regression guard)
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE orders_list_cols (
		id      BIGINT NOT NULL,
		country VARCHAR(2) NOT NULL,
		PRIMARY KEY (id, country)
	) PARTITION BY LIST COLUMNS (country) (
		PARTITION p_us VALUES IN ('US'),
		PARTITION p_gb VALUES IN ('GB')
	)`)
	tk.MustExec("INSERT INTO orders_list_cols VALUES (1, 'US'), (2, 'GB')")
	rows := tk.MustQuery(`
		SELECT PARTITION_NAME
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'orders_list_cols'
		ORDER BY PARTITION_NAME
	`).Rows()
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		names = append(names, r[0].(string))
	}
	require.Contains(t, names, "p_gb")
	require.Contains(t, names, "p_us")
}

func TestMeta_SPT_TableRowsAlwaysZeroInInfoSchema(t *testing.T) {
	// TC-META-SPT-05: TABLE_ROWS in IS.PARTITIONS is always 0 for SPT — do not use it
	tk := setupSPTListCols(t)
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (1, 42, 'US'), (2, 99, 'US')")
	rows := tk.MustQuery(`
		SELECT TABLE_ROWS
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = 'test' AND TABLE_NAME = 'orders_list_cols_sharded'
	`).Rows()
	for _, row := range rows {
		require.Equal(t, "0", row[0].(string), "TABLE_ROWS must be 0 for all physical shards of an SPT table")
	}
}

func TestMeta_SPT_SelectPartitionSyntaxGivesCorrectCount(t *testing.T) {
	// TC-META-SPT-05 (correct approach) + TC-META-SPT-06:
	// SELECT COUNT(*) FROM t PARTITION (p_name) returns correct count for SPT
	tk := setupSPTListCols(t)
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (1, 42, 'US'), (2, 99, 'US'), (3, 7, 'GB')")
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded PARTITION (p_us)").Check(testkit.Rows("2"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded PARTITION (p_gb)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT COUNT(*) FROM orders_list_cols_sharded PARTITION (p_de)").Check(testkit.Rows("0"))
}

func TestMeta_SPT_SelectPartitionReturnsCorrectRows(t *testing.T) {
	// TC-META-SPT-06: SELECT ... PARTITION (p_name) returns only rows in that partition
	tk := setupSPTListCols(t)
	tk.MustExec("INSERT INTO orders_list_cols_sharded VALUES (1, 42, 'US'), (2, 99, 'GB'), (3, 7, 'DE')")
	tk.MustQuery("SELECT country FROM orders_list_cols_sharded PARTITION (p_gb)").Check(
		testkit.Rows("GB"),
	)
	// No US or DE rows bleed into p_gb
	rows := tk.MustQuery("SELECT country FROM orders_list_cols_sharded PARTITION (p_gb)").Rows()
	for _, row := range rows {
		require.Equal(t, "GB", row[0].(string))
	}
}

// ---------------------------------------------------------------------------
// TC-DDL-PK: Shard key must be part of the primary key
// ---------------------------------------------------------------------------

func TestDDL_ERR_ShardKeyNotInPK(t *testing.T) {
	// Shard key column not in PK → rejected at DDL time.
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, company_id BIGINT NOT NULL) SHARD BY (company_id) SHARDS 4`,
		dbterror.ErrShardKeyNotInPrimaryKey,
	)
}

func TestDDL_OK_ShardKeyInPK(t *testing.T) {
	// Shard key column in PK → allowed.
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t (id BIGINT NOT NULL, company_id BIGINT NOT NULL, PRIMARY KEY (company_id, id)) SHARD BY (company_id) SHARDS 4`)
	mi := tableInfo(t, dom, "t")
	require.NotNil(t, mi.ShardKeyInfo)
	require.Equal(t, []string{"company_id"}, mi.ShardKeyInfo.Columns)
}

func TestDDL_OK_MultiColumnShardKeyAllInPK(t *testing.T) {
	// Multi-column shard key — all columns in PK → allowed.
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t (
		id         BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		user_id    BIGINT NOT NULL,
		PRIMARY KEY (company_id, user_id, id)
	) SHARD BY (company_id, user_id) SHARDS 4`)
	mi := tableInfo(t, dom, "t")
	require.NotNil(t, mi.ShardKeyInfo)
	require.Equal(t, []string{"company_id", "user_id"}, mi.ShardKeyInfo.Columns)
}

func TestDDL_ERR_MultiColumnShardKeyPartialPK(t *testing.T) {
	// Multi-column shard key — only some columns in PK → rejected.
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (
			id         BIGINT NOT NULL,
			company_id BIGINT NOT NULL,
			user_id    BIGINT NOT NULL,
			PRIMARY KEY (company_id, id)
		) SHARD BY (company_id, user_id) SHARDS 4`,
		dbterror.ErrShardKeyNotInPrimaryKey,
	)
}

func TestDDL_OK_NoPKWithShardKey(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t (
		record_id VARCHAR(64) NOT NULL,
		company_id BIGINT NOT NULL,
		KEY idx_record (record_id)
	) SHARD BY (company_id) SHARDS 4`)

	mi := tableInfo(t, dom, "t")
	require.False(t, mi.PKIsHandle)
	require.False(t, mi.IsCommonHandle)
	require.NotNil(t, mi.ShardKeyInfo)
	require.Equal(t, []string{"company_id"}, mi.ShardKeyInfo.Columns)

	tk.MustExec(`INSERT INTO t VALUES
		('r1', 10),
		('r2', 20),
		('r3', 30)`)
	tk.MustQuery("SELECT record_id, company_id FROM t ORDER BY record_id").Check(
		testkit.Rows("r1 10", "r2 20", "r3 30"),
	)
	tk.MustQuery("SELECT COUNT(DISTINCT _tidb_rowid), COUNT(*) FROM t").Check(
		testkit.Rows("3 3"),
	)
	tk.MustQuery("SELECT COUNT(*) FROM t USE INDEX(idx_record)").Check(
		testkit.Rows("3"),
	)
	tk.MustExec("ADMIN CHECK TABLE t")
	tk.MustExec("ADMIN CHECK INDEX t idx_record")
}

func TestDDL_OK_NoPKPartitionedWithShardKey(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t (
		record_id VARCHAR(64) NOT NULL,
		shard_id VARCHAR(64) NOT NULL,
		org_name VARCHAR(64) NOT NULL,
		etl_ts DATETIME NOT NULL,
		KEY idx_record (record_id)
	) SHARD BY (shard_id) SHARDS 4
	PARTITION BY LIST COLUMNS (org_name, etl_ts) (
		PARTITION p01 VALUES IN (('org_0000', '2026-01-02 00:00:00')),
		PARTITION p02 VALUES IN (('org_0001', '2026-01-02 00:00:00'))
	)`)

	mi := tableInfo(t, dom, "t")
	require.False(t, mi.PKIsHandle)
	require.False(t, mi.IsCommonHandle)
	require.Len(t, mi.Partition.Definitions, 2)
	for _, def := range mi.Partition.Definitions {
		require.Len(t, def.ShardIDs, 4)
	}

	tk.MustExec(`INSERT INTO t VALUES
		('r1', 's1', 'org_0000', '2026-01-02 00:00:00'),
		('r2', 's2', 'org_0001', '2026-01-02 00:00:00')`)
	tk.MustQuery("SELECT COUNT(*) FROM t PARTITION (p01)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT COUNT(*) FROM t PARTITION (p02)").Check(testkit.Rows("1"))
	tk.MustQuery("SELECT COUNT(DISTINCT _tidb_rowid), COUNT(*) FROM t").Check(
		testkit.Rows("2 2"),
	)
	tk.MustExec("ADMIN CHECK TABLE t")
	tk.MustExec("ADMIN CHECK INDEX t idx_record")
}
