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
		PRIMARY KEY (id)
	) SHARD_KEY (company_id) INTO 4 SHARDS`)
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
		PRIMARY KEY (id)
	) SHARD_KEY (company_id, user_id) INTO 8 SHARDS`)
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
		PRIMARY KEY (id)
	) SHARD_KEY (tenant_code) INTO 4 SHARDS`)
}

func TestDDL_SST_CharShardKey(t *testing.T) {
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE regions_sharded (
		id   BIGINT NOT NULL,
		code CHAR(3) NOT NULL,
		name VARCHAR(128),
		PRIMARY KEY (id)
	) SHARD_KEY (code) INTO 4 SHARDS`)
}

func TestDDL_SST_MinShards(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t_shard_min (id BIGINT PRIMARY KEY, k BIGINT NOT NULL) SHARD_KEY (k) INTO 2 SHARDS`)
	mi := tableInfo(t, dom, "t_shard_min")
	require.Equal(t, 2, mi.ShardKeyInfo.ShardCnt)
	require.Len(t, mi.Partition.Definitions, 2)
}

func TestDDL_SST_MaxShards(t *testing.T) {
	tk, dom := setup(t)
	tk.MustExec(`CREATE TABLE t_shard_max (id BIGINT PRIMARY KEY, k BIGINT NOT NULL) SHARD_KEY (k) INTO 64 SHARDS`)
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
	tk.MustExec(`CREATE TABLE t_names (id BIGINT PRIMARY KEY, k BIGINT NOT NULL) SHARD_KEY (k) INTO 4 SHARDS`)
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
	tk.MustExec(`CREATE TABLE t_ids (id BIGINT PRIMARY KEY, k BIGINT NOT NULL) SHARD_KEY (k) INTO 4 SHARDS`)
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
		PRIMARY KEY (id, created_at)
	) SHARD_KEY (company_id) INTO 4 SHARDS
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
		PRIMARY KEY (id, region_id)
	) SHARD_KEY (company_id) INTO 4 SHARDS
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
		PRIMARY KEY (id, country)
	) SHARD_KEY (company_id) INTO 4 SHARDS
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
		PRIMARY KEY (id, bucket_id)
	) SHARD_KEY (company_id) INTO 4 SHARDS
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
		PRIMARY KEY (id, bucket_id)
	) SHARD_KEY (company_id) INTO 4 SHARDS
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
		`CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD_KEY (k) INTO 0 SHARDS`,
		"Shard count must be greater than 1",
	)
}

func TestDDL_ERR_ShardCountOne(t *testing.T) {
	// TC-DDL-ERR-01b
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD_KEY (k) INTO 1 SHARDS`,
		"Shard count must be greater than 1",
	)
}

func TestDDL_ERR_ShardCountExceedsMax(t *testing.T) {
	// TC-DDL-ERR-02
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD_KEY (k) INTO 65 SHARDS`,
		"Shard count must be between 2 and 64",
	)
}

func TestDDL_ERR_BlobShardKey(t *testing.T) {
	// TC-DDL-ERR-03
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, data BLOB) SHARD_KEY (data) INTO 4 SHARDS`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_VarbinaryShardKey(t *testing.T) {
	// TC-DDL-ERR-04
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, data VARBINARY(256)) SHARD_KEY (data) INTO 4 SHARDS`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_BinaryShardKey(t *testing.T) {
	// TC-DDL-ERR-05
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, code BINARY(16)) SHARD_KEY (code) INTO 4 SHARDS`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_FloatShardKey(t *testing.T) {
	// TC-DDL-ERR-06
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, score FLOAT) SHARD_KEY (score) INTO 4 SHARDS`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_DecimalShardKey(t *testing.T) {
	// TC-DDL-ERR-07
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, amount DECIMAL(10,2)) SHARD_KEY (amount) INTO 4 SHARDS`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_DatetimeShardKey(t *testing.T) {
	// TC-DDL-ERR-08
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, ts DATETIME) SHARD_KEY (ts) INTO 4 SHARDS`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_JSONShardKey(t *testing.T) {
	// TC-DDL-ERR-09
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, meta JSON) SHARD_KEY (meta) INTO 4 SHARDS`,
		dbterror.ErrShardKeyColumnType,
	)
}

func TestDDL_ERR_AutoIncrementShardKey(t *testing.T) {
	// TC-DDL-ERR-10
	tk, _ := setup(t)
	tk.MustGetDBError(
		`CREATE TABLE t (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, k BIGINT) SHARD_KEY (id) INTO 4 SHARDS`,
		dbterror.ErrShardKeyAutoIncrement,
	)
}

func TestDDL_ERR_TemporaryTable(t *testing.T) {
	// TC-DDL-ERR-11
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TEMPORARY TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD_KEY (k) INTO 4 SHARDS`,
		"SHARD_KEY on temporary tables",
	)
}

func TestDDL_ERR_ShardRowIDBits(t *testing.T) {
	// TC-DDL-ERR-12
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT, k BIGINT) SHARD_KEY (k) INTO 4 SHARDS SHARD_ROW_ID_BITS = 4`,
		"SHARD_KEY together with SHARD_ROW_ID_BITS",
	)
}

func TestDDL_ERR_DuplicateShardKeyColumn(t *testing.T) {
	// TC-DDL-ERR-13
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD_KEY (k, k) INTO 4 SHARDS`,
		"k",
	)
}

func TestDDL_ERR_NonExistentColumn(t *testing.T) {
	// TC-DDL-ERR-14
	tk, _ := setup(t)
	tk.MustContainErrMsg(
		`CREATE TABLE t (id BIGINT PRIMARY KEY, k BIGINT) SHARD_KEY (nonexistent) INTO 4 SHARDS`,
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
	) SHARD_KEY (company_id) INTO 4 SHARDS
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
	) SHARD_KEY (region_id) INTO 4 SHARDS
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
		PRIMARY KEY (id, ts)
	) SHARD_KEY (company_id) INTO 4 SHARDS
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
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, company_id BIGINT) SHARD_KEY (company_id) INTO 4 SHARDS`)
	tk.MustContainErrMsg(`ALTER TABLE orders_sharded MODIFY SHARDS INTO 8 SHARDS`, "")
	tk.MustContainErrMsg(`ALTER TABLE orders_sharded DROP SHARD_KEY`, "")
	tk.MustContainErrMsg(`ALTER TABLE orders ADD SHARD_KEY (company_id) INTO 4 SHARDS`, "")
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
			tk.MustExec("CREATE TABLE t_type_test (id BIGINT PRIMARY KEY, k " + tc.colType + ") SHARD_KEY (k) INTO 4 SHARDS")
		})
	}
}

func TestDDL_EDGE_TextShardKey(t *testing.T) {
	// TC-EDGE-11: TEXT (non-binary) allowed
	tk, _ := setup(t)
	tk.MustExec(`CREATE TABLE t_text (id BIGINT PRIMARY KEY, k TEXT NOT NULL) SHARD_KEY (k) INTO 4 SHARDS`)
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
