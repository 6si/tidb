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

package tables

import (
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// allColNames lists the full set of columns on the table; shardColNames are the shard key columns.
func makeShardTableInfoFull(shardCnt int, allColNames []string, shardColNames []string) *model.TableInfo {
	cols := make([]*model.ColumnInfo, len(allColNames))
	for i, name := range allColNames {
		cols[i] = &model.ColumnInfo{
			ID:        int64(i + 1),
			Name:      pmodel.NewCIStr(name),
			Offset:    i,
			State:     model.StatePublic,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		}
	}
	lowerShardCols := make([]string, len(shardColNames))
	for i, n := range shardColNames {
		lowerShardCols[i] = pmodel.NewCIStr(n).L
	}
	shardIDs := make([]int64, shardCnt)
	for i := range shardIDs {
		shardIDs[i] = int64(100 + i)
	}
	return &model.TableInfo{
		ID:    1,
		Name:  pmodel.NewCIStr("t"),
		State: model.StatePublic,
		Columns: cols,
		ShardKeyInfo: &model.ShardKeyInfo{
			Columns:  lowerShardCols,
			ShardCnt: shardCnt,
			ShardIDs: shardIDs,
		},
	}
}

func makeShardTableInfo(shardCnt int, colNames []string) *model.TableInfo {
	return makeShardTableInfoFull(shardCnt, colNames, colNames)
}

func makeShardedTableCommon(tblInfo *model.TableInfo) (*TableCommon, error) {
	cols := make([]*table.Column, len(tblInfo.Columns))
	for i, c := range tblInfo.Columns {
		cols[i] = table.ToColumn(c)
	}
	constraints, err := table.LoadCheckConstraint(tblInfo)
	if err != nil {
		return nil, err
	}
	var tc TableCommon
	initTableCommon(&tc, tblInfo, tblInfo.ID, cols, autoid.NewAllocators(false), constraints)
	return &tc, nil
}

func TestNewShardedTable_NilShardKeyInfo(t *testing.T) {
	tblInfo := &model.TableInfo{ID: 1}
	tc := &TableCommon{}
	_, err := newShardedTable(tc, tblInfo)
	require.ErrorContains(t, err, "ShardKeyInfo is nil")
}

func TestNewShardedTable_ShardIDsLengthMismatch(t *testing.T) {
	tblInfo := makeShardTableInfo(4, []string{"a"})
	tblInfo.ShardKeyInfo.ShardIDs = []int64{100, 101} // only 2, not 4
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	_, err = newShardedTable(tc, tblInfo)
	require.ErrorContains(t, err, "ShardIDs length 2 != ShardCnt 4")
}

func TestNewShardedTable_ColumnNotFound(t *testing.T) {
	tblInfo := makeShardTableInfo(2, []string{"a"})
	tblInfo.ShardKeyInfo.Columns = []string{"nonexistent"}
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	_, err = newShardedTable(tc, tblInfo)
	require.ErrorContains(t, err, "shard key column")
	require.ErrorContains(t, err, "not found")
}

func TestNewShardedTable_CreatesCorrectShardCount(t *testing.T) {
	for _, cnt := range []int{2, 4, 8} {
		tblInfo := makeShardTableInfo(cnt, []string{"id"})
		tc, err := makeShardedTableCommon(tblInfo)
		require.NoError(t, err)
		st, err := newShardedTable(tc, tblInfo)
		require.NoError(t, err)
		require.Len(t, st.shards, cnt)
	}
}

func TestNewShardedTable_ShardPhysicalIDs(t *testing.T) {
	tblInfo := makeShardTableInfo(3, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	for i, shard := range st.shards {
		require.Equal(t, tblInfo.ShardKeyInfo.ShardIDs[i], shard.physicalTableID,
			"shard %d should have physical ID %d", i, tblInfo.ShardKeyInfo.ShardIDs[i])
		require.Equal(t, tblInfo.ID, shard.tableID,
			"shard %d should still report logical tableID", i)
	}
}

func TestLocateShard_DeterministicRouting(t *testing.T) {
	tblInfo := makeShardTableInfo(4, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	row := []types.Datum{types.NewIntDatum(42)}
	idx1, err := st.locateShard(row)
	require.NoError(t, err)
	idx2, err := st.locateShard(row)
	require.NoError(t, err)
	require.Equal(t, idx1, idx2, "same input must route to same shard")
	require.Less(t, idx1, 4)
}

func TestLocateShard_MatchesCRC32IEEE(t *testing.T) {
	// locateShard must match crc32 IEEE so the query planner and storage agree.
	tblInfo := makeShardTableInfo(4, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	for _, v := range []int64{0, 1, 100, -1, 999999} {
		row := []types.Datum{types.NewIntDatum(v)}
		idx, err := st.locateShard(row)
		require.NoError(t, err)

		h := crc32.NewIEEE()
		data, err := row[0].ToHashKey()
		require.NoError(t, err)
		h.Write(data)
		expected := int(h.Sum32() % 4)
		require.Equal(t, expected, idx, "value %d: locateShard must match crc32 IEEE", v)
	}
}

func TestLocateShard_NullValue(t *testing.T) {
	tblInfo := makeShardTableInfo(4, []string{"a"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	nullRow := []types.Datum{types.NewDatum(nil)}
	idx, err := st.locateShard(nullRow)
	require.NoError(t, err)
	require.Less(t, idx, 4)
	require.GreaterOrEqual(t, idx, 0)

	// NULL must hash consistently.
	idx2, err := st.locateShard(nullRow)
	require.NoError(t, err)
	require.Equal(t, idx, idx2)
}

func TestLocateShard_MultiColumnKey(t *testing.T) {
	tblInfo := makeShardTableInfo(4, []string{"a", "b"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	row1 := []types.Datum{types.NewIntDatum(1), types.NewIntDatum(2)}
	row2 := []types.Datum{types.NewIntDatum(2), types.NewIntDatum(1)}
	idx1, _ := st.locateShard(row1)
	idx2, _ := st.locateShard(row2)
	// (1,2) and (2,1) may or may not land in the same shard, but each must be in range.
	require.Less(t, idx1, 4)
	require.Less(t, idx2, 4)
}

func TestLocateShard_ShardColIdxUsesOffset(t *testing.T) {
	// Table has columns [a, b]; shard key is only "b" (offset 1).
	// Routing must use b's value (offset 1), not a (offset 0).
	tblInfo := makeShardTableInfoFull(4, []string{"a", "b"}, []string{"b"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	// Both rows have same a=1 but different b values; routing must differ (with high probability).
	rowB10 := []types.Datum{types.NewIntDatum(1), types.NewIntDatum(10)}
	rowB20 := []types.Datum{types.NewIntDatum(1), types.NewIntDatum(20)}
	idxB10, _ := st.locateShard(rowB10)
	idxB20, _ := st.locateShard(rowB20)

	// Verify that shardColIdx resolved to offset 1 (b's offset).
	require.Equal(t, []int{1}, st.shardColIdx)
	// Routing must agree with direct crc32 on b's value.
	h10 := crc32.NewIEEE()
	d10, _ := rowB10[1].ToHashKey()
	h10.Write(d10)
	require.Equal(t, int(h10.Sum32()%4), idxB10)

	h20 := crc32.NewIEEE()
	d20, _ := rowB20[1].ToHashKey()
	h20.Write(d20)
	require.Equal(t, int(h20.Sum32()%4), idxB20)
}

func TestShardedTable_ImplementsTableInterface(t *testing.T) {
	tblInfo := makeShardTableInfo(2, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	var _ table.Table = st // compile-time check
	_ = st
}

func TestTableFromMeta_NonShardedTableUnchanged(t *testing.T) {
	// Tables without ShardKeyInfo must not take the shard path.
	tblInfo := &model.TableInfo{
		ID:    1,
		Name:  pmodel.NewCIStr("plain"),
		State: model.StatePublic,
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: pmodel.NewCIStr("id"), Offset: 0, State: model.StatePublic, FieldType: *types.NewFieldType(mysql.TypeLonglong)},
		},
	}
	tbl, err := TableFromMeta(autoid.NewAllocators(false), tblInfo)
	require.NoError(t, err)
	_, isSharded := tbl.(*shardedTable)
	require.False(t, isSharded, "plain table must not become shardedTable")
}

func TestTableFromMeta_ShardedTablePath(t *testing.T) {
	tblInfo := makeShardTableInfo(4, []string{"id"})
	tbl, err := TableFromMeta(autoid.NewAllocators(false), tblInfo)
	require.NoError(t, err)
	_, isSharded := tbl.(*shardedTable)
	require.True(t, isSharded, "table with ShardKeyInfo and ShardIDs must become shardedTable")
}

func TestTableFromMeta_ShardedTable_SyntheticPartitionInfo(t *testing.T) {
	tblInfo := makeShardTableInfo(4, []string{"id"})
	tbl, err := TableFromMeta(autoid.NewAllocators(false), tblInfo)
	require.NoError(t, err)
	pi := tbl.Meta().GetPartitionInfo()
	require.NotNil(t, pi, "sharded table must have synthetic PartitionInfo so planner scans physical shards")
	require.Equal(t, 4, len(pi.Definitions))
	require.True(t, pi.Enable)
	for i, def := range pi.Definitions {
		require.Equal(t, tblInfo.ShardKeyInfo.ShardIDs[i], def.ID,
			"synthetic partition def[%d].ID must equal ShardIDs[%d]", i, i)
	}
}

// --- partitioned+sharded table helpers ---

func makePartitionedShardTableInfo(partCnt, shardCnt int, colNames []string) *model.TableInfo {
	tblInfo := makeShardTableInfo(shardCnt, colNames)
	tblInfo.ShardKeyInfo.ShardIDs = nil // will be on partition definitions

	defs := make([]model.PartitionDefinition, partCnt)
	physID := int64(1000)
	for i := range defs {
		ids := make([]int64, shardCnt)
		for j := range ids {
			physID++
			ids[j] = physID
		}
		defs[i] = model.PartitionDefinition{
			ID:       physID + int64(i+1)*100, // partition physical ID
			Name:     pmodel.NewCIStr(fmt.Sprintf("p%d", i)),
			ShardIDs: ids,
		}
	}
	tblInfo.Partition = &model.PartitionInfo{
		Type:        3, // LIST
		Enable:      true,
		Definitions: defs,
	}
	return tblInfo
}

func TestNewShardedTable_PartitionedSharded_PhysicalIDCount(t *testing.T) {
	partCnt, shardCnt := 3, 4
	tblInfo := makePartitionedShardTableInfo(partCnt, shardCnt, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)
	// Must have partCnt * shardCnt physical shards total.
	require.Len(t, st.shards, partCnt*shardCnt)
	require.Len(t, st.physicalIDs, partCnt*shardCnt)
}

func TestNewShardedTable_PartitionedSharded_UniquePhysicalIDs(t *testing.T) {
	tblInfo := makePartitionedShardTableInfo(3, 4, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	seen := make(map[int64]struct{})
	for _, id := range st.physicalIDs {
		require.NotContains(t, seen, id, "duplicate physical ID %d", id)
		seen[id] = struct{}{}
	}
}

func TestNewShardedTable_PartitionedSharded_ShardIDsNilOnSKI(t *testing.T) {
	// For partitioned+sharded tables, ShardKeyInfo.ShardIDs must be nil;
	// shard IDs live on each PartitionDefinition.ShardIDs instead.
	tblInfo := makePartitionedShardTableInfo(2, 3, []string{"id"})
	require.Nil(t, tblInfo.ShardKeyInfo.ShardIDs,
		"ShardKeyInfo.ShardIDs must be nil for partitioned+sharded tables")
}

func TestNewShardedTable_PartitionedSharded_MismatchedShardIDs(t *testing.T) {
	tblInfo := makePartitionedShardTableInfo(2, 3, []string{"id"})
	// Corrupt one partition's ShardIDs length.
	tblInfo.Partition.Definitions[0].ShardIDs = []int64{100}
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	_, err = newShardedTable(tc, tblInfo)
	require.ErrorContains(t, err, "ShardIDs length")
}

// --- PartitionedTable interface tests ---

func TestShardedTable_ImplementsPartitionedTable(t *testing.T) {
	tblInfo := makeShardTableInfo(4, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	var _ table.PartitionedTable = st // compile-time check
	require.NotNil(t, st.GetPartitionedTable())
}

func TestShardedTable_GetAllPartitionIDs(t *testing.T) {
	tblInfo := makeShardTableInfo(4, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	ids := st.GetAllPartitionIDs()
	require.Len(t, ids, 4)
	// Must match the physical IDs from ShardKeyInfo.
	for i, id := range ids {
		require.Equal(t, tblInfo.ShardKeyInfo.ShardIDs[i], id)
	}
}

func TestShardedTable_GetPartition_Found(t *testing.T) {
	tblInfo := makeShardTableInfo(4, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	for _, physID := range tblInfo.ShardKeyInfo.ShardIDs {
		pt := st.GetPartition(physID)
		require.NotNil(t, pt, "GetPartition(%d) must return non-nil", physID)
		require.Equal(t, physID, pt.GetPhysicalID())
	}
}

func TestShardedTable_GetPartition_NotFound(t *testing.T) {
	tblInfo := makeShardTableInfo(4, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	pt := st.GetPartition(99999)
	require.Nil(t, pt, "GetPartition with unknown physicalID must return nil")
}

func TestShardedTable_GetPartitionColumnIDs(t *testing.T) {
	tblInfo := makeShardTableInfoFull(4, []string{"a", "b"}, []string{"b"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	ids := st.GetPartitionColumnIDs()
	require.Len(t, ids, 1)
	// Column "b" has ID 2 (second column, ID = offset+1 in makeShardTableInfoFull).
	require.Equal(t, int64(2), ids[0])
}

func TestShardedTable_GetPartitionColumnNames(t *testing.T) {
	tblInfo := makeShardTableInfoFull(4, []string{"a", "b"}, []string{"b"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	names := st.GetPartitionColumnNames()
	require.Len(t, names, 1)
	require.Equal(t, "b", names[0].L)
}

func TestShardedTable_PartitionedSharded_GetAllPartitionIDs(t *testing.T) {
	partCnt, shardCnt := 2, 3
	tblInfo := makePartitionedShardTableInfo(partCnt, shardCnt, []string{"id"})
	tc, err := makeShardedTableCommon(tblInfo)
	require.NoError(t, err)
	st, err := newShardedTable(tc, tblInfo)
	require.NoError(t, err)

	ids := st.GetAllPartitionIDs()
	require.Len(t, ids, partCnt*shardCnt)

	// All must be unique.
	seen := make(map[int64]struct{})
	for _, id := range ids {
		require.NotContains(t, seen, id)
		seen[id] = struct{}{}
	}
}
