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
	"hash/crc32"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
)

var _ table.Table = &shardedTable{}
var _ table.PartitionedTable = &shardedTable{}
var _ table.ShardedPartitionedTable = &shardedTable{}

// shardPhysical is the physical backing store for one shard slot.
// It has its own physicalTableID and recordPrefix so TiKV rows land in a
// non-overlapping key range per shard, enabling PD placement rule enforcement.
type shardPhysical struct {
	TableCommon
	owner *shardedTable
}

// GetPhysicalID implements table.PhysicalTable.
func (sp *shardPhysical) GetPhysicalID() int64 {
	return sp.physicalTableID
}

// shardedTable is a logical table that routes writes to per-shard physical tables
// and implements PartitionedTable so the query planner scans each shard's physical
// key range rather than the logical table prefix (which contains no data).
//
// For a non-partitioned sharded table, shards is a flat slice of ShardCnt entries.
// For a partitioned+sharded table, shards contains ShardCnt entries per partition,
// laid out as shards[partitionIdx*ShardCnt + shardIdx]. physicalIDs mirrors this layout.
type shardedTable struct {
	TableCommon
	shards       []*shardPhysical
	shardColIdx  []int                // column Offset per shard-key column, resolved once at open time
	shardCnt     int                  // number of shards per partition (== ShardKeyInfo.ShardCnt)
	partCnt      int                  // number of partitions (1 for non-partitioned tables)
	physicalIDs  []int64              // flat: physicalIDs[partIdx*shardCnt + shardIdx]
	origPartInfo *model.PartitionInfo // original PartitionInfo before shard synthesis (nil for shard-only)
	partExpr     *PartitionExpr       // lazily built from origPartInfo for LIST pruning; nil for shard-only
}

// newShardedTable builds a shardedTable from a TableInfo that has ShardKeyInfo
// populated. Handles both non-partitioned and partitioned+sharded tables.
//
// For non-partitioned sharded tables we inject a synthetic PartitionInfo into a
// copy of tblInfo so that the planner's GetPartitionInfo() check succeeds and it
// scans each shard's physical key range rather than the empty logical table prefix.
func newShardedTable(tbl *TableCommon, tblInfo *model.TableInfo) (*shardedTable, error) {
	ski := tblInfo.ShardKeyInfo
	if ski == nil {
		return nil, errors.New("newShardedTable: ShardKeyInfo is nil")
	}

	st := &shardedTable{
		TableCommon: tbl.Copy(),
		shardCnt:    ski.ShardCnt,
	}

	// Resolve column offsets for the shard key once, avoiding per-row lookups.
	st.shardColIdx = make([]int, len(ski.Columns))
	for i, colName := range ski.Columns {
		found := false
		for _, col := range tblInfo.Columns {
			if col.Name.L == colName {
				st.shardColIdx[i] = col.Offset
				found = true
				break
			}
		}
		if !found {
			return nil, errors.Errorf("newShardedTable: shard key column %q not found", colName)
		}
	}

	partInfo := tblInfo.GetPartitionInfo()
	// A non-nil partInfo with PartitionDefinitions that have no ShardIDs means this
	// is a shard-only table whose synthetic PartitionInfo was injected by the
	// InfoSchema builder. In that case, use ski.ShardIDs directly.
	isSyntheticPI := partInfo != nil && len(ski.ShardIDs) > 0 &&
		(len(partInfo.Definitions) == 0 || len(partInfo.Definitions[0].ShardIDs) == 0)
	if partInfo != nil && !isSyntheticPI {
		// Partitioned + sharded: each partition has its own ShardIDs slice.
		st.origPartInfo = partInfo
		// Build the PartitionExpr from the original PartitionInfo so that
		// findUsedListPartitions can use it for LIST COLUMNS pruning.
		if pe, err := newPartitionExpr(tblInfo, partInfo.Type, partInfo.Expr, partInfo.Columns, partInfo.Definitions); err == nil {
			st.partExpr = pe
		}
		st.partCnt = len(partInfo.Definitions)
		st.shards = make([]*shardPhysical, st.partCnt*st.shardCnt)
		st.physicalIDs = make([]int64, st.partCnt*st.shardCnt)
		for pi, def := range partInfo.Definitions {
			if len(def.ShardIDs) != ski.ShardCnt {
				return nil, errors.Errorf(
					"newShardedTable: partition %q ShardIDs length %d != ShardCnt %d",
					def.Name.O, len(def.ShardIDs), ski.ShardCnt)
			}
			for si, physID := range def.ShardIDs {
				idx := pi*st.shardCnt + si
				var sp shardPhysical
				if err := initTableCommonWithIndices(&sp.TableCommon, tblInfo, physID,
					tbl.Columns, tbl.allocs, tbl.Constraints); err != nil {
					return nil, err
				}
				sp.owner = st
				st.shards[idx] = &sp
				st.physicalIDs[idx] = physID
			}
		}
	} else {
		// Sharded only: ShardKeyInfo.ShardIDs is the flat shard ID list.
		// (partInfo may be the synthetic one injected by the InfoSchema builder)
		if len(ski.ShardIDs) != ski.ShardCnt {
			return nil, errors.Errorf("newShardedTable: ShardIDs length %d != ShardCnt %d",
				len(ski.ShardIDs), ski.ShardCnt)
		}
		st.partCnt = 1
		st.shards = make([]*shardPhysical, ski.ShardCnt)
		st.physicalIDs = make([]int64, ski.ShardCnt)
		for i, physID := range ski.ShardIDs {
			var sp shardPhysical
			if err := initTableCommonWithIndices(&sp.TableCommon, tblInfo, physID,
				tbl.Columns, tbl.allocs, tbl.Constraints); err != nil {
				return nil, err
			}
			sp.owner = st
			st.shards[i] = &sp
			st.physicalIDs[i] = physID
		}
	}
	return st, nil
}

// shardIdx returns the flat index into st.shards for a given (partitionIdx, shardIdx).
func (t *shardedTable) shardIdx(partIdx, shardSlot int) int {
	return partIdx*t.shardCnt + shardSlot
}

// locateShard returns the shard slot (0..shardCnt-1) for a row using CRC32/IEEE,
// matching ForKeyPruning.LocateKeyPartition so planner and storage agree.
func (t *shardedTable) locateShard(r []types.Datum) (int, error) {
	h := crc32.NewIEEE()
	for _, idx := range t.shardColIdx {
		val := r[idx]
		if val.Kind() == types.KindNull {
			h.Write([]byte{0})
		} else {
			data, err := val.ToHashKey()
			if err != nil {
				return 0, err
			}
			h.Write(data)
		}
	}
	return int(h.Sum32() % uint32(t.shardCnt)), nil
}

// shardForRow returns the shardPhysical for a row. For non-partitioned tables
// partIdx is always 0.
func (t *shardedTable) shardForRow(r []types.Datum, partIdx int) (*shardPhysical, error) {
	slot, err := t.locateShard(r)
	if err != nil {
		return nil, err
	}
	return t.shards[t.shardIdx(partIdx, slot)], nil
}

// --- table.Table write methods ---

// AddRecord routes the insert to the correct physical shard.
func (t *shardedTable) AddRecord(ctx table.MutateContext, txn kv.Transaction,
	r []types.Datum, opts ...table.AddRecordOption) (kv.Handle, error) {
	sp, err := t.shardForRow(r, 0)
	if err != nil {
		return nil, err
	}
	opt := table.NewAddRecordOpt(opts...)
	return sp.addRecord(ctx, txn, r, opt)
}

// UpdateRecord routes the update to the correct shard; handles cross-shard moves.
func (t *shardedTable) UpdateRecord(ctx table.MutateContext, txn kv.Transaction,
	h kv.Handle, oldData, newData []types.Datum, touched []bool,
	opts ...table.UpdateRecordOption) error {
	fromSlot, err := t.locateShard(oldData)
	if err != nil {
		return err
	}
	toSlot, err := t.locateShard(newData)
	if err != nil {
		return err
	}
	opt := table.NewUpdateRecordOpt(opts...)
	fromShard := t.shards[t.shardIdx(0, fromSlot)]
	toShard := t.shards[t.shardIdx(0, toSlot)]
	if fromShard == toShard {
		return fromShard.updateRecord(ctx, txn, h, oldData, newData, touched, opt)
	}
	// Cross-shard update: delete from old shard, insert into new shard.
	if err := fromShard.removeRecord(ctx, txn, h, oldData, table.NewRemoveRecordOpt()); err != nil {
		return err
	}
	_, err = toShard.addRecord(ctx, txn, newData, opt.GetAddRecordOpt())
	return err
}

// RemoveRecord routes the delete to the correct physical shard.
func (t *shardedTable) RemoveRecord(ctx table.MutateContext, txn kv.Transaction,
	h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	sp, err := t.shardForRow(r, 0)
	if err != nil {
		return err
	}
	opt := table.NewRemoveRecordOpt(opts...)
	return sp.removeRecord(ctx, txn, h, r, opt)
}

// --- table.PartitionedTable implementation ---
// The planner uses PartitionedTable to enumerate physical ranges for scans.
// Shards map 1:1 to "partitions" from the planner's perspective.

// GetPartitionedTable implements table.Table.
func (t *shardedTable) GetPartitionedTable() table.PartitionedTable {
	return t
}

// GetPartition returns the shardPhysical whose physicalTableID matches physicalID.
func (t *shardedTable) GetPartition(physicalID int64) table.PhysicalTable {
	for _, sp := range t.shards {
		if sp.physicalTableID == physicalID {
			return sp
		}
	}
	return nil
}

// GetPartitionByRow returns the shard that owns the given row (partition 0 for
// non-partitioned tables).
func (t *shardedTable) GetPartitionByRow(_ expression.EvalContext, r []types.Datum) (table.PhysicalTable, error) {
	sp, err := t.shardForRow(r, 0)
	if err != nil {
		return nil, err
	}
	return sp, nil
}

// GetPartitionIdxByRow returns the flat index into t.shards for the given row.
func (t *shardedTable) GetPartitionIdxByRow(_ expression.EvalContext, r []types.Datum) (int, error) {
	slot, err := t.locateShard(r)
	if err != nil {
		return 0, err
	}
	return t.shardIdx(0, slot), nil
}

// GetAllPartitionIDs returns all physical shard IDs, used by the planner to build
// key ranges for full table scans.
func (t *shardedTable) GetAllPartitionIDs() []int64 {
	ids := make([]int64, len(t.physicalIDs))
	copy(ids, t.physicalIDs)
	return ids
}

// GetPartitionColumnIDs returns the column IDs of the shard key columns.
func (t *shardedTable) GetPartitionColumnIDs() []int64 {
	ski := t.meta.ShardKeyInfo
	ids := make([]int64, 0, len(ski.Columns))
	for _, colName := range ski.Columns {
		for _, col := range t.meta.Columns {
			if col.Name.L == colName {
				ids = append(ids, col.ID)
				break
			}
		}
	}
	return ids
}

// GetPartitionColumnNames returns the CIStr names of the shard key columns.
func (t *shardedTable) GetPartitionColumnNames() []pmodel.CIStr {
	ski := t.meta.ShardKeyInfo
	names := make([]pmodel.CIStr, len(ski.Columns))
	for i, colName := range ski.Columns {
		names[i] = pmodel.NewCIStr(colName)
	}
	return names
}

// CheckForExchangePartition is not applicable to sharded tables.
func (t *shardedTable) CheckForExchangePartition(_ expression.EvalContext, _ *model.PartitionInfo, _ []types.Datum, _, _ int64) error {
	return errors.New("EXCHANGE PARTITION is not supported on sharded tables")
}

// OrigPartitionInfo returns the real PartitionInfo for partitioned+sharded tables,
// before the synthetic flat PartitionInfo was injected for planner purposes.
// Returns nil for shard-only (non-partitioned) tables.
func (t *shardedTable) OrigPartitionInfo() *model.PartitionInfo {
	return t.origPartInfo
}

// PartitionExpr returns the PartitionExpr built from the original PartitionInfo
// (e.g. LIST COLUMNS). This satisfies the partitionTable interface in
// rule_partition_processor.go so that findUsedListPartitions can run LIST pruning
// on the logical partitions of a SHARD BY + LIST COLUMNS table.
// Returns nil for shard-only (non-partitioned) tables.
func (t *shardedTable) PartitionExpr() *PartitionExpr {
	return t.partExpr
}
