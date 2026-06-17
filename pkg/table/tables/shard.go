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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
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
	shards           []*shardPhysical
	shardColIdx      []int                // column Offset per shard-key column, resolved once at open time
	shardCnt         int                  // number of shards per partition (== ShardKeyInfo.ShardCnt)
	partCnt          int                  // number of partitions (1 for non-partitioned tables)
	physicalIDs      []int64              // flat: physicalIDs[partIdx*shardCnt + shardIdx]
	shardsDefCount   int                  // len(origPartInfo.Definitions) when shards/physicalIDs were last built
	origPartInfo     *model.PartitionInfo // original LIST/RANGE PartitionInfo (nil for shard-only tables)
	flatPI           *model.PartitionInfo // synthetic flat PI: one entry per physical shard (nil for shard-only)
	partExpr         *PartitionExpr       // cached PartitionExpr for LIST pruning; rebuilt when def count changes
	partExprDefCount int                  // len(origPartInfo.Definitions) when partExpr was last built
}

// newShardedTable builds a shardedTable from a TableInfo that has ShardKeyInfo
// populated. Handles both non-partitioned and partitioned+sharded tables.
//
// For non-partitioned sharded tables we inject a synthetic PartitionInfo into a
// copy of tblInfo so that the planner's GetPartitionInfo() check succeeds and it
// scans each shard's physical key range rather than the empty logical table prefix.
//
// For partitioned+sharded tables two optional parameters may be passed:
//   - origListRangePI: the original LIST/RANGE PartitionInfo (stored as origPartInfo for
//     two-stage pruning; tblInfo.Partition still holds the real LIST/RANGE PI).
//   - flatPI: the synthetic flat PartitionInfo (one entry per physical shard); stored
//     on the shardedTable so that Meta() can return a tblInfo copy with this substituted in,
//     giving builder.go physical shard IDs while leaving t.meta unchanged for DDL.
func newShardedTable(tbl *TableCommon, tblInfo *model.TableInfo, extras ...*model.PartitionInfo) (*shardedTable, error) {
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
	// Unpack optional extras: [0]=origListRangePI, [1]=flatPI.
	var origPI *model.PartitionInfo
	var flatPI *model.PartitionInfo
	if len(extras) > 0 {
		origPI = extras[0]
	}
	if len(extras) > 1 {
		flatPI = extras[1]
	}
	// A non-nil partInfo with PartitionDefinitions that have no ShardIDs means this
	// is a shard-only table whose synthetic PartitionInfo was injected by the
	// InfoSchema builder. In that case, use ski.ShardIDs directly.
	isSyntheticPI := origPI == nil && partInfo != nil && len(ski.ShardIDs) > 0 &&
		(len(partInfo.Definitions) == 0 || len(partInfo.Definitions[0].ShardIDs) == 0)
	if origPI != nil || (partInfo != nil && !isSyntheticPI) {
		// Partitioned + sharded: each partition has its own ShardIDs slice.
		// When origPI is set, use it; otherwise partInfo IS the real LIST/RANGE PI.
		if origPI == nil {
			origPI = partInfo
		}
		st.origPartInfo = origPI
		// Build the PartitionExpr from the original PartitionInfo so that
		// findUsedListPartitions can use it for LIST COLUMNS pruning.
		if pe, err := newPartitionExpr(tblInfo, origPI.Type, origPI.Expr, origPI.Columns, origPI.Definitions); err == nil {
			st.partExpr = pe
		}
		st.partExprDefCount = len(origPI.Definitions)
		st.partCnt = len(origPI.Definitions)
		st.shards = make([]*shardPhysical, st.partCnt*st.shardCnt)
		st.physicalIDs = make([]int64, st.partCnt*st.shardCnt)
		for pi, def := range origPI.Definitions {
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
		st.shardsDefCount = len(origPI.Definitions)
		// Store flatPI so Meta() can return a tblInfo copy with the flat synthetic PI
		// substituted in. This allows builder.go's pi.Definitions[idx].ID to resolve to
		// physical shard IDs while t.meta retains the original LIST/RANGE PI for DDL.
		st.flatPI = flatPI
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

// locateOrigPartition returns the logical partition index (into origPartInfo.Definitions)
// for the given row. For shard-only tables (origPartInfo == nil) it returns 0.
// For partitioned+sharded tables with LIST/RANGE partitioning, it uses the cached
// PartitionExpr to locate the partition.
func (t *shardedTable) locateOrigPartition(evalCtx expression.EvalContext, r []types.Datum) (int, error) {
	if t.origPartInfo == nil {
		return 0, nil
	}
	// Rebuild shards if partition count changed (e.g. after ADD/DROP PARTITION).
	t.rebuildShards()
	pe := t.PartitionExpr()
	if pe == nil {
		return 0, errors.New("shardedTable: PartitionExpr is nil for partitioned+sharded table")
	}
	switch t.origPartInfo.Type {
	case ast.PartitionTypeList:
		return pe.locateListPartition(evalCtx, r)
	case ast.PartitionTypeRange:
		if len(t.origPartInfo.Columns) > 0 {
			// RANGE COLUMNS partitioning
			return locateRangeColumnPartitionByExpr(evalCtx, pe, r)
		}
		return locateRangePartitionByExpr(evalCtx, pe, r)
	case ast.PartitionTypeHash:
		return locateHashPartitionByExpr(evalCtx, pe, uint64(t.partCnt), r) //nolint:gosec
	case ast.PartitionTypeKey:
		// KEY partitions use MySQL's internal column hashing, not a SQL expression.
		// partExpr.Expr is nil for KEY — must use ForKeyPruning.LocateKeyPartition.
		if pe.ForKeyPruning == nil {
			return 0, errors.New("shardedTable: ForKeyPruning is nil for KEY partition")
		}
		return pe.ForKeyPruning.LocateKeyPartition(uint64(t.partCnt), r) //nolint:gosec
	default:
		return 0, errors.Errorf("shardedTable: unsupported partition type %v in locateOrigPartition", t.origPartInfo.Type)
	}
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
	return int(h.Sum32() % uint32(t.shardCnt)), nil //nolint:gosec
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
// For partitioned+sharded tables the row is first assigned to a logical partition
// (LIST/RANGE matching), then to a shard slot within that partition (CRC32).
func (t *shardedTable) AddRecord(ctx table.MutateContext, txn kv.Transaction,
	r []types.Datum, opts ...table.AddRecordOption) (kv.Handle, error) {
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	partIdx, err := t.locateOrigPartition(evalCtx, r)
	if err != nil {
		return nil, err
	}
	sp, err := t.shardForRow(r, partIdx)
	if err != nil {
		return nil, err
	}
	opt := table.NewAddRecordOpt(opts...)
	return sp.addRecord(ctx, txn, r, opt)
}

// UpdateRecord routes the update to the correct shard; handles cross-shard moves.
// For partitioned+sharded tables the logical partition is resolved for both old and new rows.
func (t *shardedTable) UpdateRecord(ctx table.MutateContext, txn kv.Transaction,
	h kv.Handle, oldData, newData []types.Datum, touched []bool,
	opts ...table.UpdateRecordOption) error {
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	fromPartIdx, err := t.locateOrigPartition(evalCtx, oldData)
	if err != nil {
		return err
	}
	toPartIdx, err := t.locateOrigPartition(evalCtx, newData)
	if err != nil {
		return err
	}
	fromSlot, err := t.locateShard(oldData)
	if err != nil {
		return err
	}
	toSlot, err := t.locateShard(newData)
	if err != nil {
		return err
	}
	opt := table.NewUpdateRecordOpt(opts...)
	fromShard := t.shards[t.shardIdx(fromPartIdx, fromSlot)]
	toShard := t.shards[t.shardIdx(toPartIdx, toSlot)]
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
// For partitioned+sharded tables the logical partition is resolved first.
func (t *shardedTable) RemoveRecord(ctx table.MutateContext, txn kv.Transaction,
	h kv.Handle, r []types.Datum, opts ...table.RemoveRecordOption) error {
	evalCtx := ctx.GetExprCtx().GetEvalCtx()
	partIdx, err := t.locateOrigPartition(evalCtx, r)
	if err != nil {
		return err
	}
	sp, err := t.shardForRow(r, partIdx)
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
// It lazily rebuilds the shards slice if origPartInfo has changed since construction
// (e.g. after DROP PARTITION).
func (t *shardedTable) GetPartition(physicalID int64) table.PhysicalTable {
	t.rebuildShards()
	for _, sp := range t.shards {
		if sp != nil && sp.physicalTableID == physicalID {
			return sp
		}
	}
	return nil
}

// GetPartitionByRow returns the shard that owns the given row.
// For partitioned+sharded tables the logical partition is resolved first.
func (t *shardedTable) GetPartitionByRow(evalCtx expression.EvalContext, r []types.Datum) (table.PhysicalTable, error) {
	partIdx, err := t.locateOrigPartition(evalCtx, r)
	if err != nil {
		return nil, err
	}
	sp, err := t.shardForRow(r, partIdx)
	if err != nil {
		return nil, err
	}
	return sp, nil
}

// GetPartitionIdxByRow returns the flat index into t.shards for the given row.
// For partitioned+sharded tables the logical partition is resolved first.
func (t *shardedTable) GetPartitionIdxByRow(evalCtx expression.EvalContext, r []types.Datum) (int, error) {
	partIdx, err := t.locateOrigPartition(evalCtx, r)
	if err != nil {
		return 0, err
	}
	slot, err := t.locateShard(r)
	if err != nil {
		return 0, err
	}
	return t.shardIdx(partIdx, slot), nil
}

// GetAllPartitionIDs returns all physical shard IDs, used by the planner to build
// key ranges for full table scans. Lazily rebuilds after DROP PARTITION.
func (t *shardedTable) GetAllPartitionIDs() []int64 {
	t.rebuildShards()
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
func (t *shardedTable) GetPartitionColumnNames() []ast.CIStr {
	ski := t.meta.ShardKeyInfo
	names := make([]ast.CIStr, len(ski.Columns))
	for i, colName := range ski.Columns {
		names[i] = ast.NewCIStr(colName)
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

// Meta overrides TableCommon.Meta to return a TableInfo where Partition is the
// synthetic flat PI (one entry per physical shard) for partitioned+sharded tables.
// This ensures builder.go's pi.Definitions[idx].ID resolves to physical shard IDs.
// For DDL paths that call t.Meta() directly, CheckDropTablePartition etc. must use
// FlatPartitionInfo returns the synthetic flat PartitionInfo for partitioned+sharded
// tables — one entry per physical shard, laid out as flatIdx = partIdx*shardCnt + shardSlot.
// Used by partitionPruning in builder.go and PartitionPruning in partition_prune.go to
// map pruner result indices to physical shard IDs via pi.Definitions[idx].ID.
// Lazily rebuilds after DROP PARTITION when origPartInfo count changes.
// Returns nil for shard-only tables (their flat PI is in t.meta.Partition directly).
func (t *shardedTable) FlatPartitionInfo() *model.PartitionInfo {
	if t.flatPI == nil {
		return nil
	}
	t.rebuildShards()
	return t.flatPI
}

// rebuildShards rebuilds the shards and physicalIDs slices from origPartInfo.Definitions
// when the partition count has changed (e.g. after DROP PARTITION). Since origPartInfo is
// the same pointer as tblInfo.Partition, it always reflects the post-DDL state.
// Only applicable to partitioned+sharded tables (origPartInfo != nil).
func (t *shardedTable) rebuildShards() {
	if t.origPartInfo == nil {
		return
	}
	if len(t.origPartInfo.Definitions) == t.shardsDefCount {
		return
	}
	newPartCnt := len(t.origPartInfo.Definitions)
	newShards := make([]*shardPhysical, newPartCnt*t.shardCnt)
	newIDs := make([]int64, newPartCnt*t.shardCnt)
	for pi, def := range t.origPartInfo.Definitions {
		if len(def.ShardIDs) != t.shardCnt {
			// Definitions incomplete (shouldn't happen post-DDL) — abort rebuild.
			return
		}
		for si, physID := range def.ShardIDs {
			idx := pi*t.shardCnt + si
			// Reuse existing shardPhysical if the physID is already present.
			found := false
			for _, sp := range t.shards {
				if sp != nil && sp.physicalTableID == physID {
					newShards[idx] = sp
					found = true
					break
				}
			}
			if !found {
				var sp shardPhysical
				if err := initTableCommonWithIndices(&sp.TableCommon, t.meta, physID,
					t.Columns, t.allocs, t.Constraints); err != nil {
					return
				}
				sp.owner = t
				newShards[idx] = &sp
			}
			newIDs[idx] = physID
		}
	}
	t.shards = newShards
	t.physicalIDs = newIDs
	t.partCnt = newPartCnt
	t.shardsDefCount = newPartCnt
	// Rebuild flatPI to reflect the updated physical shard layout.
	if t.flatPI != nil {
		flatDefs := make([]model.PartitionDefinition, 0, newPartCnt*t.shardCnt)
		for _, def := range t.origPartInfo.Definitions {
			for si, physID := range def.ShardIDs {
				flatDefs = append(flatDefs, model.PartitionDefinition{
					ID:   physID,
					Name: ast.NewCIStr(fmt.Sprintf("%s_s%d", def.Name.L, si)),
				})
			}
		}
		t.flatPI = &model.PartitionInfo{
			Type:        0,
			Enable:      true,
			Num:         uint64(len(flatDefs)),
			Definitions: flatDefs,
		}
	}
}

// PartitionExpr returns a PartitionExpr reflecting the current logical partition
// layout for LIST COLUMNS pruning. It rebuilds the expression whenever the number
// of logical partitions in origPartInfo has changed since the cached copy was built
// (e.g. after DROP PARTITION). This ensures the pruner's ColPrunes / valueMap
// partition indices are always consistent with origPartInfo.Definitions.
// Returns nil for shard-only (non-partitioned) tables.
func (t *shardedTable) PartitionExpr() *PartitionExpr {
	if t.origPartInfo == nil {
		return nil
	}
	// Rebuild when the cached expr is stale (partition count changed after DDL).
	if t.partExpr == nil || len(t.origPartInfo.Definitions) != t.partExprDefCount {
		pe, err := newPartitionExpr(t.meta, t.origPartInfo.Type, t.origPartInfo.Expr,
			t.origPartInfo.Columns, t.origPartInfo.Definitions)
		if err != nil {
			// On error, fall back to nil — caller will use FullRange.
			t.partExpr = nil
			t.partExprDefCount = 0
			return nil
		}
		t.partExpr = pe
		t.partExprDefCount = len(t.origPartInfo.Definitions)
	}
	return t.partExpr
}
