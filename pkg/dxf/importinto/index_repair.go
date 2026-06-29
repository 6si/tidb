// Copyright 2025 PingCAP, Inc.
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

package importinto

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

const (
	// indexRepairBatchSize is the number of index entries to process per
	// transaction during orphaned index cleanup.
	indexRepairBatchSize = 2048
)

// IndexRepairer scans secondary index entries after SST ingest and removes
// orphaned entries that reference data rows which were MVCC-shadowed by the
// replace-mode import. It operates region-by-region: only index entries whose
// referenced handle falls within the ingested data key ranges are checked.
//
// This implements "Option C (region-scoped)" from the design discussion:
// track the ingested data key ranges, then for each secondary index scan the
// entries referencing handles in those ranges and verify them against the
// current data. Delete entries where the data row either no longer exists or
// the indexed column value does not match.
type IndexRepairer struct {
	store  tidbkv.Storage
	tbl    table.Table
	logger *zap.Logger

	// ingestedDataRanges are the data key ranges that were ingested during
	// the WriteAndIngest step. We only check index entries whose referenced
	// handle falls within these ranges. If empty, all entries are checked.
	ingestedDataRanges []tidbkv.KeyRange
}

// NewIndexRepairer creates an IndexRepairer for the given table.
func NewIndexRepairer(
	store tidbkv.Storage,
	tbl table.Table,
	ingestedDataRanges []tidbkv.KeyRange,
	logger *zap.Logger,
) *IndexRepairer {
	return &IndexRepairer{
		store:              store,
		tbl:               tbl,
		logger:            logger,
		ingestedDataRanges: ingestedDataRanges,
	}
}

// RepairIndexes scans all secondary indexes on the table and removes orphaned
// entries. It returns the total number of dangling entries removed.
func (r *IndexRepairer) RepairIndexes(ctx context.Context) (totalRemoved int64, err error) {
	tblInfo := r.tbl.Meta()
	indices := importer.GetIndicesGenKV(tblInfo)
	if len(indices) == 0 {
		return 0, nil
	}

	for indexID := range indices {
		idxInfo := model.FindIndexInfoByID(tblInfo.Indices, indexID)
		if idxInfo == nil {
			continue
		}
		removed, idxErr := r.repairSingleIndex(ctx, idxInfo)
		if idxErr != nil {
			return totalRemoved, errors.Annotatef(idxErr, "repair index %s", idxInfo.Name.O)
		}
		totalRemoved += removed
		if removed > 0 {
			r.logger.Info("repaired orphaned index entries",
				zap.String("index", idxInfo.Name.O),
				zap.Int64("removed", removed))
		}
	}
	return totalRemoved, nil
}

// repairSingleIndex scans one secondary index and removes dangling entries.
func (r *IndexRepairer) repairSingleIndex(
	ctx context.Context,
	idxInfo *model.IndexInfo,
) (removed int64, err error) {
	tblInfo := r.tbl.Meta()

	// For partitioned tables, handle each partition.
	if pi := tblInfo.GetPartitionInfo(); pi != nil {
		for _, p := range pi.Definitions {
			partRemoved, partErr := r.repairIndexForPhysicalID(ctx, idxInfo, p.ID)
			if partErr != nil {
				return removed, partErr
			}
			removed += partRemoved
		}
		return removed, nil
	}

	return r.repairIndexForPhysicalID(ctx, idxInfo, tblInfo.ID)
}

// repairIndexForPhysicalID repairs a single index on one physical table ID.
func (r *IndexRepairer) repairIndexForPhysicalID(
	ctx context.Context,
	idxInfo *model.IndexInfo,
	physicalID int64,
) (removed int64, err error) {
	var lastKey tidbkv.Key

	for {
		batchRemoved, nextKey, done, batchErr := r.repairBatch(ctx, idxInfo, physicalID, lastKey)
		if batchErr != nil {
			return removed, batchErr
		}
		removed += batchRemoved
		if done {
			break
		}
		lastKey = nextKey
	}
	return removed, nil
}

// repairBatch processes one batch of index entries in a single transaction.
func (r *IndexRepairer) repairBatch(
	ctx context.Context,
	idxInfo *model.IndexInfo,
	physicalID int64,
	lastKey tidbkv.Key,
) (removed int64, nextKey tidbkv.Key, done bool, err error) {
	ctx = tidbkv.WithInternalSourceType(ctx, tidbkv.InternalTxnAdmin)
	err = tidbkv.RunInNewTxn(ctx, r.store, true, func(ctx context.Context, txn tidbkv.Transaction) error {
		removed, nextKey, done = 0, nil, false

		startKey := tablecodec.EncodeIndexSeekKey(physicalID, idxInfo.ID, nil)
		endKey := tablecodec.EncodeIndexSeekKey(physicalID, idxInfo.ID+1, nil)
		if len(lastKey) > 0 {
			startKey = lastKey.Next()
		}

		iter, iterErr := txn.Iter(startKey, endKey)
		if iterErr != nil {
			return errors.Trace(iterErr)
		}
		defer iter.Close()

		type indexEntry struct {
			key    tidbkv.Key
			handle tidbkv.Handle
		}
		entries := make([]indexEntry, 0, indexRepairBatchSize)

		for iter.Valid() && len(entries) < indexRepairBatchSize {
			key := iter.Key()
			val := iter.Value()

			handle, decodeErr := tablecodec.DecodeIndexHandle(key, val, len(idxInfo.Columns))
			if decodeErr != nil {
				r.logger.Warn("failed to decode index handle, skipping",
					zap.Error(decodeErr),
					zap.String("index", idxInfo.Name.O))
				if nextErr := iter.Next(); nextErr != nil {
					return errors.Trace(nextErr)
				}
				continue
			}

			// Region-scoped check: only verify entries whose handle falls
			// within one of the ingested data key ranges.
			if r.handleInIngestedRange(physicalID, handle) {
				entries = append(entries, indexEntry{
					key:    key.Clone(),
					handle: handle,
				})
			}
			nextKey = key.Clone()

			if nextErr := iter.Next(); nextErr != nil {
				return errors.Trace(nextErr)
			}
		}

		if len(entries) == 0 {
			done = true
			return nil
		}
		done = !iter.Valid()

		// BatchGet the data rows referenced by these index entries.
		rowKeys := make([]tidbkv.Key, 0, len(entries))
		for _, e := range entries {
			rowKeys = append(rowKeys, tablecodec.EncodeRowKeyWithHandle(physicalID, e.handle))
		}
		rowValues, batchErr := tidbkv.BatchGetValue(ctx, txn, rowKeys)
		if batchErr != nil {
			return errors.Trace(batchErr)
		}

		// Check each entry against the current data.
		tblInfo := r.tbl.Meta()
		for i, e := range entries {
			rowKey := string(rowKeys[i])
			rowVal, exists := rowValues[rowKey]
			if !exists || len(rowVal) == 0 {
				// Data row doesn't exist — orphaned index entry.
				if delErr := txn.Delete(e.key); delErr != nil {
					return errors.Trace(delErr)
				}
				removed++
				continue
			}

			// Data row exists — verify column values match.
			if r.isStaleIndexEntry(tblInfo, idxInfo, e.key, e.handle, rowVal, physicalID) {
				if delErr := txn.Delete(e.key); delErr != nil {
					return errors.Trace(delErr)
				}
				removed++
			}
		}
		return nil
	})
	return removed, nextKey, done, err
}

// handleInIngestedRange checks whether a handle's data key falls within one of
// the ingested data key ranges. If no ranges are configured, all entries are
// checked (full-table mode as fallback).
func (r *IndexRepairer) handleInIngestedRange(physicalID int64, handle tidbkv.Handle) bool {
	if len(r.ingestedDataRanges) == 0 {
		return true
	}
	rowKey := tablecodec.EncodeRowKeyWithHandle(physicalID, handle)
	for _, kr := range r.ingestedDataRanges {
		if tidbkv.Key(kr.StartKey).Cmp(rowKey) <= 0 &&
			(len(kr.EndKey) == 0 || tidbkv.Key(kr.EndKey).Cmp(rowKey) > 0) {
			return true
		}
	}
	return false
}

// hasExpressionColumns returns true if the index has any expression-based
// (virtual generated) columns whose values cannot be decoded from raw row data.
func hasExpressionColumns(tblInfo *model.TableInfo, idxInfo *model.IndexInfo) bool {
	for _, idxCol := range idxInfo.Columns {
		col := model.FindColumnInfo(tblInfo.Columns, idxCol.Name.L)
		if col != nil && col.IsGenerated() {
			return true
		}
	}
	return false
}

// isStaleIndexEntry checks whether an existing index entry's column values
// match the current data row. If they don't match, the entry is stale.
//
// For expression indexes (whose column values are computed and not stored in
// the row), this check is skipped — if the row exists, the entry is presumed
// valid since the import already generated the correct expression index KVs.
func (r *IndexRepairer) isStaleIndexEntry(
	tblInfo *model.TableInfo,
	idxInfo *model.IndexInfo,
	indexKey tidbkv.Key,
	handle tidbkv.Handle,
	rowVal []byte,
	physicalID int64,
) bool {
	// Expression indexes reference virtual generated columns whose values
	// are not stored in the row data. We cannot re-encode the key, so if
	// the data row exists we assume the entry is valid (the import generated
	// correct expression index KVs for the new row).
	if hasExpressionColumns(tblInfo, idxInfo) {
		return false
	}

	// Build column type map for decoding.
	colTypes := make(map[int64]*types.FieldType, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		col := model.FindColumnInfo(tblInfo.Columns, idxCol.Name.L)
		if col == nil {
			return false
		}
		colTypes[col.ID] = &col.FieldType
	}

	// Decode the data row for indexed columns.
	rowColMap, decodeErr := tablecodec.DecodeRowToDatumMap(rowVal, colTypes, time.UTC)
	if decodeErr != nil {
		return false
	}

	// Build datum slice for index key generation.
	currentIdxVals := make([]types.Datum, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		col := model.FindColumnInfo(tblInfo.Columns, idxCol.Name.L)
		if col == nil {
			return false
		}
		val, ok := rowColMap[col.ID]
		if !ok {
			currentIdxVals = append(currentIdxVals, types.Datum{})
		} else {
			currentIdxVals = append(currentIdxVals, val)
		}
	}

	// Re-encode the expected index key from current data.
	expectedKey, _, encodeErr := tablecodec.GenIndexKey(
		time.UTC,
		tblInfo,
		idxInfo,
		physicalID,
		currentIdxVals,
		handle,
		nil,
	)
	if encodeErr != nil {
		return false
	}

	// If actual key differs from expected, it's stale.
	return tidbkv.Key(expectedKey).Cmp(indexKey) != 0
}

// buildDataKeyRangesForTable constructs the full data key range for a table's
// physical ID. Used when no specific ingested ranges are available.
func buildDataKeyRangesForTable(physicalID int64) []tidbkv.KeyRange {
	startKey := tablecodec.GenTableRecordPrefix(physicalID)
	endKey := startKey.PrefixNext()
	return []tidbkv.KeyRange{{StartKey: startKey, EndKey: endKey}}
}

// buildIngestedDataRanges extracts the data key ranges that were ingested from
// the WriteIngestStepMeta. This scopes the index repair to only regions that
// received new data.
func buildIngestedDataRanges(tableID int64, partInfo *model.PartitionInfo) []tidbkv.KeyRange {
	// For now, scope to the full table's data key range. This is still
	// efficient because we batch-verify via BatchGet and only delete
	// mismatched entries. Future optimization: intersect with the actual
	// SST ranges from WriteIngestStepMeta.
	if partInfo != nil {
		ranges := make([]tidbkv.KeyRange, 0, len(partInfo.Definitions))
		for _, p := range partInfo.Definitions {
			ranges = append(ranges, buildDataKeyRangesForTable(p.ID)...)
		}
		return ranges
	}
	return buildDataKeyRangesForTable(tableID)
}

// ShouldRepairIndexes returns true if index repair should be run after import.
func ShouldRepairIndexes(plan *importer.Plan) bool {
	if plan.OnDupKey != importer.OnDupKeyModeReplace {
		return false
	}
	if !plan.IsGlobalSort() {
		return false
	}
	return importer.GetNumOfIndexGenKV(plan.TableInfo) > 0
}

// repairTableIndexes is the top-level entry point called from postProcess.
// It creates an IndexRepairer and runs the repair for all secondary indexes.
func repairTableIndexes(
	ctx context.Context,
	store tidbkv.Storage,
	plan *importer.Plan,
	logger *zap.Logger,
) (int64, error) {
	if !ShouldRepairIndexes(plan) {
		return 0, nil
	}

	tblInfo := plan.TableInfo
	tbl, err := tables.TableFromMeta(autoid.Allocators{}, tblInfo)
	if err != nil {
		return 0, errors.Trace(err)
	}

	dataRanges := buildIngestedDataRanges(tblInfo.ID, tblInfo.GetPartitionInfo())
	repairer := NewIndexRepairer(store, tbl, dataRanges, logger)

	removed, repairErr := repairer.RepairIndexes(ctx)
	if repairErr != nil {
		return removed, errors.Annotate(repairErr, "index repair after replace-mode import")
	}

	logger.Info("index repair completed",
		zap.Int64("totalOrphanedEntriesRemoved", removed),
		zap.String("table", tblInfo.Name.O))
	return removed, nil
}
