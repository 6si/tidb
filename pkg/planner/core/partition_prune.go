// Copyright 2020 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	tmodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"golang.org/x/exp/slices"
)

// PartitionPruning finds all used partitions according to query conditions, it will
// return nil if condition match none of partitions. The return value is a array of the
// idx in the partition definitions array, use pi.Definitions[idx] to get the partition ID
func PartitionPruning(ctx base.PlanContext, tbl table.PartitionedTable, conds []expression.Expression, partitionNames []model.CIStr,
	columns []*expression.Column, names types.NameSlice) ([]int, error) {
	s := PartitionProcessor{}
	tblInfo := tbl.Meta()
	pi := tblInfo.Partition
	// Shard-key tables: pi.Type=0 (PartitionTypeNone) but ShardKeyInfo is set.
	if tblInfo.ShardKeyInfo != nil {
		// For partitioned+sharded tables (SHARD BY on top of LIST/RANGE/HASH).
		// pi (= tblInfo.Partition) is the original logical PI, not the flat synthetic PI.
		// We must use the flat PI so physical shard IDs resolve properly.
		if spt, ok := tbl.(table.ShardedPartitionedTable); ok {
			if origPI := spt.OrigPartitionInfo(); origPI != nil {
				flatPI := pi
				if fpi := spt.FlatPartitionInfo(); fpi != nil {
					flatPI = fpi
				}
				if origPI.Type == model.PartitionTypeList {
					return pruneShardedListPartitionDynamic(ctx, s, tbl, flatPI, origPI, tblInfo, conds, partitionNames, columns)
				}
				// RANGE/HASH partitioned+sharded: shard-slot pruning over the flat PI.
				// We cannot do logical RANGE/HASH pruning without a proper PartitionExpr bound
				// to the original PI, so conservatively scan all shards within the matched slots.
				rangeOr, err := s.pruneShardKeyPartition(ctx, flatPI, tblInfo, conds, columns)
				if err != nil {
					return nil, err
				}
				return s.convertToIntSlice(rangeOr, flatPI, partitionNames), nil
			}
		}
		rangeOr, err := s.pruneShardKeyPartition(ctx, pi, tblInfo, conds, columns)
		if err != nil {
			return nil, err
		}
		return s.convertToIntSlice(rangeOr, pi, partitionNames), nil
	}
	switch pi.Type {
	case model.PartitionTypeHash, model.PartitionTypeKey:
		return s.pruneHashOrKeyPartition(ctx, tbl, partitionNames, conds, columns, names)
	case model.PartitionTypeRange:
		rangeOr, err := s.pruneRangePartition(ctx, pi, tbl, conds, columns, names)
		if err != nil {
			return nil, err
		}
		ret := s.convertToIntSlice(rangeOr, pi, partitionNames)
		ret = handleDroppingForRange(pi, partitionNames, ret)
		return ret, nil
	case model.PartitionTypeList:
		return s.pruneListPartition(ctx, tbl, partitionNames, conds, columns)
	}
	return []int{FullRange}, nil
}

// pruneShardedListPartitionDynamic is the dynamic-pruning equivalent of
// processShardedListPartition: two-stage LIST + shard-slot pruning for
// SHARD BY + LIST COLUMNS tables.
func pruneShardedListPartitionDynamic(
	ctx base.PlanContext,
	s PartitionProcessor,
	tbl table.PartitionedTable,
	flatPI, origPI *tmodel.PartitionInfo,
	tblInfo *tmodel.TableInfo,
	conds []expression.Expression,
	partitionNames []model.CIStr,
	columns []*expression.Column,
) ([]int, error) {
	ski := tblInfo.ShardKeyInfo
	shardCnt := ski.ShardCnt

	// Stage 1: LIST COLUMNS pruning on the logical partitions.
	// Note: pruneListPartition reads tbl.Meta().Partition internally, which is the flat
	// synthetic PI (logicalPartCnt*shardCnt entries). Its FullRange fallback iterates over
	// flat indices, not logical ones. We clamp any returned index >= len(origPI.Definitions)
	// back to FullRange so that flat indices are never mistaken for logical indices.
	logicalUsed, err := s.pruneListPartition(ctx, tbl, partitionNames, conds, columns)
	if err != nil {
		return nil, err
	}
	if len(logicalUsed) != 1 || logicalUsed[0] != FullRange {
		for _, idx := range logicalUsed {
			if idx >= len(origPI.Definitions) {
				logicalUsed = []int{FullRange}
				break
			}
		}
	}
	if len(logicalUsed) == 1 && logicalUsed[0] == FullRange {
		logicalUsed = make([]int, len(origPI.Definitions))
		for i := range origPI.Definitions {
			logicalUsed[i] = i
		}
	}

	// Stage 2: shard slot pruning.
	shardSlots, err := s.pruneShardKeyPartition(ctx, flatPI, tblInfo, conds, columns)
	if err != nil {
		return nil, err
	}
	slotSet := make(map[int]struct{}, shardCnt)
	if len(shardSlots) == 1 && shardSlots[0].start == 0 && shardSlots[0].end == len(flatPI.Definitions) {
		for i := 0; i < shardCnt; i++ {
			slotSet[i] = struct{}{}
		}
	} else {
		for _, r := range shardSlots {
			for idx := r.start; idx < r.end; idx++ {
				slotSet[idx%shardCnt] = struct{}{}
			}
		}
	}

	flatLen := len(flatPI.Definitions)
	surviving := make([]int, 0, len(logicalUsed)*len(slotSet))
	for _, partIdx := range logicalUsed {
		for slot := range slotSet {
			if idx := partIdx*shardCnt + slot; idx < flatLen {
				surviving = append(surviving, idx)
			}
		}
	}
	slices.Sort(surviving)

	if len(surviving) == flatLen {
		return []int{FullRange}, nil
	}
	return surviving, nil
}

// pruneShardedRangeOrHashDynamic handles dynamic pruning for RANGE/HASH partitioned+sharded tables.
// All logical partitions survive (not prunable by shard key), but the CRC32 shard slot is
// prunable when equality predicates cover all shard-key columns.
// Expands each surviving slot to cover all logical partitions in the flat PI.
func pruneShardedRangeOrHashDynamic(
	ctx base.PlanContext,
	s PartitionProcessor,
	flatPI *tmodel.PartitionInfo,
	tblInfo *tmodel.TableInfo,
	conds []expression.Expression,
	_ []model.CIStr,
	columns []*expression.Column,
) ([]int, error) {
	ski := tblInfo.ShardKeyInfo
	shardCnt := ski.ShardCnt
	flatLen := len(flatPI.Definitions)
	partCnt := flatLen / shardCnt
	if partCnt == 0 || shardCnt == 0 {
		return []int{FullRange}, nil
	}

	shardSlots, err := s.pruneShardKeyPartition(ctx, flatPI, tblInfo, conds, columns)
	if err != nil {
		return nil, err
	}

	// If full range, all physical shards are needed.
	if len(shardSlots) == 1 && shardSlots[0].start == 0 && shardSlots[0].end == flatLen {
		return []int{FullRange}, nil
	}

	// Build slot set (0..shardCnt-1).
	slotSet := make(map[int]struct{}, shardCnt)
	for _, r := range shardSlots {
		for idx := r.start; idx < r.end; idx++ {
			slotSet[idx%shardCnt] = struct{}{}
		}
	}

	// Expand each slot to all logical partitions.
	surviving := make([]int, 0, partCnt*len(slotSet))
	for partIdx := range partCnt {
		for slot := range slotSet {
			if idx := partIdx*shardCnt + slot; idx < flatLen {
				surviving = append(surviving, idx)
			}
		}
	}
	slices.Sort(surviving)

	if len(surviving) == flatLen {
		return []int{FullRange}, nil
	}
	return surviving, nil
}

func handleDroppingForRange(pi *tmodel.PartitionInfo, partitionNames []model.CIStr, usedPartitions []int) []int {
	if pi.CanHaveOverlappingDroppingPartition() {
		if len(usedPartitions) == 1 && usedPartitions[0] == FullRange {
			usedPartitions = make([]int, 0, len(pi.Definitions))
			for i := range pi.Definitions {
				usedPartitions = append(usedPartitions, i)
			}
		}
		ret := make([]int, 0, len(usedPartitions))
		for i := range usedPartitions {
			idx := pi.GetOverlappingDroppingPartitionIdx(usedPartitions[i])
			if idx == -1 {
				// dropped without overlapping partition, skip it
				continue
			}
			if idx == usedPartitions[i] {
				// non-dropped partition
				ret = append(ret, idx)
				continue
			}
			// partition being dropped, remove the consecutive range of dropping partitions
			// and add the overlapping partition.
			end := i + 1
			for ; end < len(usedPartitions) && usedPartitions[end] < idx; end++ {
				continue
			}
			// add the overlapping partition, if not already included
			if end >= len(usedPartitions) || usedPartitions[end] != idx {
				// It must also match partitionNames if explicitly given
				s := PartitionProcessor{}
				if len(partitionNames) == 0 || s.findByName(partitionNames, pi.Definitions[idx].Name.L) {
					ret = append(ret, idx)
				}
			}
			if end < len(usedPartitions) {
				ret = append(ret, usedPartitions[end:]...)
			}
			break
		}
		usedPartitions = ret
	}
	if len(usedPartitions) == len(pi.Definitions) {
		return []int{FullRange}
	}
	return usedPartitions
}
