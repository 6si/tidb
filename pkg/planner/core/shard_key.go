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

package core

import (
	"strings"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
)

// extractTableFromPlan traverses down the plan tree to find the underlying
// PhysicalTableScan or PhysicalIndexScan and returns its TableInfo.
func extractTableFromPlan(plan base.PhysicalPlan) *model.TableInfo {
	for plan != nil {
		switch p := plan.(type) {
		case *physicalop.PhysicalTableScan:
			return p.Table
		case *physicalop.PhysicalIndexScan:
			return p.Table
		case *physicalop.PhysicalExchangeReceiver:
			if len(p.Children()) > 0 {
				plan = p.Children()[0]
			} else {
				return nil
			}
		default:
			if len(p.Children()) > 0 {
				plan = p.Children()[0]
			} else {
				return nil
			}
		}
	}
	return nil
}

// shardKeyColumnsMatch checks whether all the given MPP partition columns are covered by the
// shard key columns of the table.
func shardKeyColumnsMatch(shardKey *model.ShardKeyInfo, partitionCols []*property.MPPPartitionColumn) bool {
	if shardKey == nil || len(shardKey.Columns) == 0 {
		return false
	}
	shardColSet := make(map[string]struct{}, len(shardKey.Columns))
	for _, col := range shardKey.Columns {
		shardColSet[col] = struct{}{}
	}
	for _, pc := range partitionCols {
		colName := pc.Col.OrigName
		if idx := strings.LastIndex(colName, "."); idx >= 0 {
			colName = colName[idx+1:]
		}
		if _, ok := shardColSet[strings.ToLower(colName)]; !ok {
			return false
		}
	}
	return true
}

// bothCoLocated returns true when both MPP tasks can participate in a co-located join:
// each underlying table's shard key must cover the given partition columns, and both
// shard keys must be compatible (same columns and same shard count).
func bothCoLocated(lTask, rTask *physicalop.MppTask, lPartCols, rPartCols []*property.MPPPartitionColumn) bool {
	if len(lPartCols) == 0 || len(rPartCols) == 0 {
		return false
	}
	lTbl := extractTableFromPlan(lTask.Plan())
	rTbl := extractTableFromPlan(rTask.Plan())
	if lTbl == nil || rTbl == nil {
		return false
	}
	if lTbl.ShardKeyInfo == nil || rTbl.ShardKeyInfo == nil {
		return false
	}
	if !shardKeyColumnsMatch(lTbl.ShardKeyInfo, lPartCols) {
		return false
	}
	if !shardKeyColumnsMatch(rTbl.ShardKeyInfo, rPartCols) {
		return false
	}
	return model.ShardKeysCompatible(lTbl.ShardKeyInfo, rTbl.ShardKeyInfo)
}
