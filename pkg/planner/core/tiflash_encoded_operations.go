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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

// EncodedOperationHint contains hints for TiFlash about which operations
// can be performed on dictionary-encoded data. These hints are attached to
// the DAGRequest when the session variable tidb_tiflash_encoded_operations is ON.
type EncodedOperationHint struct {
	// EnableDictEncoding indicates whether TiFlash should use dictionary encoding
	// for eligible columns in this scan.
	EnableDictEncoding bool

	// MaxCardinality is the maximum number of distinct values for a column
	// to be eligible for dictionary encoding.
	MaxCardinality int64

	// DictEligibleColumnIDs lists column IDs that the planner has identified
	// as candidates for dictionary encoding (based on statistics).
	DictEligibleColumnIDs []int64

	// EnableEncodedFilter indicates that filter operations on dictionary-encoded
	// columns should use the pre-computed dictionary lookup optimization.
	EnableEncodedFilter bool

	// EnableEncodedGroupBy indicates that group-by operations on dictionary-encoded
	// columns should use array-indexed aggregation.
	EnableEncodedGroupBy bool

	// EnableEncodedBloomFilter indicates that bloom filter pushdown should operate
	// on dictionary entries rather than decoded values.
	EnableEncodedBloomFilter bool
}

// BuildEncodedOperationHint constructs an encoding hint based on session variables
// and table/column statistics. Returns nil if encoded operations are not enabled.
func BuildEncodedOperationHint(vars *variable.SessionVars, scan *PhysicalTableScan) *EncodedOperationHint {
	if !vars.TiFlashEncodedOperations {
		return nil
	}

	hint := &EncodedOperationHint{
		EnableDictEncoding:       true,
		MaxCardinality:           vars.TiFlashDictEncodingMaxCardinality,
		EnableEncodedFilter:      true,
		EnableEncodedGroupBy:     true,
		EnableEncodedBloomFilter: true,
	}

	// Identify columns eligible for dictionary encoding based on NDV statistics
	if scan.StatsInfo() != nil && scan.Table != nil {
		for _, col := range scan.Columns {
			ndv := estimateColumnNDV(scan, col.ID)
			if ndv > 0 && ndv <= hint.MaxCardinality {
				hint.DictEligibleColumnIDs = append(hint.DictEligibleColumnIDs, col.ID)
			}
		}
	}

	return hint
}

// estimateColumnNDV estimates the number of distinct values for a column
// based on table statistics. Returns 0 if stats are unavailable.
func estimateColumnNDV(scan *PhysicalTableScan, colID int64) int64 {
	statsInfo := scan.StatsInfo()
	if statsInfo == nil {
		return 0
	}

	return getColNDVFromStats(statsInfo, colID)
}

// getColNDVFromStats extracts NDV for a column from StatsInfo.
func getColNDVFromStats(stats *property.StatsInfo, colID int64) int64 {
	if stats.ColNDVs == nil {
		return 0
	}

	if ndv, ok := stats.ColNDVs[colID]; ok {
		return int64(ndv)
	}
	return 0
}

// IsColumnDictEligible checks if a specific column is eligible for dictionary
// encoding based on its NDV and the cardinality threshold.
func IsColumnDictEligible(
	ctx base.PlanContext,
	col *expression.Column,
	maxCardinality int64,
) bool {
	if col == nil || maxCardinality <= 0 {
		return false
	}
	// String and integer types are candidates for dictionary encoding
	// Float/double types are not good candidates (too many distinct values)
	tp := col.RetType
	if tp == nil {
		return false
	}
	switch tp.GetType() {
	case 1, 2, 3, 4, 5, 8, 9: // TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, etc.
		return true
	case 15, 253, 254: // VARCHAR, VARBINARY, CHAR
		return true
	default:
		return false
	}
}
