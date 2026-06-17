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
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
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
func BuildEncodedOperationHint(vars *variable.SessionVars, scan *physicalop.PhysicalTableScan) *EncodedOperationHint {
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
	if scan != nil && scan.StatsInfo() != nil && scan.Table != nil {
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
func estimateColumnNDV(scan *physicalop.PhysicalTableScan, colID int64) int64 {
	statsInfo := scan.StatsInfo()
	if statsInfo == nil {
		return 0
	}

	return getColNDVFromStats(statsInfo, colID)
}

// getColNDVFromStats extracts NDV for a column from StatsInfo.
func getColNDVFromStats(stats *property.StatsInfo, colID int64) int64 {
	if stats == nil || stats.ColNDVs == nil {
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
	case 1, 2, 3, 8, 9: // TINYINT(1), SMALLINT(2), INT(3), BIGINT(8), MEDIUMINT(9)
		return true
	case 15, 253, 254: // VARCHAR(15), VARBINARY(253), CHAR(254)
		return true
	default:
		return false
	}
}

// EncodedFilterHint describes how a filter should be applied on encoded data.
type EncodedFilterHint struct {
	// ColumnID is the column being filtered
	ColumnID int64

	// FilterType: "eq", "in", "like", "ne", "range"
	FilterType string

	// Values are the filter constant values (for eq/in predicates)
	Values []interface{}

	// CanUseEncodedPath indicates the filter can run without decoding
	CanUseEncodedPath bool
}

// EncodedGroupByHint describes a group-by that can run on dictionary IDs.
type EncodedGroupByHint struct {
	// GroupByColumnIDs lists columns used in GROUP BY
	GroupByColumnIDs []int64

	// AggFuncs lists the aggregate functions (sum, count, min, max, any)
	AggFuncs []string

	// AggColumnIDs lists the columns being aggregated
	AggColumnIDs []int64

	// EstimatedGroups is the estimated number of groups (from NDV stats)
	EstimatedGroups int64

	// CanUseEncodedPath indicates array-indexed aggregation is feasible
	CanUseEncodedPath bool
}

// EncodedStarJoinHint describes a star-schema join suitable for encoded execution.
type EncodedStarJoinHint struct {
	// FactTableID is the fact table being scanned
	FactTableID int64

	// DimensionJoins lists each dimension join
	DimensionJoins []DimensionJoinInfo

	// HasGroupByAbove indicates GROUP BY is present above the join
	HasGroupByAbove bool

	// CanUseFusedPath indicates fused scan→join→agg is feasible
	CanUseFusedPath bool
}

// DimensionJoinInfo describes one dimension in a star join.
type DimensionJoinInfo struct {
	// DimensionTableID is the dimension table
	DimensionTableID int64

	// FactJoinColumnID is the foreign key column in the fact table
	FactJoinColumnID int64

	// DimJoinColumnID is the primary key column in the dimension table
	DimJoinColumnID int64

	// DimGroupColumnID is the column from dimension used in GROUP BY (if any)
	DimGroupColumnID int64

	// EstimatedDimSize is the estimated row count of the dimension table
	EstimatedDimSize int64

	// IsManyToOne indicates the join is fact→dimension (many-to-one)
	IsManyToOne bool
}

// BuildEncodedFilterHints analyzes filter conditions and produces hints for
// which filters can be evaluated on dictionary-encoded data.
func BuildEncodedFilterHints(
	vars *variable.SessionVars,
	eligibleColumnIDs []int64,
	conditions []expression.Expression,
) []EncodedFilterHint {
	if !vars.TiFlashEncodedOperations {
		return nil
	}

	eligibleSet := make(map[int64]bool, len(eligibleColumnIDs))
	for _, id := range eligibleColumnIDs {
		eligibleSet[id] = true
	}

	var hints []EncodedFilterHint
	for _, cond := range conditions {
		hint := analyzeFilterCondition(cond, eligibleSet)
		if hint != nil {
			hints = append(hints, *hint)
		}
	}
	return hints
}

// analyzeFilterCondition checks if a single filter condition can be applied encoded.
func analyzeFilterCondition(cond expression.Expression, eligible map[int64]bool) *EncodedFilterHint {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok {
		return nil
	}

	switch sf.FuncName.L {
	case "eq", "ne":
		col, _ := extractColumnAndConstant(sf)
		if col == nil {
			return nil
		}
		if !eligible[col.ID] {
			return nil
		}
		return &EncodedFilterHint{
			ColumnID:          col.ID,
			FilterType:        sf.FuncName.L,
			CanUseEncodedPath: true,
		}
	case "in":
		if len(sf.GetArgs()) < 2 {
			return nil
		}
		col, ok := sf.GetArgs()[0].(*expression.Column)
		if !ok || !eligible[col.ID] {
			return nil
		}
		return &EncodedFilterHint{
			ColumnID:          col.ID,
			FilterType:        "in",
			CanUseEncodedPath: true,
		}
	case "like":
		if len(sf.GetArgs()) < 2 {
			return nil
		}
		col, ok := sf.GetArgs()[0].(*expression.Column)
		if !ok || !eligible[col.ID] {
			return nil
		}
		return &EncodedFilterHint{
			ColumnID:          col.ID,
			FilterType:        "like",
			CanUseEncodedPath: true,
		}
	}
	return nil
}

// extractColumnAndConstant extracts the column and constant from a binary comparison.
func extractColumnAndConstant(sf *expression.ScalarFunction) (*expression.Column, *expression.Constant) {
	args := sf.GetArgs()
	if len(args) != 2 {
		return nil, nil
	}
	col, ok1 := args[0].(*expression.Column)
	con, ok2 := args[1].(*expression.Constant)
	if ok1 && ok2 {
		return col, con
	}
	// Try reversed order
	col, ok1 = args[1].(*expression.Column)
	con, ok2 = args[0].(*expression.Constant)
	if ok1 && ok2 {
		return col, con
	}
	return nil, nil
}

// BuildEncodedGroupByHint checks if a group-by aggregation can use encoded path.
func BuildEncodedGroupByHint(
	vars *variable.SessionVars,
	groupByColumnIDs []int64,
	aggFuncs []string,
	aggColumnIDs []int64,
	columnNDVs map[int64]int64,
) *EncodedGroupByHint {
	if !vars.TiFlashEncodedOperations {
		return nil
	}

	maxCardinality := vars.TiFlashDictEncodingMaxCardinality

	// Check that all group-by columns have NDV below threshold
	var estimatedGroups int64 = 1
	for _, colID := range groupByColumnIDs {
		ndv, ok := columnNDVs[colID]
		if !ok || ndv <= 0 || ndv > maxCardinality {
			return nil
		}
		estimatedGroups *= ndv
	}

	// Product of NDVs must also be below threshold (for composite keys)
	if estimatedGroups > maxCardinality {
		return nil
	}

	return &EncodedGroupByHint{
		GroupByColumnIDs:  groupByColumnIDs,
		AggFuncs:          aggFuncs,
		AggColumnIDs:      aggColumnIDs,
		EstimatedGroups:   estimatedGroups,
		CanUseEncodedPath: true,
	}
}

// BuildEncodedStarJoinHint checks if a star-schema join pattern is suitable
// for the fused encoded star join execution path.
func BuildEncodedStarJoinHint(
	vars *variable.SessionVars,
	factTableID int64,
	dimensions []DimensionJoinInfo,
	hasGroupByAbove bool,
) *EncodedStarJoinHint {
	if !vars.TiFlashEncodedOperations {
		return nil
	}

	if !hasGroupByAbove {
		return nil
	}

	// All dimensions must be many-to-one with small estimated size
	for _, dim := range dimensions {
		if !dim.IsManyToOne {
			return nil
		}
		if dim.EstimatedDimSize > 100000 {
			return nil
		}
	}

	return &EncodedStarJoinHint{
		FactTableID:     factTableID,
		DimensionJoins:  dimensions,
		HasGroupByAbove: hasGroupByAbove,
		CanUseFusedPath: true,
	}
}
