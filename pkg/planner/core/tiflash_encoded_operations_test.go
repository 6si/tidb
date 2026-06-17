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
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// TC-ENC-01: Session variable controls
// ---------------------------------------------------------------------------

func TestEncodedOps_DisabledByDefault(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	require.False(t, vars.TiFlashEncodedOperations,
		"tidb_tiflash_encoded_operations should default to OFF")
	require.Equal(t, int64(4096), vars.TiFlashDictEncodingMaxCardinality,
		"default max cardinality should be 4096")
}

func TestEncodedOps_BuildHintReturnsNilWhenDisabled(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	hint := BuildEncodedOperationHint(vars, nil)
	require.Nil(t, hint)
}

func TestEncodedOps_BuildHintReturnsHintWhenEnabled(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.TiFlashEncodedOperations = true
	vars.TiFlashDictEncodingMaxCardinality = 2048
	hint := BuildEncodedOperationHint(vars, nil)
	require.NotNil(t, hint)
	require.True(t, hint.EnableDictEncoding)
	require.True(t, hint.EnableEncodedFilter)
	require.True(t, hint.EnableEncodedGroupBy)
	require.True(t, hint.EnableEncodedBloomFilter)
	require.Equal(t, int64(2048), hint.MaxCardinality)
	require.Empty(t, hint.DictEligibleColumnIDs, "no columns without scan stats")
}

// ---------------------------------------------------------------------------
// TC-ENC-02: Column eligibility by type
// ---------------------------------------------------------------------------

func TestEncodedOps_ColumnEligibility_IntTypes(t *testing.T) {
	intTypes := []byte{mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong}
	for _, tp := range intTypes {
		col := &expression.Column{
			RetType: types.NewFieldType(tp),
		}
		require.True(t, IsColumnDictEligible(nil, col, 4096),
			"int type %d should be eligible", tp)
	}
}

func TestEncodedOps_ColumnEligibility_StringTypes(t *testing.T) {
	strTypes := []byte{mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString}
	for _, tp := range strTypes {
		col := &expression.Column{
			RetType: types.NewFieldType(tp),
		}
		require.True(t, IsColumnDictEligible(nil, col, 4096),
			"string type %d should be eligible", tp)
	}
}

func TestEncodedOps_ColumnEligibility_FloatNotEligible(t *testing.T) {
	floatTypes := []byte{mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal}
	for _, tp := range floatTypes {
		col := &expression.Column{
			RetType: types.NewFieldType(tp),
		}
		require.False(t, IsColumnDictEligible(nil, col, 4096),
			"float type %d should NOT be eligible", tp)
	}
}

func TestEncodedOps_ColumnEligibility_NilColumn(t *testing.T) {
	require.False(t, IsColumnDictEligible(nil, nil, 4096))
}

func TestEncodedOps_ColumnEligibility_ZeroCardinality(t *testing.T) {
	col := &expression.Column{
		RetType: types.NewFieldType(mysql.TypeLong),
	}
	require.False(t, IsColumnDictEligible(nil, col, 0))
}

// ---------------------------------------------------------------------------
// TC-ENC-03: Encoded filter hint generation
// ---------------------------------------------------------------------------

func TestEncodedOps_FilterHints_DisabledReturnsNil(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	hints := BuildEncodedFilterHints(vars, []int64{1}, nil)
	require.Nil(t, hints)
}

func TestEncodedOps_FilterHints_EqFilter(t *testing.T) {
	sctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()

	vars := sctx.GetSessionVars()
	vars.TiFlashEncodedOperations = true

	col := &expression.Column{
		UniqueID: 1,
		ID:       10,
		RetType:  types.NewFieldType(mysql.TypeVarchar),
	}
	cnst := &expression.Constant{
		Value:   types.NewStringDatum("active"),
		RetType: types.NewFieldType(mysql.TypeVarchar),
	}
	eqFunc, err := expression.NewFunction(
		sctx.GetExprCtx(),
		ast.EQ,
		types.NewFieldType(mysql.TypeTiny),
		col, cnst,
	)
	require.NoError(t, err)

	hints := BuildEncodedFilterHints(vars, []int64{10}, []expression.Expression{eqFunc})
	require.Len(t, hints, 1)
	require.Equal(t, int64(10), hints[0].ColumnID)
	require.Equal(t, "eq", hints[0].FilterType)
	require.True(t, hints[0].CanUseEncodedPath)
}

func TestEncodedOps_FilterHints_NonEligibleColumnSkipped(t *testing.T) {
	sctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()

	vars := sctx.GetSessionVars()
	vars.TiFlashEncodedOperations = true

	col := &expression.Column{
		UniqueID: 1,
		ID:       10,
		RetType:  types.NewFieldType(mysql.TypeVarchar),
	}
	cnst := &expression.Constant{
		Value:   types.NewStringDatum("active"),
		RetType: types.NewFieldType(mysql.TypeVarchar),
	}
	eqFunc, err := expression.NewFunction(
		sctx.GetExprCtx(),
		ast.EQ,
		types.NewFieldType(mysql.TypeTiny),
		col, cnst,
	)
	require.NoError(t, err)

	// Column ID 10 is NOT in the eligible set (only 99 is)
	hints := BuildEncodedFilterHints(vars, []int64{99}, []expression.Expression{eqFunc})
	require.Empty(t, hints, "non-eligible column should produce no hints")
}

func TestEncodedOps_FilterHints_MultipleFilters(t *testing.T) {
	sctx := coretestsdk.MockContext()
	defer func() {
		domain.GetDomain(sctx).StatsHandle().Close()
	}()

	vars := sctx.GetSessionVars()
	vars.TiFlashEncodedOperations = true

	col1 := &expression.Column{UniqueID: 1, ID: 10, RetType: types.NewFieldType(mysql.TypeVarchar)}
	col2 := &expression.Column{UniqueID: 2, ID: 20, RetType: types.NewFieldType(mysql.TypeLong)}
	cnst1 := &expression.Constant{Value: types.NewStringDatum("active"), RetType: types.NewFieldType(mysql.TypeVarchar)}
	cnst2 := &expression.Constant{Value: types.NewIntDatum(42), RetType: types.NewFieldType(mysql.TypeLong)}

	eq1, err := expression.NewFunction(sctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny), col1, cnst1)
	require.NoError(t, err)
	ne2, err := expression.NewFunction(sctx.GetExprCtx(), ast.NE, types.NewFieldType(mysql.TypeTiny), col2, cnst2)
	require.NoError(t, err)

	hints := BuildEncodedFilterHints(vars, []int64{10, 20}, []expression.Expression{eq1, ne2})
	require.Len(t, hints, 2)
	require.Equal(t, "eq", hints[0].FilterType)
	require.Equal(t, "ne", hints[1].FilterType)
}

// ---------------------------------------------------------------------------
// TC-ENC-04: Encoded group-by hint generation
// ---------------------------------------------------------------------------

func TestEncodedOps_GroupByHint_DisabledReturnsNil(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	hint := BuildEncodedGroupByHint(vars, []int64{1}, []string{"sum"}, []int64{2}, map[int64]int64{1: 50})
	require.Nil(t, hint)
}

func TestEncodedOps_GroupByHint_LowCardinality(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.TiFlashEncodedOperations = true
	vars.TiFlashDictEncodingMaxCardinality = 4096

	hint := BuildEncodedGroupByHint(vars,
		[]int64{1},
		[]string{"sum", "count"},
		[]int64{2, 0},
		map[int64]int64{1: 50, 2: 100},
	)
	require.NotNil(t, hint)
	require.True(t, hint.CanUseEncodedPath)
	require.Equal(t, int64(50), hint.EstimatedGroups)
	require.Equal(t, []int64{1}, hint.GroupByColumnIDs)
}

func TestEncodedOps_GroupByHint_HighCardinalityReturnsNil(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.TiFlashEncodedOperations = true
	vars.TiFlashDictEncodingMaxCardinality = 100

	hint := BuildEncodedGroupByHint(vars,
		[]int64{1},
		[]string{"sum"},
		[]int64{2},
		map[int64]int64{1: 5000},
	)
	require.Nil(t, hint, "high cardinality group-by should not use encoded path")
}

func TestEncodedOps_GroupByHint_CompositeKeyProduct(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.TiFlashEncodedOperations = true
	vars.TiFlashDictEncodingMaxCardinality = 4096

	// GROUP BY (col1, col2): NDV(col1)=50, NDV(col2)=100 → product=5000 > 4096 → nil
	hint := BuildEncodedGroupByHint(vars,
		[]int64{1, 2},
		[]string{"sum"},
		[]int64{3},
		map[int64]int64{1: 50, 2: 100},
	)
	require.Nil(t, hint, "composite key NDV product 5000 exceeds threshold 4096")
}

func TestEncodedOps_GroupByHint_CompositeKeyUnderThreshold(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.TiFlashEncodedOperations = true
	vars.TiFlashDictEncodingMaxCardinality = 4096

	hint := BuildEncodedGroupByHint(vars,
		[]int64{1, 2},
		[]string{"sum", "count"},
		[]int64{3, 0},
		map[int64]int64{1: 10, 2: 20},
	)
	require.NotNil(t, hint)
	require.Equal(t, int64(200), hint.EstimatedGroups)
	require.True(t, hint.CanUseEncodedPath)
}

func TestEncodedOps_GroupByHint_MissingNDVReturnsNil(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.TiFlashEncodedOperations = true
	vars.TiFlashDictEncodingMaxCardinality = 4096

	// Column 1 has no NDV entry → nil
	hint := BuildEncodedGroupByHint(vars,
		[]int64{1},
		[]string{"sum"},
		[]int64{2},
		map[int64]int64{99: 50}, // col 1 missing
	)
	require.Nil(t, hint, "missing NDV should block encoded path")
}

// ---------------------------------------------------------------------------
// TC-ENC-05: Encoded star join hint generation
// ---------------------------------------------------------------------------

func TestEncodedOps_StarJoinHint_DisabledReturnsNil(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	hint := BuildEncodedStarJoinHint(vars, 1, nil, true)
	require.Nil(t, hint)
}

func TestEncodedOps_StarJoinHint_NoGroupByReturnsNil(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.TiFlashEncodedOperations = true
	hint := BuildEncodedStarJoinHint(vars, 1, []DimensionJoinInfo{
		{DimensionTableID: 2, IsManyToOne: true, EstimatedDimSize: 100},
	}, false)
	require.Nil(t, hint)
}

func TestEncodedOps_StarJoinHint_SmallDimensions(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.TiFlashEncodedOperations = true
	dims := []DimensionJoinInfo{
		{DimensionTableID: 2, FactJoinColumnID: 10, DimJoinColumnID: 1, DimGroupColumnID: 2, EstimatedDimSize: 500, IsManyToOne: true},
		{DimensionTableID: 3, FactJoinColumnID: 11, DimJoinColumnID: 1, DimGroupColumnID: 3, EstimatedDimSize: 100, IsManyToOne: true},
	}
	hint := BuildEncodedStarJoinHint(vars, 1, dims, true)
	require.NotNil(t, hint)
	require.True(t, hint.CanUseFusedPath)
	require.Equal(t, int64(1), hint.FactTableID)
	require.Len(t, hint.DimensionJoins, 2)
}

func TestEncodedOps_StarJoinHint_LargeDimReturnsNil(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.TiFlashEncodedOperations = true
	dims := []DimensionJoinInfo{
		{DimensionTableID: 2, EstimatedDimSize: 50, IsManyToOne: true},
		{DimensionTableID: 3, EstimatedDimSize: 200000, IsManyToOne: true},
	}
	hint := BuildEncodedStarJoinHint(vars, 1, dims, true)
	require.Nil(t, hint, "dimension with >100k rows should block star join")
}

func TestEncodedOps_StarJoinHint_NonManyToOneReturnsNil(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.TiFlashEncodedOperations = true
	dims := []DimensionJoinInfo{
		{DimensionTableID: 2, EstimatedDimSize: 50, IsManyToOne: false},
	}
	hint := BuildEncodedStarJoinHint(vars, 1, dims, true)
	require.Nil(t, hint, "non-many-to-one dimension should block star join")
}

// ---------------------------------------------------------------------------
// TC-ENC-06: NDV extraction from stats
// ---------------------------------------------------------------------------

func TestEncodedOps_GetColNDV_NilStats(t *testing.T) {
	require.Equal(t, int64(0), getColNDVFromStats(nil, 1))
}

func TestEncodedOps_GetColNDV_NilMap(t *testing.T) {
	stats := &property.StatsInfo{ColNDVs: nil}
	require.Equal(t, int64(0), getColNDVFromStats(stats, 1))
}

func TestEncodedOps_GetColNDV_Found(t *testing.T) {
	stats := &property.StatsInfo{ColNDVs: map[int64]float64{10: 42.0, 20: 100.5}}
	require.Equal(t, int64(42), getColNDVFromStats(stats, 10))
	require.Equal(t, int64(100), getColNDVFromStats(stats, 20))
}

func TestEncodedOps_GetColNDV_NotFound(t *testing.T) {
	stats := &property.StatsInfo{ColNDVs: map[int64]float64{10: 42.0}}
	require.Equal(t, int64(0), getColNDVFromStats(stats, 99))
}
