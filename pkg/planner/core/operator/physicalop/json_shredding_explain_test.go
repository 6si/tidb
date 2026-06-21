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

package physicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func genTestColumn(tp byte, id int64) *expression.Column {
	return &expression.Column{
		RetType: types.NewFieldType(tp),
		ID:      id,
		Index:   int(id),
	}
}

func TestExtractJSONPaths(t *testing.T) {
	jsonCol := genTestColumn(mysql.TypeJSON, 1)

	// json_extract(col, '$.event')
	pathConst := &expression.Constant{
		Value:   types.NewStringDatum("$.event"),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	fn, err := expression.NewFunction(mock.NewContext(), ast.JSONExtract, types.NewFieldType(mysql.TypeJSON), jsonCol, pathConst)
	require.NoError(t, err)

	paths := extractJSONPaths(fn)
	require.Equal(t, []string{"$.event"}, paths)
}

func TestExtractJSONPathsMultiple(t *testing.T) {
	jsonCol := genTestColumn(mysql.TypeJSON, 1)

	// json_extract(col, '$.event', '$.score')
	pathConst1 := &expression.Constant{
		Value:   types.NewStringDatum("$.event"),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	pathConst2 := &expression.Constant{
		Value:   types.NewStringDatum("$.score"),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	fn, err := expression.NewFunction(mock.NewContext(), ast.JSONExtract, types.NewFieldType(mysql.TypeJSON), jsonCol, pathConst1, pathConst2)
	require.NoError(t, err)

	paths := extractJSONPaths(fn)
	require.Equal(t, []string{"$.event", "$.score"}, paths)
}

func TestExtractJSONPathsNested(t *testing.T) {
	jsonCol := genTestColumn(mysql.TypeJSON, 1)

	// eq(json_extract(col, '$.event'), 'purchase')
	pathConst := &expression.Constant{
		Value:   types.NewStringDatum("$.event"),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	extractFn, err := expression.NewFunction(mock.NewContext(), ast.JSONExtract, types.NewFieldType(mysql.TypeJSON), jsonCol, pathConst)
	require.NoError(t, err)

	valConst := &expression.Constant{
		Value:   types.NewStringDatum("purchase"),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	eqFn, err := expression.NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), extractFn, valConst)
	require.NoError(t, err)

	paths := extractJSONPaths(eqFn)
	require.Equal(t, []string{"$.event"}, paths)
}

func TestExtractJSONPathsNoExtract(t *testing.T) {
	intCol := genTestColumn(mysql.TypeLonglong, 1)

	// eq(int_col, 42)
	valConst := &expression.Constant{
		Value:   types.NewDatum(42),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	eqFn, err := expression.NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), intCol, valConst)
	require.NoError(t, err)

	paths := extractJSONPaths(eqFn)
	require.Empty(t, paths)
}

func TestCollectJSONExtractPaths(t *testing.T) {
	jsonCol := genTestColumn(mysql.TypeJSON, 1)

	// Build two filters: json_extract(col, '$.event') = 'x' AND json_extract(col, '$.score') > 50
	pathEvent := &expression.Constant{
		Value:   types.NewStringDatum("$.event"),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	extractEvent, err := expression.NewFunction(mock.NewContext(), ast.JSONExtract, types.NewFieldType(mysql.TypeJSON), jsonCol, pathEvent)
	require.NoError(t, err)
	valStr := &expression.Constant{
		Value:   types.NewStringDatum("purchase"),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	filter1, err := expression.NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), extractEvent, valStr)
	require.NoError(t, err)

	pathScore := &expression.Constant{
		Value:   types.NewStringDatum("$.score"),
		RetType: types.NewFieldType(mysql.TypeString),
	}
	extractScore, err := expression.NewFunction(mock.NewContext(), ast.JSONExtract, types.NewFieldType(mysql.TypeJSON), jsonCol, pathScore)
	require.NoError(t, err)
	val50 := &expression.Constant{
		Value:   types.NewDatum(50),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	filter2, err := expression.NewFunction(mock.NewContext(), ast.GT, types.NewFieldType(mysql.TypeLonglong), extractScore, val50)
	require.NoError(t, err)

	paths := collectJSONExtractPaths([]expression.Expression{filter1, filter2})
	require.Equal(t, []string{"$.event", "$.score"}, paths)
}

func TestDedup(t *testing.T) {
	require.Equal(t, []string{"$.event"}, dedup([]string{"$.event", "$.event"}))
	require.Equal(t, []string{"$.event", "$.score"}, dedup([]string{"$.event", "$.score", "$.event"}))
	require.Empty(t, dedup(nil))
	require.Equal(t, []string{"a"}, dedup([]string{"a"}))
}

func TestHasEqualityOnColumns(t *testing.T) {
	tenantCol := genTestColumn(mysql.TypeLonglong, 1)
	tenantCol.OrigName = "tenant_id"

	valConst := &expression.Constant{
		Value:   types.NewDatum(42),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	// eq(tenant_id, 42) — should match
	eqFn, err := expression.NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), tenantCol, valConst)
	require.NoError(t, err)

	colNames := map[string]struct{}{"tenant_id": {}}
	require.True(t, hasEqualityOnColumns([]expression.Expression{eqFn}, colNames))

	// gt(tenant_id, 42) — should NOT match (not equality)
	gtFn, err := expression.NewFunction(mock.NewContext(), ast.GT, types.NewFieldType(mysql.TypeLonglong), tenantCol, valConst)
	require.NoError(t, err)
	require.False(t, hasEqualityOnColumns([]expression.Expression{gtFn}, colNames))

	// eq on a different column — should NOT match
	otherCol := genTestColumn(mysql.TypeLonglong, 2)
	otherCol.OrigName = "status"
	eqOther, err := expression.NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), otherCol, valConst)
	require.NoError(t, err)
	require.False(t, hasEqualityOnColumns([]expression.Expression{eqOther}, colNames))

	// eq with fully qualified OrigName (e.g. "test.table.tenant_id") — should match
	fqCol := genTestColumn(mysql.TypeLonglong, 3)
	fqCol.OrigName = "test.sharded_t.tenant_id"
	eqFQ, err := expression.NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), fqCol, valConst)
	require.NoError(t, err)
	require.True(t, hasEqualityOnColumns([]expression.Expression{eqFQ}, colNames))
}

func TestCheckEqualityOnColumnNested(t *testing.T) {
	tenantCol := genTestColumn(mysql.TypeLonglong, 1)
	tenantCol.OrigName = "tenant_id"
	statusCol := genTestColumn(mysql.TypeString, 2)
	statusCol.OrigName = "status"

	val42 := &expression.Constant{
		Value:   types.NewDatum(42),
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	valActive := &expression.Constant{
		Value:   types.NewStringDatum("active"),
		RetType: types.NewFieldType(mysql.TypeString),
	}

	// AND(eq(tenant_id, 42), eq(status, 'active'))
	eqTenant, err := expression.NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), tenantCol, val42)
	require.NoError(t, err)
	eqStatus, err := expression.NewFunction(mock.NewContext(), ast.EQ, types.NewFieldType(mysql.TypeLonglong), statusCol, valActive)
	require.NoError(t, err)
	andFn, err := expression.NewFunction(mock.NewContext(), ast.LogicAnd, types.NewFieldType(mysql.TypeLonglong), eqTenant, eqStatus)
	require.NoError(t, err)

	colNames := map[string]struct{}{"tenant_id": {}}
	require.True(t, checkEqualityOnColumn(andFn, colNames))

	// AND doesn't contain shard key
	otherNames := map[string]struct{}{"region_id": {}}
	require.False(t, checkEqualityOnColumn(andFn, otherNames))
}
