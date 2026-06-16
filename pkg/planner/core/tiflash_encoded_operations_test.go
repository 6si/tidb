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

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestBuildEncodedOperationHintDisabled(t *testing.T) {
	vars := &variable.SessionVars{
		TiFlashEncodedOperations: false,
	}
	hint := BuildEncodedOperationHint(vars, &PhysicalTableScan{})
	require.Nil(t, hint)
}

func TestBuildEncodedOperationHintEnabled(t *testing.T) {
	vars := &variable.SessionVars{
		TiFlashEncodedOperations:          true,
		TiFlashDictEncodingMaxCardinality: 4096,
	}
	scan := &PhysicalTableScan{}
	hint := BuildEncodedOperationHint(vars, scan)
	require.NotNil(t, hint)
	require.True(t, hint.EnableDictEncoding)
	require.True(t, hint.EnableEncodedFilter)
	require.True(t, hint.EnableEncodedGroupBy)
	require.True(t, hint.EnableEncodedBloomFilter)
	require.Equal(t, int64(4096), hint.MaxCardinality)
}

func TestBuildEncodedOperationHintCustomCardinality(t *testing.T) {
	vars := &variable.SessionVars{
		TiFlashEncodedOperations:          true,
		TiFlashDictEncodingMaxCardinality: 2048,
	}
	scan := &PhysicalTableScan{}
	hint := BuildEncodedOperationHint(vars, scan)
	require.NotNil(t, hint)
	require.Equal(t, int64(2048), hint.MaxCardinality)
}

func TestEncodedOperationHintStruct(t *testing.T) {
	hint := &EncodedOperationHint{
		EnableDictEncoding:       true,
		MaxCardinality:           4096,
		DictEligibleColumnIDs:    []int64{1, 2, 3},
		EnableEncodedFilter:      true,
		EnableEncodedGroupBy:     true,
		EnableEncodedBloomFilter: false,
	}
	require.Len(t, hint.DictEligibleColumnIDs, 3)
	require.Contains(t, hint.DictEligibleColumnIDs, int64(1))
	require.Contains(t, hint.DictEligibleColumnIDs, int64(2))
	require.Contains(t, hint.DictEligibleColumnIDs, int64(3))
}
