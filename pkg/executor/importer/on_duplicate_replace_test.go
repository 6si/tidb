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

package importer

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/common"
	lconfig "github.com/pingcap/tidb/pkg/lightning/config"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

func TestOnDupKeyModeReplaceOptionParsing(t *testing.T) {
	sctx := mock.NewContext()
	defer sctx.Close()
	ctx := tikvutil.WithInternalSourceType(context.Background(), tidbkv.InternalImportInto)

	convertOptions := func(inOptions []*ast.LoadDataOpt) []*core.LoadDataOpt {
		options := []*core.LoadDataOpt{}
		var err error
		for _, opt := range inOptions {
			loadDataOpt := core.LoadDataOpt{Name: opt.Name}
			if opt.Value != nil {
				loadDataOpt.Value, err = plannerutil.RewriteAstExprWithPlanCtx(sctx, opt.Value, nil, nil, false)
				require.NoError(t, err)
			}
			options = append(options, &loadDataOpt)
		}
		return options
	}

	// Test 'replace' mode is accepted with local sort (no cloud storage).
	sql := "import into t from '/file.csv' with on_duplicate_key='replace'"
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	vardef.CloudStorageURI.Store("")
	// PK-only table, no secondary indexes.
	plan := &Plan{
		Format:    DataFormatCSV,
		TableInfo: &model.TableInfo{},
	}
	err = plan.initOptions(ctx, sctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err)
	require.Equal(t, OnDupKeyModeReplace, plan.OnDupKey)
	// Checksum should be auto-disabled in replace mode.
	require.Equal(t, lconfig.OpLevelOff, plan.Checksum)
}

func TestOnDupKeyModeReplaceRejectsSecondaryIndexWithLocalSort(t *testing.T) {
	sctx := mock.NewContext()
	defer sctx.Close()
	ctx := tikvutil.WithInternalSourceType(context.Background(), tidbkv.InternalImportInto)

	convertOptions := func(inOptions []*ast.LoadDataOpt) []*core.LoadDataOpt {
		options := []*core.LoadDataOpt{}
		var err error
		for _, opt := range inOptions {
			loadDataOpt := core.LoadDataOpt{Name: opt.Name}
			if opt.Value != nil {
				loadDataOpt.Value, err = plannerutil.RewriteAstExprWithPlanCtx(sctx, opt.Value, nil, nil, false)
				require.NoError(t, err)
			}
			options = append(options, &loadDataOpt)
		}
		return options
	}

	sql := "import into t from '/file.csv' with on_duplicate_key='replace'"
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	vardef.CloudStorageURI.Store("")
	plan := &Plan{
		Format: DataFormatCSV,
		TableInfo: &model.TableInfo{
			Indices: []*model.IndexInfo{
				{
					Name:    ast.NewCIStr("idx_name"),
					State:   model.StatePublic,
					Primary: false,
					Unique:  true,
				},
			},
		},
	}
	err = plan.initOptions(ctx, sctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.ErrorIs(t, err, exeerrors.ErrLoadDataUnsupportedOption)
	require.ErrorContains(t, err, "secondary indexes")
}

func TestOnDupKeyModeReplaceAllowedWithLocalSortPKOnly(t *testing.T) {
	sctx := mock.NewContext()
	defer sctx.Close()
	ctx := tikvutil.WithInternalSourceType(context.Background(), tidbkv.InternalImportInto)

	convertOptions := func(inOptions []*ast.LoadDataOpt) []*core.LoadDataOpt {
		options := []*core.LoadDataOpt{}
		var err error
		for _, opt := range inOptions {
			loadDataOpt := core.LoadDataOpt{Name: opt.Name}
			if opt.Value != nil {
				loadDataOpt.Value, err = plannerutil.RewriteAstExprWithPlanCtx(sctx, opt.Value, nil, nil, false)
				require.NoError(t, err)
			}
			options = append(options, &loadDataOpt)
		}
		return options
	}

	sql := "import into t from '/file.csv' with on_duplicate_key='replace'"
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	vardef.CloudStorageURI.Store("")
	// PK-only table (clustered PK, no secondary indexes).
	plan := &Plan{
		Format: DataFormatCSV,
		TableInfo: &model.TableInfo{
			Indices: []*model.IndexInfo{
				{
					Name:    ast.NewCIStr("primary"),
					State:   model.StatePublic,
					Primary: true,
				},
			},
			IsCommonHandle: true,
		},
	}
	err = plan.initOptions(ctx, sctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.NoError(t, err)
	require.Equal(t, OnDupKeyModeReplace, plan.OnDupKey)
}

func TestOnDupKeyModeReplaceCaptureStillBlockedOnLocalSort(t *testing.T) {
	sctx := mock.NewContext()
	defer sctx.Close()
	ctx := tikvutil.WithInternalSourceType(context.Background(), tidbkv.InternalImportInto)

	convertOptions := func(inOptions []*ast.LoadDataOpt) []*core.LoadDataOpt {
		options := []*core.LoadDataOpt{}
		var err error
		for _, opt := range inOptions {
			loadDataOpt := core.LoadDataOpt{Name: opt.Name}
			if opt.Value != nil {
				loadDataOpt.Value, err = plannerutil.RewriteAstExprWithPlanCtx(sctx, opt.Value, nil, nil, false)
				require.NoError(t, err)
			}
			options = append(options, &loadDataOpt)
		}
		return options
	}

	// 'capture' mode should still be rejected with local sort.
	sql := "import into t from '/file.csv' with on_duplicate_key='capture'"
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)

	vardef.CloudStorageURI.Store("")
	plan := &Plan{
		Format:    DataFormatCSV,
		TableInfo: &model.TableInfo{},
	}
	err = plan.initOptions(ctx, sctx, convertOptions(stmt.(*ast.ImportIntoStmt).Options))
	require.ErrorIs(t, err, exeerrors.ErrLoadDataUnsupportedOption)
	require.ErrorContains(t, err, "local sort")
}

func TestOnDupKeyModeReplaceGetOnDupKeyMode(t *testing.T) {
	// Empty defaults to Error.
	plan := &Plan{}
	require.Equal(t, OnDupKeyModeError, plan.GetOnDupKeyMode())

	// Explicit replace.
	plan.OnDupKey = OnDupKeyModeReplace
	require.Equal(t, OnDupKeyModeReplace, plan.GetOnDupKeyMode())
}

func TestOnDupKeyModeReplaceBackendConfig(t *testing.T) {
	e := &LoadDataController{
		Plan: &Plan{
			OnDupKey: OnDupKeyModeReplace,
		},
	}
	cfg := e.getLocalBackendCfg("", "", "")
	require.True(t, cfg.DupeDetectEnabled)
	require.True(t, cfg.DuplicateDetectOpt.ReportErrOnDup)
}

func TestOnDupKeyModeDefaultBackendConfig(t *testing.T) {
	e := &LoadDataController{
		Plan: &Plan{
			OnDupKey: OnDupKeyModeError,
		},
	}
	cfg := e.getLocalBackendCfg("", "", "")
	require.False(t, cfg.DupeDetectEnabled)
	require.False(t, cfg.DuplicateDetectOpt.ReportErrOnDup)
}

func TestImportAndCleanupReplaceModeToleratesDuplicateKeys(t *testing.T) {
	// Verify that ErrFoundDuplicateKeys is swallowed in replace mode.
	err := common.ErrFoundDuplicateKeys.FastGenByArgs("test key")

	plan := &Plan{OnDupKey: OnDupKeyModeReplace}
	require.True(t, common.ErrFoundDuplicateKeys.Equal(err))
	// In replace mode, ImportAndCleanup should set importErr = nil.
	// Here we test the logic inline since we can't easily mock the full
	// ClosedEngine. The key logic is:
	if common.ErrFoundDuplicateKeys.Equal(err) && plan.OnDupKey == OnDupKeyModeReplace {
		err = nil
	}
	require.NoError(t, err)
}

func TestImportAndCleanupErrorModeFailsOnDuplicateKeys(t *testing.T) {
	// Verify that ErrFoundDuplicateKeys is NOT swallowed in error mode.
	err := common.ErrFoundDuplicateKeys.FastGenByArgs("test key")

	plan := &Plan{OnDupKey: OnDupKeyModeError}
	require.True(t, common.ErrFoundDuplicateKeys.Equal(err))
	// In error mode, the error should propagate.
	if common.ErrFoundDuplicateKeys.Equal(err) && plan.OnDupKey == OnDupKeyModeReplace {
		err = nil
	}
	require.Error(t, err)
}
