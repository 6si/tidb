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

package importer_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestReplaceModeSkipsTableEmptyCheck(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := util.WithInternalSourceType(context.Background(), kv.InternalImportInto)
	conn := tk.Session().GetSQLExecutor()

	_, err := conn.Execute(ctx, "create table test.t_replace(id int primary key, val varchar(64))")
	require.NoError(t, err)
	is := tk.Session().GetLatestInfoSchema().(infoschema.InfoSchema)
	tableObj, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_replace"))
	require.NoError(t, err)

	// Insert a row so the table is non-empty.
	_, err = conn.Execute(ctx, "insert into test.t_replace values(1, 'existing')")
	require.NoError(t, err)

	// With default mode (error), CheckRequirements should fail because table is not empty.
	c := &importer.LoadDataController{
		Plan: &importer.Plan{
			DBName:         "test",
			DataSourceType: importer.DataSourceTypeFile,
			TableInfo:      tableObj.Meta(),
			TotalFileSize:  1,
		},
		Table: tableObj,
	}
	err = c.CheckRequirements(ctx, tk.Session())
	require.ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)
	require.ErrorContains(t, err, "target table is not empty")

	// With replace mode, CheckRequirements should NOT fail on table emptiness.
	c2 := &importer.LoadDataController{
		Plan: &importer.Plan{
			DBName:          "test",
			DataSourceType:  importer.DataSourceTypeFile,
			TableInfo:       tableObj.Meta(),
			TotalFileSize:   1,
			OnDupKey:        importer.OnDupKeyModeReplace,
			DisablePrecheck: true,
		},
		Table: tableObj,
	}
	// With DisablePrecheck we skip CDC/PiTR checks that need etcd.
	// The key assertion: replace mode passes despite the table being non-empty.
	err = c2.CheckRequirements(ctx, tk.Session())
	require.NoError(t, err)
}
