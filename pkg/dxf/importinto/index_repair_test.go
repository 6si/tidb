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

package importinto_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIndexRepairerRemovesOrphanedEntries(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t_repair (id bigint primary key, status int, name varchar(100), index idx_status(status))")

	// Insert initial data.
	tk.MustExec("insert into t_repair values (1, 1, 'Active'), (2, 1, 'Active2'), (3, 2, 'Done')")

	// Verify index is consistent before we corrupt it.
	tk.MustExec("admin check table t_repair")

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	tblObj, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_repair"))
	require.NoError(t, err)
	tblInfo := tblObj.Meta()

	// Now simulate orphaned index entry: write a stale index entry that
	// points to handle=1 but with an old status value (status=99).
	// This simulates what happens when a row is replaced via SST ingest
	// but the old index entry is not deleted.
	var idxInfo *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == "idx_status" {
			idxInfo = idx
			break
		}
	}
	require.NotNil(t, idxInfo)

	// Encode a stale index key: (status=99, handle=1).
	staleVals := []types.Datum{types.NewIntDatum(99)}
	staleKey, _, encErr := tablecodec.GenIndexKey(
		nil, // location (nil uses UTC)
		tblInfo,
		idxInfo,
		tblInfo.ID,
		staleVals,
		tidbkv.IntHandle(1),
		nil,
	)
	require.NoError(t, encErr)

	// Write the stale index entry directly into the store.
	ctx := context.Background()
	txn, err := store.Begin()
	require.NoError(t, err)
	// For non-unique index, value encodes the handle.
	err = txn.Set(staleKey, []byte{0}) // minimal value
	require.NoError(t, err)
	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Now run IndexRepairer — it should detect and remove the stale entry.
	tbl, err := tables.TableFromMeta(autoid.Allocators{}, tblInfo)
	require.NoError(t, err)

	repairer := importinto.NewIndexRepairer(store, tbl, nil, zap.NewNop())
	removed, repairErr := repairer.RepairIndexes(ctx)
	require.NoError(t, repairErr)
	require.Equal(t, int64(1), removed)

	// Verify ADMIN CHECK TABLE passes now.
	tk.MustExec("admin check table t_repair")
}

func TestIndexRepairerNoOpWithoutSecondaryIndexes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t_no_idx (id bigint primary key, val int)")
	tk.MustExec("insert into t_no_idx values (1, 100)")

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	tblObj, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_no_idx"))
	require.NoError(t, err)
	tblInfo := tblObj.Meta()

	tbl, err := tables.TableFromMeta(autoid.Allocators{}, tblInfo)
	require.NoError(t, err)

	ctx := context.Background()
	repairer := importinto.NewIndexRepairer(store, tbl, nil, zap.NewNop())
	removed, repairErr := repairer.RepairIndexes(ctx)
	require.NoError(t, repairErr)
	require.Equal(t, int64(0), removed)
}

func TestShouldRepairIndexes(t *testing.T) {
	tblInfo := &model.TableInfo{
		Indices: []*model.IndexInfo{
			{
				ID:    1,
				Name:  ast.NewCIStr("idx_test"),
				State: model.StatePublic,
			},
		},
	}

	// Not replace mode → no repair.
	plan := &importer.Plan{
		OnDupKey:        importer.OnDupKeyModeError,
		TableInfo:       tblInfo,
		CloudStorageURI: "s3://bucket/path",
	}
	require.False(t, importinto.ShouldRepairIndexes(plan))

	// Replace mode + global sort + secondary indexes → repair.
	plan.OnDupKey = importer.OnDupKeyModeReplace
	require.True(t, importinto.ShouldRepairIndexes(plan))

	// Replace mode + global sort + no secondary indexes → no repair.
	plan.TableInfo = &model.TableInfo{}
	require.False(t, importinto.ShouldRepairIndexes(plan))

	// Replace mode + local sort + secondary indexes → no repair (already rejected at precheck).
	plan.TableInfo = tblInfo
	plan.CloudStorageURI = "" // local sort
	require.False(t, importinto.ShouldRepairIndexes(plan))
}

func TestIndexRepairerWithRangeScoping(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t_range (id bigint primary key, category int, index idx_cat(category))")
	tk.MustExec("insert into t_range values (1, 10), (2, 20), (3, 30)")

	dom, err := session.GetDomain(store)
	require.NoError(t, err)
	tblObj, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_range"))
	require.NoError(t, err)
	tblInfo := tblObj.Meta()

	var idxInfo *model.IndexInfo
	for _, idx := range tblInfo.Indices {
		if idx.Name.L == "idx_cat" {
			idxInfo = idx
			break
		}
	}
	require.NotNil(t, idxInfo)

	// Add stale entries for handles 1 and 3.
	ctx := context.Background()
	txn, err := store.Begin()
	require.NoError(t, err)

	staleKey1, _, _ := tablecodec.GenIndexKey(nil, tblInfo, idxInfo, tblInfo.ID,
		[]types.Datum{types.NewIntDatum(99)}, tidbkv.IntHandle(1), nil)
	staleKey3, _, _ := tablecodec.GenIndexKey(nil, tblInfo, idxInfo, tblInfo.ID,
		[]types.Datum{types.NewIntDatum(88)}, tidbkv.IntHandle(3), nil)
	err = txn.Set(staleKey1, []byte{0})
	require.NoError(t, err)
	err = txn.Set(staleKey3, []byte{0})
	require.NoError(t, err)
	err = txn.Commit(ctx)
	require.NoError(t, err)

	// Scope repair to only handle=1's range (should only clean up staleKey1).
	handle1Key := tablecodec.EncodeRowKeyWithHandle(tblInfo.ID, tidbkv.IntHandle(1))
	handle2Key := tablecodec.EncodeRowKeyWithHandle(tblInfo.ID, tidbkv.IntHandle(2))
	ranges := []tidbkv.KeyRange{{StartKey: handle1Key, EndKey: handle2Key}}

	tbl, err := tables.TableFromMeta(autoid.Allocators{}, tblInfo)
	require.NoError(t, err)

	repairer := importinto.NewIndexRepairer(store, tbl, ranges, zap.NewNop())
	removed, repairErr := repairer.RepairIndexes(ctx)
	require.NoError(t, repairErr)
	require.Equal(t, int64(1), removed) // Only handle=1's stale entry

	// Now run without range restriction — should clean up handle=3 too.
	repairer2 := importinto.NewIndexRepairer(store, tbl, nil, zap.NewNop())
	removed2, repairErr2 := repairer2.RepairIndexes(ctx)
	require.NoError(t, repairErr2)
	require.Equal(t, int64(1), removed2) // handle=3's stale entry

	tk.MustExec("admin check table t_range")
}
