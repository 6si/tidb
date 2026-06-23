// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	tidb "github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/util/cdcutil"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

// GetEtcdClient returns an etcd client.
// exported for testing.
var GetEtcdClient = store.NewEtcdCli

// CheckRequirements checks the requirements for IMPORT INTO.
// we check the following things here:
//   - when import from file
//     1. there is no active job on the target table
//     2. the total file size > 0
//     3. if global sort, check required privileges
//   - target table should be empty
//   - no CDC or PiTR tasks running
//
// we check them one by one, and return the first error we meet.
func (e *LoadDataController) CheckRequirements(ctx context.Context, se sessionctx.Context) error {
	return e.checkRequirements(ctx, se, true)
}

// CheckRequirementsBeforeInitDataFiles checks requirements that don't depend on
// discovered data files, and is used by async-prepare submit path.
func (e *LoadDataController) CheckRequirementsBeforeInitDataFiles(ctx context.Context, se sessionctx.Context) error {
	return e.checkRequirements(ctx, se, false)
}

func (e *LoadDataController) checkRequirements(ctx context.Context, se sessionctx.Context, checkTotalFileSize bool) error {
	conn := se.GetSQLExecutor()
	if e.DataSourceType == DataSourceTypeFile {
		cnt, err := GetActiveJobCnt(ctx, conn, e.Plan.DBName, e.Plan.TableInfo.Name.L)
		if err != nil {
			return errors.Trace(err)
		}
		if cnt > 0 {
			return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("there is active job on the target table already")
		}
		if checkTotalFileSize {
			if err := e.CheckImportDataSize(); err != nil {
				return err
			}
		}
	}
	if err := e.checkTableEmpty(ctx, conn); err != nil {
		return err
	}
	if !e.DisablePrecheck {
		if err := e.checkCDCPiTRTasks(ctx, se); err != nil {
			return err
		}
	}
	if e.IsGlobalSort() {
		return e.checkGlobalSortStorePrivilege(ctx)
	}
	return nil
}

// CheckImportDataSize checks whether source data files were discovered and are
// within configured size limits.
func (e *LoadDataController) CheckImportDataSize() error {
	if e.TotalFileSize == 0 {
		// this happens when:
		// 1. no file matched when using wildcard
		// 2. all matched file is empty(with or without wildcard)
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("No file matched, or the file is empty. Please provide a valid file location.")
	}
	return e.checkStarterMaxImportDataSize()
}

func (e *LoadDataController) checkStarterMaxImportDataSize() error {
	if !deploymode.IsStarter() {
		return nil
	}
	maxImportDataSize := tidb.GetGlobalConfig().StarterParams.MaxImportDataSize
	if maxImportDataSize == 0 || e.TotalRealSize <= 0 || uint64(e.TotalRealSize) <= uint64(maxImportDataSize) {
		return nil
	}
	return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(fmt.Sprintf(
		"total real import data size %s exceeds maximum import size limit %s (total file size %s)",
		units.BytesSize(float64(e.TotalRealSize)),
		units.BytesSize(float64(maxImportDataSize)),
		units.BytesSize(float64(e.TotalFileSize)),
	))
}

func (e *LoadDataController) checkTableEmpty(ctx context.Context, conn sqlexec.SQLExecutor) error {
	if len(e.TargetPartitions) > 0 {
		return e.checkPartitionsEmpty(ctx, conn)
	}
	sql := common.SprintfWithIdentifiers("SELECT 1 FROM %s.%s USE INDEX() LIMIT 1", e.DBName, e.Table.Meta().Name.L)
	rs, err := conn.ExecuteInternal(ctx, sql)
	if err != nil {
		return err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("target table is not empty")
	}
	return nil
}

// checkPartitionsEmpty checks that only the targeted partitions/shards are empty,
// allowing IMPORT INTO on a table whose other partitions contain data.
func (e *LoadDataController) checkPartitionsEmpty(ctx context.Context, conn sqlexec.SQLExecutor) error {
	tblInfo := e.Table.Meta()
	// Validate the table is partitioned or sharded.
	if tblInfo.GetPartitionInfo() == nil && tblInfo.ShardKeyInfo == nil {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(
			"PARTITION clause requires a partitioned or SHARD BY table")
	}
	// Safety guard: reject if the table has unique secondary indexes. Unique
	// indexes span all partitions, so even if the target partition range is
	// empty, SST ingest could produce duplicate unique-index keys that
	// collide with rows in other partitions.
	if err := e.rejectExtraUniqueIndexes(tblInfo); err != nil {
		return err
	}
	// Build: SELECT 1 FROM db.tbl PARTITION(p1, p2, ...) USE INDEX() LIMIT 1
	var partList strings.Builder
	for i, p := range e.TargetPartitions {
		if i > 0 {
			partList.WriteString(", ")
		}
		partList.WriteString(common.EscapeIdentifier(p.L))
	}
	sql := fmt.Sprintf("SELECT 1 FROM %s.%s PARTITION(%s) USE INDEX() LIMIT 1",
		common.EscapeIdentifier(e.DBName),
		common.EscapeIdentifier(tblInfo.Name.L),
		partList.String())
	rs, err := conn.ExecuteInternal(ctx, sql)
	if err != nil {
		return errors.Annotatef(err, "checking target partitions empty")
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(
			"target partition(s) are not empty")
	}
	return nil
}

// rejectExtraUniqueIndexes returns an error if the table has any unique
// secondary index. Unique indexes span all partitions, so partition-scoped
// IMPORT INTO cannot guarantee no collisions on those indexes without an
// expensive cross-partition duplicate-detect pass (Option B territory).
// The clustered PK is safe because its key range is partition-scoped.
func (*LoadDataController) rejectExtraUniqueIndexes(tblInfo *model.TableInfo) error {
	for _, idx := range tblInfo.Indices {
		if idx.State != model.StatePublic {
			continue
		}
		if idx.Primary {
			continue
		}
		if idx.Unique {
			return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(
				fmt.Sprintf("partition-scoped IMPORT INTO does not support tables "+
					"with unique secondary indexes (found index %q); use whole-table "+
					"IMPORT INTO or the double-ingestion pattern instead", idx.Name.O))
		}
	}
	return nil
}

func (*LoadDataController) checkCDCPiTRTasks(ctx context.Context, se sessionctx.Context) error {
	cli, err := GetEtcdClient(se.GetStore())
	if err != nil {
		return err
	}
	defer terror.Call(cli.Close)

	pitrCli := streamhelper.NewMetaDataClient(cli)
	tasks, err := pitrCli.GetAllTasks(ctx)
	if err != nil {
		return err
	}
	if len(tasks) > 0 {
		names := make([]string, 0, len(tasks))
		for _, task := range tasks {
			names = append(names, task.Info.GetName())
		}
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(fmt.Sprintf("found PiTR log streaming task(s): %v,", names))
	}

	nameSet, err := cdcutil.GetRunningChangefeeds(ctx, cli)
	if err != nil {
		return errors.Trace(err)
	}

	if !nameSet.Empty() {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs(nameSet.MessageToUser())
	}
	return nil
}

func (e *LoadDataController) checkGlobalSortStorePrivilege(ctx context.Context) error {
	// we need read/put/delete/list privileges on global sort store.
	target := "cloud storage"
	cloudStorageURL, err3 := objstore.ParseRawURL(e.Plan.CloudStorageURI)
	if err3 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(target, err3.Error())
	}
	b, err2 := objstore.ParseBackendFromURL(cloudStorageURL, nil)
	if err2 != nil {
		return exeerrors.ErrLoadDataInvalidURI.GenWithStackByArgs(target, errors.GetErrStackMsg(err2))
	}

	if !isSupportedCloudStorageBackend(b) {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("unsupported cloud storage uri scheme: " + cloudStorageURL.Scheme)
	}

	opt := &storeapi.Options{
		CheckPermissions: []storeapi.Permission{
			storeapi.GetObject,
			storeapi.ListObjects,
			storeapi.PutAndDeleteObject,
		},
	}
	if intest.InTest {
		opt.NoCredentials = true
	}
	_, err := objstore.New(ctx, b, opt)
	if err != nil {
		return exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("check cloud storage uri access: " + err.Error())
	}
	return nil
}
