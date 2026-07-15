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

package importer_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/cdcutil"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

const addrFmt = "http://127.0.0.1:%d"

func createMockETCD(t *testing.T) (string, *embed.Etcd) {
	cfg := embed.NewConfig()
	cfg.Dir = t.TempDir()
	// rand port in [20000, 60000)
	randPort := int(rand.Int31n(40000)) + 20000
	clientAddr := fmt.Sprintf(addrFmt, randPort)
	lcurl, _ := url.Parse(clientAddr)
	cfg.ListenClientUrls, cfg.AdvertiseClientUrls = []url.URL{*lcurl}, []url.URL{*lcurl}
	lpurl, _ := url.Parse(fmt.Sprintf(addrFmt, randPort+1))
	cfg.ListenPeerUrls, cfg.AdvertisePeerUrls = []url.URL{*lpurl}, []url.URL{*lpurl}
	cfg.InitialCluster = "default=" + lpurl.String()
	cfg.Logger = "zap"
	embedEtcd, err := embed.StartEtcd(cfg)
	require.NoError(t, err)

	select {
	case <-embedEtcd.Server.ReadyNotify():
	case <-time.After(5 * time.Second):
		embedEtcd.Server.Stop() // trigger a shutdown
		require.False(t, true, "server took too long to start")
	}
	return clientAddr, embedEtcd
}

func TestCheckPartitionsEmpty(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	ctx := util.WithInternalSourceType(context.Background(), kv.InternalImportInto)
	conn := tk.Session().GetSQLExecutor()

	controller := func(tableName string, targets ...string) *importer.LoadDataController {
		is := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema)
		tableObj, err := is.TableByName(ctx, model.NewCIStr("test"), model.NewCIStr(tableName))
		require.NoError(t, err)
		names := make([]model.CIStr, 0, len(targets))
		for _, target := range targets {
			names = append(names, model.NewCIStr(target))
		}
		return &importer.LoadDataController{
			Plan: &importer.Plan{
				DBName:           "test",
				DataSourceType:   importer.DataSourceTypeQuery,
				TableInfo:        tableObj.Meta(),
				TargetPartitions: names,
				DisablePrecheck:  true,
			},
			Table: tableObj,
		}
	}

	tk.MustExec(`CREATE TABLE pt (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		ts DATE NOT NULL,
		KEY idx_id (id)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (ts) (
		PARTITION p0 VALUES LESS THAN ('2025-01-01'),
		PARTITION p1 VALUES LESS THAN ('2026-01-01')
	)`)
	tk.MustExec("INSERT INTO pt VALUES (1, 10, '2024-01-01')")

	is := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema)
	pt, err := is.TableByName(ctx, model.NewCIStr("test"), model.NewCIStr("pt"))
	require.NoError(t, err)
	require.Len(t, pt.Meta().Partition.Definitions[0].ShardIDs, 4)
	require.Len(t, pt.Meta().Partition.Definitions[1].ShardIDs, 4)

	require.NoError(t, controller("pt", "p1").CheckRequirements(ctx, tk.Session(), conn))
	err = controller("pt", "p0").CheckRequirements(ctx, tk.Session(), conn)
	require.ErrorContains(t, err, "target partition(s)/shard(s) are not empty")
	err = controller("pt", "missing").CheckRequirements(ctx, tk.Session(), conn)
	require.ErrorContains(t, err, "unknown partition")

	tk.MustExec(`CREATE TABLE shard_only (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		PRIMARY KEY (id, company_id)
	) SHARD BY (company_id) SHARDS 4`)
	is = tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema)
	shardOnly, err := is.TableByName(ctx, model.NewCIStr("test"), model.NewCIStr("shard_only"))
	require.NoError(t, err)
	require.Len(t, shardOnly.Meta().ShardKeyInfo.ShardIDs, 4)
	require.NoError(t, controller("shard_only", "shard_0").CheckRequirements(ctx, tk.Session(), conn))

	tk.MustExec("CREATE TABLE plain (id BIGINT PRIMARY KEY)")
	err = controller("plain", "p0").CheckRequirements(ctx, tk.Session(), conn)
	require.ErrorContains(t, err, "requires a partitioned or SHARD BY table")

	tk.MustExec(`CREATE TABLE unique_idx (
		id BIGINT NOT NULL,
		company_id BIGINT NOT NULL,
		ts DATE NOT NULL,
		PRIMARY KEY (id, company_id, ts),
		UNIQUE KEY uk_company_ts (company_id, ts)
	) SHARD BY (company_id) SHARDS 4
	PARTITION BY RANGE COLUMNS (ts) (
		PARTITION p0 VALUES LESS THAN ('2025-01-01'),
		PARTITION p1 VALUES LESS THAN ('2026-01-01')
	)`)
	err = controller("unique_idx", "p1").CheckRequirements(ctx, tk.Session(), conn)
	require.ErrorContains(t, err, "does not support unique secondary index")
}

func TestCheckRequirements(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	ctx := util.WithInternalSourceType(context.Background(), kv.InternalImportInto)
	conn := tk.Session().GetSQLExecutor()

	_, err := conn.Execute(ctx, "create table test.t(id int primary key)")
	require.NoError(t, err)
	is := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema)
	tableObj, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	c := &importer.LoadDataController{
		Plan: &importer.Plan{
			DBName:         "test",
			DataSourceType: importer.DataSourceTypeFile,
			TableInfo:      tableObj.Meta(),
		},
		Table: tableObj,
	}

	// create a dummy job
	_, err = importer.CreateJob(ctx, conn, "test", "tttt", tableObj.Meta().ID, "root", &importer.ImportParameters{}, 0)
	require.NoError(t, err)
	// there is active job on the target table already
	jobID, err := importer.CreateJob(ctx, conn, "test", "t", tableObj.Meta().ID, "root", &importer.ImportParameters{}, 0)
	require.NoError(t, err)
	err = c.CheckRequirements(ctx, tk.Session(), conn)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)
	require.ErrorContains(t, err, "there is active job on the target table already")
	// cancel the job
	require.NoError(t, importer.CancelJob(ctx, conn, jobID))

	// source data file size = 0
	require.ErrorIs(t, c.CheckRequirements(ctx, tk.Session(), conn), exeerrors.ErrLoadDataPreCheckFailed)

	// make checkTotalFileSize pass
	c.TotalFileSize = 1
	// global sort with thread count < 8
	c.ThreadCnt = 7
	c.CloudStorageURI = "s3://test"
	err = c.CheckRequirements(ctx, tk.Session(), conn)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)
	require.ErrorContains(t, err, "global sort requires at least 8 threads")

	// reset fields, make global sort thread check pass
	c.ThreadCnt = 1
	c.CloudStorageURI = ""
	// non-empty table
	_, err = conn.Execute(ctx, "insert into test.t values(1)")
	require.NoError(t, err)
	require.ErrorIs(t, c.CheckRequirements(ctx, tk.Session(), conn), exeerrors.ErrLoadDataPreCheckFailed)
	// table not exists
	_, err = conn.Execute(ctx, "drop table if exists test.t")
	require.NoError(t, err)
	require.ErrorContains(t, c.CheckRequirements(ctx, tk.Session(), conn), "doesn't exist")

	// create table again, now checkTableEmpty pass
	_, err = conn.Execute(ctx, "create table test.t(id int primary key)")
	require.NoError(t, err)

	clientAddr, embedEtcd := createMockETCD(t)
	require.NotNil(t, embedEtcd)
	t.Cleanup(func() {
		embedEtcd.Close()
	})
	backup := importer.GetEtcdClient
	importer.GetEtcdClient = func() (*etcd.Client, error) {
		etcdCli, err := clientv3.New(clientv3.Config{
			Endpoints: []string{clientAddr},
		})
		require.NoError(t, err)
		return etcd.NewClient(etcdCli, ""), nil
	}
	t.Cleanup(func() {
		importer.GetEtcdClient = backup
	})
	// mock a PiTR task
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints: []string{clientAddr},
	})
	t.Cleanup(func() {
		require.NoError(t, etcdCli.Close())
	})
	require.NoError(t, err)
	pitrKey := streamhelper.PrefixOfTask() + "test"
	_, err = etcdCli.Put(ctx, pitrKey, "")
	require.NoError(t, err)
	err = c.CheckRequirements(ctx, tk.Session(), conn)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)
	require.ErrorContains(t, err, "found PiTR log streaming")
	// disable precheck, should pass
	c.DisablePrecheck = true
	require.NoError(t, c.CheckRequirements(ctx, tk.Session(), conn))
	c.DisablePrecheck = false // revert back

	// remove PiTR task, and mock a CDC task
	_, err = etcdCli.Delete(ctx, pitrKey)
	require.NoError(t, err)
	// example: /tidb/cdc/<clusterID>/<namespace>/changefeed/info/<changefeedID>
	cdcKey := cdcutil.CDCPrefix + "testcluster/test_ns/changefeed/info/test_cf"
	_, err = etcdCli.Put(ctx, cdcKey, `{"state":"normal"}`)
	require.NoError(t, err)
	err = c.CheckRequirements(ctx, tk.Session(), conn)
	require.ErrorIs(t, err, exeerrors.ErrLoadDataPreCheckFailed)
	require.ErrorContains(t, err, "found CDC changefeed")

	// remove CDC task, pass
	_, err = etcdCli.Delete(ctx, cdcKey)
	require.NoError(t, err)
	require.NoError(t, c.CheckRequirements(ctx, tk.Session(), conn))

	// with global sort
	c.Plan.ThreadCnt = 8
	c.Plan.CloudStorageURI = ":"
	require.ErrorIs(t, c.CheckRequirements(ctx, tk.Session(), conn), exeerrors.ErrLoadDataInvalidURI)
	c.Plan.CloudStorageURI = "sdsdsdsd://sdsdsdsd"
	require.ErrorIs(t, c.CheckRequirements(ctx, tk.Session(), conn), exeerrors.ErrLoadDataInvalidURI)
	c.Plan.CloudStorageURI = "local:///tmp"
	require.ErrorContains(t, c.CheckRequirements(ctx, tk.Session(), conn), "unsupported cloud storage uri scheme: local")
	// this mock cannot mock credential check, so we just skip it.
	backend := s3mem.New()
	faker := gofakes3.New(backend)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()
	require.NoError(t, backend.CreateBucket("test-bucket"))
	c.Plan.CloudStorageURI = fmt.Sprintf("s3://test-bucket/path?region=us-east-1&endpoint=%s&access-key=xxxxxx&secret-access-key=xxxxxx", ts.URL)
	require.NoError(t, c.CheckRequirements(ctx, tk.Session(), conn))
}
