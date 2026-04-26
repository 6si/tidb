// Copyright 2019 PingCAP, Inc.
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

package core_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestRowSizeInMPP(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(10), b varchar(20), c varchar(256))")
	tk.MustExec("insert into t values (space(10), space(20), space(256))")
	tk.MustExec("analyze table t")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(pmodel.NewCIStr("test"))
	require.True(t, exists)
	tblInfos, err := is.SchemaTableInfos(context.Background(), db.Name)
	require.NoError(t, err)
	for _, tblInfo := range tblInfos {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec(`set @@tidb_opt_tiflash_concurrency_factor=1`)
	tk.MustExec(`set @@tidb_allow_mpp=1`)
	var costs [3]float64
	for i, col := range []string{"a", "b", "c"} {
		rs := tk.MustQuery(fmt.Sprintf(`explain format='verbose' select /*+ read_from_storage(tiflash[t]) */ %v from t`, col)).Rows()
		cost, err := strconv.ParseFloat(rs[0][2].(string), 64)
		require.NoError(t, err)
		costs[i] = cost
	}
	require.True(t, costs[0] < costs[1] && costs[1] < costs[2]) // rowSize can affect the final cost
}

func TestBothCoLocated(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(company_id bigint, v int)")
	tk.MustExec("create table t2(company_id bigint, v int)")

	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	testkit.SetTiFlashReplica(t, dom, "test", "t2")

	is := dom.InfoSchema()
	t1Info, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t1"))
	require.NoError(t, err)
	t2Info, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t2"))
	require.NoError(t, err)

	t1Info.Meta().ShardKeyInfo = &model.ShardKeyInfo{Columns: []string{"company_id"}, ShardCnt: 4}
	t2Info.Meta().ShardKeyInfo = &model.ShardKeyInfo{Columns: []string{"company_id"}, ShardCnt: 4}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")

	// Both co-located: plan should NOT contain HashPartition ExchangeSender before the join.
	// Use shuffle_join hint to force shuffle join (not broadcast), which is the relevant path
	// for co-location optimization.
	rows := tk.MustQuery(
		"explain format='brief' select /*+ shuffle_join(t1, t2) */ count(*) from t1 join t2 on t1.company_id = t2.company_id",
	).Rows()
	planStr := strings.Join(func() []string {
		s := make([]string, 0, len(rows))
		for _, r := range rows {
			s = append(s, fmt.Sprintf("%v", r))
		}
		return s
	}(), "\n")
	// The only ExchangeSender allowed is the final PassThrough sender that returns results to TiDB.
	// There should be no Hash-partition ExchangeSender feeding into the join.
	require.NotContains(t, planStr, "HashPartition", "co-located join should not need a hash-partition exchange")

	// Different shard count: plan MUST contain a HashPartition ExchangeSender.
	t2Info.Meta().ShardKeyInfo = &model.ShardKeyInfo{Columns: []string{"company_id"}, ShardCnt: 8}
	rows = tk.MustQuery(
		"explain format='brief' select /*+ shuffle_join(t1, t2) */ count(*) from t1 join t2 on t1.company_id = t2.company_id",
	).Rows()
	planStr = strings.Join(func() []string {
		s := make([]string, 0, len(rows))
		for _, r := range rows {
			s = append(s, fmt.Sprintf("%v", r))
		}
		return s
	}(), "\n")
	require.Contains(t, planStr, "HashPartition", "mismatched shard count must use a hash-partition exchange")
}

// planRows formats EXPLAIN rows into a single string.
func planRows(rows [][]any) string {
	s := make([]string, 0, len(rows))
	for _, r := range rows {
		s = append(s, fmt.Sprintf("%v", r))
	}
	return strings.Join(s, "\n")
}

// TestCoLocatedJoinGroupBy compares the MPP plan for a JOIN+GROUP BY query on co-sharded tables
// against the same query on plain (non-sharded) TiFlash tables.
//
// Sharded:   no HashPartition exchanges at all (join co-located, agg 1-phase)
// Plain:     two HashPartition exchanges (one per join side + one for the agg shuffle)
func TestCoLocatedJoinGroupBy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// ── Plain (non-sharded) tables ──────────────────────────────────────────
	tk.MustExec("create table p1(company_id bigint, v int)")
	tk.MustExec("create table p2(company_id bigint, score int)")

	// ── Sharded tables ──────────────────────────────────────────────────────
	tk.MustExec("create table s1(company_id bigint, v int)")
	tk.MustExec("create table s2(company_id bigint, score int)")

	dom := domain.GetDomain(tk.Session())
	for _, tbl := range []string{"p1", "p2", "s1", "s2"} {
		testkit.SetTiFlashReplica(t, dom, "test", tbl)
	}

	is := dom.InfoSchema()
	for _, name := range []string{"s1", "s2"} {
		info, err := is.TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr(name))
		require.NoError(t, err)
		info.Meta().ShardKeyInfo = &model.ShardKeyInfo{Columns: []string{"company_id"}, ShardCnt: 4}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")

	const q = "explain format='brief' select /*+ shuffle_join(%s, %s) */ %s.company_id, count(*), sum(%s.score) " +
		"from %s join %s on %s.company_id = %s.company_id group by %s.company_id"

	plainPlan := planRows(tk.MustQuery(fmt.Sprintf(q, "p1", "p2", "p1", "p2", "p1", "p2", "p1", "p2", "p1")).Rows())
	shardPlan := planRows(tk.MustQuery(fmt.Sprintf(q, "s1", "s2", "s1", "s2", "s1", "s2", "s1", "s2", "s1")).Rows())

	t.Logf("Plain (non-sharded) plan:\n%s", plainPlan)
	t.Logf("Sharded plan:\n%s", shardPlan)

	plainHP := strings.Count(plainPlan, "HashPartition")
	shardHP := strings.Count(shardPlan, "HashPartition")

	t.Logf("HashPartition exchange count — plain: %d, sharded: %d", plainHP, shardHP)

	// Plain tables require at least 2 HashPartition exchanges: one per join side.
	require.GreaterOrEqual(t, plainHP, 2,
		"plain join+groupby must have HashPartition exchanges for both join sides")

	// Sharded co-located join+groupby must have zero HashPartition exchanges.
	require.Equal(t, 0, shardHP,
		"co-located sharded join+groupby must not insert any HashPartition exchange; plan:\n%s", shardPlan)

	// The sharded plan must be strictly cheaper in terms of exchange operations.
	require.Less(t, shardHP, plainHP,
		"sharded plan must have fewer HashPartition exchanges than plain")
}

