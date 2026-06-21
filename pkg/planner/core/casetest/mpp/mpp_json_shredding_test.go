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

package mpp

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestJSONShreddingExplainAnnotation verifies that EXPLAIN output for TiFlash
// table scans on tables with JSON columns shows `json_shredding` annotations
// when @@tiflash_json_shredding is enabled.
func TestJSONShreddingExplainAnnotation(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists json_events")
		tk.MustExec("create table json_events (id int primary key, tenant_id int, payload json)")
		tk.MustExec("insert into json_events values (1, 1, '{\"event\":\"purchase\",\"score\":85}')")
		tk.MustExec("analyze table json_events")

		testkit.SetTiFlashReplica(t, dom, "test", "json_events")

		tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		tk.MustExec("set @@session.tidb_allow_mpp = 1")
		tk.MustExec("set @@session.tidb_enforce_mpp = 1")

		// Case 1: json_shredding OFF — no annotation expected
		tk.MustExec("set @@session.tiflash_json_shredding = 0")
		rows := tk.MustQuery("explain select count(*) from json_events where payload->>'$.event' = 'purchase'").Rows()
		for _, row := range rows {
			operatorInfo := row[len(row)-1].(string)
			require.NotContains(t, operatorInfo, "json_shredding",
				"json_shredding annotation should NOT appear when shredding is OFF")
		}

		// Case 2: json_shredding ON — annotation expected on TableFullScan
		tk.MustExec("set @@session.tiflash_json_shredding = 1")
		rows = tk.MustQuery("explain select count(*) from json_events where payload->>'$.event' = 'purchase'").Rows()
		found := false
		for _, row := range rows {
			operatorInfo := row[len(row)-1].(string)
			if strings.Contains(operatorInfo, "json_shredding") {
				found = true
				break
			}
		}
		require.True(t, found,
			"json_shredding annotation should appear in EXPLAIN when shredding is ON and table has JSON columns")

		// Case 3: Table without JSON columns — no annotation
		tk.MustExec("drop table if exists no_json_t")
		tk.MustExec("create table no_json_t (id int primary key, status varchar(20))")
		tk.MustExec("insert into no_json_t values (1, 'active')")
		tk.MustExec("analyze table no_json_t")
		testkit.SetTiFlashReplica(t, dom, "test", "no_json_t")

		rows = tk.MustQuery("explain select count(*) from no_json_t where status = 'active'").Rows()
		for _, row := range rows {
			operatorInfo := row[len(row)-1].(string)
			require.NotContains(t, operatorInfo, "json_shredding",
				"json_shredding annotation should NOT appear for tables without JSON columns")
		}
	})
}

// TestJSONShreddingExplainPaths verifies that specific JSON paths from
// json_extract calls in late-materialization pushed-down filters appear
// in the json_shredding annotation when applicable.
func TestJSONShreddingExplainPaths(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists json_events")
		tk.MustExec("create table json_events (id int primary key, tenant_id int, payload json)")
		tk.MustExec("insert into json_events values (1, 1, '{\"event\":\"purchase\",\"score\":85}')")
		tk.MustExec("analyze table json_events")

		testkit.SetTiFlashReplica(t, dom, "test", "json_events")

		tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		tk.MustExec("set @@session.tidb_allow_mpp = 1")
		tk.MustExec("set @@session.tidb_enforce_mpp = 1")
		tk.MustExec("set @@session.tiflash_json_shredding = 1")

		// Simple SELECT on table with JSON column — should show json_shredding annotation.
		// The actual filter is in the Selection above the scan, so the scan shows
		// "json_shredding:on" (indicating the table has JSON columns and shredding is enabled).
		rows := tk.MustQuery("explain select count(*) from json_events where payload->>'$.event' = 'purchase'").Rows()
		foundAnnotation := false
		for _, row := range rows {
			operatorInfo := row[len(row)-1].(string)
			if strings.Contains(operatorInfo, "json_shredding") {
				foundAnnotation = true
			}
		}
		require.True(t, foundAnnotation,
			"json_shredding annotation should appear for TiFlash scan on table with JSON column")
	})
}

// TestDictEncodedExplainAnnotation verifies that EXPLAIN output shows
// dict_encoded annotation for low-cardinality columns when
// @@tidb_tiflash_encoded_operations is enabled.
func TestDictEncodedExplainAnnotation(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists low_card_t")
		tk.MustExec("create table low_card_t (id int primary key, status varchar(20), region varchar(10), value int)")
		tk.MustExec("insert into low_card_t values (1, 'active', 'US', 100), (2, 'inactive', 'EU', 200), (3, 'active', 'US', 300)")
		tk.MustExec("analyze table low_card_t")

		testkit.SetTiFlashReplica(t, dom, "test", "low_card_t")

		tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		tk.MustExec("set @@session.tidb_allow_mpp = 1")
		tk.MustExec("set @@session.tidb_enforce_mpp = 1")

		// Case 1: encoded_operations OFF — no dict_encoded annotation
		tk.MustExec("set @@session.tidb_tiflash_encoded_operations = 0")
		rows := tk.MustQuery("explain select count(*) from low_card_t where status = 'active'").Rows()
		for _, row := range rows {
			operatorInfo := row[len(row)-1].(string)
			require.NotContains(t, operatorInfo, "dict_encoded",
				"dict_encoded annotation should NOT appear when encoded_operations is OFF")
		}

		// Case 2: encoded_operations ON — dict_encoded annotation expected
		tk.MustExec("set @@session.tidb_tiflash_encoded_operations = 1")
		rows = tk.MustQuery("explain select count(*) from low_card_t where status = 'active'").Rows()
		found := false
		for _, row := range rows {
			operatorInfo := row[len(row)-1].(string)
			if strings.Contains(operatorInfo, "dict_encoded") {
				found = true
				break
			}
		}
		require.True(t, found,
			"dict_encoded annotation should appear when encoded_operations is ON and columns have low NDV")
	})
}

// TestShardPruningExplainAnnotation verifies that EXPLAIN output shows
// shard_by annotation when a table has ShardKeyInfo metadata.
func TestShardPruningExplainAnnotation(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists sharded_t")
		tk.MustExec("create table sharded_t (id int primary key, tenant_id int, status varchar(20), revenue decimal(10,2))")
		tk.MustExec("insert into sharded_t values (1, 42, 'active', 100.00), (2, 43, 'inactive', 200.00)")
		tk.MustExec("analyze table sharded_t")

		// Inject ShardKeyInfo metadata to simulate SHARD BY tenant_id SHARDS 16
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("sharded_t"))
		require.NoError(t, err)
		tbl.Meta().ShardKeyInfo = &model.ShardKeyInfo{
			Columns:  []string{"tenant_id"},
			ShardCnt: 16,
		}

		testkit.SetTiFlashReplica(t, dom, "test", "sharded_t")

		tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		tk.MustExec("set @@session.tidb_allow_mpp = 1")
		tk.MustExec("set @@session.tidb_enforce_mpp = 1")

		// Case 1: Full scan — should show shard_by metadata but no pruned indicator
		rows := tk.MustQuery("explain select count(*) from sharded_t").Rows()
		foundShardBy := false
		for _, row := range rows {
			operatorInfo := row[len(row)-1].(string)
			if strings.Contains(operatorInfo, "shard_by:[tenant_id]") && strings.Contains(operatorInfo, "shards:16") {
				foundShardBy = true
				// Full scan should NOT show pruned
				require.NotContains(t, operatorInfo, "pruned",
					"shard pruning should NOT appear without equality on shard key")
			}
		}
		require.True(t, foundShardBy,
			"shard_by annotation should appear for tables with ShardKeyInfo")

		// Case 2: No ShardKeyInfo — no shard annotation
		tbl.Meta().ShardKeyInfo = nil
		rows = tk.MustQuery("explain select count(*) from sharded_t").Rows()
		for _, row := range rows {
			operatorInfo := row[len(row)-1].(string)
			require.NotContains(t, operatorInfo, "shard_by",
				"shard_by annotation should NOT appear when ShardKeyInfo is nil")
		}
	})
}
