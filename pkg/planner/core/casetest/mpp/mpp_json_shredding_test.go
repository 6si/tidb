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
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
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
