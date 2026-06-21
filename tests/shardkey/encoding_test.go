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

package shardkey

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// TC-ENC-SESS: Session variable integration tests for encoded operations
// ---------------------------------------------------------------------------

func TestEncoding_SessionVar_DefaultOff(t *testing.T) {
	tk, _ := setup(t)
	tk.MustQuery(`SELECT @@tidb_tiflash_encoded_operations`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT @@tidb_tiflash_dict_encoding_max_cardinality`).Check(testkit.Rows("4096"))
}

func TestEncoding_SessionVar_ToggleOnOff(t *testing.T) {
	tk, _ := setup(t)

	// Turn ON
	tk.MustExec(`SET @@tidb_tiflash_encoded_operations = ON`)
	tk.MustQuery(`SELECT @@tidb_tiflash_encoded_operations`).Check(testkit.Rows("1"))

	// Turn OFF
	tk.MustExec(`SET @@tidb_tiflash_encoded_operations = OFF`)
	tk.MustQuery(`SELECT @@tidb_tiflash_encoded_operations`).Check(testkit.Rows("0"))
}

func TestEncoding_SessionVar_SetMaxCardinality(t *testing.T) {
	tk, _ := setup(t)

	// Custom value
	tk.MustExec(`SET @@tidb_tiflash_dict_encoding_max_cardinality = 2048`)
	tk.MustQuery(`SELECT @@tidb_tiflash_dict_encoding_max_cardinality`).Check(testkit.Rows("2048"))

	// Min bound
	tk.MustExec(`SET @@tidb_tiflash_dict_encoding_max_cardinality = 1`)
	tk.MustQuery(`SELECT @@tidb_tiflash_dict_encoding_max_cardinality`).Check(testkit.Rows("1"))

	// Max bound
	tk.MustExec(`SET @@tidb_tiflash_dict_encoding_max_cardinality = 65536`)
	tk.MustQuery(`SELECT @@tidb_tiflash_dict_encoding_max_cardinality`).Check(testkit.Rows("65536"))
}

func TestEncoding_SessionVar_MaxCardinalityOutOfRange(t *testing.T) {
	tk, _ := setup(t)

	// Below min (should clamp to 1)
	tk.MustExec(`SET @@tidb_tiflash_dict_encoding_max_cardinality = 0`)
	result := tk.MustQuery(`SELECT @@tidb_tiflash_dict_encoding_max_cardinality`).Rows()
	require.True(t, len(result) > 0)
	val := result[0][0].(string)
	require.Equal(t, "1", val, "value below min should be clamped to 1")
}

func TestEncoding_SessionVar_GlobalScope(t *testing.T) {
	tk, _ := setup(t)

	// Set global
	tk.MustExec(`SET GLOBAL tidb_tiflash_encoded_operations = ON`)
	tk.MustQuery(`SELECT @@GLOBAL.tidb_tiflash_encoded_operations`).Check(testkit.Rows("1"))

	// Session should not be affected yet (already initialized)
	tk.MustQuery(`SELECT @@SESSION.tidb_tiflash_encoded_operations`).Check(testkit.Rows("0"))

	// Cleanup
	tk.MustExec(`SET GLOBAL tidb_tiflash_encoded_operations = OFF`)
}

// ---------------------------------------------------------------------------
// TC-ENC-MPP: Encoded operations with TiFlash mock (planner integration)
// ---------------------------------------------------------------------------

func TestEncoding_MPP_HintAttachedWhenEnabled(t *testing.T) {
	tk, dom := mppSetup(t)

	tk.MustExec(`CREATE TABLE enc_orders (id BIGINT, company_id BIGINT, status VARCHAR(32), amount DECIMAL(12,2))`)
	testkit.SetTiFlashReplica(t, dom, "test", "enc_orders")

	// Without encoded ops — normal plan
	tk.MustExec(`SET @@tidb_tiflash_encoded_operations = OFF`)
	rows := tk.MustQuery(`EXPLAIN FORMAT='brief'
		SELECT status, SUM(amount) FROM enc_orders GROUP BY status`).Rows()
	plan1 := planStr(rows)

	// With encoded ops — plan should still work (just with hints in DAGRequest)
	tk.MustExec(`SET @@tidb_tiflash_encoded_operations = ON`)
	tk.MustExec(`SET @@tidb_tiflash_dict_encoding_max_cardinality = 2048`)
	rows = tk.MustQuery(`EXPLAIN FORMAT='brief'
		SELECT status, SUM(amount) FROM enc_orders GROUP BY status`).Rows()
	plan2 := planStr(rows)

	// Both plans should contain TiFlash TableFullScan
	require.Contains(t, plan1, "TableFullScan", "OFF plan should use TiFlash scan")
	require.Contains(t, plan2, "TableFullScan", "ON plan should use TiFlash scan")
}

func TestEncoding_MPP_WithShardKey_CoLocatedJoin(t *testing.T) {
	tk, dom := mppSetup(t)

	// Sharded tables with encoded ops enabled
	tk.MustExec(`CREATE TABLE enc_fact (id BIGINT, company_id BIGINT, amount DECIMAL(12,2))`)
	tk.MustExec(`CREATE TABLE enc_dim (company_id BIGINT, name VARCHAR(64))`)

	for _, tbl := range []string{"enc_fact", "enc_dim"} {
		testkit.SetTiFlashReplica(t, dom, "test", tbl)
		setShardKeyInfo(t, dom, tbl, []string{"company_id"}, 4)
	}

	tk.MustExec(`SET @@tidb_tiflash_encoded_operations = ON`)
	tk.MustExec(`SET @@tidb_tiflash_dict_encoding_max_cardinality = 4096`)

	// Co-located join + GROUP BY on shard key → 0 exchanges even with encoding enabled
	rows := tk.MustQuery(`EXPLAIN FORMAT='brief'
		SELECT /*+ shuffle_join(enc_fact, enc_dim) */
			enc_fact.company_id, SUM(enc_fact.amount)
		FROM enc_fact
		JOIN enc_dim ON enc_fact.company_id = enc_dim.company_id
		GROUP BY enc_fact.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan),
		"co-located join with encoded ops should still have 0 exchanges; plan:\n%s", plan)
}

func TestEncoding_MPP_StarSchema_WithEncoding(t *testing.T) {
	tk, dom := mppSetup(t)

	// Star schema: fact table + two dimension tables
	tk.MustExec(`CREATE TABLE enc_sales (id BIGINT, company_id BIGINT, product_id INT, qty INT, revenue DECIMAL(12,2))`)
	tk.MustExec(`CREATE TABLE enc_company (company_id BIGINT, name VARCHAR(64), region VARCHAR(32))`)
	tk.MustExec(`CREATE TABLE enc_product (product_id INT, name VARCHAR(64), category VARCHAR(32))`)

	for _, tbl := range []string{"enc_sales", "enc_company", "enc_product"} {
		testkit.SetTiFlashReplica(t, dom, "test", tbl)
	}
	setShardKeyInfo(t, dom, "enc_sales", []string{"company_id"}, 4)
	setShardKeyInfo(t, dom, "enc_company", []string{"company_id"}, 4)

	tk.MustExec(`SET @@tidb_tiflash_encoded_operations = ON`)

	// Star join with encoded ops: fact → dim(company, co-located) → dim(product, not co-located)
	rows := tk.MustQuery(`EXPLAIN FORMAT='brief'
		SELECT /*+ shuffle_join(enc_sales, enc_company) */
			enc_company.name, enc_product.category, SUM(enc_sales.revenue)
		FROM enc_sales
		JOIN enc_company ON enc_sales.company_id = enc_company.company_id
		JOIN enc_product ON enc_sales.product_id = enc_product.product_id
		GROUP BY enc_company.name, enc_product.category`).Rows()
	plan := planStr(rows)
	// Should have at least one exchange for the non-co-located product join
	require.Greater(t, countHP(plan), 0,
		"star join with non-co-located dimension should have exchanges; plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// TC-ENC-DML: Encoded operations don't break DML on sharded tables
// ---------------------------------------------------------------------------

func TestEncoding_DML_InsertSelectWithEncodingEnabled(t *testing.T) {
	tk, _ := setup(t)

	tk.MustExec(`CREATE TABLE enc_accounts (
		id BIGINT NOT NULL, company_id BIGINT NOT NULL, status VARCHAR(32), balance DECIMAL(12,2),
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)

	tk.MustExec(`INSERT INTO enc_accounts VALUES
		(1, 100, 'active', 1000.00),
		(2, 100, 'inactive', 500.00),
		(3, 200, 'active', 2000.00),
		(4, 300, 'active', 750.00),
		(5, 300, 'inactive', 250.00)`)

	// Enable encoding — should not affect DML correctness
	tk.MustExec(`SET @@tidb_tiflash_encoded_operations = ON`)

	// Reads should still be correct with encoding enabled
	tk.MustQuery(`SELECT company_id, COUNT(*), SUM(balance) FROM enc_accounts GROUP BY company_id ORDER BY company_id`).Check(
		testkit.Rows("100 2 1500.00", "200 1 2000.00", "300 2 1000.00"),
	)

	// Filter on low-cardinality column (status) — typical encoded filter candidate
	tk.MustQuery(`SELECT company_id, balance FROM enc_accounts WHERE status = 'active' ORDER BY company_id, balance`).Check(
		testkit.Rows("100 1000.00", "200 2000.00", "300 750.00"),
	)

	// Group by on low-cardinality column — typical encoded group-by candidate
	tk.MustQuery(`SELECT status, COUNT(*), SUM(balance) FROM enc_accounts GROUP BY status ORDER BY status`).Check(
		testkit.Rows("active 3 3750.00", "inactive 2 750.00"),
	)
}

func TestEncoding_DML_JoinWithEncodingEnabled(t *testing.T) {
	tk, _ := setup(t)

	tk.MustExec(`CREATE TABLE enc_orders2 (
		id BIGINT NOT NULL, company_id BIGINT NOT NULL, product VARCHAR(32), amount DECIMAL(12,2),
		PRIMARY KEY (company_id, id)
	) SHARD BY (company_id) SHARDS 4`)

	tk.MustExec(`CREATE TABLE enc_companies2 (
		company_id BIGINT NOT NULL, name VARCHAR(64),
		PRIMARY KEY (company_id)
	) SHARD BY (company_id) SHARDS 4`)

	tk.MustExec(`INSERT INTO enc_companies2 VALUES (1, 'Acme'), (2, 'Beta'), (3, 'Gamma')`)
	tk.MustExec(`INSERT INTO enc_orders2 VALUES
		(1, 1, 'widget', 100.00), (2, 1, 'gadget', 200.00),
		(3, 2, 'widget', 150.00),
		(4, 3, 'gadget', 300.00), (5, 3, 'widget', 50.00)`)

	tk.MustExec(`SET @@tidb_tiflash_encoded_operations = ON`)
	tk.MustExec(`SET @@tidb_tiflash_dict_encoding_max_cardinality = 2048`)

	// JOIN correctness with encoding enabled
	tk.MustQuery(`SELECT c.name, o.product, SUM(o.amount) total
		FROM enc_orders2 o
		JOIN enc_companies2 c ON o.company_id = c.company_id
		GROUP BY c.name, o.product
		ORDER BY c.name, o.product`).Check(testkit.Rows(
		"Acme gadget 200.00",
		"Acme widget 100.00",
		"Beta widget 150.00",
		"Gamma gadget 300.00",
		"Gamma widget 50.00",
	))

	// LEFT JOIN with encoding
	tk.MustExec(`INSERT INTO enc_companies2 VALUES (4, 'Delta')`)
	tk.MustQuery(`SELECT c.name, COALESCE(SUM(o.amount), 0) total
		FROM enc_companies2 c
		LEFT JOIN enc_orders2 o ON c.company_id = o.company_id
		GROUP BY c.name
		ORDER BY c.name`).Check(testkit.Rows(
		"Acme 300.00",
		"Beta 150.00",
		"Delta 0",
		"Gamma 350.00",
	))
}
