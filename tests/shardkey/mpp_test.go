package shardkey

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// setShardKeyInfo injects ShardKeyInfo into an existing table's metadata.
// This simulates the effect of DDL-created shard key for MPP plan tests that
// create tables without the full SHARD_KEY DDL (to avoid testkit DDL overhead).
func setShardKeyInfo(t *testing.T, dom *domain.Domain, tblName string, cols []string, cnt int) {
	t.Helper()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(tblName))
	require.NoError(t, err)
	tbl.Meta().ShardKeyInfo = &model.ShardKeyInfo{Columns: cols, ShardCnt: cnt}
}

// mppSetup sets up a test kit with TiFlash read isolation and MPP enabled.
func mppSetup(t *testing.T) (*testkit.TestKit, *domain.Domain) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@session.tidb_allow_mpp = 1")
	dom := domain.GetDomain(tk.Session())
	return tk, dom
}

func planStr(rows [][]any) string {
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		parts = append(parts, fmt.Sprintf("%v", r))
	}
	return strings.Join(parts, "\n")
}

func countHP(plan string) int {
	return strings.Count(plan, "HashPartition")
}

// ---------------------------------------------------------------------------
// TC-MPP-SST-01: Same shard key + same shard count → no HashPartition exchange
// ---------------------------------------------------------------------------

func TestMPP_SST_BothCoLocated_NoExchange(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT, company_id BIGINT)`)
	tk.MustExec(`CREATE TABLE customers_sharded (id BIGINT, company_id BIGINT, name VARCHAR(128))`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	testkit.SetTiFlashReplica(t, dom, "test", "customers_sharded")

	setShardKeyInfo(t, dom, "orders_sharded", []string{"company_id"}, 4)
	setShardKeyInfo(t, dom, "customers_sharded", []string{"company_id"}, 4)

	rows := tk.MustQuery(`EXPLAIN FORMAT='brief' SELECT /*+ shuffle_join(orders_sharded, customers_sharded) */ COUNT(*) FROM orders_sharded JOIN customers_sharded ON orders_sharded.company_id = customers_sharded.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan), "co-located join must not need HashPartition exchange; plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// TC-MPP-SST-02: JOIN + GROUP BY on shard key → 0 exchanges
// ---------------------------------------------------------------------------

func TestMPP_SST_JoinGroupBy_NoExchange(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT, company_id BIGINT, amount DECIMAL(12,2))`)
	tk.MustExec(`CREATE TABLE customers_sharded (id BIGINT, company_id BIGINT, name VARCHAR(128))`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	testkit.SetTiFlashReplica(t, dom, "test", "customers_sharded")

	setShardKeyInfo(t, dom, "orders_sharded", []string{"company_id"}, 4)
	setShardKeyInfo(t, dom, "customers_sharded", []string{"company_id"}, 4)

	rows := tk.MustQuery(`EXPLAIN FORMAT='brief' SELECT /*+ shuffle_join(orders_sharded, customers_sharded) */ orders_sharded.company_id, SUM(orders_sharded.amount), COUNT(customers_sharded.id) FROM orders_sharded JOIN customers_sharded ON orders_sharded.company_id = customers_sharded.company_id GROUP BY orders_sharded.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan), "co-located join+groupby must have 0 HashPartition exchanges; plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// TC-MPP-SST-03: Left join — co-location removes exchange
// ---------------------------------------------------------------------------

func TestMPP_SST_LeftJoin_NoExchange(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT, company_id BIGINT)`)
	tk.MustExec(`CREATE TABLE customers_sharded (id BIGINT, company_id BIGINT, name VARCHAR(128))`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	testkit.SetTiFlashReplica(t, dom, "test", "customers_sharded")

	setShardKeyInfo(t, dom, "orders_sharded", []string{"company_id"}, 4)
	setShardKeyInfo(t, dom, "customers_sharded", []string{"company_id"}, 4)

	rows := tk.MustQuery(`EXPLAIN FORMAT='brief' SELECT /*+ shuffle_join(orders_sharded, customers_sharded) */ orders_sharded.id, customers_sharded.name FROM orders_sharded LEFT JOIN customers_sharded ON orders_sharded.company_id = customers_sharded.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan), "co-located left join must not need HashPartition exchange; plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// TC-MPP-SST-05: Different shard counts → must use HashPartition exchange
// ---------------------------------------------------------------------------

func TestMPP_SST_MismatchedShardCount_UsesExchange(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT, company_id BIGINT)`)
	tk.MustExec(`CREATE TABLE orders_sharded_8 (id BIGINT, company_id BIGINT)`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded_8")

	setShardKeyInfo(t, dom, "orders_sharded", []string{"company_id"}, 4)
	setShardKeyInfo(t, dom, "orders_sharded_8", []string{"company_id"}, 8) // different count

	rows := tk.MustQuery(`EXPLAIN FORMAT='brief' SELECT /*+ shuffle_join(orders_sharded, orders_sharded_8) */ COUNT(*) FROM orders_sharded JOIN orders_sharded_8 ON orders_sharded.company_id = orders_sharded_8.company_id`).Rows()
	plan := planStr(rows)
	require.Greater(t, countHP(plan), 0, "mismatched shard count must require HashPartition exchange; plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// TC-MPP-SST-06: Same count, different shard column → must use exchange
// ---------------------------------------------------------------------------

func TestMPP_SST_MismatchedShardColumn_UsesExchange(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT, company_id BIGINT)`)
	tk.MustExec(`CREATE TABLE events_sharded_user (id BIGINT, company_id BIGINT, user_id BIGINT)`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	testkit.SetTiFlashReplica(t, dom, "test", "events_sharded_user")

	setShardKeyInfo(t, dom, "orders_sharded", []string{"company_id"}, 4)
	setShardKeyInfo(t, dom, "events_sharded_user", []string{"user_id"}, 4) // different shard column

	rows := tk.MustQuery(`EXPLAIN FORMAT='brief' SELECT /*+ shuffle_join(orders_sharded, events_sharded_user) */ COUNT(*) FROM orders_sharded JOIN events_sharded_user ON orders_sharded.company_id = events_sharded_user.company_id`).Rows()
	plan := planStr(rows)
	require.Greater(t, countHP(plan), 0, "mismatched shard column must require HashPartition exchange; plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// TC-MPP-SST-ST-01: Sharded joined with plain table → exchange required
// ---------------------------------------------------------------------------

func TestMPP_SST_JoinPlain_UsesExchange(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT, company_id BIGINT)`)
	tk.MustExec(`CREATE TABLE orders (id BIGINT, company_id BIGINT)`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	testkit.SetTiFlashReplica(t, dom, "test", "orders")

	setShardKeyInfo(t, dom, "orders_sharded", []string{"company_id"}, 4)
	// orders has no ShardKeyInfo

	rows := tk.MustQuery(`EXPLAIN FORMAT='brief' SELECT /*+ shuffle_join(orders_sharded, orders) */ COUNT(*) FROM orders_sharded JOIN orders ON orders_sharded.id = orders.id`).Rows()
	plan := planStr(rows)
	require.Greater(t, countHP(plan), 0, "sharded + plain join must require HashPartition exchange; plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// TC-MPP-MULTI-01: Three-way join on same shard key → 0 exchanges
// ---------------------------------------------------------------------------

func TestMPP_SST_ThreeWayJoin_NoExchange(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT, company_id BIGINT)`)
	tk.MustExec(`CREATE TABLE customers_sharded (id BIGINT, company_id BIGINT)`)
	tk.MustExec(`CREATE TABLE events_sharded (id BIGINT, company_id BIGINT)`)

	for _, tbl := range []string{"orders_sharded", "customers_sharded", "events_sharded"} {
		testkit.SetTiFlashReplica(t, dom, "test", tbl)
		setShardKeyInfo(t, dom, tbl, []string{"company_id"}, 4)
	}

	rows := tk.MustQuery(`EXPLAIN FORMAT='brief' SELECT /*+ shuffle_join(orders_sharded, customers_sharded, events_sharded) */ COUNT(*) FROM orders_sharded JOIN customers_sharded ON orders_sharded.company_id = customers_sharded.company_id JOIN events_sharded ON events_sharded.company_id = customers_sharded.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan), "three-way co-located join must have 0 HashPartition exchanges; plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// TC-EDGE-04: Join on non-shard column → exchange required
// ---------------------------------------------------------------------------

func TestMPP_SST_JoinNonShardColumn_UsesExchange(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT, company_id BIGINT)`)
	tk.MustExec(`CREATE TABLE customers_sharded (id BIGINT, company_id BIGINT)`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	testkit.SetTiFlashReplica(t, dom, "test", "customers_sharded")

	setShardKeyInfo(t, dom, "orders_sharded", []string{"company_id"}, 4)
	setShardKeyInfo(t, dom, "customers_sharded", []string{"company_id"}, 4)

	// Join on id (not company_id) → no co-location, exchange required
	rows := tk.MustQuery(`EXPLAIN FORMAT='brief' SELECT /*+ shuffle_join(orders_sharded, customers_sharded) */ COUNT(*) FROM orders_sharded JOIN customers_sharded ON orders_sharded.id = customers_sharded.id`).Rows()
	plan := planStr(rows)
	require.Greater(t, countHP(plan), 0, "join on non-shard column must require HashPartition exchange; plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// TC-EDGE-05: Self-join on shard key → co-located (no exchange)
// ---------------------------------------------------------------------------

func TestMPP_SST_SelfJoin_NoExchange(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE orders_sharded (id BIGINT, company_id BIGINT, amount DECIMAL(12,2))`)

	testkit.SetTiFlashReplica(t, dom, "test", "orders_sharded")
	setShardKeyInfo(t, dom, "orders_sharded", []string{"company_id"}, 4)

	rows := tk.MustQuery(`EXPLAIN FORMAT='brief' SELECT /*+ shuffle_join(a, b) */ a.id, b.amount FROM orders_sharded a JOIN orders_sharded b ON a.company_id = b.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan), "self-join on shard key must be co-located; plan:\n%s", plan)
}

// ---------------------------------------------------------------------------
// TC-MPP-JOIN: Multi-table joins with co-located shard keys
// ---------------------------------------------------------------------------

func TestMPP_SST_FourWayJoin_CoLocated(t *testing.T) {
	tk, dom := mppSetup(t)
	tables := []string{"fact_events", "dim_company", "dim_campaign", "dim_account"}
	for _, tbl := range tables {
		tk.MustExec(fmt.Sprintf(`CREATE TABLE %s (id BIGINT, company_id BIGINT, val VARCHAR(64))`, tbl))
		testkit.SetTiFlashReplica(t, dom, "test", tbl)
		setShardKeyInfo(t, dom, tbl, []string{"company_id"}, 4)
	}

	// All four tables co-sharded on company_id → 0 HashPartition exchanges
	rows := tk.MustQuery(`EXPLAIN FORMAT='brief'
		SELECT /*+ shuffle_join(fact_events, dim_company, dim_campaign, dim_account) */
			COUNT(*)
		FROM fact_events f
		JOIN dim_company c ON f.company_id = c.company_id
		JOIN dim_campaign ca ON f.company_id = ca.company_id
		JOIN dim_account a ON f.company_id = a.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan),
		"four-way co-located join must have 0 HashPartition exchanges; plan:\n%s", plan)
}

func TestMPP_SST_LeftJoinThreeTable_CoLocated(t *testing.T) {
	tk, dom := mppSetup(t)
	for _, tbl := range []string{"orders", "customers", "shipments"} {
		tk.MustExec(fmt.Sprintf(`CREATE TABLE %s (id BIGINT, company_id BIGINT, val VARCHAR(64))`, tbl))
		testkit.SetTiFlashReplica(t, dom, "test", tbl)
		setShardKeyInfo(t, dom, tbl, []string{"company_id"}, 4)
	}

	// LEFT JOIN chain: orders → customers → shipments, all on company_id
	rows := tk.MustQuery(`EXPLAIN FORMAT='brief'
		SELECT /*+ shuffle_join(orders, customers, shipments) */
			orders.id, customers.val, shipments.val
		FROM orders
		LEFT JOIN customers ON orders.company_id = customers.company_id
		LEFT JOIN shipments ON orders.company_id = shipments.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan),
		"three-way LEFT JOIN co-located must have 0 exchanges; plan:\n%s", plan)
}

func TestMPP_SST_MixedJoinTypes_CoLocated(t *testing.T) {
	tk, dom := mppSetup(t)
	for _, tbl := range []string{"t_a", "t_b", "t_c"} {
		tk.MustExec(fmt.Sprintf(`CREATE TABLE %s (id BIGINT, company_id BIGINT, val INT)`, tbl))
		testkit.SetTiFlashReplica(t, dom, "test", tbl)
		setShardKeyInfo(t, dom, tbl, []string{"company_id"}, 4)
	}

	// INNER JOIN + LEFT JOIN in same query, all on shard key
	rows := tk.MustQuery(`EXPLAIN FORMAT='brief'
		SELECT /*+ shuffle_join(t_a, t_b, t_c) */
			t_a.id, t_b.val, t_c.val
		FROM t_a
		JOIN t_b ON t_a.company_id = t_b.company_id
		LEFT JOIN t_c ON t_a.company_id = t_c.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan),
		"mixed INNER+LEFT JOIN on shard key must be co-located; plan:\n%s", plan)
}

func TestMPP_SST_PartialCoLocation_OneExchange(t *testing.T) {
	tk, dom := mppSetup(t)
	// Two tables co-sharded, one not
	tk.MustExec(`CREATE TABLE t_co1 (id BIGINT, company_id BIGINT)`)
	tk.MustExec(`CREATE TABLE t_co2 (id BIGINT, company_id BIGINT)`)
	tk.MustExec(`CREATE TABLE t_noco (id BIGINT, company_id BIGINT)`)

	for _, tbl := range []string{"t_co1", "t_co2", "t_noco"} {
		testkit.SetTiFlashReplica(t, dom, "test", tbl)
	}
	setShardKeyInfo(t, dom, "t_co1", []string{"company_id"}, 4)
	setShardKeyInfo(t, dom, "t_co2", []string{"company_id"}, 4)
	// t_noco has no shard key

	// t_co1 JOIN t_co2 is co-located (0 exchanges between them)
	// but t_noco join requires at least 1 exchange
	rows := tk.MustQuery(`EXPLAIN FORMAT='brief'
		SELECT /*+ shuffle_join(t_co1, t_co2, t_noco) */
			COUNT(*)
		FROM t_co1
		JOIN t_co2 ON t_co1.company_id = t_co2.company_id
		JOIN t_noco ON t_co1.company_id = t_noco.company_id`).Rows()
	plan := planStr(rows)
	require.Greater(t, countHP(plan), 0,
		"joining co-located + non-sharded table needs exchange; plan:\n%s", plan)
}

func TestMPP_SST_JoinWithGroupByShardKey_CoLocated(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE fact (id BIGINT, company_id BIGINT, amount DECIMAL(12,2))`)
	tk.MustExec(`CREATE TABLE dim (company_id BIGINT, name VARCHAR(64))`)

	for _, tbl := range []string{"fact", "dim"} {
		testkit.SetTiFlashReplica(t, dom, "test", tbl)
		setShardKeyInfo(t, dom, tbl, []string{"company_id"}, 8)
	}

	// JOIN + GROUP BY on shard key — join and aggregation are both co-located
	rows := tk.MustQuery(`EXPLAIN FORMAT='brief'
		SELECT /*+ shuffle_join(fact, dim) */
			fact.company_id, SUM(fact.amount), COUNT(*)
		FROM fact
		JOIN dim ON fact.company_id = dim.company_id
		GROUP BY fact.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan),
		"co-located join+group-by-on-shard-key must have 0 exchanges; plan:\n%s", plan)
}

func TestMPP_SST_LeftJoinGroupByShardKey_CoLocated(t *testing.T) {
	tk, dom := mppSetup(t)
	tk.MustExec(`CREATE TABLE companies (company_id BIGINT, name VARCHAR(64))`)
	tk.MustExec(`CREATE TABLE orders (id BIGINT, company_id BIGINT, amount DECIMAL(12,2))`)

	for _, tbl := range []string{"companies", "orders"} {
		testkit.SetTiFlashReplica(t, dom, "test", tbl)
		setShardKeyInfo(t, dom, tbl, []string{"company_id"}, 4)
	}

	// LEFT JOIN + GROUP BY on shard key — both join and aggregation are co-located
	rows := tk.MustQuery(`EXPLAIN FORMAT='brief'
		SELECT /*+ shuffle_join(companies, orders) */
			companies.company_id, COUNT(orders.id), COALESCE(SUM(orders.amount), 0)
		FROM companies
		LEFT JOIN orders ON companies.company_id = orders.company_id
		GROUP BY companies.company_id`).Rows()
	plan := planStr(rows)
	require.Equal(t, 0, countHP(plan),
		"co-located LEFT JOIN+GROUP BY on shard key must have 0 exchanges; plan:\n%s", plan)
}
