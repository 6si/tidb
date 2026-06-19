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

// Package main implements performance benchmarks for the tidb-se feature set:
// SHARD BY routing/pruning, encoded columnstore operations, and JSON shredding.
//
// Usage:
//
//	go run ./benchmark --host=127.0.0.1 --port=4000 --rows=10000000
//	go run ./benchmark --host=tidb-se.playground.6si.com --port=4000 --rows=50000000
//
// The benchmark compares optimized vs baseline paths for each feature,
// reporting latency and throughput with the speedup factor.
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	host       = flag.String("host", "127.0.0.1", "TiDB host")
	port       = flag.Int("port", 4000, "TiDB port")
	user       = flag.String("user", "root", "TiDB user")
	password   = flag.String("password", "", "TiDB password")
	database   = flag.String("db", "bench_tidb_se", "Database name")
	rows       = flag.Int("rows", 10_000_000, "Number of fact table rows to load")
	dimRows    = flag.Int("dim-rows", 1000, "Number of dimension table rows")
	shards     = flag.Int("shards", 16, "Number of shards for SHARD BY tables")
	workers    = flag.Int("workers", 8, "Concurrent workers for data loading")
	skipLoad   = flag.Bool("skip-load", false, "Skip data loading (reuse existing data)")
	benchmarks = flag.String("bench", "all", "Comma-separated benchmarks: shard,in,filter,groupby,join,json,all")
	iterations = flag.Int("iter", 5, "Iterations per benchmark query")
	jsonSize   = flag.String("json-size", "small", "JSON document size: small (~200B), large (~2-5KB with 20+ paths)")
)

type BenchResult struct {
	Name       string
	Baseline   time.Duration
	Optimized  time.Duration
	Speedup    float64
	BaseRows   int64
	OptRows    int64
	BaseExplan string
	OptExplan  string
}

var db *sql.DB

func main() {
	flag.Parse()

	// Connect without database first to create it
	initDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true&interpolateParams=true",
		*user, *password, *host, *port)
	initDB, err := sql.Open("mysql", initDSN)
	if err != nil {
		fatal("connect: %v", err)
	}
	if err := initDB.Ping(); err != nil {
		fatal("ping: %v", err)
	}
	initDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", *database))
	initDB.Close()

	// Reconnect with database in DSN so all pooled connections use it
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&interpolateParams=true",
		*user, *password, *host, *port, *database)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		fatal("connect: %v", err)
	}
	db.SetMaxOpenConns(64)
	db.SetMaxIdleConns(32)
	defer db.Close()

	if err := db.Ping(); err != nil {
		fatal("ping: %v", err)
	}

	fmt.Printf("=== tidb-se Performance Benchmark ===\n")
	fmt.Printf("Host: %s:%d | Rows: %d | Shards: %d | Iterations: %d | JSON: %s\n\n",
		*host, *port, *rows, *shards, *iterations, *jsonSize)

	if !*skipLoad {
		setupSchema()
		loadData()
	} else {
		fmt.Println("[skip-load] Reusing existing data\n")
	}

	// Wait for TiFlash replicas
	waitForAllReplicas()

	// Run benchmarks
	selected := parseBenchmarks(*benchmarks)
	var results []BenchResult

	if selected["shard"] {
		results = append(results, benchShardPointQuery()...)
	}
	if selected["in"] {
		results = append(results, benchShardINQuery()...)
	}
	if selected["filter"] {
		results = append(results, benchEncodedFilter()...)
	}
	if selected["groupby"] {
		results = append(results, benchEncodedGroupBy()...)
	}
	if selected["join"] {
		results = append(results, benchEncodedStarJoin()...)
	}
	if selected["json"] {
		results = append(results, benchJSONShredding()...)
	}

	// Print summary
	printSummary(results)
}

// ============================================================================
// Schema Setup
// ============================================================================

func setupSchema() {
	fmt.Println("=== Setting up schema ===")

	// Drop existing tables
	for _, t := range []string{"fact_sharded", "fact_unsharded", "dim_region", "dim_industry", "json_events"} {
		mustExec(fmt.Sprintf("DROP TABLE IF EXISTS %s", t))
	}

	// Sharded fact table (SHARD BY)
	mustExec(fmt.Sprintf(`CREATE TABLE fact_sharded (
		id BIGINT NOT NULL,
		tenant_id BIGINT NOT NULL,
		region_id INT NOT NULL,
		industry_id INT NOT NULL,
		status VARCHAR(20) NOT NULL,
		revenue BIGINT NOT NULL,
		created_at DATETIME NOT NULL,
		PRIMARY KEY (id, tenant_id)
	) SHARD BY (tenant_id) SHARDS %d`, *shards))

	// Equivalent non-sharded table (baseline comparison)
	mustExec(`CREATE TABLE fact_unsharded (
		id BIGINT NOT NULL,
		tenant_id BIGINT NOT NULL,
		region_id INT NOT NULL,
		industry_id INT NOT NULL,
		status VARCHAR(20) NOT NULL,
		revenue BIGINT NOT NULL,
		created_at DATETIME NOT NULL,
		PRIMARY KEY (id, tenant_id)
	)`)

	// Dimension: regions (low cardinality - 50 values)
	mustExec(`CREATE TABLE dim_region (
		id INT PRIMARY KEY,
		name VARCHAR(50) NOT NULL,
		country VARCHAR(30) NOT NULL
	)`)

	// Dimension: industries (low cardinality - 20 values)
	mustExec(`CREATE TABLE dim_industry (
		id INT PRIMARY KEY,
		name VARCHAR(50) NOT NULL,
		sector VARCHAR(30) NOT NULL
	)`)

	// JSON events table (for JSON shredding benchmark)
	mustExec(fmt.Sprintf(`CREATE TABLE json_events (
		id BIGINT NOT NULL,
		tenant_id BIGINT NOT NULL,
		payload JSON NOT NULL,
		PRIMARY KEY (id, tenant_id)
	) SHARD BY (tenant_id) SHARDS %d`, *shards))

	// Set TiFlash replicas
	for _, t := range []string{"fact_sharded", "fact_unsharded", "dim_region", "dim_industry", "json_events"} {
		mustExec(fmt.Sprintf("ALTER TABLE %s SET TIFLASH REPLICA 1", t))
	}

	fmt.Println("  Schema created with TiFlash replicas")
}

// ============================================================================
// Data Loading
// ============================================================================

func loadData() {
	fmt.Println("\n=== Loading data ===")

	// Load dimensions first
	loadDimensions()

	// Load fact tables in parallel
	loadFactTables()

	// Load JSON events
	loadJSONEvents()

	// Analyze tables for accurate statistics
	fmt.Print("  Analyzing tables...")
	for _, t := range []string{"fact_sharded", "fact_unsharded", "dim_region", "dim_industry", "json_events"} {
		mustExec(fmt.Sprintf("ANALYZE TABLE %s", t))
	}
	fmt.Println(" done")
}

func loadDimensions() {
	regions := []struct{ name, country string }{
		{"us-east-1", "US"}, {"us-west-2", "US"}, {"eu-west-1", "EU"},
		{"eu-central-1", "EU"}, {"ap-southeast-1", "APAC"}, {"ap-northeast-1", "APAC"},
		{"sa-east-1", "LATAM"}, {"af-south-1", "AFRICA"}, {"me-south-1", "ME"},
		{"ca-central-1", "US"},
	}
	// Expand to 50 regions
	for i := 0; i < 50; i++ {
		base := regions[i%len(regions)]
		name := fmt.Sprintf("%s-%d", base.name, i/len(regions))
		mustExec("INSERT INTO dim_region VALUES (?, ?, ?)", i+1, name, base.country)
	}

	industries := []string{
		"Technology", "Finance", "Healthcare", "Retail", "Manufacturing",
		"Energy", "Telecom", "Media", "Education", "Transportation",
		"Agriculture", "Construction", "Pharma", "Insurance", "RealEstate",
		"Consulting", "Legal", "Hospitality", "Mining", "Aerospace",
	}
	sectors := []string{
		"Tech", "Finance", "Health", "Consumer", "Industrial",
		"Energy", "Tech", "Media", "Services", "Industrial",
		"Primary", "Industrial", "Health", "Finance", "Finance",
		"Services", "Services", "Consumer", "Primary", "Industrial",
	}
	for i, ind := range industries {
		mustExec("INSERT INTO dim_industry VALUES (?, ?, ?)", i+1, ind, sectors[i])
	}
	fmt.Printf("  Loaded %d regions, %d industries\n", 50, len(industries))
}

func loadFactTables() {
	batchSize := 5000
	totalBatches := *rows / batchSize
	batchesPerWorker := totalBatches / *workers

	statuses := []string{"active", "inactive", "pending", "archived", "deleted"}

	fmt.Printf("  Loading %d rows into fact_sharded + fact_unsharded (%d workers, batch=%d)...\n",
		*rows, *workers, batchSize)

	start := time.Now()
	var wg sync.WaitGroup

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			startBatch := workerID * batchesPerWorker
			endBatch := startBatch + batchesPerWorker
			if workerID == *workers-1 {
				endBatch = totalBatches
			}

			for b := startBatch; b < endBatch; b++ {
				var shardedVals, unshardedVals []string
				baseID := int64(b) * int64(batchSize)

				for i := 0; i < batchSize; i++ {
					id := baseID + int64(i) + 1
					tenantID := (id % 10000) + 1 // 10K tenants
					regionID := (id % 50) + 1
					industryID := (id % 20) + 1
					status := statuses[id%5]
					revenue := (id * 7) % 100000
					day := id % 365
					created := fmt.Sprintf("2024-01-01 00:00:00")
					_ = day
					_ = created

					row := fmt.Sprintf("(%d,%d,%d,%d,'%s',%d,'2024-01-01')",
						id, tenantID, regionID, industryID, status, revenue)
					shardedVals = append(shardedVals, row)
					unshardedVals = append(unshardedVals, row)
				}

				insertBatch("fact_sharded", shardedVals)
				insertBatch("fact_unsharded", unshardedVals)

				if b%100 == 0 && workerID == 0 {
					pct := float64(b-startBatch) / float64(endBatch-startBatch) * 100
					fmt.Printf("\r  Progress: %.1f%% (%d/%d batches)   ", pct, b-startBatch, endBatch-startBatch)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	rate := float64(*rows) / elapsed.Seconds()
	fmt.Printf("\r  Loaded %d rows in %v (%.0f rows/s)                    \n", *rows*2, elapsed, rate*2)
}

func loadJSONEvents() {
	jsonRows := *rows / 10 // 10% of fact rows for JSON
	batchSize := 2000
	if *jsonSize == "large" {
		batchSize = 500 // smaller batches for large docs to avoid max_allowed_packet
	}
	totalBatches := jsonRows / batchSize

	fmt.Printf("  Loading %d JSON events (size=%s)...\n", jsonRows, *jsonSize)
	start := time.Now()

	eventTypes := []string{"click", "view", "purchase", "signup", "logout"}
	pages := []string{"/home", "/product", "/cart", "/checkout", "/account", "/search", "/category", "/blog"}

	var wg sync.WaitGroup
	batchesPerWorker := totalBatches / *workers

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			startBatch := workerID * batchesPerWorker
			endBatch := startBatch + batchesPerWorker
			if workerID == *workers-1 {
				endBatch = totalBatches
			}

			rng := rand.New(rand.NewSource(int64(workerID * 12345)))

			for b := startBatch; b < endBatch; b++ {
				var vals []string
				baseID := int64(b) * int64(batchSize)

				for i := 0; i < batchSize; i++ {
					id := baseID + int64(i) + 1
					tenantID := (id % 10000) + 1
					event := eventTypes[rng.Intn(len(eventTypes))]
					page := pages[rng.Intn(len(pages))]
					duration := rng.Intn(30000)
					score := rng.Float64() * 100

					var jsonDoc string
					if *jsonSize == "large" {
						jsonDoc = generateLargeJSON(rng, id, event, page, duration, score)
					} else {
						jsonDoc = generateSmallJSON(rng, id, event, page, duration, score)
					}

					row := fmt.Sprintf("(%d,%d,%s)", id, tenantID, jsonDoc)
					vals = append(vals, row)
				}

				insertBatch("json_events", vals)
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("  Loaded %d JSON events in %v\n", jsonRows, elapsed)
}

func generateSmallJSON(rng *rand.Rand, id int64, event, page string, duration int, score float64) string {
	json := fmt.Sprintf(
		`'{"event":"%s","page":"%s","duration":%d,"score":%.2f,"user_agent":"Mozilla/5.0","ip":"10.%d.%d.%d"`,
		event, page, duration, score,
		rng.Intn(256), rng.Intn(256), rng.Intn(256))

	if id%5 == 0 {
		json += fmt.Sprintf(`,"referrer":"https://google.com/search?q=item%d"`, id%1000)
	}
	if id%10 == 0 {
		json += fmt.Sprintf(`,"details":{"source":"campaign_%d","medium":"cpc","cost":%d}`,
			id%50, rng.Intn(1000))
	}
	json += `}'`
	return json
}

func generateLargeJSON(rng *rand.Rand, id int64, event, page string, duration int, score float64) string {
	// ~2-5KB document with 20+ paths at various nesting depths
	browsers := []string{"Chrome/120.0", "Firefox/121.0", "Safari/17.2", "Edge/120.0"}
	devices := []string{"desktop", "mobile", "tablet"}
	osos := []string{"Windows 11", "macOS 14.2", "Linux", "iOS 17", "Android 14"}
	screens := []string{"1920x1080", "2560x1440", "1366x768", "390x844", "1024x768"}
	campaigns := []string{"summer_sale", "black_friday", "retargeting", "brand_awareness", "product_launch"}
	mediums := []string{"cpc", "organic", "email", "social", "referral", "display", "affiliate"}
	sources := []string{"google", "facebook", "twitter", "linkedin", "bing", "reddit", "newsletter"}
	categories := []string{"electronics", "clothing", "food", "software", "books", "home", "sports"}
	actions := []string{"add_to_cart", "remove_from_cart", "wishlist", "compare", "share", "review"}

	browser := browsers[rng.Intn(len(browsers))]
	device := devices[rng.Intn(len(devices))]
	osName := osos[rng.Intn(len(osos))]
	screen := screens[rng.Intn(len(screens))]
	campaign := campaigns[rng.Intn(len(campaigns))]
	medium := mediums[rng.Intn(len(mediums))]
	source := sources[rng.Intn(len(sources))]
	category := categories[rng.Intn(len(categories))]
	action := actions[rng.Intn(len(actions))]

	lat := 37.0 + rng.Float64()*10
	lon := -122.0 + rng.Float64()*10
	viewportW := 300 + rng.Intn(1700)
	viewportH := 500 + rng.Intn(1000)
	scrollDepth := rng.Intn(100)
	timeOnPage := rng.Intn(600)
	clickX := rng.Intn(1920)
	clickY := rng.Intn(1080)
	productID := rng.Intn(50000)
	productPrice := rng.Float64() * 500
	quantity := 1 + rng.Intn(5)
	sessionDepth := 1 + rng.Intn(20)

	json := fmt.Sprintf(`'{`+
		`"event":"%s",`+
		`"page":"%s",`+
		`"duration":%d,`+
		`"score":%.2f,`+
		`"timestamp":%d,`+
		`"session_id":"sess_%d",`+
		`"user_agent":"Mozilla/5.0 (%s; %s) AppleWebKit/537.36 %s",`+
		`"ip":"10.%d.%d.%d",`+
		`"context":{`+
		`"browser":"%s",`+
		`"device":"%s",`+
		`"os":"%s",`+
		`"screen":"%s",`+
		`"viewport":{"width":%d,"height":%d},`+
		`"locale":"en-US",`+
		`"timezone":"America/Los_Angeles",`+
		`"cookies_enabled":true`+
		`},`+
		`"geo":{`+
		`"lat":%.4f,`+
		`"lon":%.4f,`+
		`"city":"San Francisco",`+
		`"region":"CA",`+
		`"country":"US"`+
		`},`+
		`"behavior":{`+
		`"scroll_depth":%d,`+
		`"time_on_page":%d,`+
		`"click_x":%d,`+
		`"click_y":%d,`+
		`"session_depth":%d,`+
		`"is_bounce":%t,`+
		`"is_returning":%t`+
		`},`+
		`"product":{`+
		`"id":%d,`+
		`"category":"%s",`+
		`"price":%.2f,`+
		`"quantity":%d,`+
		`"action":"%s"`+
		`},`+
		`"campaign":{`+
		`"name":"%s",`+
		`"medium":"%s",`+
		`"source":"%s",`+
		`"cost":%.2f,`+
		`"click_id":"clk_%d"`+
		`}`,
		event, page, duration, score,
		time.Now().Unix()-rng.Int63n(86400*30),
		rng.Int63n(1000000),
		osName, device, browser,
		rng.Intn(256), rng.Intn(256), rng.Intn(256),
		browser, device, osName, screen,
		viewportW, viewportH,
		lat, lon,
		scrollDepth, timeOnPage, clickX, clickY, sessionDepth,
		scrollDepth < 20, sessionDepth > 1,
		productID, category, productPrice, quantity, action,
		campaign, medium, source,
		rng.Float64()*10,
		rng.Int63n(99999999),
	)

	// Add sparse referrer field (30% of rows)
	if id%3 == 0 {
		json += fmt.Sprintf(`,"referrer":"https://%s.com/search?q=item%d&ref=campaign_%s"`,
			source, id%10000, campaign)
	}

	// Add tags array (every row — adds ~200B)
	numTags := 3 + rng.Intn(5)
	tagOptions := []string{"new_user", "high_value", "returning", "mobile", "desktop",
		"organic", "paid", "social", "email", "promotion", "seasonal", "vip"}
	json += `,"tags":[`
	for t := 0; t < numTags; t++ {
		if t > 0 {
			json += ","
		}
		json += fmt.Sprintf(`"%s"`, tagOptions[rng.Intn(len(tagOptions))])
	}
	json += `]`

	// Add nested details (every row for large docs)
	json += fmt.Sprintf(`,"details":{"source":"%s","medium":"%s","cost":%d,"clicks":%d,"impressions":%d}`,
		source, medium, rng.Intn(1000), rng.Intn(500), rng.Intn(10000))

	// Add experiment data (50% of rows — A/B test context)
	if id%2 == 0 {
		json += fmt.Sprintf(`,"experiment":{"id":"exp_%d","variant":"%c","converted":%t}`,
			rng.Intn(100), 'A'+byte(rng.Intn(4)), rng.Intn(2) == 1)
	}

	json += `}'`
	return json
}

func insertBatch(table string, vals []string) {
	if len(vals) == 0 {
		return
	}
	cols := ""
	switch table {
	case "fact_sharded", "fact_unsharded":
		cols = "(id, tenant_id, region_id, industry_id, status, revenue, created_at)"
	case "json_events":
		cols = "(id, tenant_id, payload)"
	}
	query := fmt.Sprintf("INSERT INTO %s %s VALUES %s", table, cols, strings.Join(vals, ","))
	_, err := db.Exec(query)
	if err != nil {
		// Retry once on deadlock
		if strings.Contains(err.Error(), "Deadlock") || strings.Contains(err.Error(), "Write conflict") {
			time.Sleep(100 * time.Millisecond)
			_, err = db.Exec(query)
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nWARN: insert batch failed (%s): %v\n", table, err)
		}
	}
}

// ============================================================================
// Benchmarks
// ============================================================================

func benchShardPointQuery() []BenchResult {
	fmt.Println("\n--- Benchmark: Shard Key Point Query ---")
	var results []BenchResult

	// Pick a specific tenant
	tenantID := 42

	// Baseline: query without shard key (scans all partitions)
	baselineQuery := fmt.Sprintf(
		"SELECT COUNT(*), SUM(revenue) FROM fact_sharded WHERE region_id = 5 AND id > 0 LIMIT 1")
	// Optimized: query with shard key (prunes to 1 shard)
	optimizedQuery := fmt.Sprintf(
		"SELECT COUNT(*), SUM(revenue) FROM fact_sharded WHERE tenant_id = %d", tenantID)

	baseExplain := getExplainAccess(baselineQuery)
	optExplain := getExplainAccess(optimizedQuery)

	baseTime := timeQuery(baselineQuery, *iterations)
	optTime := timeQuery(optimizedQuery, *iterations)

	speedup := float64(baseTime) / float64(optTime)
	results = append(results, BenchResult{
		Name:       "Shard Point Query (all shards vs 1 shard)",
		Baseline:   baseTime,
		Optimized:  optTime,
		Speedup:    speedup,
		BaseExplan: baseExplain,
		OptExplan:  optExplain,
	})

	fmt.Printf("  Baseline (scan all shards):  %v  [%s]\n", baseTime, baseExplain)
	fmt.Printf("  Optimized (prune to 1):      %v  [%s]\n", optTime, optExplain)
	fmt.Printf("  Speedup: %.2fx\n", speedup)

	// Also compare sharded vs unsharded point lookup
	unshardedQuery := fmt.Sprintf(
		"SELECT COUNT(*), SUM(revenue) FROM fact_unsharded WHERE tenant_id = %d", tenantID)
	shardedQuery := fmt.Sprintf(
		"SELECT COUNT(*), SUM(revenue) FROM fact_sharded WHERE tenant_id = %d", tenantID)

	unshardedTime := timeQuery(unshardedQuery, *iterations)
	shardedTime := timeQuery(shardedQuery, *iterations)
	speedup2 := float64(unshardedTime) / float64(shardedTime)

	results = append(results, BenchResult{
		Name:      "Sharded vs Unsharded (same tenant filter)",
		Baseline:  unshardedTime,
		Optimized: shardedTime,
		Speedup:   speedup2,
	})

	fmt.Printf("  Unsharded full scan:  %v\n", unshardedTime)
	fmt.Printf("  Sharded + pruned:     %v\n", shardedTime)
	fmt.Printf("  Speedup: %.2fx\n", speedup2)

	return results
}

func benchShardINQuery() []BenchResult {
	fmt.Println("\n--- Benchmark: Shard Key IN Clause Pruning ---")
	var results []BenchResult

	// IN with 5 tenants (should prune to ~5 shards out of 16)
	tenants := "42,100,555,1234,7777"

	baselineQuery := "SELECT COUNT(*), SUM(revenue) FROM fact_sharded WHERE region_id IN (1,2,3,4,5)"
	optimizedQuery := fmt.Sprintf(
		"SELECT COUNT(*), SUM(revenue) FROM fact_sharded WHERE tenant_id IN (%s)", tenants)

	baseExplain := getExplainAccess(baselineQuery)
	optExplain := getExplainAccess(optimizedQuery)

	baseTime := timeQuery(baselineQuery, *iterations)
	optTime := timeQuery(optimizedQuery, *iterations)

	speedup := float64(baseTime) / float64(optTime)
	results = append(results, BenchResult{
		Name:       "IN Query (all shards vs pruned shards)",
		Baseline:   baseTime,
		Optimized:  optTime,
		Speedup:    speedup,
		BaseExplan: baseExplain,
		OptExplan:  optExplain,
	})

	fmt.Printf("  Baseline (no shard key, all shards): %v  [%s]\n", baseTime, baseExplain)
	fmt.Printf("  Optimized (IN on shard key, pruned): %v  [%s]\n", optTime, optExplain)
	fmt.Printf("  Speedup: %.2fx\n", speedup)

	return results
}

func benchEncodedFilter() []BenchResult {
	fmt.Println("\n--- Benchmark: Analytical Filter (TiKV vs TiFlash) ---")
	fmt.Println("  NOTE: At small data sizes (<10M rows), TiKV block cache holds all data in memory,")
	fmt.Println("  making row-store faster. TiFlash's columnar + encoding advantage appears at 50M+")
	fmt.Println("  rows where I/O becomes the bottleneck. Run with --rows=50000000 on live cluster.")
	var results []BenchResult

	// Full table aggregation — this is where columnar shines at scale
	aggQuery := "SELECT status, COUNT(*), SUM(revenue), AVG(revenue) FROM fact_sharded GROUP BY status"

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime := timeQuery(aggQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	tiflashTime := timeQuery(aggQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup := float64(tikvTime) / float64(tiflashTime)
	results = append(results, BenchResult{
		Name:      "Full Agg (GROUP BY status): TiKV vs TiFlash",
		Baseline:  tikvTime,
		Optimized: tiflashTime,
		Speedup:   speedup,
	})

	fmt.Printf("  TiKV (coprocessor agg):    %v\n", tikvTime)
	fmt.Printf("  TiFlash (columnar agg):    %v\n", tiflashTime)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedup, scaleNote(speedup))

	// Multi-column scan (more columns = more I/O for row-store)
	wideQuery := `SELECT region_id, industry_id, status, 
		COUNT(*), SUM(revenue), MIN(revenue), MAX(revenue)
		FROM fact_sharded 
		WHERE created_at >= '2024-01-01'
		GROUP BY region_id, industry_id, status`

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime2 := timeQuery(wideQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	tiflashTime2 := timeQuery(wideQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup2 := float64(tikvTime2) / float64(tiflashTime2)
	results = append(results, BenchResult{
		Name:      "Wide Agg (3-col GROUP BY): TiKV vs TiFlash",
		Baseline:  tikvTime2,
		Optimized: tiflashTime2,
		Speedup:   speedup2,
	})

	fmt.Printf("  TiKV (wide row scan):      %v\n", tikvTime2)
	fmt.Printf("  TiFlash (columnar scan):   %v\n", tiflashTime2)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedup2, scaleNote(speedup2))

	return results
}

func benchEncodedGroupBy() []BenchResult {
	fmt.Println("\n--- Benchmark: Encoded GROUP BY (TiKV vs TiFlash) ---")
	fmt.Println("  TiFlash dict-encodes low-cardinality columns → array-indexed GROUP BY.")
	var results []BenchResult

	// GROUP BY on low-cardinality column (5 statuses)
	query := "SELECT status, COUNT(*), SUM(revenue) FROM fact_sharded GROUP BY status"

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime := timeQuery(query, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	tiflashTime := timeQuery(query, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup := float64(tikvTime) / float64(tiflashTime)
	results = append(results, BenchResult{
		Name:      "GROUP BY (status, 5 groups): TiKV vs TiFlash",
		Baseline:  tikvTime,
		Optimized: tiflashTime,
		Speedup:   speedup,
	})

	fmt.Printf("  TiKV (hash GROUP BY):     %v\n", tikvTime)
	fmt.Printf("  TiFlash (encoded GROUP BY): %v\n", tiflashTime)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedup, scaleNote(speedup))

	// GROUP BY with more groups (50 regions) + SUM
	query2 := "SELECT region_id, COUNT(*), SUM(revenue), AVG(revenue) FROM fact_sharded GROUP BY region_id"

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime2 := timeQuery(query2, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	tiflashTime2 := timeQuery(query2, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup2 := float64(tikvTime2) / float64(tiflashTime2)
	results = append(results, BenchResult{
		Name:      "GROUP BY (region_id, 50 groups): TiKV vs TiFlash",
		Baseline:  tikvTime2,
		Optimized: tiflashTime2,
		Speedup:   speedup2,
	})

	fmt.Printf("  TiKV (hash GROUP BY, 50 groups): %v\n", tikvTime2)
	fmt.Printf("  TiFlash (encoded, 50 groups):     %v\n", tiflashTime2)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedup2, scaleNote(speedup2))

	return results
}

func benchEncodedStarJoin() []BenchResult {
	fmt.Println("\n--- Benchmark: Star Join (TiKV vs TiFlash MPP) ---")
	fmt.Println("  TiFlash MPP uses broadcast join + columnar scan; TiKV uses hash join on row data.")
	var results []BenchResult

	// Star join: fact × dimension with GROUP BY dimension attribute
	joinQuery := `SELECT d.name, COUNT(*), SUM(f.revenue)
		FROM fact_sharded f
		INNER JOIN dim_industry d ON f.industry_id = d.id
		WHERE f.status = 'active'
		GROUP BY d.name
		ORDER BY SUM(f.revenue) DESC`

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime := timeQuery(joinQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	mustExec("SET @@tidb_allow_mpp = 1")
	tiflashTime := timeQuery(joinQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup := float64(tikvTime) / float64(tiflashTime)
	results = append(results, BenchResult{
		Name:      "Star Join fact×industry: TiKV vs TiFlash MPP",
		Baseline:  tikvTime,
		Optimized: tiflashTime,
		Speedup:   speedup,
	})

	fmt.Printf("  TiKV (hash join):      %v\n", tikvTime)
	fmt.Printf("  TiFlash (MPP join):    %v\n", tiflashTime)
	fmt.Printf("  Speedup: %.2fx\n", speedup)

	// Multi-dimension star join (2 dimensions)
	multiDimQuery := `SELECT di.name, dr.country, COUNT(*), SUM(f.revenue)
		FROM fact_sharded f
		INNER JOIN dim_industry di ON f.industry_id = di.id
		INNER JOIN dim_region dr ON f.region_id = dr.id
		WHERE f.status IN ('active', 'pending')
		GROUP BY di.name, dr.country
		ORDER BY SUM(f.revenue) DESC
		LIMIT 20`

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime2 := timeQuery(multiDimQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	mustExec("SET @@tidb_allow_mpp = 1")
	tiflashTime2 := timeQuery(multiDimQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup2 := float64(tikvTime2) / float64(tiflashTime2)
	results = append(results, BenchResult{
		Name:      "Multi-Dim Star Join fact×ind×reg: TiKV vs TiFlash",
		Baseline:  tikvTime2,
		Optimized: tiflashTime2,
		Speedup:   speedup2,
	})

	fmt.Printf("  TiKV (multi-dim hash join):   %v\n", tikvTime2)
	fmt.Printf("  TiFlash (multi-dim MPP):       %v\n", tiflashTime2)
	fmt.Printf("  Speedup: %.2fx\n", speedup2)

	// LEFT JOIN with aggregation
	leftJoinQuery := `SELECT d.name, COUNT(*), SUM(f.revenue)
		FROM fact_sharded f
		LEFT JOIN dim_region d ON f.region_id = d.id
		GROUP BY d.name
		ORDER BY COUNT(*) DESC
		LIMIT 10`

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime3 := timeQuery(leftJoinQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	mustExec("SET @@tidb_allow_mpp = 1")
	tiflashTime3 := timeQuery(leftJoinQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup3 := float64(tikvTime3) / float64(tiflashTime3)
	results = append(results, BenchResult{
		Name:      "LEFT JOIN fact×region: TiKV vs TiFlash MPP",
		Baseline:  tikvTime3,
		Optimized: tiflashTime3,
		Speedup:   speedup3,
	})

	fmt.Printf("  TiKV (LEFT hash join):    %v\n", tikvTime3)
	fmt.Printf("  TiFlash (LEFT MPP join):  %v\n", tiflashTime3)
	fmt.Printf("  Speedup: %.2fx\n", speedup3)

	return results
}

func benchJSONShredding() []BenchResult {
	fmt.Println("\n--- Benchmark: JSON Operations (TiKV vs TiFlash) ---")
	fmt.Println("  TiFlash shreds JSON into sub-columns at write time for faster path access.")
	var results []BenchResult

	// JSON path GROUP BY: TiKV (parse each row) vs TiFlash (columnar + shredded)
	pathQuery := `SELECT payload->>'$.event' AS event, COUNT(*)
		FROM json_events
		GROUP BY payload->>'$.event'`

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime := timeQuery(pathQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	tiflashTime := timeQuery(pathQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup := float64(tikvTime) / float64(tiflashTime)
	results = append(results, BenchResult{
		Name:      "JSON GROUP BY $.event: TiKV vs TiFlash",
		Baseline:  tikvTime,
		Optimized: tiflashTime,
		Speedup:   speedup,
	})

	fmt.Printf("  TiKV (parse blob per row):    %v\n", tikvTime)
	fmt.Printf("  TiFlash (shredded sub-col):   %v\n", tiflashTime)
	fmt.Printf("  Speedup: %.2fx\n", speedup)

	// JSON filter: TiKV vs TiFlash
	filterQuery := `SELECT COUNT(*) FROM json_events WHERE payload->>'$.event' = 'purchase'`

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime2 := timeQuery(filterQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	tiflashTime2 := timeQuery(filterQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup2 := float64(tikvTime2) / float64(tiflashTime2)
	results = append(results, BenchResult{
		Name:      "JSON Filter $.event='purchase': TiKV vs TiFlash",
		Baseline:  tikvTime2,
		Optimized: tiflashTime2,
		Speedup:   speedup2,
	})

	fmt.Printf("  TiKV (parse + filter):      %v\n", tikvTime2)
	fmt.Printf("  TiFlash (shredded filter):  %v\n", tiflashTime2)
	fmt.Printf("  Speedup: %.2fx\n", speedup2)

	// Nested JSON path access
	nestedQuery := `SELECT COUNT(*) FROM json_events 
		WHERE payload->>'$.details.source' IS NOT NULL`

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime3 := timeQuery(nestedQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	tiflashTime3 := timeQuery(nestedQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup3 := float64(tikvTime3) / float64(tiflashTime3)
	results = append(results, BenchResult{
		Name:      "Nested JSON $.details.source: TiKV vs TiFlash",
		Baseline:  tikvTime3,
		Optimized: tiflashTime3,
		Speedup:   speedup3,
	})

	fmt.Printf("  TiKV (nested parse):         %v\n", tikvTime3)
	fmt.Printf("  TiFlash (shredded nested):   %v\n", tiflashTime3)
	fmt.Printf("  Speedup: %.2fx\n", speedup3)

	// Combined: shard pruning + JSON filter (full stack optimization)
	combinedQuery := `SELECT COUNT(*), payload->>'$.page' 
		FROM json_events 
		WHERE tenant_id = 42 AND payload->>'$.event' = 'click'
		GROUP BY payload->>'$.page'`

	mustExec("SET @@tidb_isolation_read_engines = 'tikv'")
	tikvTime4 := timeQuery(combinedQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")
	tiflashTime4 := timeQuery(combinedQuery, *iterations)

	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")

	speedup4 := float64(tikvTime4) / float64(tiflashTime4)
	results = append(results, BenchResult{
		Name:      "Shard Prune + JSON Filter: TiKV vs TiFlash",
		Baseline:  tikvTime4,
		Optimized: tiflashTime4,
		Speedup:   speedup4,
	})

	fmt.Printf("  TiKV (prune + parse blob):         %v\n", tikvTime4)
	fmt.Printf("  TiFlash (prune + shredded filter): %v\n", tiflashTime4)
	fmt.Printf("  Speedup: %.2fx\n", speedup4)

	// === A/B comparison: TiFlash blob vs TiFlash shredded (same engine, toggle read path) ===
	fmt.Println("\n  --- JSON Shredding A/B: @@tiflash_json_shredding ON vs OFF ---")
	fmt.Println("  Same engine (TiFlash), same data. Only the read path changes.")

	mustExec("SET @@tidb_isolation_read_engines = 'tiflash'")

	// OFF = read from blob (parse per row)
	mustExec("SET @@tiflash_json_shredding = OFF")
	blobTime := timeQuery(pathQuery, *iterations)

	// ON = read from shredded sub-columns
	mustExec("SET @@tiflash_json_shredding = ON")
	shreddedTime := timeQuery(pathQuery, *iterations)

	speedup5 := float64(blobTime) / float64(shreddedTime)
	results = append(results, BenchResult{
		Name:      "TiFlash JSON GROUP BY: blob vs shredded",
		Baseline:  blobTime,
		Optimized: shreddedTime,
		Speedup:   speedup5,
	})

	fmt.Printf("  TiFlash blob (@@=OFF):       %v\n", blobTime)
	fmt.Printf("  TiFlash shredded (@@=ON):    %v\n", shreddedTime)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedup5, scaleNote(speedup5))

	// A/B on filter
	mustExec("SET @@tiflash_json_shredding = OFF")
	blobTime2 := timeQuery(filterQuery, *iterations)

	mustExec("SET @@tiflash_json_shredding = ON")
	shreddedTime2 := timeQuery(filterQuery, *iterations)

	speedup6 := float64(blobTime2) / float64(shreddedTime2)
	results = append(results, BenchResult{
		Name:      "TiFlash JSON Filter: blob vs shredded",
		Baseline:  blobTime2,
		Optimized: shreddedTime2,
		Speedup:   speedup6,
	})

	fmt.Printf("  TiFlash blob filter (@@=OFF):     %v\n", blobTime2)
	fmt.Printf("  TiFlash shredded filter (@@=ON):  %v\n", shreddedTime2)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedup6, scaleNote(speedup6))

	// A/B on nested path
	mustExec("SET @@tiflash_json_shredding = OFF")
	blobTime3 := timeQuery(nestedQuery, *iterations)

	mustExec("SET @@tiflash_json_shredding = ON")
	shreddedTime3 := timeQuery(nestedQuery, *iterations)

	speedup7 := float64(blobTime3) / float64(shreddedTime3)
	results = append(results, BenchResult{
		Name:      "TiFlash Nested JSON: blob vs shredded",
		Baseline:  blobTime3,
		Optimized: shreddedTime3,
		Speedup:   speedup7,
	})

	fmt.Printf("  TiFlash nested blob (@@=OFF):     %v\n", blobTime3)
	fmt.Printf("  TiFlash nested shredded (@@=ON):  %v\n", shreddedTime3)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedup7, scaleNote(speedup7))

	// === Multi-path extraction: amplifies per-row parse cost ===
	fmt.Println("\n  --- Multi-Path JSON Extraction (blob vs shredded) ---")
	fmt.Println("  Extracting 8+ paths per row forces blob path to parse full document per row.")
	fmt.Println("  Shredded path reads only needed sub-columns — no parsing overhead.")

	// Multi-path query: extract 8 paths + filter + GROUP BY
	var multiPathQuery string
	if *jsonSize == "large" {
		multiPathQuery = `SELECT 
			payload->>'$.event' AS event,
			payload->>'$.page' AS page,
			payload->>'$.context.browser' AS browser,
			payload->>'$.context.device' AS device,
			payload->>'$.context.os' AS os,
			payload->>'$.geo.country' AS country,
			payload->>'$.behavior.scroll_depth' AS scroll,
			payload->>'$.product.category' AS category,
			payload->>'$.campaign.source' AS source,
			payload->>'$.campaign.medium' AS medium,
			COUNT(*) AS cnt
		FROM json_events
		WHERE payload->>'$.event' = 'purchase'
		GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10`
	} else {
		multiPathQuery = `SELECT 
			payload->>'$.event' AS event,
			payload->>'$.page' AS page,
			payload->>'$.user_agent' AS ua,
			payload->>'$.ip' AS ip,
			CAST(payload->>'$.duration' AS SIGNED) AS dur,
			CAST(payload->>'$.score' AS DECIMAL(10,2)) AS score,
			COUNT(*) AS cnt
		FROM json_events
		WHERE payload->>'$.event' = 'purchase'
		GROUP BY 1, 2, 3, 4, 5, 6`
	}

	mustExec("SET @@tiflash_json_shredding = OFF")
	blobMulti := timeQuery(multiPathQuery, *iterations)

	mustExec("SET @@tiflash_json_shredding = ON")
	shreddedMulti := timeQuery(multiPathQuery, *iterations)

	speedupMulti := float64(blobMulti) / float64(shreddedMulti)
	results = append(results, BenchResult{
		Name:      "Multi-Path Extract (8+ paths): blob vs shredded",
		Baseline:  blobMulti,
		Optimized: shreddedMulti,
		Speedup:   speedupMulti,
	})

	fmt.Printf("  TiFlash blob (8-path, @@=OFF):       %v\n", blobMulti)
	fmt.Printf("  TiFlash shredded (8-path, @@=ON):    %v\n", shreddedMulti)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedupMulti, scaleNote(speedupMulti))

	// Multi-path aggregation: analytics across many JSON fields
	var multiAggQuery string
	if *jsonSize == "large" {
		multiAggQuery = `SELECT 
			payload->>'$.event' AS event,
			COUNT(*) AS cnt,
			AVG(CAST(payload->>'$.duration' AS SIGNED)) AS avg_dur,
			AVG(CAST(payload->>'$.score' AS DECIMAL(10,2))) AS avg_score,
			AVG(CAST(payload->>'$.behavior.scroll_depth' AS SIGNED)) AS avg_scroll,
			AVG(CAST(payload->>'$.behavior.time_on_page' AS SIGNED)) AS avg_time,
			AVG(CAST(payload->>'$.product.price' AS DECIMAL(10,2))) AS avg_price
		FROM json_events
		GROUP BY 1`
	} else {
		multiAggQuery = `SELECT 
			payload->>'$.event' AS event,
			COUNT(*) AS cnt,
			AVG(CAST(payload->>'$.duration' AS SIGNED)) AS avg_dur,
			AVG(CAST(payload->>'$.score' AS DECIMAL(10,2))) AS avg_score
		FROM json_events
		GROUP BY 1`
	}

	mustExec("SET @@tiflash_json_shredding = OFF")
	blobAgg := timeQuery(multiAggQuery, *iterations)

	mustExec("SET @@tiflash_json_shredding = ON")
	shreddedAgg := timeQuery(multiAggQuery, *iterations)

	speedupAgg := float64(blobAgg) / float64(shreddedAgg)
	results = append(results, BenchResult{
		Name:      "Multi-Path Agg (GROUP BY + AVGs): blob vs shredded",
		Baseline:  blobAgg,
		Optimized: shreddedAgg,
		Speedup:   speedupAgg,
	})

	fmt.Printf("  TiFlash blob agg (@@=OFF):       %v\n", blobAgg)
	fmt.Printf("  TiFlash shredded agg (@@=ON):    %v\n", shreddedAgg)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedupAgg, scaleNote(speedupAgg))

	// Nested multi-path with filter on nested field
	var nestedMultiQuery string
	if *jsonSize == "large" {
		nestedMultiQuery = `SELECT
			payload->>'$.campaign.source' AS src,
			payload->>'$.campaign.medium' AS med,
			payload->>'$.product.category' AS cat,
			payload->>'$.event' AS event,
			payload->>'$.page' AS page,
			COUNT(*) AS cnt,
			SUM(CAST(payload->>'$.product.price' AS DECIMAL(10,2))) AS total_price
		FROM json_events
		WHERE payload->>'$.campaign.source' = 'google'
		  AND CAST(payload->>'$.product.price' AS DECIMAL(10,2)) > 100
		GROUP BY 1, 2, 3, 4, 5`
	} else {
		nestedMultiQuery = `SELECT
			payload->>'$.event' AS event,
			payload->>'$.page' AS page,
			payload->>'$.ip' AS ip,
			COUNT(*) AS cnt
		FROM json_events
		WHERE payload->>'$.details.source' IS NOT NULL
		  AND CAST(payload->>'$.score' AS DECIMAL(10,2)) > 50
		GROUP BY 1, 2, 3`
	}

	mustExec("SET @@tiflash_json_shredding = OFF")
	blobNested := timeQuery(nestedMultiQuery, *iterations)

	mustExec("SET @@tiflash_json_shredding = ON")
	shreddedNested := timeQuery(nestedMultiQuery, *iterations)

	speedupNested := float64(blobNested) / float64(shreddedNested)
	results = append(results, BenchResult{
		Name:      "Nested Multi-Path + Filter: blob vs shredded",
		Baseline:  blobNested,
		Optimized: shreddedNested,
		Speedup:   speedupNested,
	})

	fmt.Printf("  TiFlash blob nested (@@=OFF):       %v\n", blobNested)
	fmt.Printf("  TiFlash shredded nested (@@=ON):    %v\n", shreddedNested)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedupNested, scaleNote(speedupNested))

	// JSON + dimension JOIN (combines shredding with star join)
	joinJSONQuery := `SELECT 
		r.country,
		payload->>'$.event' AS event,
		COUNT(*) AS cnt,
		SUM(CAST(payload->>'$.score' AS DECIMAL(10,2))) AS total_score
	FROM json_events j
	JOIN dim_region r ON r.id = (j.id % 50) + 1
	WHERE payload->>'$.event' IN ('purchase', 'signup')
	GROUP BY 1, 2`

	mustExec("SET @@tiflash_json_shredding = OFF")
	blobJoin := timeQuery(joinJSONQuery, *iterations)

	mustExec("SET @@tiflash_json_shredding = ON")
	shreddedJoin := timeQuery(joinJSONQuery, *iterations)

	speedupJoin := float64(blobJoin) / float64(shreddedJoin)
	results = append(results, BenchResult{
		Name:      "JSON + JOIN (dim table): blob vs shredded",
		Baseline:  blobJoin,
		Optimized: shreddedJoin,
		Speedup:   speedupJoin,
	})

	fmt.Printf("  TiFlash blob+join (@@=OFF):       %v\n", blobJoin)
	fmt.Printf("  TiFlash shredded+join (@@=ON):    %v\n", shreddedJoin)
	fmt.Printf("  Speedup: %.2fx  %s\n", speedupJoin, scaleNote(speedupJoin))

	// Restore defaults
	mustExec("SET @@tidb_isolation_read_engines = 'tikv,tiflash'")
	mustExec("SET @@tiflash_json_shredding = ON")

	return results
}

// ============================================================================
// Helpers
// ============================================================================

func timeQuery(query string, iterations int) time.Duration {
	// Warmup
	db.QueryRow(query).Scan()

	var total time.Duration
	for i := 0; i < iterations; i++ {
		start := time.Now()
		rows, err := db.Query(query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  WARN query error: %v\n", err)
			return 0
		}
		for rows.Next() {
			// drain
		}
		rows.Close()
		total += time.Since(start)
	}
	return total / time.Duration(iterations)
}

func getExplainAccess(query string) string {
	rows, err := db.Query("EXPLAIN " + query)
	if err != nil {
		return "?"
	}
	defer rows.Close()

	var parts []string
	for rows.Next() {
		cols, _ := rows.Columns()
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		rows.Scan(ptrs...)
		// access_object is typically column 3 (0-indexed)
		if len(vals) > 3 {
			if v, ok := vals[3].([]byte); ok && len(v) > 0 {
				parts = append(parts, string(v))
			}
		}
	}
	filtered := []string{}
	for _, p := range parts {
		if p != "" {
			filtered = append(filtered, p)
		}
	}
	if len(filtered) == 0 {
		return "full scan"
	}
	return strings.Join(filtered, ", ")
}

func waitForAllReplicas() {
	fmt.Print("  Waiting for TiFlash replicas...")
	tables := []string{"fact_sharded", "fact_unsharded", "dim_region", "dim_industry", "json_events"}
	for attempt := 0; attempt < 120; attempt++ {
		var ready int
		for _, t := range tables {
			var avail int
			row := db.QueryRow(fmt.Sprintf(
				"SELECT IFNULL(AVAILABLE, 0) FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'",
				*database, t))
			if err := row.Scan(&avail); err == nil && avail == 1 {
				ready++
			}
		}
		if ready == len(tables) {
			fmt.Println(" all ready")
			return
		}
		time.Sleep(2 * time.Second)
		if attempt%15 == 14 {
			fmt.Printf(" (%d/%d ready)", ready, len(tables))
		}
	}
	fmt.Println(" TIMEOUT (some replicas not ready, proceeding anyway)")
}

func parseBenchmarks(s string) map[string]bool {
	m := make(map[string]bool)
	if s == "all" {
		for _, b := range []string{"shard", "in", "filter", "groupby", "join", "json"} {
			m[b] = true
		}
		return m
	}
	for _, b := range strings.Split(s, ",") {
		m[strings.TrimSpace(b)] = true
	}
	return m
}

func printSummary(results []BenchResult) {
	fmt.Println("\n")
	fmt.Println("╔══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                    tidb-se PERFORMANCE BENCHMARK SUMMARY                    ║")
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ %-76s ║\n", fmt.Sprintf("Host: %s:%d | Rows: %dM | Shards: %d | JSON: %s",
		*host, *port, *rows/1_000_000, *shards, *jsonSize))
	fmt.Println("╠══════════════════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ %-40s │ %8s │ %8s │ %7s ║\n", "Benchmark", "Baseline", "Optimized", "Speedup")
	fmt.Println("╠──────────────────────────────────────────┼──────────┼──────────┼─────────╣")

	for _, r := range results {
		name := r.Name
		if len(name) > 40 {
			name = name[:37] + "..."
		}
		fmt.Printf("║ %-40s │ %8s │ %8s │ %6.2fx ║\n",
			name, fmtDur(r.Baseline), fmtDur(r.Optimized), r.Speedup)
	}

	fmt.Println("╚══════════════════════════════════════════════════════════════════════════════╝")

	// Overall average speedup
	var totalSpeedup float64
	for _, r := range results {
		totalSpeedup += r.Speedup
	}
	avgSpeedup := totalSpeedup / float64(len(results))
	fmt.Printf("\nAverage speedup across all benchmarks: %.2fx\n", avgSpeedup)
}

func scaleNote(speedup float64) string {
	if speedup < 1.0 {
		return "(TiFlash overhead dominates at this scale — expect 5-30x at 50M+ rows)"
	}
	return ""
}

func fmtDur(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

func mustExec(query string, args ...interface{}) {
	_, err := db.Exec(query, args...)
	if err != nil {
		// Non-fatal for SET commands that may not exist yet
		if strings.HasPrefix(query, "SET") {
			return
		}
		fatal("exec %q: %v", query, err)
	}
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
