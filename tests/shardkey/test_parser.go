package shardkey

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	ast "github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
)

func TestParseCreateTableWithShardKey(t *testing.T) {
	p := parser.New()

	sql := `CREATE TABLE test_table (
		id BIGINT PRIMARY KEY,
		company_id BIGINT,
		name VARCHAR(255)
	) SHARD_KEY(company_id) INTO 4 SHARDS`

	nodes, _, err := p.Parse(sql, "", "")
	assert.NoError(t, err, "Should parse CREATE TABLE with SHARD_KEY")
	assert.Len(t, nodes, 1)

	createStmt, ok := nodes[0].(*ast.CreateTableStmt)
	assert.True(t, ok, "Should be CreateTableStmt")
	assert.NotNil(t, createStmt.ShardKeyInfo, "ShardKeyInfo should not be nil")
	assert.Equal(t, 1, len(createStmt.ShardKeyInfo.Columns), "Should have 1 shard key column")
	assert.Equal(t, 4, createStmt.ShardKeyInfo.ShardCnt, "Should have 4 shards")
}

func TestParseCreateTableWithoutShardKey(t *testing.T) {
	p := parser.New()

	sql := `CREATE TABLE test_table (
		id BIGINT PRIMARY KEY,
		company_id BIGINT
	)`

	nodes, _, err := p.Parse(sql, "", "")
	assert.NoError(t, err)

	createStmt, ok := nodes[0].(*ast.CreateTableStmt)
	assert.True(t, ok)
	assert.Nil(t, createStmt.ShardKeyInfo, "ShardKeyInfo should be nil when not specified")
}

func TestParseCreateTableMultipleShardKeyColumns(t *testing.T) {
	p := parser.New()

	sql := `CREATE TABLE test_table (
		id BIGINT PRIMARY KEY,
		company_id BIGINT,
		tenant_id BIGINT
	) SHARD_KEY(company_id, tenant_id) INTO 8 SHARDS`

	nodes, _, err := p.Parse(sql, "", "")
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)

	createStmt, ok := nodes[0].(*ast.CreateTableStmt)
	assert.True(t, ok)
	assert.NotNil(t, createStmt.ShardKeyInfo)
	assert.Equal(t, 2, len(createStmt.ShardKeyInfo.Columns))
	assert.Equal(t, 8, createStmt.ShardKeyInfo.ShardCnt)
}

func TestParseCreateTableShardKeyZero(t *testing.T) {
	p := parser.New()

	sql := `CREATE TABLE test_table (
		id BIGINT PRIMARY KEY
	) SHARD_KEY(id) INTO 0 SHARDS`

	_, _, err := p.Parse(sql, "", "")
	assert.Error(t, err, "Should reject shard count of 0")
}
