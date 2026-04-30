package shardkey

import (
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/assert"
)

func TestShardKeyInfoSerialization(t *testing.T) {
	sk := &model.ShardKeyInfo{
		Columns:  []string{"company_id", "tenant_id"},
		ShardCnt: 4,
	}

	data, err := json.Marshal(sk)
	assert.NoError(t, err)

	var sk2 model.ShardKeyInfo
	err = json.Unmarshal(data, &sk2)
	assert.NoError(t, err)
	assert.Equal(t, sk.Columns, sk2.Columns)
	assert.Equal(t, sk.ShardCnt, sk2.ShardCnt)
}

func TestShardKeyMatchSameCount(t *testing.T) {
	left := &model.ShardKeyInfo{
		Columns:  []string{"company_id"},
		ShardCnt: 4,
	}
	right := &model.ShardKeyInfo{
		Columns:  []string{"company_id"},
		ShardCnt: 4,
	}

	assert.Equal(t, left.ShardCnt, right.ShardCnt)
}

func TestShardKeyMatchDifferentCount(t *testing.T) {
	left := &model.ShardKeyInfo{
		Columns:  []string{"company_id"},
		ShardCnt: 4,
	}
	right := &model.ShardKeyInfo{
		Columns:  []string{"company_id"},
		ShardCnt: 8,
	}

	assert.NotEqual(t, left.ShardCnt, right.ShardCnt)
}

func TestShardKeysCompatible(t *testing.T) {
	// same columns, same count → compatible
	a := &model.ShardKeyInfo{Columns: []string{"company_id"}, ShardCnt: 4}
	b := &model.ShardKeyInfo{Columns: []string{"company_id"}, ShardCnt: 4}
	assert.True(t, model.ShardKeysCompatible(a, b))

	// same columns, different count → not compatible
	c := &model.ShardKeyInfo{Columns: []string{"company_id"}, ShardCnt: 8}
	assert.False(t, model.ShardKeysCompatible(a, c))

	// different columns, same count → not compatible
	d := &model.ShardKeyInfo{Columns: []string{"tenant_id"}, ShardCnt: 4}
	assert.False(t, model.ShardKeysCompatible(a, d))

	// multi-column: same set (order-independent) → compatible
	e := &model.ShardKeyInfo{Columns: []string{"company_id", "tenant_id"}, ShardCnt: 4}
	f := &model.ShardKeyInfo{Columns: []string{"tenant_id", "company_id"}, ShardCnt: 4}
	assert.True(t, model.ShardKeysCompatible(e, f))

	// nil on either side → not compatible
	assert.False(t, model.ShardKeysCompatible(nil, b))
	assert.False(t, model.ShardKeysCompatible(a, nil))
	assert.False(t, model.ShardKeysCompatible(nil, nil))
}
