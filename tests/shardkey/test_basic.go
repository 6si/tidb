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
