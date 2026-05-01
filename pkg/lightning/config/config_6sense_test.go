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

package config

import (
	"encoding/json"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
)

func TestColumnConstantsTableFilter(t *testing.T) {
	tomlData := `
[[mydumper.column-constants]]
table-filter = ["mydb.mytable"]
[mydumper.column-constants.values]
ts = "2026-04-17 21:00:00"
`
	cfg := NewConfig()
	_, err := toml.Decode(tomlData, cfg)
	require.NoError(t, err)
	require.NoError(t, cfg.Mydumper.adjustIgnoreColumns())

	cc, err := cfg.Mydumper.ColumnConstants.GetColumnConstants("mydb", "mytable", false)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"ts": "2026-04-17 21:00:00"}, cc)

	ic, err := cfg.Mydumper.IgnoreColumns.GetIgnoreColumns("mydb", "mytable", false)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"ts"}, ic.Columns)
}

// TestColumnConstantsDuplicateKey verifies that duplicate keys in column-constants
// values (after lowercasing) return an error.
func TestColumnConstantsDuplicateKey(t *testing.T) {
	// TOML allows "TS" and "ts" as distinct keys (case-sensitive), but our
	// normalization in adjustIgnoreColumns lowercases both to "ts", triggering
	// the duplicate-detection path.
	tomlData := `
[[mydumper.column-constants]]
db    = "mydb"
table = "mytable"
[mydumper.column-constants.values]
TS = "2026-04-17 21:00:00"
ts = "2026-04-17 22:00:00"
`
	cfg := NewConfig()
	_, err := toml.Decode(tomlData, cfg)
	require.NoError(t, err)
	err = cfg.Mydumper.adjustIgnoreColumns()
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate column-constants entry for column ts")
}

func TestTargetPartitionConfig(t *testing.T) {
	cfg := NewConfig()
	require.Equal(t, "", cfg.Mydumper.TargetPartition)

	tomlStr := `
[mydumper]
data-source-dir = "."
target-partition = "p_acme"
`
	err := toml.Unmarshal([]byte(tomlStr), cfg)
	require.NoError(t, err)
	require.Equal(t, "p_acme", cfg.Mydumper.TargetPartition)

	jsonBytes, err := json.Marshal(cfg.Mydumper)
	require.NoError(t, err)
	require.Contains(t, string(jsonBytes), `"target-partition":"p_acme"`)
}
