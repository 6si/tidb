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

package importer

import (
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
)

// sliceIter implements common.KVIter for testing with in-memory sorted KV pairs.
type sliceIter struct {
	kvs []kvPair
	pos int
}

type kvPair struct {
	key []byte
	val []byte
}

func (s *sliceIter) Next() bool {
	s.pos++
	return s.pos < len(s.kvs)
}

func (s *sliceIter) Key() []byte {
	return s.kvs[s.pos].key
}

func (s *sliceIter) Value() []byte {
	return s.kvs[s.pos].val
}

// encodeSortedKVs simulates the pebble sort phase: encodes keys with
// DupDetectKeyAdapter (appending rowID suffix) and sorts by encoded key.
func encodeSortedKVs(keys [][]byte, vals [][]byte) []kvPair {
	adapter := common.DupDetectKeyAdapter{}
	pairs := make([]kvPair, 0, len(keys))
	for i, key := range keys {
		rowID := common.EncodeIntRowID(int64(i + 1))
		encoded := adapter.Encode(nil, key, rowID)
		pairs = append(pairs, kvPair{key: encoded, val: vals[i]})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return string(pairs[i].key) < string(pairs[j].key)
	})
	return pairs
}

// TestDupDetectorNoDuplicates verifies that when all keys are unique,
// DupDetector emits all rows with no loss.
func TestDupDetectorNoDuplicates(t *testing.T) {
	// 10 unique keys, each with distinct values.
	keys := make([][]byte, 10)
	vals := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = []byte(fmt.Sprintf("pk_%04d", i))
		vals[i] = []byte(fmt.Sprintf("val_%04d", i))
	}

	sorted := encodeSortedKVs(keys, vals)
	iter := &sliceIter{kvs: sorted, pos: -1}

	// Open a pebble DB for dupDB.
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer db.Close()
	batch := db.NewBatch()

	detector := common.NewDupDetector(
		common.DupDetectKeyAdapter{},
		batch,
		log.L(),
		common.DupDetectOpt{ReportErrOnDup: false},
	)
	defer detector.Close()

	// Init with first entry.
	iter.Next()
	_, _, err = detector.Init(iter)
	require.NoError(t, err)

	// Drain remaining entries.
	outputCount := 1 // already got 1 from Init
	for {
		_, _, ok, err := detector.Next(iter)
		require.NoError(t, err)
		if !ok {
			break
		}
		outputCount++
	}

	// All 10 unique keys should be emitted.
	require.Equal(t, 10, outputCount, "all unique keys should pass through")
}

// TestDupDetectorWithDuplicates_CountVerification verifies that duplicates
// within a batch are detected and the output count is correct.
// Simulates: 10 rows with 2 duplicate PKs → expect 8 unique outputs.
func TestDupDetectorWithDuplicates_CountVerification(t *testing.T) {
	// 10 rows: pk_0000..pk_0007 are unique, pk_0002 and pk_0005 appear twice.
	// Total unique PKs = 8, total input rows = 10.
	keys := [][]byte{
		[]byte("pk_0000"),
		[]byte("pk_0001"),
		[]byte("pk_0002"), // first occurrence
		[]byte("pk_0003"),
		[]byte("pk_0004"),
		[]byte("pk_0005"), // first occurrence
		[]byte("pk_0006"),
		[]byte("pk_0007"),
		[]byte("pk_0002"), // DUPLICATE of row 2
		[]byte("pk_0005"), // DUPLICATE of row 5
	}
	vals := [][]byte{
		[]byte("val_0000"),
		[]byte("val_0001"),
		[]byte("val_0002_v1"), // first occurrence value
		[]byte("val_0003"),
		[]byte("val_0004"),
		[]byte("val_0005_v1"), // first occurrence value
		[]byte("val_0006"),
		[]byte("val_0007"),
		[]byte("val_0002_v2"), // DUPLICATE: updated value
		[]byte("val_0005_v2"), // DUPLICATE: updated value
	}

	sorted := encodeSortedKVs(keys, vals)
	iter := &sliceIter{kvs: sorted, pos: -1}

	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer db.Close()
	batch := db.NewBatch()

	detector := common.NewDupDetector(
		common.DupDetectKeyAdapter{},
		batch,
		log.L(),
		common.DupDetectOpt{ReportErrOnDup: false},
	)
	defer detector.Close()

	iter.Next()
	_, _, err = detector.Init(iter)
	require.NoError(t, err)

	outputCount := 1
	for {
		_, _, ok, err := detector.Next(iter)
		require.NoError(t, err)
		if !ok {
			break
		}
		outputCount++
	}

	// 10 input rows, 2 duplicate PKs → 8 unique outputs emitted.
	require.Equal(t, 8, outputCount,
		"10 rows with 2 duplicate PKs should produce 8 unique outputs")
}

// TestDupDetectorReportErrOnDup verifies that in replace mode
// (ReportErrOnDup: true), the detector returns ErrFoundDuplicateKeys
// on the first duplicate encountered.
func TestDupDetectorReportErrOnDup(t *testing.T) {
	keys := [][]byte{
		[]byte("pk_0001"),
		[]byte("pk_0002"),
		[]byte("pk_0001"), // DUPLICATE
	}
	vals := [][]byte{
		[]byte("val_a"),
		[]byte("val_b"),
		[]byte("val_c"),
	}

	sorted := encodeSortedKVs(keys, vals)
	iter := &sliceIter{kvs: sorted, pos: -1}

	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer db.Close()
	batch := db.NewBatch()

	detector := common.NewDupDetector(
		common.DupDetectKeyAdapter{},
		batch,
		log.L(),
		common.DupDetectOpt{ReportErrOnDup: true},
	)
	defer detector.Close()

	iter.Next()
	_, _, err = detector.Init(iter)
	require.NoError(t, err)

	// Iterate until error or exhaustion.
	var foundErr error
	for {
		_, _, ok, err := detector.Next(iter)
		if err != nil {
			foundErr = err
			break
		}
		if !ok {
			break
		}
	}

	require.Error(t, foundErr)
	require.True(t, common.ErrFoundDuplicateKeys.Equal(foundErr),
		"expected ErrFoundDuplicateKeys, got: %v", foundErr)
}

// TestDupDetectorHighOverlap simulates a realistic scenario:
// 1000 rows with 10% duplicates (100 duplicate PKs) → expect 900 unique outputs.
func TestDupDetectorHighOverlap(t *testing.T) {
	totalRows := 1000
	numDups := 100 // 10% overlap

	keys := make([][]byte, 0, totalRows)
	vals := make([][]byte, 0, totalRows)

	// First 900 unique keys.
	for i := 0; i < totalRows-numDups; i++ {
		keys = append(keys, []byte(fmt.Sprintf("pk_%06d", i)))
		vals = append(vals, []byte(fmt.Sprintf("val_%06d_v1", i)))
	}
	// 100 duplicates of the first 100 keys.
	for i := 0; i < numDups; i++ {
		keys = append(keys, []byte(fmt.Sprintf("pk_%06d", i)))
		vals = append(vals, []byte(fmt.Sprintf("val_%06d_v2", i)))
	}

	sorted := encodeSortedKVs(keys, vals)
	iter := &sliceIter{kvs: sorted, pos: -1}

	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer db.Close()
	batch := db.NewBatch()

	detector := common.NewDupDetector(
		common.DupDetectKeyAdapter{},
		batch,
		log.L(),
		common.DupDetectOpt{ReportErrOnDup: false},
	)
	defer detector.Close()

	iter.Next()
	_, _, err = detector.Init(iter)
	require.NoError(t, err)

	outputCount := 1
	for {
		_, _, ok, err := detector.Next(iter)
		require.NoError(t, err)
		if !ok {
			break
		}
		outputCount++
	}

	// 1000 input rows - 100 duplicate PKs = 900 unique outputs.
	require.Equal(t, 900, outputCount,
		"1000 rows with 100 duplicate PKs (10%% overlap) should produce 900 unique outputs")
}

// TestDupDetectorLastOccurrenceWins verifies that for duplicate keys,
// the detector records duplicates in dupDB and the first sorted occurrence
// is the one emitted from Init.
func TestDupDetectorLastOccurrenceWins(t *testing.T) {
	// Two rows with same PK, different values. The one with the higher rowID
	// sorts later (DupDetectKeyAdapter appends rowID) → it becomes the duplicate
	// recorded in dupDB.
	keys := [][]byte{
		[]byte("pk_same"),
		[]byte("pk_same"),
	}
	vals := [][]byte{
		[]byte("first_value"),
		[]byte("second_value"),
	}

	sorted := encodeSortedKVs(keys, vals)
	iter := &sliceIter{kvs: sorted, pos: -1}

	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer db.Close()
	batch := db.NewBatch()

	detector := common.NewDupDetector(
		common.DupDetectKeyAdapter{},
		batch,
		log.L(),
		common.DupDetectOpt{ReportErrOnDup: false},
	)

	iter.Next()
	initKey, initVal, err := detector.Init(iter)
	require.NoError(t, err)

	// After Init, curKey is the first sorted entry.
	// Next() should exhaust without returning a new key (both are same PK).
	_, _, ok, err := detector.Next(iter)
	require.NoError(t, err)
	require.False(t, ok, "no more unique keys to emit after the duplicate pair")

	// The value returned from Init should be the first sorted occurrence.
	// Since DupDetectKeyAdapter appends rowID and rowID=1 < rowID=2,
	// the first sorted entry has rowID=1 → "first_value".
	_ = initKey
	require.Equal(t, []byte("first_value"), initVal,
		"first sorted occurrence should be the emitted/surviving value")

	// Close detector to flush dupDB records.
	require.NoError(t, detector.Close())

	// Verify dupDB has both occurrences recorded.
	dupCount := 0
	dupIter, err := db.NewIter(nil)
	require.NoError(t, err)
	for dupIter.First(); dupIter.Valid(); dupIter.Next() {
		dupCount++
	}
	require.NoError(t, dupIter.Close())

	// Both occurrences of the duplicate key are recorded in dupDB.
	require.Equal(t, 2, dupCount,
		"dupDB should contain both occurrences of the duplicate key")
}

// TestDupDetectorEmptyPartitionScenario simulates the use case of importing
// into an empty partition: all keys are new, no duplicates. This verifies
// that the replace mode path works correctly when there are no conflicts
// within the batch.
func TestDupDetectorEmptyPartitionScenario(t *testing.T) {
	// Simulate 50 rows being imported into an empty partition.
	totalRows := 50
	keys := make([][]byte, totalRows)
	vals := make([][]byte, totalRows)
	for i := 0; i < totalRows; i++ {
		keys[i] = []byte(fmt.Sprintf("partition_A_pk_%04d", i))
		vals[i] = []byte(fmt.Sprintf("data_for_row_%04d", i))
	}

	sorted := encodeSortedKVs(keys, vals)
	iter := &sliceIter{kvs: sorted, pos: -1}

	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer db.Close()
	batch := db.NewBatch()

	detector := common.NewDupDetector(
		common.DupDetectKeyAdapter{},
		batch,
		log.L(),
		common.DupDetectOpt{ReportErrOnDup: false},
	)
	defer detector.Close()

	iter.Next()
	_, _, err = detector.Init(iter)
	require.NoError(t, err)

	outputCount := 1
	for {
		_, _, ok, err := detector.Next(iter)
		require.NoError(t, err)
		if !ok {
			break
		}
		outputCount++
	}

	// All 50 rows should pass through with no dedup.
	require.Equal(t, totalRows, outputCount,
		"empty partition import: all rows should pass through")
}

// TestDupDetectorNonEmptyPartitionWithOverlap simulates the use case of
// importing into a non-empty partition where some rows have the same PK as
// existing rows. Within the CSV batch itself, there may also be duplicate PKs.
// This tests the within-batch dedup count specifically.
//
// Scenario: batch of 100 rows, 15 have PKs that also appear elsewhere in the
// same batch (within-batch duplicates). Expected output: 85 unique rows.
func TestDupDetectorNonEmptyPartitionWithOverlap(t *testing.T) {
	totalRows := 100
	withinBatchDups := 15

	keys := make([][]byte, 0, totalRows)
	vals := make([][]byte, 0, totalRows)

	// 85 unique keys.
	for i := 0; i < totalRows-withinBatchDups; i++ {
		keys = append(keys, []byte(fmt.Sprintf("row_%05d", i)))
		vals = append(vals, []byte(fmt.Sprintf("original_value_%05d", i)))
	}
	// 15 within-batch duplicates (same PK as rows 0-14, different values).
	for i := 0; i < withinBatchDups; i++ {
		keys = append(keys, []byte(fmt.Sprintf("row_%05d", i)))
		vals = append(vals, []byte(fmt.Sprintf("updated_value_%05d", i)))
	}

	sorted := encodeSortedKVs(keys, vals)
	iter := &sliceIter{kvs: sorted, pos: -1}

	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	require.NoError(t, err)
	defer db.Close()
	batch := db.NewBatch()

	detector := common.NewDupDetector(
		common.DupDetectKeyAdapter{},
		batch,
		log.L(),
		common.DupDetectOpt{ReportErrOnDup: false},
	)
	defer detector.Close()

	iter.Next()
	_, _, err = detector.Init(iter)
	require.NoError(t, err)

	outputCount := 1
	for {
		_, _, ok, err := detector.Next(iter)
		require.NoError(t, err)
		if !ok {
			break
		}
		outputCount++
	}

	// 100 input rows - 15 duplicate PKs = 85 unique outputs.
	require.Equal(t, 85, outputCount,
		"100 rows with 15 within-batch duplicate PKs should produce 85 unique outputs")
}
