package transport

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChooseBlockSize(t *testing.T) {
	tests := []struct {
		fileSize int64
		wantMin  int
		wantMax  int
	}{
		{100, 512, 512},          // clamped to min
		{256 * 1024, 512, 1024},  // sqrt(256K) ~= 512
		{1024 * 1024, 512, 1200}, // sqrt(1M) ~= 1024
		{1 << 30, 32000, 33000},  // sqrt(1G) ~= 32768
		{1 << 40, 131072, 131072}, // clamped to max
	}

	for _, tt := range tests {
		bs := ChooseBlockSize(tt.fileSize)
		assert.GreaterOrEqual(t, bs, tt.wantMin, "fileSize=%d", tt.fileSize)
		assert.LessOrEqual(t, bs, tt.wantMax, "fileSize=%d", tt.fileSize)
	}
}

func TestDelta_IdenticalFiles(t *testing.T) {
	// Two identical files: delta should be all block matches, zero literals.
	data := makeTestData(t, 4096)

	sig, err := ComputeSignature(bytes.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	assert.Greater(t, len(sig.Blocks), 0)

	ops, err := MatchBlocks(bytes.NewReader(data), sig)
	require.NoError(t, err)

	matched, literal := DeltaStats(ops)
	assert.Greater(t, matched, 0)
	assert.Equal(t, int64(0), literal)

	// Reconstruct and verify.
	var out bytes.Buffer
	require.NoError(t, ApplyDelta(bytes.NewReader(data), ops, &out))
	assert.Equal(t, data, out.Bytes())
}

func TestDelta_CompletelyDifferent(t *testing.T) {
	// Two completely different files: delta should be all literal.
	basis := makeTestData(t, 4096)
	source := makeTestData(t, 4096)

	sig, err := ComputeSignature(bytes.NewReader(basis), int64(len(basis)))
	require.NoError(t, err)

	ops, err := MatchBlocks(bytes.NewReader(source), sig)
	require.NoError(t, err)

	matched, literal := DeltaStats(ops)
	assert.Equal(t, 0, matched)
	assert.Equal(t, int64(len(source)), literal)

	// Reconstruct and verify.
	var out bytes.Buffer
	require.NoError(t, ApplyDelta(bytes.NewReader(basis), ops, &out))
	assert.Equal(t, source, out.Bytes())
}

func TestDelta_PartialMatch(t *testing.T) {
	// Source is basis with some blocks modified.
	basis := bytes.Repeat([]byte("ABCDEFGHIJKLMNOP"), 256) // 4KB
	source := make([]byte, len(basis))
	copy(source, basis)

	// Modify the middle section.
	blockSize := ChooseBlockSize(int64(len(basis)))
	midOffset := blockSize * 2
	if midOffset+blockSize <= len(source) {
		for i := midOffset; i < midOffset+blockSize; i++ {
			source[i] = 'X'
		}
	}

	sig, err := ComputeSignature(bytes.NewReader(basis), int64(len(basis)))
	require.NoError(t, err)

	ops, err := MatchBlocks(bytes.NewReader(source), sig)
	require.NoError(t, err)

	matched, literal := DeltaStats(ops)
	assert.Greater(t, matched, 0, "should have some matching blocks")
	assert.Greater(t, literal, int64(0), "should have some literal data")
	assert.Less(t, literal, int64(len(source)), "should not be all literal")

	// Reconstruct and verify.
	var out bytes.Buffer
	require.NoError(t, ApplyDelta(bytes.NewReader(basis), ops, &out))
	assert.Equal(t, source, out.Bytes())
}

func TestDelta_EmptyFiles(t *testing.T) {
	// Both files empty.
	sig, err := ComputeSignature(bytes.NewReader(nil), 0)
	require.NoError(t, err)
	assert.Empty(t, sig.Blocks)

	ops, err := MatchBlocks(bytes.NewReader(nil), sig)
	require.NoError(t, err)
	assert.Empty(t, ops)

	var out bytes.Buffer
	require.NoError(t, ApplyDelta(bytes.NewReader(nil), ops, &out))
	assert.Empty(t, out.Bytes())
}

func TestDelta_SourceLargerThanBasis(t *testing.T) {
	// Source has additional data appended after matching basis.
	basis := makeTestData(t, 2048)
	extra := makeTestData(t, 1024)
	source := append(append([]byte{}, basis...), extra...)

	sig, err := ComputeSignature(bytes.NewReader(basis), int64(len(basis)))
	require.NoError(t, err)

	ops, err := MatchBlocks(bytes.NewReader(source), sig)
	require.NoError(t, err)

	matched, literal := DeltaStats(ops)
	assert.Greater(t, matched, 0, "basis portion should match")
	assert.Greater(t, literal, int64(0), "extra portion is literal")

	// Reconstruct and verify.
	var out bytes.Buffer
	require.NoError(t, ApplyDelta(bytes.NewReader(basis), ops, &out))
	assert.Equal(t, source, out.Bytes())
}

func TestDelta_Roundtrip_Large(t *testing.T) {
	// 256KB file with a few blocks changed.
	size := 256 * 1024
	basis := makeTestData(t, size)
	source := make([]byte, size)
	copy(source, basis)

	// Flip a few regions.
	blockSize := ChooseBlockSize(int64(size))
	for i := blockSize; i < blockSize*2 && i < size; i++ {
		source[i] ^= 0xFF
	}

	sig, err := ComputeSignature(bytes.NewReader(basis), int64(len(basis)))
	require.NoError(t, err)

	ops, err := MatchBlocks(bytes.NewReader(source), sig)
	require.NoError(t, err)

	var out bytes.Buffer
	require.NoError(t, ApplyDelta(bytes.NewReader(basis), ops, &out))
	assert.Equal(t, source, out.Bytes())

	matched, literal := DeltaStats(ops)
	t.Logf("blockSize=%d blocks=%d matched=%d literal=%d", blockSize, len(sig.Blocks), matched, literal)
	assert.Greater(t, matched, 1, "most blocks should match")
}

func makeTestData(t *testing.T, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	_, err := io.ReadFull(rand.Reader, data)
	require.NoError(t, err)
	return data
}
