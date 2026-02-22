package transport

import (
	"io"
	"math"

	"github.com/cespare/xxhash/v2"
	"github.com/zeebo/blake3"
)

// deltaMinFileSize is the minimum file size for delta transfer to be worthwhile.
// Below this, a full copy is cheaper than computing signatures.
const deltaMinFileSize = 64 * 1024

// DeltaMinFileSize returns the minimum file size for delta transfer.
func DeltaMinFileSize() int64 {
	return deltaMinFileSize
}

// BlockSignature holds weak+strong hashes for a single block in the basis file.
type BlockSignature struct {
	Index      int
	Offset     int64
	WeakHash   uint64
	StrongHash [32]byte
}

// Signature holds the block-level signature of a basis file.
type Signature struct {
	Blocks    []BlockSignature
	BlockSize int
	FileSize  int64
}

// DeltaOp is a single instruction for reconstructing a file.
// If BlockIdx >= 0, copy that block from the basis file.
// Otherwise, Literal contains new data.
type DeltaOp struct {
	Literal  []byte // non-nil only for literal ops
	Offset   int64  // offset in basis file (for block ops)
	BlockIdx int    // -1 = literal data, >= 0 = copy from basis
	Length   int    // length of block or literal
}

// ChooseBlockSize selects an appropriate block size for a file.
// Uses sqrt(fileSize) clamped to [512, 128KB].
func ChooseBlockSize(fileSize int64) int {
	bs := int(math.Sqrt(float64(fileSize)))
	if bs < 512 {
		bs = 512
	}
	if bs > 131072 {
		bs = 131072
	}
	return bs
}

// ComputeSignature reads the entire basis file and produces a Signature
// with weak (xxHash) and strong (BLAKE3) hashes per block.
//
//nolint:revive // cognitive-complexity: block-by-block hashing with EOF handling
func ComputeSignature(r io.Reader, fileSize int64) (Signature, error) {
	blockSize := ChooseBlockSize(fileSize)
	sig := Signature{
		BlockSize: blockSize,
		FileSize:  fileSize,
	}

	buf := make([]byte, blockSize)
	var offset int64
	idx := 0

	for {
		n, err := io.ReadFull(r, buf)
		if n > 0 {
			block := buf[:n]
			weak := xxhash.Sum64(block)
			strong := blake3.Sum256(block)

			sig.Blocks = append(sig.Blocks, BlockSignature{
				Index:      idx,
				Offset:     offset,
				WeakHash:   weak,
				StrongHash: strong,
			})
			offset += int64(n)
			idx++
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return Signature{}, err
		}
	}

	return sig, nil
}

// MatchBlocks reads the source file and matches it against the basis
// signature, producing a list of DeltaOps. Matching blocks reference
// the basis file; non-matching regions become literal data.
//
//nolint:gocyclo,revive // cyclomatic: rsync-style rolling hash block matching is inherently complex
func MatchBlocks(src io.Reader, sig Signature) ([]DeltaOp, error) {
	if len(sig.Blocks) == 0 {
		// No basis blocks — entire file is literal.
		data, err := io.ReadAll(src)
		if err != nil {
			return nil, err
		}
		if len(data) == 0 {
			return nil, nil
		}
		return []DeltaOp{{BlockIdx: -1, Length: len(data), Literal: data}}, nil
	}

	// Build weak hash → block index lookup.
	// Multiple blocks can share a weak hash; store all candidates.
	type candidate struct {
		index  int
		strong [32]byte
		offset int64
	}
	weakMap := make(map[uint64][]candidate, len(sig.Blocks))
	for _, b := range sig.Blocks {
		weakMap[b.WeakHash] = append(weakMap[b.WeakHash], candidate{
			index:  b.Index,
			strong: b.StrongHash,
			offset: b.Offset,
		})
	}

	blockSize := sig.BlockSize
	srcData, err := io.ReadAll(src)
	if err != nil {
		return nil, err
	}

	var ops []DeltaOp
	var literalBuf []byte

	flushLiteral := func() {
		if len(literalBuf) > 0 {
			ops = append(ops, DeltaOp{
				BlockIdx: -1,
				Length:   len(literalBuf),
				Literal:  literalBuf,
			})
			literalBuf = nil
		}
	}

	i := 0
	for i < len(srcData) {
		// Try to match a block at position i.
		end := i + blockSize
		if end > len(srcData) {
			end = len(srcData)
		}
		chunk := srcData[i:end]

		matched := false
		if len(chunk) >= blockSize || (len(chunk) > 0 && i+len(chunk) == len(srcData)) {
			weak := xxhash.Sum64(chunk)
			if candidates, ok := weakMap[weak]; ok {
				strong := blake3.Sum256(chunk)
				for _, c := range candidates {
					if c.strong == strong {
						flushLiteral()
						ops = append(ops, DeltaOp{
							BlockIdx: c.index,
							Offset:   c.offset,
							Length:   len(chunk),
						})
						i += len(chunk)
						matched = true
						break
					}
				}
			}
		}

		if !matched {
			literalBuf = append(literalBuf, srcData[i])
			i++
		}
	}

	flushLiteral()
	return ops, nil
}

// ApplyDelta reconstructs a file by applying DeltaOps against a basis file.
//
//nolint:revive // cognitive-complexity: delta application with basis seek and literal writes
func ApplyDelta(basis io.ReadSeeker, ops []DeltaOp, dst io.Writer) error {
	for _, op := range ops {
		if op.BlockIdx >= 0 {
			// Copy from basis.
			if _, err := basis.Seek(op.Offset, io.SeekStart); err != nil {
				return err
			}
			buf := make([]byte, op.Length)
			if _, err := io.ReadFull(basis, buf); err != nil {
				return err
			}
			if _, err := dst.Write(buf); err != nil {
				return err
			}
		} else {
			// Write literal data.
			if _, err := dst.Write(op.Literal); err != nil {
				return err
			}
		}
	}
	return nil
}

// DeltaStats returns the number of matched blocks and literal bytes in a delta.
func DeltaStats(ops []DeltaOp) (matchedBlocks int, literalBytes int64) {
	for _, op := range ops {
		if op.BlockIdx >= 0 {
			matchedBlocks++
		} else {
			literalBytes += int64(op.Length)
		}
	}
	return matchedBlocks, literalBytes
}
