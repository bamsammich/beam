package proto

import (
	"os"
	"time"

	"github.com/bamsammich/beam/internal/transport"
)

// ToFileEntry converts a wire FileEntryMsg to a transport.FileEntry.
func ToFileEntry(m FileEntryMsg) transport.FileEntry {
	return transport.FileEntry{
		RelPath:    m.RelPath,
		Size:       m.Size,
		Mode:       os.FileMode(m.Mode),
		ModTime:    time.Unix(0, m.ModTime),
		AccTime:    time.Unix(0, m.AccTime),
		IsDir:      m.IsDir,
		IsSymlink:  m.IsSymlink,
		LinkTarget: m.LinkTarget,
		UID:        m.UID,
		GID:        m.GID,
		Nlink:      m.Nlink,
		Dev:        m.Dev,
		Ino:        m.Ino,
	}
}

// FromFileEntry converts a transport.FileEntry to a wire FileEntryMsg.
func FromFileEntry(e transport.FileEntry) FileEntryMsg {
	return FileEntryMsg{
		RelPath:    e.RelPath,
		Size:       e.Size,
		Mode:       uint32(e.Mode),
		ModTime:    e.ModTime.UnixNano(),
		AccTime:    e.AccTime.UnixNano(),
		IsDir:      e.IsDir,
		IsSymlink:  e.IsSymlink,
		LinkTarget: e.LinkTarget,
		UID:        e.UID,
		GID:        e.GID,
		Nlink:      e.Nlink,
		Dev:        e.Dev,
		Ino:        e.Ino,
	}
}

// ToCaps converts a CapsResp to a transport.Capabilities.
func ToCaps(c CapsResp) transport.Capabilities {
	return transport.Capabilities{
		SparseDetect:  c.SparseDetect,
		Hardlinks:     c.Hardlinks,
		Xattrs:        c.Xattrs,
		AtomicRename:  c.AtomicRename,
		FastCopy:      c.FastCopy,
		NativeHash:    c.NativeHash,
		DeltaTransfer: c.DeltaTransfer,
	}
}

// FromCaps converts a transport.Capabilities to a CapsResp.
func FromCaps(c transport.Capabilities) CapsResp {
	return CapsResp{
		SparseDetect:  c.SparseDetect,
		Hardlinks:     c.Hardlinks,
		Xattrs:        c.Xattrs,
		AtomicRename:  c.AtomicRename,
		FastCopy:      c.FastCopy,
		NativeHash:    c.NativeHash,
		DeltaTransfer: c.DeltaTransfer,
	}
}

// ToMetadataOpts converts a wire MetadataOptsMsg to a transport.MetadataOpts.
func ToMetadataOpts(m MetadataOptsMsg) transport.MetadataOpts {
	return transport.MetadataOpts{
		Mode:  m.Mode,
		Times: m.Times,
		Owner: m.Owner,
		Xattr: m.Xattr,
	}
}

// FromMetadataOpts converts a transport.MetadataOpts to a wire MetadataOptsMsg.
func FromMetadataOpts(o transport.MetadataOpts) MetadataOptsMsg {
	return MetadataOptsMsg{
		Mode:  o.Mode,
		Times: o.Times,
		Owner: o.Owner,
		Xattr: o.Xattr,
	}
}

// ToSignature converts wire BlockSignatureMsg slice to a transport.Signature.
func ToSignature(blockSize int, msgs []BlockSignatureMsg) transport.Signature {
	blocks := make([]transport.BlockSignature, len(msgs))
	for i, m := range msgs {
		var strong [32]byte
		copy(strong[:], m.StrongHash)
		blocks[i] = transport.BlockSignature{
			Index:      m.Index,
			Offset:     m.Offset,
			WeakHash:   m.WeakHash,
			StrongHash: strong,
		}
	}
	return transport.Signature{BlockSize: blockSize, Blocks: blocks}
}

// FromSignature converts a transport.Signature to wire BlockSignatureMsg slice.
func FromSignature(sig transport.Signature) (int, []BlockSignatureMsg) {
	msgs := make([]BlockSignatureMsg, len(sig.Blocks))
	for i, b := range sig.Blocks {
		msgs[i] = BlockSignatureMsg{
			Index:      b.Index,
			Offset:     b.Offset,
			WeakHash:   b.WeakHash,
			StrongHash: b.StrongHash[:],
		}
	}
	return sig.BlockSize, msgs
}

// ToDeltaOps converts wire DeltaOpMsg slice to transport DeltaOps.
func ToDeltaOps(msgs []DeltaOpMsg) []transport.DeltaOp {
	ops := make([]transport.DeltaOp, len(msgs))
	for i, m := range msgs {
		ops[i] = transport.DeltaOp{
			BlockIdx: m.BlockIdx,
			Offset:   m.Offset,
			Length:   m.Length,
			Literal:  m.Literal,
		}
	}
	return ops
}

// FromDeltaOps converts transport DeltaOps to wire DeltaOpMsg slice.
func FromDeltaOps(ops []transport.DeltaOp) []DeltaOpMsg {
	msgs := make([]DeltaOpMsg, len(ops))
	for i, op := range ops {
		msgs[i] = DeltaOpMsg{
			BlockIdx: op.BlockIdx,
			Offset:   op.Offset,
			Length:   op.Length,
			Literal:  op.Literal,
		}
	}
	return msgs
}
