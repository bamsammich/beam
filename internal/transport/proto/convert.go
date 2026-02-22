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
		Uid:        m.UID,
		Gid:        m.GID,
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
		UID:        e.Uid,
		GID:        e.Gid,
		Nlink:      e.Nlink,
		Dev:        e.Dev,
		Ino:        e.Ino,
	}
}

// ToCaps converts a CapsResp to a transport.Capabilities.
func ToCaps(c CapsResp) transport.Capabilities {
	return transport.Capabilities{
		SparseDetect: c.SparseDetect,
		Hardlinks:    c.Hardlinks,
		Xattrs:       c.Xattrs,
		AtomicRename: c.AtomicRename,
		FastCopy:     c.FastCopy,
		NativeHash:   c.NativeHash,
	}
}

// FromCaps converts a transport.Capabilities to a CapsResp.
func FromCaps(c transport.Capabilities) CapsResp {
	return CapsResp{
		SparseDetect: c.SparseDetect,
		Hardlinks:    c.Hardlinks,
		Xattrs:       c.Xattrs,
		AtomicRename: c.AtomicRename,
		FastCopy:     c.FastCopy,
		NativeHash:   c.NativeHash,
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
