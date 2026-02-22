package proto

//go:generate msgp

// Protocol version. Bump only on breaking wire changes.
const ProtocolVersion = 1

// Message type constants for the beam wire protocol.
// Control messages use stream 0; data messages use worker streams 1+.
const (
	// Control (stream 0).
	MsgHandshakeReq  byte = 0x01
	MsgHandshakeResp byte = 0x02
	MsgPingReq       byte = 0x03
	MsgPongResp      byte = 0x04

	// ReadEndpoint operations.
	MsgWalkReq    byte = 0x10
	MsgWalkEntry  byte = 0x11
	MsgWalkEnd    byte = 0x12
	MsgStatReq    byte = 0x13
	MsgStatResp   byte = 0x14
	MsgReadDirReq byte = 0x15
	MsgReadDirResp byte = 0x16
	MsgOpenReadReq byte = 0x17
	MsgReadData    byte = 0x18
	MsgReadDone    byte = 0x19
	MsgHashReq     byte = 0x1A
	MsgHashResp    byte = 0x1B

	// WriteEndpoint operations.
	MsgMkdirAllReq    byte = 0x20
	MsgAckResp        byte = 0x21
	MsgCreateTempReq  byte = 0x22
	MsgCreateTempResp byte = 0x23
	MsgWriteData      byte = 0x24
	MsgWriteDoneReq   byte = 0x25
	MsgWriteDoneResp  byte = 0x26
	MsgRenameReq      byte = 0x27
	MsgRemoveReq      byte = 0x29
	MsgRemoveAllReq   byte = 0x2B
	MsgSymlinkReq     byte = 0x2D
	MsgLinkReq        byte = 0x2F
	MsgSetMetadataReq byte = 0x31

	// Utility.
	MsgCapsReq  byte = 0x40
	MsgCapsResp byte = 0x41
	MsgErrorResp byte = 0xFF
)

// HandshakeReq is sent by the client on stream 0 after TLS connection.
type HandshakeReq struct {
	Version      int      `msg:"version"`
	Capabilities []string `msg:"capabilities"`
	AuthToken    string   `msg:"auth_token"`
}

// HandshakeResp is sent by the server on stream 0 in response to HandshakeReq.
type HandshakeResp struct {
	Version      int      `msg:"version"`
	Capabilities []string `msg:"capabilities"`
	Root         string   `msg:"root"`
}

// PingReq is a keep-alive request on stream 0.
type PingReq struct {
	Seq uint64 `msg:"seq"`
}

// PongResp is the response to a PingReq.
type PongResp struct {
	Seq uint64 `msg:"seq"`
}

// FileEntryMsg is the wire representation of transport.FileEntry.
// Uses map encoding (string keys) for backward compatibility.
type FileEntryMsg struct {
	RelPath    string `msg:"rel_path"`
	Size       int64  `msg:"size"`
	Mode       uint32 `msg:"mode"`
	ModTime    int64  `msg:"mod_time"`  // unix nanoseconds
	AccTime    int64  `msg:"acc_time"`  // unix nanoseconds
	IsDir      bool   `msg:"is_dir"`
	IsSymlink  bool   `msg:"is_symlink"`
	LinkTarget string `msg:"link_target"`
	UID        uint32 `msg:"uid"`
	GID        uint32 `msg:"gid"`
	Nlink      uint32 `msg:"nlink"`
	Dev        uint64 `msg:"dev"`
	Ino        uint64 `msg:"ino"`
}

// WalkReq requests a recursive walk of the endpoint.
type WalkReq struct{}

// WalkEntry is a single entry streamed during a walk.
type WalkEntry struct {
	Entry FileEntryMsg `msg:"entry"`
}

// WalkEnd signals the end of a walk stream.
type WalkEnd struct{}

// StatReq requests metadata for a single path.
type StatReq struct {
	RelPath string `msg:"rel_path"`
}

// StatResp returns metadata for a single path.
type StatResp struct {
	Entry FileEntryMsg `msg:"entry"`
}

// ReadDirReq requests the immediate children of a directory.
type ReadDirReq struct {
	RelPath string `msg:"rel_path"`
}

// ReadDirResp returns the immediate children of a directory.
type ReadDirResp struct {
	Entries []FileEntryMsg `msg:"entries"`
}

// OpenReadReq requests opening a file for reading.
type OpenReadReq struct {
	RelPath string `msg:"rel_path"`
}

// ReadDataMsg is a chunk of file data streamed during a read.
type ReadDataMsg struct {
	Data []byte `msg:"data"`
}

// ReadDone signals the end of a file read stream.
type ReadDone struct{}

// HashReq requests a server-side BLAKE3 hash of a file.
type HashReq struct {
	RelPath string `msg:"rel_path"`
}

// HashResp returns the BLAKE3 hex hash of a file.
type HashResp struct {
	Hash string `msg:"hash"`
}

// MkdirAllReq requests creating a directory and all parents.
type MkdirAllReq struct {
	RelPath string `msg:"rel_path"`
	Perm    uint32 `msg:"perm"`
}

// AckResp is a generic success acknowledgment.
type AckResp struct{}

// CreateTempReq requests creating a temporary file.
type CreateTempReq struct {
	RelPath string `msg:"rel_path"`
	Perm    uint32 `msg:"perm"`
}

// CreateTempResp returns the handle for an opened temp file.
type CreateTempResp struct {
	Handle string `msg:"handle"` // server-assigned handle ID
	Name   string `msg:"name"`   // relative path of temp file
}

// WriteDataMsg is a chunk of data to write to a temp file.
type WriteDataMsg struct {
	Handle string `msg:"handle"`
	Data   []byte `msg:"data"`
}

// WriteDoneReq signals that writing to a temp file is complete.
type WriteDoneReq struct {
	Handle string `msg:"handle"`
}

// WriteDoneResp acknowledges a completed write.
type WriteDoneResp struct{}

// RenameReq requests an atomic rename.
type RenameReq struct {
	OldRel string `msg:"old_rel"`
	NewRel string `msg:"new_rel"`
}

// RemoveReq requests deleting a single file.
type RemoveReq struct {
	RelPath string `msg:"rel_path"`
}

// RemoveAllReq requests recursively deleting a directory.
type RemoveAllReq struct {
	RelPath string `msg:"rel_path"`
}

// SymlinkReq requests creating a symbolic link.
type SymlinkReq struct {
	Target string `msg:"target"`
	NewRel string `msg:"new_rel"`
}

// LinkReq requests creating a hard link.
type LinkReq struct {
	OldRel string `msg:"old_rel"`
	NewRel string `msg:"new_rel"`
}

// SetMetadataReq requests setting file metadata.
type SetMetadataReq struct {
	RelPath string       `msg:"rel_path"`
	Entry   FileEntryMsg `msg:"entry"`
	Opts    MetadataOptsMsg `msg:"opts"`
}

// MetadataOptsMsg is the wire representation of transport.MetadataOpts.
type MetadataOptsMsg struct {
	Mode  bool `msg:"mode"`
	Times bool `msg:"times"`
	Owner bool `msg:"owner"`
	Xattr bool `msg:"xattr"`
}

// CapsReq requests the server's capabilities.
type CapsReq struct{}

// CapsResp returns the server's capabilities.
type CapsResp struct {
	SparseDetect bool `msg:"sparse_detect"`
	Hardlinks    bool `msg:"hardlinks"`
	Xattrs       bool `msg:"xattrs"`
	AtomicRename bool `msg:"atomic_rename"`
	FastCopy     bool `msg:"fast_copy"`
	NativeHash   bool `msg:"native_hash"`
}

// ErrorResp is a generic error response for any request.
type ErrorResp struct {
	Message string `msg:"message"`
	Code    int    `msg:"code"` // 0 = generic, reserved for future use
}
