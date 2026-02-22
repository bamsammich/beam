package proto_test

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp" // for manual msgpack construction in TestUnknownFieldsIgnored

	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/proto"
)

func TestHandshakeRoundTrip(t *testing.T) {
	t.Parallel()

	orig := proto.HandshakeReq{
		Version:      1,
		Capabilities: []string{"walk", "hash"},
		AuthToken:    "secret-token-123",
	}

	data, err := orig.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded proto.HandshakeReq
	_, err = decoded.UnmarshalMsg(data)
	require.NoError(t, err)

	assert.Equal(t, orig, decoded)
}

func TestFileEntryMsgRoundTrip(t *testing.T) {
	t.Parallel()

	orig := proto.FileEntryMsg{
		RelPath:    "dir/file.txt",
		Size:       42000,
		Mode:       0o644,
		ModTime:    time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC).UnixNano(),
		AccTime:    time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC).UnixNano(),
		IsDir:      false,
		IsSymlink:  false,
		LinkTarget: "",
		UID:        1000,
		GID:        1000,
		Nlink:      2,
		Dev:        66306,
		Ino:        123456,
	}

	data, err := orig.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded proto.FileEntryMsg
	_, err = decoded.UnmarshalMsg(data)
	require.NoError(t, err)

	assert.Equal(t, orig, decoded)
}

func TestFileEntryConversion(t *testing.T) {
	t.Parallel()

	now := time.Now().Truncate(time.Nanosecond)
	entry := transport.FileEntry{
		RelPath:    "sub/dir/file.bin",
		Size:       1024 * 1024,
		Mode:       0o755,
		ModTime:    now,
		AccTime:    now.Add(-time.Hour),
		IsDir:      false,
		IsSymlink:  true,
		LinkTarget: "../other",
		UID:        501,
		GID:        20,
		Nlink:      1,
		Dev:        123,
		Ino:        456,
	}

	msg := proto.FromFileEntry(entry)
	roundTripped := proto.ToFileEntry(msg)

	assert.Equal(t, entry.RelPath, roundTripped.RelPath)
	assert.Equal(t, entry.Size, roundTripped.Size)
	assert.Equal(t, entry.Mode, roundTripped.Mode)
	assert.Equal(t, entry.ModTime.UnixNano(), roundTripped.ModTime.UnixNano())
	assert.Equal(t, entry.AccTime.UnixNano(), roundTripped.AccTime.UnixNano())
	assert.Equal(t, entry.IsDir, roundTripped.IsDir)
	assert.Equal(t, entry.IsSymlink, roundTripped.IsSymlink)
	assert.Equal(t, entry.LinkTarget, roundTripped.LinkTarget)
	assert.Equal(t, entry.UID, roundTripped.UID)
	assert.Equal(t, entry.GID, roundTripped.GID)
	assert.Equal(t, entry.Nlink, roundTripped.Nlink)
	assert.Equal(t, entry.Dev, roundTripped.Dev)
	assert.Equal(t, entry.Ino, roundTripped.Ino)
}

func TestCapsConversion(t *testing.T) {
	t.Parallel()

	caps := transport.Capabilities{
		SparseDetect: true,
		Hardlinks:    true,
		Xattrs:       false,
		AtomicRename: true,
		FastCopy:     false,
		NativeHash:   true,
	}

	msg := proto.FromCaps(caps)
	roundTripped := proto.ToCaps(msg)

	assert.Equal(t, caps, roundTripped)
}

func TestMetadataOptsConversion(t *testing.T) {
	t.Parallel()

	opts := transport.MetadataOpts{
		Mode:  true,
		Times: true,
		Owner: false,
		Xattr: true,
	}

	msg := proto.FromMetadataOpts(opts)
	roundTripped := proto.ToMetadataOpts(msg)

	assert.Equal(t, opts, roundTripped)
}

func TestMapEncoding(t *testing.T) {
	t.Parallel()

	// Verify that messages use map encoding (string keys, not positional arrays).
	orig := proto.HandshakeReq{
		Version:      1,
		Capabilities: []string{"test"},
		AuthToken:    "tok",
	}

	data, err := orig.MarshalMsg(nil)
	require.NoError(t, err)

	// Parse the first byte to check it's a msgpack map (fixmap 0x80-0x8f, map16 0xde, map32 0xdf).
	// Array encoding would be fixarray 0x90-0x9f, array16 0xdc, array32 0xdd.
	require.NotEmpty(t, data)
	firstByte := data[0]
	isMap := (firstByte >= 0x80 && firstByte <= 0x8f) || firstByte == 0xde || firstByte == 0xdf
	assert.True(t, isMap, "expected map encoding, got first byte 0x%02x", firstByte)
}

func TestErrorRespRoundTrip(t *testing.T) {
	t.Parallel()

	orig := proto.ErrorResp{
		Message: "file not found: foo/bar.txt",
		Code:    0,
	}

	data, err := orig.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded proto.ErrorResp
	_, err = decoded.UnmarshalMsg(data)
	require.NoError(t, err)

	assert.Equal(t, orig, decoded)
}

func TestUnknownFieldsIgnored(t *testing.T) {
	t.Parallel()

	// Simulate a newer version sending extra fields.
	// Encode a map with known fields plus an unknown one.
	var buf []byte
	buf = msgp.AppendMapHeader(buf, 4) // 3 known + 1 unknown
	buf = msgp.AppendString(buf, "version")
	buf = msgp.AppendInt(buf, 2)
	buf = msgp.AppendString(buf, "capabilities")
	buf = msgp.AppendArrayHeader(buf, 0)
	buf = msgp.AppendString(buf, "auth_token")
	buf = msgp.AppendString(buf, "mytoken")
	buf = msgp.AppendString(buf, "future_field")
	buf = msgp.AppendString(buf, "future_value")

	var decoded proto.HandshakeReq
	_, err := decoded.UnmarshalMsg(buf)
	require.NoError(t, err)

	assert.Equal(t, 2, decoded.Version)
	assert.Equal(t, "mytoken", decoded.AuthToken)
}

func TestSetMetadataReqRoundTrip(t *testing.T) {
	t.Parallel()

	orig := proto.SetMetadataReq{
		RelPath: "some/file.txt",
		Entry: proto.FileEntryMsg{
			RelPath: "some/file.txt",
			Mode:    uint32(os.FileMode(0o644)),
			ModTime: time.Now().UnixNano(),
			UID:     1000,
			GID:     1000,
		},
		Opts: proto.MetadataOptsMsg{
			Mode:  true,
			Times: true,
			Owner: true,
			Xattr: false,
		},
	}

	data, err := orig.MarshalMsg(nil)
	require.NoError(t, err)

	var decoded proto.SetMetadataReq
	_, err = decoded.UnmarshalMsg(data)
	require.NoError(t, err)

	assert.Equal(t, orig, decoded)
}
