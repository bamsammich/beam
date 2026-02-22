package transport

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/zeebo/blake3"
	"golang.org/x/sys/unix"
)

// Compile-time interface checks.
var (
	_ ReadEndpoint  = (*LocalReadEndpoint)(nil)
	_ WriteEndpoint = (*LocalWriteEndpoint)(nil)
)

// LocalReadEndpoint reads from the local filesystem.
type LocalReadEndpoint struct {
	root string
}

// NewLocalReadEndpoint creates a new local read endpoint rooted at root.
func NewLocalReadEndpoint(root string) *LocalReadEndpoint {
	return &LocalReadEndpoint{root: root}
}

func (e *LocalReadEndpoint) Walk(fn func(entry FileEntry) error) error {
	return filepath.WalkDir(e.root, func(path string, _ os.DirEntry, err error) error {
		if err != nil {
			return nil // skip inaccessible entries
		}
		relPath, err := filepath.Rel(e.root, path)
		if err != nil || relPath == "." {
			return nil
		}
		entry, err := e.statAbsolute(path, relPath)
		if err != nil {
			return nil
		}
		return fn(entry)
	})
}

func (e *LocalReadEndpoint) Stat(relPath string) (FileEntry, error) {
	absPath := filepath.Join(e.root, relPath)
	return e.statAbsolute(absPath, relPath)
}

func (e *LocalReadEndpoint) ReadDir(relPath string) ([]FileEntry, error) {
	absPath := filepath.Join(e.root, relPath)
	entries, err := os.ReadDir(absPath)
	if err != nil {
		return nil, fmt.Errorf("readdir %s: %w", absPath, err)
	}

	result := make([]FileEntry, 0, len(entries))
	for _, d := range entries {
		childRel := filepath.Join(relPath, d.Name())
		childAbs := filepath.Join(absPath, d.Name())
		entry, err := e.statAbsolute(childAbs, childRel)
		if err != nil {
			continue
		}
		result = append(result, entry)
	}
	return result, nil
}

func (e *LocalReadEndpoint) OpenRead(relPath string) (io.ReadCloser, error) {
	absPath := filepath.Join(e.root, relPath)
	return os.Open(absPath)
}

func (e *LocalReadEndpoint) Hash(relPath string) (string, error) {
	absPath := filepath.Join(e.root, relPath)
	return hashLocalFile(absPath)
}

func (e *LocalReadEndpoint) Root() string { return e.root }
func (*LocalReadEndpoint) Close() error   { return nil }

func (*LocalReadEndpoint) Caps() Capabilities {
	return Capabilities{
		SparseDetect:  true,
		Hardlinks:     true,
		Xattrs:        true,
		AtomicRename:  true,
		FastCopy:      true,
		NativeHash:    true,
		DeltaTransfer: true,
	}
}

// AbsPath returns the absolute path for a relative path. This is the
// escape hatch for local-only operations that need raw filesystem access
// (e.g. platform.CopyFile, DetectSparseSegments).
func (e *LocalReadEndpoint) AbsPath(relPath string) string {
	return filepath.Join(e.root, relPath)
}

func (*LocalReadEndpoint) statAbsolute(absPath, relPath string) (FileEntry, error) {
	info, err := os.Lstat(absPath)
	if err != nil {
		return FileEntry{}, err
	}
	return fileInfoToEntry(info, relPath, absPath)
}

// LocalWriteEndpoint writes to the local filesystem.
type LocalWriteEndpoint struct {
	root string
}

// NewLocalWriteEndpoint creates a new local write endpoint rooted at root.
func NewLocalWriteEndpoint(root string) *LocalWriteEndpoint {
	return &LocalWriteEndpoint{root: root}
}

func (e *LocalWriteEndpoint) MkdirAll(relPath string, perm os.FileMode) error {
	absPath := filepath.Join(e.root, relPath)
	return os.MkdirAll(absPath, perm)
}

//nolint:ireturn // implements WriteEndpoint interface
func (e *LocalWriteEndpoint) CreateTemp(relPath string, perm os.FileMode) (WriteFile, error) {
	absPath := filepath.Join(e.root, relPath)
	dir := filepath.Dir(absPath)
	base := filepath.Base(absPath)
	tmpName := fmt.Sprintf(".%s.%s.beam-tmp", base, uuid.New().String()[:8])
	tmpPath := filepath.Join(dir, tmpName)

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return nil, fmt.Errorf("create temp %s: %w", tmpPath, err)
	}
	return &localWriteFile{File: f, relPath: relPathFromRoot(e.root, tmpPath)}, nil
}

func (e *LocalWriteEndpoint) Rename(oldRel, newRel string) error {
	oldAbs := filepath.Join(e.root, oldRel)
	newAbs := filepath.Join(e.root, newRel)
	return os.Rename(oldAbs, newAbs)
}

func (e *LocalWriteEndpoint) Remove(relPath string) error {
	absPath := filepath.Join(e.root, relPath)
	return os.Remove(absPath)
}

func (e *LocalWriteEndpoint) RemoveAll(relPath string) error {
	absPath := filepath.Join(e.root, relPath)
	return os.RemoveAll(absPath)
}

func (e *LocalWriteEndpoint) Symlink(target, newRel string) error {
	absNew := filepath.Join(e.root, newRel)
	_ = os.Remove(absNew)
	return os.Symlink(target, absNew)
}

func (e *LocalWriteEndpoint) Link(oldRel, newRel string) error {
	oldAbs := filepath.Join(e.root, oldRel)
	newAbs := filepath.Join(e.root, newRel)
	_ = os.Remove(newAbs)
	return os.Link(oldAbs, newAbs)
}

func (e *LocalWriteEndpoint) SetMetadata(relPath string, entry FileEntry, opts MetadataOpts) error {
	absPath := filepath.Join(e.root, relPath)

	if opts.Mode {
		if err := os.Chmod(absPath, entry.Mode.Perm()); err != nil {
			return fmt.Errorf("chmod %s: %w", relPath, err)
		}
	}

	if opts.Times {
		atime := unix.NsecToTimespec(entry.AccTime.UnixNano())
		mtime := unix.NsecToTimespec(entry.ModTime.UnixNano())
		times := []unix.Timespec{atime, mtime}
		if err := unix.UtimesNanoAt(
			unix.AT_FDCWD,
			absPath,
			times,
			unix.AT_SYMLINK_NOFOLLOW,
		); err != nil {
			return fmt.Errorf("utimensat %s: %w", relPath, err)
		}
	}

	if opts.Owner {
		//nolint:errcheck // best-effort ownership; may fail without root
		_ = syscall.Lchown(absPath, int(entry.UID), int(entry.GID))
	}

	return nil
}

//nolint:revive // cognitive-complexity: directory walk with multiple error checks
func (e *LocalWriteEndpoint) Walk(fn func(entry FileEntry) error) error {
	return filepath.WalkDir(e.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		relPath, err := filepath.Rel(e.root, path)
		if err != nil || relPath == "." {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		entry, err := fileInfoToEntry(info, relPath, path)
		if err != nil {
			return nil
		}
		return fn(entry)
	})
}

func (e *LocalWriteEndpoint) Stat(relPath string) (FileEntry, error) {
	absPath := filepath.Join(e.root, relPath)
	info, err := os.Lstat(absPath)
	if err != nil {
		return FileEntry{}, err
	}
	return fileInfoToEntry(info, relPath, absPath)
}

func (e *LocalWriteEndpoint) OpenRead(relPath string) (io.ReadCloser, error) {
	absPath := filepath.Join(e.root, relPath)
	return os.Open(absPath)
}

func (e *LocalWriteEndpoint) Hash(relPath string) (string, error) {
	absPath := filepath.Join(e.root, relPath)
	return hashLocalFile(absPath)
}

func (e *LocalWriteEndpoint) Root() string { return e.root }

func (*LocalWriteEndpoint) Close() error { return nil }
func (*LocalWriteEndpoint) Caps() Capabilities {
	return Capabilities{
		SparseDetect:  true,
		Hardlinks:     true,
		Xattrs:        true,
		AtomicRename:  true,
		FastCopy:      true,
		NativeHash:    true,
		DeltaTransfer: true,
	}
}

// AbsPath returns the absolute path for a relative path. This is the
// escape hatch for local-only operations that need raw fd access.
func (e *LocalWriteEndpoint) AbsPath(relPath string) string {
	return filepath.Join(e.root, relPath)
}

// LocalFile extracts the underlying *os.File from a WriteFile created by
// this endpoint. Returns nil if wf is not from a LocalWriteEndpoint.
func LocalFile(wf WriteFile) *os.File {
	if lf, ok := wf.(*localWriteFile); ok {
		return lf.File
	}
	return nil
}

// localWriteFile wraps *os.File to implement WriteFile.
type localWriteFile struct {
	*os.File
	relPath string
}

// Name returns the relative path of the temp file within the endpoint root.
// This overrides *os.File.Name() so that callers using the WriteFile interface
// get a path usable with endpoint methods (Remove, Rename, etc.).
func (f *localWriteFile) Name() string {
	return f.relPath
}

// RelPath returns the relative path of the temp file within the endpoint root.
func (f *localWriteFile) RelPath() string {
	return f.relPath
}

// TestableWriteFile is an alias for testing access to RelPath.
// It is only exported for use in tests.
type TestableWriteFile = localWriteFile

func relPathFromRoot(root, absPath string) string {
	rel, err := filepath.Rel(root, absPath)
	if err != nil {
		return absPath
	}
	return rel
}

// fileInfoToEntry converts os.FileInfo + metadata to FileEntry.
func fileInfoToEntry(info os.FileInfo, relPath, absPath string) (FileEntry, error) {
	entry := FileEntry{
		RelPath: relPath,
		Size:    info.Size(),
		Mode:    info.Mode(),
		ModTime: info.ModTime(),
		IsDir:   info.IsDir(),
	}

	if info.Mode()&os.ModeSymlink != 0 {
		entry.IsSymlink = true
		target, err := os.Readlink(absPath)
		if err == nil {
			entry.LinkTarget = target
		}
	}

	// Extract Unix-specific metadata from syscall.Stat_t.
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		entry.UID = stat.Uid
		entry.GID = stat.Gid
		entry.Nlink = uint32(stat.Nlink) //nolint:gosec // G115: nlink fits in uint32 on Linux
		entry.Dev = stat.Dev
		entry.Ino = stat.Ino
		entry.AccTime = time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
	}

	return entry, nil
}

func hashLocalFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	h := blake3.New()
	buf := make([]byte, 32*1024)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return "", fmt.Errorf("hash %s: %w", path, err)
	}

	digest := h.Sum(nil)
	return hex.EncodeToString(digest), nil
}
