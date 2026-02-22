package transport

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/pkg/sftp"
	"github.com/zeebo/blake3"
	"golang.org/x/crypto/ssh"
)

// Compile-time interface checks.
var (
	_ ReadEndpoint  = (*SFTPReadEndpoint)(nil)
	_ WriteEndpoint = (*SFTPWriteEndpoint)(nil)
)

// SFTPReadEndpoint reads from a remote filesystem over SFTP.
type SFTPReadEndpoint struct {
	client *sftp.Client
	ssh    *ssh.Client
	root   string
}

// NewSFTPReadEndpoint creates a read endpoint backed by an SFTP connection.
// The caller must call Close when done.
func NewSFTPReadEndpoint(sshClient *ssh.Client, root string) (*SFTPReadEndpoint, error) {
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, fmt.Errorf("sftp client: %w", err)
	}
	return &SFTPReadEndpoint{
		client: sftpClient,
		ssh:    sshClient,
		root:   root,
	}, nil
}

func (e *SFTPReadEndpoint) Walk(fn func(entry FileEntry) error) error {
	walker := e.client.Walk(e.root)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			continue
		}
		relPath, err := filepath.Rel(e.root, walker.Path())
		if err != nil || relPath == "." {
			continue
		}
		entry := sftpFileInfoToEntry(walker.Stat(), relPath)
		if entry.IsSymlink {
			target, err := e.client.ReadLink(walker.Path())
			if err == nil {
				entry.LinkTarget = target
			}
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (e *SFTPReadEndpoint) Stat(relPath string) (FileEntry, error) {
	absPath := path.Join(e.root, relPath)
	info, err := e.client.Lstat(absPath)
	if err != nil {
		return FileEntry{}, err
	}
	entry := sftpFileInfoToEntry(info, relPath)
	if entry.IsSymlink {
		target, err := e.client.ReadLink(absPath)
		if err == nil {
			entry.LinkTarget = target
		}
	}
	return entry, nil
}

func (e *SFTPReadEndpoint) ReadDir(relPath string) ([]FileEntry, error) {
	absPath := path.Join(e.root, relPath)
	infos, err := e.client.ReadDir(absPath)
	if err != nil {
		return nil, fmt.Errorf("sftp readdir %s: %w", absPath, err)
	}
	entries := make([]FileEntry, 0, len(infos))
	for _, info := range infos {
		childRel := path.Join(relPath, info.Name())
		entry := sftpFileInfoToEntry(info, childRel)
		if entry.IsSymlink {
			childAbs := path.Join(absPath, info.Name())
			target, err := e.client.ReadLink(childAbs)
			if err == nil {
				entry.LinkTarget = target
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (e *SFTPReadEndpoint) OpenRead(relPath string) (io.ReadCloser, error) {
	absPath := path.Join(e.root, relPath)
	return e.client.Open(absPath)
}

func (e *SFTPReadEndpoint) Hash(relPath string) (string, error) {
	absPath := path.Join(e.root, relPath)
	f, err := e.client.Open(absPath)
	if err != nil {
		return "", fmt.Errorf("sftp open %s: %w", absPath, err)
	}
	defer f.Close()
	return hashReader(f)
}

func (e *SFTPReadEndpoint) Root() string { return e.root }

func (e *SFTPReadEndpoint) Caps() Capabilities {
	return Capabilities{
		SparseDetect: false,
		Hardlinks:    false,
		Xattrs:       false,
		AtomicRename: true,
		FastCopy:     false,
		NativeHash:   false,
	}
}

func (e *SFTPReadEndpoint) Close() error {
	err := e.client.Close()
	if sshErr := e.ssh.Close(); sshErr != nil && err == nil {
		err = sshErr
	}
	return err
}

// SFTPWriteEndpoint writes to a remote filesystem over SFTP.
type SFTPWriteEndpoint struct {
	client *sftp.Client
	ssh    *ssh.Client
	root   string
}

// NewSFTPWriteEndpoint creates a write endpoint backed by an SFTP connection.
// The caller must call Close when done.
func NewSFTPWriteEndpoint(sshClient *ssh.Client, root string) (*SFTPWriteEndpoint, error) {
	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		return nil, fmt.Errorf("sftp client: %w", err)
	}
	return &SFTPWriteEndpoint{
		client: sftpClient,
		ssh:    sshClient,
		root:   root,
	}, nil
}

func (e *SFTPWriteEndpoint) MkdirAll(relPath string, perm os.FileMode) error {
	absPath := path.Join(e.root, relPath)
	return e.client.MkdirAll(absPath)
}

func (e *SFTPWriteEndpoint) CreateTemp(relPath string, perm os.FileMode) (WriteFile, error) {
	absPath := path.Join(e.root, relPath)
	dir := path.Dir(absPath)
	base := path.Base(absPath)
	tmpName := fmt.Sprintf(".%s.%s.beam-tmp", base, uuid.New().String()[:8])
	tmpPath := path.Join(dir, tmpName)

	f, err := e.client.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return nil, fmt.Errorf("sftp create temp %s: %w", tmpPath, err)
	}

	// Set permissions after creation since OpenFile doesn't accept a mode.
	if err := e.client.Chmod(tmpPath, perm); err != nil {
		f.Close()
		_ = e.client.Remove(tmpPath)
		return nil, fmt.Errorf("sftp chmod temp %s: %w", tmpPath, err)
	}

	tmpRel := relPathFrom(e.root, tmpPath)
	return &sftpWriteFile{File: f, relPath: tmpRel}, nil
}

func (e *SFTPWriteEndpoint) Rename(oldRel, newRel string) error {
	oldAbs := path.Join(e.root, oldRel)
	newAbs := path.Join(e.root, newRel)
	// SFTP rename fails if target exists; remove first.
	_ = e.client.Remove(newAbs)
	return e.client.Rename(oldAbs, newAbs)
}

func (e *SFTPWriteEndpoint) Remove(relPath string) error {
	absPath := path.Join(e.root, relPath)
	return e.client.Remove(absPath)
}

func (e *SFTPWriteEndpoint) RemoveAll(relPath string) error {
	absPath := path.Join(e.root, relPath)
	return removeAllSFTP(e.client, absPath)
}

func (e *SFTPWriteEndpoint) Symlink(target, newRel string) error {
	absNew := path.Join(e.root, newRel)
	_ = e.client.Remove(absNew)
	return e.client.Symlink(target, absNew)
}

func (e *SFTPWriteEndpoint) Link(oldRel, newRel string) error {
	oldAbs := path.Join(e.root, oldRel)
	newAbs := path.Join(e.root, newRel)
	_ = e.client.Remove(newAbs)
	return e.client.Link(oldAbs, newAbs)
}

func (e *SFTPWriteEndpoint) SetMetadata(relPath string, entry FileEntry, opts MetadataOpts) error {
	absPath := path.Join(e.root, relPath)

	if opts.Mode {
		if err := e.client.Chmod(absPath, entry.Mode.Perm()); err != nil {
			return fmt.Errorf("sftp chmod %s: %w", relPath, err)
		}
	}

	if opts.Times {
		if err := e.client.Chtimes(absPath, entry.AccTime, entry.ModTime); err != nil {
			return fmt.Errorf("sftp chtimes %s: %w", relPath, err)
		}
	}

	if opts.Owner {
		if err := e.client.Chown(absPath, int(entry.Uid), int(entry.Gid)); err != nil {
			// Ownership changes often fail on remote hosts without root.
			// Silently ignore to match rsync behavior.
			_ = err
		}
	}

	// Xattrs are not supported over SFTP.
	return nil
}

func (e *SFTPWriteEndpoint) Walk(fn func(entry FileEntry) error) error {
	walker := e.client.Walk(e.root)
	for walker.Step() {
		if err := walker.Err(); err != nil {
			continue
		}
		relPath, err := filepath.Rel(e.root, walker.Path())
		if err != nil || relPath == "." {
			continue
		}
		entry := sftpFileInfoToEntry(walker.Stat(), relPath)
		if entry.IsSymlink {
			target, err := e.client.ReadLink(walker.Path())
			if err == nil {
				entry.LinkTarget = target
			}
		}
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (e *SFTPWriteEndpoint) Stat(relPath string) (FileEntry, error) {
	absPath := path.Join(e.root, relPath)
	info, err := e.client.Lstat(absPath)
	if err != nil {
		return FileEntry{}, err
	}
	entry := sftpFileInfoToEntry(info, relPath)
	if entry.IsSymlink {
		target, err := e.client.ReadLink(absPath)
		if err == nil {
			entry.LinkTarget = target
		}
	}
	return entry, nil
}

func (e *SFTPWriteEndpoint) OpenRead(relPath string) (io.ReadCloser, error) {
	absPath := path.Join(e.root, relPath)
	return e.client.Open(absPath)
}

func (e *SFTPWriteEndpoint) Hash(relPath string) (string, error) {
	absPath := path.Join(e.root, relPath)
	f, err := e.client.Open(absPath)
	if err != nil {
		return "", fmt.Errorf("sftp open %s: %w", absPath, err)
	}
	defer f.Close()
	return hashReader(f)
}

func (e *SFTPWriteEndpoint) Root() string { return e.root }

func (e *SFTPWriteEndpoint) Caps() Capabilities {
	return Capabilities{
		SparseDetect: false,
		Hardlinks:    false,
		Xattrs:       false,
		AtomicRename: true,
		FastCopy:     false,
		NativeHash:   false,
	}
}

func (e *SFTPWriteEndpoint) Close() error {
	err := e.client.Close()
	if sshErr := e.ssh.Close(); sshErr != nil && err == nil {
		err = sshErr
	}
	return err
}

// sftpWriteFile wraps *sftp.File to implement WriteFile.
type sftpWriteFile struct {
	*sftp.File
	relPath string
}

func (f *sftpWriteFile) Name() string {
	return f.relPath
}

// sftpFileInfoToEntry converts os.FileInfo from SFTP to a FileEntry.
// SFTP doesn't expose dev/ino/nlink, so those remain zero.
func sftpFileInfoToEntry(info os.FileInfo, relPath string) FileEntry {
	entry := FileEntry{
		RelPath: relPath,
		Size:    info.Size(),
		Mode:    info.Mode(),
		ModTime: info.ModTime(),
		IsDir:   info.IsDir(),
	}
	if info.Mode()&os.ModeSymlink != 0 {
		entry.IsSymlink = true
	}
	return entry
}

// hashReader computes a BLAKE3 hash from an io.Reader.
func hashReader(r io.Reader) (string, error) {
	h := blake3.New()
	buf := make([]byte, 32*1024)
	if _, err := io.CopyBuffer(h, r, buf); err != nil {
		return "", fmt.Errorf("hash: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// removeAllSFTP recursively removes a directory over SFTP.
func removeAllSFTP(client *sftp.Client, absPath string) error {
	info, err := client.Lstat(absPath)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return client.Remove(absPath)
	}

	entries, err := client.ReadDir(absPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		childPath := path.Join(absPath, entry.Name())
		if entry.IsDir() {
			if err := removeAllSFTP(client, childPath); err != nil {
				return err
			}
		} else {
			if err := client.Remove(childPath); err != nil {
				return err
			}
		}
	}
	return client.RemoveDirectory(absPath)
}

// relPathFrom computes a relative path from root to absPath using
// path (not filepath) since remote paths use forward slashes.
func relPathFrom(root, absPath string) string {
	// Use filepath.Rel since it handles ".." correctly.
	rel, err := filepath.Rel(root, absPath)
	if err != nil {
		return absPath
	}
	return rel
}
