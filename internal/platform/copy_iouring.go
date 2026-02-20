//go:build linux

package platform

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

const iouringBufSize = 1 << 20 // 1 MiB

var iouringBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, iouringBufSize)
		return &b
	},
}

// io_uring constants.
const (
	ioringSetupCQSize = 1 << 3

	ioringOpRead  = 22
	ioringOpWrite = 23

	ioringEnterGetevents = 1 << 0
)

// io_uring_sqe — submission queue entry (64 bytes).
type ioUringSQE struct {
	opcode      uint8
	flags       uint8
	ioprio      uint16
	fd          int32
	off         uint64
	addr        uint64
	len         uint32
	opcodeFlags uint32
	userData    uint64
	bufIG       uint16
	personality uint16
	spliceFdIn  int32
	_pad2       [2]uint64
}

// io_uring_cqe — completion queue entry (16 bytes).
type ioUringCQE struct {
	userData uint64
	res      int32
	flags    uint32
}

// io_uring_params — setup parameters.
type ioUringParams struct {
	sqEntries    uint32
	cqEntries    uint32
	flags        uint32
	sqThreadCPU  uint32
	sqThreadIdle uint32
	features     uint32
	wqFd         uint32
	resv         [3]uint32
	sqOff        ioUringSQRingOffsets
	cqOff        ioUringCQRingOffsets
}

type ioUringSQRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	flags       uint32
	dropped     uint32
	array       uint32
	resv1       uint32
	userAddr    uint64
}

type ioUringCQRingOffsets struct {
	head        uint32
	tail        uint32
	ringMask    uint32
	ringEntries uint32
	overflow    uint32
	cqes        uint32
	flags       uint32
	resv1       uint32
	userAddr    uint64
}

// ring wraps the memory-mapped io_uring state.
type ring struct {
	fd        int
	sqEntries uint32
	cqEntries uint32

	// SQ ring pointers (into mmap'd memory).
	sqHead    *uint32
	sqTail    *uint32
	sqMask    *uint32
	sqArray   unsafe.Pointer
	sqes      unsafe.Pointer
	sqRingMem []byte

	// CQ ring pointers.
	cqHead    *uint32
	cqTail    *uint32
	cqMask    *uint32
	cqes      unsafe.Pointer
	cqRingMem []byte

	sqesMem []byte
}

// IOURingCopier wraps a raw io_uring ring for file copy operations.
type IOURingCopier struct {
	r *ring
}

// NewIOURingCopier creates a copier backed by io_uring. Returns (nil, nil) if
// the kernel does not support io_uring (< 5.6).
func NewIOURingCopier(queueDepth uint) (*IOURingCopier, error) {
	if !kernelSupportsIOURing() {
		return nil, nil
	}

	r, err := setupRing(uint32(queueDepth))
	if err != nil {
		return nil, err
	}
	return &IOURingCopier{r: r}, nil
}

// Close releases the io_uring ring.
func (c *IOURingCopier) Close() error {
	if c == nil || c.r == nil {
		return nil
	}
	return c.r.close()
}

// CopyFile copies a single file using io_uring pread/pwrite.
func (c *IOURingCopier) CopyFile(params CopyFileParams) (CopyResult, error) {
	srcFd, err := os.Open(params.SrcPath)
	if err != nil {
		return CopyResult{}, err
	}
	defer srcFd.Close()

	remaining := copyLength(params)
	offset := params.SrcOffset
	var totalWritten int64

	srcRawFd := int32(srcFd.Fd())
	dstRawFd := int32(params.DstFd.Fd())

	for remaining > 0 {
		toRead := int64(iouringBufSize)
		if toRead > remaining {
			toRead = remaining
		}

		bufp := iouringBufPool.Get().(*[]byte)
		buf := (*bufp)[:toRead]

		// Read via io_uring.
		n, err := c.r.submitAndWait(ioringOpRead, srcRawFd, buf, uint64(offset))
		if err != nil {
			iouringBufPool.Put(bufp)
			return CopyResult{BytesWritten: totalWritten, Method: IOURing}, fmt.Errorf("iouring read: %w", err)
		}
		if n == 0 {
			iouringBufPool.Put(bufp)
			break
		}

		// Write via io_uring.
		w, err := c.r.submitAndWait(ioringOpWrite, dstRawFd, buf[:n], uint64(offset))
		iouringBufPool.Put(bufp)
		if err != nil {
			return CopyResult{BytesWritten: totalWritten, Method: IOURing}, fmt.Errorf("iouring write: %w", err)
		}

		offset += int64(w)
		remaining -= int64(w)
		totalWritten += int64(w)
	}

	return CopyResult{BytesWritten: totalWritten, Method: IOURing}, nil
}

// CopyBatch copies multiple small files.
func (c *IOURingCopier) CopyBatch(paramsList []CopyFileParams) ([]CopyResult, []error) {
	results := make([]CopyResult, len(paramsList))
	errs := make([]error, len(paramsList))
	for i, p := range paramsList {
		results[i], errs[i] = c.CopyFile(p)
	}
	return results, errs
}

// setupRing creates and maps an io_uring instance.
func setupRing(entries uint32) (*ring, error) {
	var params ioUringParams
	fd, _, errno := syscall.Syscall(
		unix.SYS_IO_URING_SETUP,
		uintptr(entries),
		uintptr(unsafe.Pointer(&params)),
		0,
	)
	if errno != 0 {
		return nil, fmt.Errorf("io_uring_setup: %w", errno)
	}

	r := &ring{
		fd:        int(fd),
		sqEntries: params.sqEntries,
		cqEntries: params.cqEntries,
	}

	if err := r.mmap(&params); err != nil {
		_ = syscall.Close(r.fd)
		return nil, err
	}

	return r, nil
}

const sqeSize = 64
const cqeSize = 16

func (r *ring) mmap(params *ioUringParams) error {
	// Map submission queue ring.
	sqRingSize := uintptr(params.sqOff.array) + uintptr(params.sqEntries)*4
	sqMem, err := syscall.Mmap(r.fd, 0, int(sqRingSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		return fmt.Errorf("mmap sq ring: %w", err)
	}
	r.sqRingMem = sqMem

	base := unsafe.Pointer(&sqMem[0])
	r.sqHead = (*uint32)(unsafe.Add(base, params.sqOff.head))
	r.sqTail = (*uint32)(unsafe.Add(base, params.sqOff.tail))
	r.sqMask = (*uint32)(unsafe.Add(base, params.sqOff.ringMask))
	r.sqArray = unsafe.Add(base, params.sqOff.array)

	// Map SQEs.
	sqesMem, err := syscall.Mmap(r.fd, 0x10000000, int(uintptr(params.sqEntries)*sqeSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		_ = syscall.Munmap(r.sqRingMem)
		return fmt.Errorf("mmap sqes: %w", err)
	}
	r.sqesMem = sqesMem
	r.sqes = unsafe.Pointer(&sqesMem[0])

	// Map completion queue ring.
	cqRingSize := uintptr(params.cqOff.cqes) + uintptr(params.cqEntries)*cqeSize
	cqMem, err := syscall.Mmap(r.fd, 0x8000000, int(cqRingSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED|syscall.MAP_POPULATE)
	if err != nil {
		_ = syscall.Munmap(r.sqesMem)
		_ = syscall.Munmap(r.sqRingMem)
		return fmt.Errorf("mmap cq ring: %w", err)
	}
	r.cqRingMem = cqMem

	cqBase := unsafe.Pointer(&cqMem[0])
	r.cqHead = (*uint32)(unsafe.Add(cqBase, params.cqOff.head))
	r.cqTail = (*uint32)(unsafe.Add(cqBase, params.cqOff.tail))
	r.cqMask = (*uint32)(unsafe.Add(cqBase, params.cqOff.ringMask))
	r.cqes = unsafe.Add(cqBase, params.cqOff.cqes)

	return nil
}

func (r *ring) close() error {
	var firstErr error
	if r.cqRingMem != nil {
		if err := syscall.Munmap(r.cqRingMem); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.sqesMem != nil {
		if err := syscall.Munmap(r.sqesMem); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if r.sqRingMem != nil {
		if err := syscall.Munmap(r.sqRingMem); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if err := syscall.Close(r.fd); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// submitAndWait submits a single SQE and waits for its CQE.
func (r *ring) submitAndWait(op uint8, fd int32, buf []byte, offset uint64) (int, error) {
	// Get next SQE slot.
	tail := *r.sqTail
	idx := tail & *r.sqMask

	// Fill SQE.
	sqe := (*ioUringSQE)(unsafe.Add(r.sqes, uintptr(idx)*sqeSize))
	*sqe = ioUringSQE{} // zero out
	sqe.opcode = op
	sqe.fd = fd
	sqe.off = offset
	sqe.addr = uint64(uintptr(unsafe.Pointer(&buf[0])))
	sqe.len = uint32(len(buf))
	sqe.userData = uint64(tail)

	// Write SQ array entry (index into SQEs array).
	sqArr := (*uint32)(unsafe.Add(r.sqArray, uintptr(idx)*4))
	*sqArr = uint32(idx)

	// Advance SQ tail.
	*r.sqTail = tail + 1

	// Submit and wait via io_uring_enter.
	_, _, errno := syscall.Syscall6(
		unix.SYS_IO_URING_ENTER,
		uintptr(r.fd),
		1, // to_submit
		1, // min_complete
		uintptr(ioringEnterGetevents),
		0, 0,
	)
	if errno != 0 {
		return 0, fmt.Errorf("io_uring_enter: %w", errno)
	}

	// Read CQE.
	cqHead := *r.cqHead
	cqIdx := cqHead & *r.cqMask
	cqe := (*ioUringCQE)(unsafe.Add(r.cqes, uintptr(cqIdx)*cqeSize))

	res := cqe.res
	*r.cqHead = cqHead + 1

	if res < 0 {
		return 0, syscall.Errno(-res)
	}
	return int(res), nil
}

// kernelSupportsIOURing checks if the kernel version is >= 5.6.
func kernelSupportsIOURing() bool {
	var uname unix.Utsname
	if err := unix.Uname(&uname); err != nil {
		return false
	}

	release := unix.ByteSliceToString(uname.Release[:])
	parts := strings.SplitN(release, ".", 3)
	if len(parts) < 2 {
		return false
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}

	minorStr := parts[1]
	if idx := strings.IndexFunc(minorStr, func(r rune) bool { return r < '0' || r > '9' }); idx > 0 {
		minorStr = minorStr[:idx]
	}
	minor, err := strconv.Atoi(minorStr)
	if err != nil {
		return false
	}

	return major > 5 || (major == 5 && minor >= 6)
}

// KernelSupportsIOURing is exported for testing.
func KernelSupportsIOURing() bool {
	return kernelSupportsIOURing()
}
