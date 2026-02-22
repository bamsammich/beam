package engine

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// Segment describes a contiguous region of a file.
type Segment struct {
	Offset int64
	Length int64
	IsData bool
}

// DetectSparseSegments walks SEEK_DATA/SEEK_HOLE to map out the sparse
// layout of a file. Returns a single data segment covering the whole file
// if the filesystem doesn't support sparse detection.
//
//nolint:revive // cognitive-complexity: SEEK_DATA/SEEK_HOLE state machine with error recovery
func DetectSparseSegments(fd *os.File, fileSize int64) ([]Segment, error) {
	if fileSize == 0 {
		return nil, nil
	}

	rawFd := int(fd.Fd()) //nolint:gosec // G115: fd conversion is safe for file descriptors
	var segments []Segment
	offset := int64(0)

	for offset < fileSize {
		// Find next data region.
		dataStart, err := seekData(rawFd, offset)
		if err != nil {
			if isENXIO(err) {
				// Rest of file is a hole.
				if offset < fileSize {
					segments = append(segments, Segment{
						Offset: offset,
						Length: fileSize - offset,
						IsData: false,
					})
				}
				break
			}
			if isEINVAL(err) {
				// Filesystem doesn't support SEEK_DATA/SEEK_HOLE.
				return wholeFileSegment(fileSize), nil
			}
			return nil, err
		}

		// If there's a hole before this data region, record it.
		if dataStart > offset {
			segments = append(segments, Segment{
				Offset: offset,
				Length: dataStart - offset,
				IsData: false,
			})
		}

		// Find the end of this data region (start of next hole).
		holeStart, err := seekHole(rawFd, dataStart)
		if err != nil {
			switch {
			case isENXIO(err):
				// Data extends to EOF.
				holeStart = fileSize
			case isEINVAL(err):
				return wholeFileSegment(fileSize), nil
			default:
				return nil, err
			}
		}

		if holeStart > fileSize {
			holeStart = fileSize
		}

		segments = append(segments, Segment{
			Offset: dataStart,
			Length: holeStart - dataStart,
			IsData: true,
		})

		offset = holeStart
	}

	if len(segments) == 0 {
		return wholeFileSegment(fileSize), nil
	}
	return segments, nil
}

func wholeFileSegment(size int64) []Segment {
	return []Segment{{Offset: 0, Length: size, IsData: true}}
}

func seekData(fd int, offset int64) (int64, error) {
	return unix.Seek(fd, offset, unix.SEEK_DATA)
}

func seekHole(fd int, offset int64) (int64, error) {
	return unix.Seek(fd, offset, unix.SEEK_HOLE)
}

func isENXIO(err error) bool {
	return err == syscall.ENXIO
}

func isEINVAL(err error) bool {
	return err == syscall.EINVAL
}
