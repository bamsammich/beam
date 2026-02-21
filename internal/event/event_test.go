package event

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTypeString(t *testing.T) {
	tests := []struct {
		typ  Type
		want string
	}{
		{ScanStarted, "ScanStarted"},
		{ScanComplete, "ScanComplete"},
		{FileStarted, "FileStarted"},
		{FileProgress, "FileProgress"},
		{FileCompleted, "FileCompleted"},
		{FileFailed, "FileFailed"},
		{FileSkipped, "FileSkipped"},
		{DirCreated, "DirCreated"},
		{HardlinkCreated, "HardlinkCreated"},
		{DeleteFile, "DeleteFile"},
		{VerifyStarted, "VerifyStarted"},
		{VerifyOK, "VerifyOK"},
		{VerifyFailed, "VerifyFailed"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.typ.String())
		})
	}
}

func TestTypeStringUnknown(t *testing.T) {
	assert.Equal(t, "Unknown", Type(999).String())
}

func TestEventZeroValue(t *testing.T) {
	var e Event
	assert.Equal(t, Type(0), e.Type)
	assert.True(t, e.Timestamp.IsZero())
	assert.Empty(t, e.Path)
	assert.Zero(t, e.Size)
	assert.Zero(t, e.Total)
	assert.Zero(t, e.TotalSize)
	assert.Nil(t, e.Error)
	assert.Zero(t, e.WorkerID)
}

func TestEventFields(t *testing.T) {
	now := time.Now()
	e := Event{
		Type:      FileCompleted,
		Timestamp: now,
		Path:      "dir/file.txt",
		Size:      1024,
		WorkerID:  3,
	}
	assert.Equal(t, FileCompleted, e.Type)
	assert.Equal(t, now, e.Timestamp)
	assert.Equal(t, "dir/file.txt", e.Path)
	assert.Equal(t, int64(1024), e.Size)
	assert.Equal(t, 3, e.WorkerID)
}
