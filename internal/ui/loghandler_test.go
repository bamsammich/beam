package ui_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bamsammich/beam/internal/ui"
)

func TestMultiHandler_FansOut(t *testing.T) {
	t.Parallel()

	var textBuf, jsonBuf bytes.Buffer
	textH := slog.NewTextHandler(&textBuf, &slog.HandlerOptions{Level: slog.LevelInfo})
	jsonH := slog.NewJSONHandler(&jsonBuf, &slog.HandlerOptions{Level: slog.LevelInfo})

	logger := slog.New(ui.NewMultiHandler(textH, jsonH))
	logger.Info("test message", "key", "value")

	// Text handler output.
	assert.Contains(t, textBuf.String(), "test message")
	assert.Contains(t, textBuf.String(), "key=value")

	// JSON handler output.
	var rec map[string]any
	require.NoError(t, json.Unmarshal(jsonBuf.Bytes(), &rec))
	assert.Equal(t, "test message", rec["msg"])
	assert.Equal(t, "value", rec["key"])
}

func TestMultiHandler_LevelFiltering(t *testing.T) {
	t.Parallel()

	var debugBuf, warnBuf bytes.Buffer
	debugH := slog.NewTextHandler(&debugBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	warnH := slog.NewTextHandler(&warnBuf, &slog.HandlerOptions{Level: slog.LevelWarn})

	logger := slog.New(ui.NewMultiHandler(debugH, warnH))
	logger.Info("info msg")
	logger.Warn("warn msg")

	// Debug handler sees both.
	assert.Contains(t, debugBuf.String(), "info msg")
	assert.Contains(t, debugBuf.String(), "warn msg")

	// Warn handler sees only warn.
	assert.NotContains(t, warnBuf.String(), "info msg")
	assert.Contains(t, warnBuf.String(), "warn msg")
}

func TestMultiHandler_Enabled(t *testing.T) {
	t.Parallel()

	warnH := slog.NewTextHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelWarn})
	errH := slog.NewTextHandler(&bytes.Buffer{}, &slog.HandlerOptions{Level: slog.LevelError})

	m := ui.NewMultiHandler(warnH, errH)

	// Enabled if ANY handler accepts the level.
	assert.True(t, m.Enabled(context.Background(), slog.LevelWarn))
	assert.True(t, m.Enabled(context.Background(), slog.LevelError))
	assert.False(t, m.Enabled(context.Background(), slog.LevelInfo))
}

func TestMultiHandler_WithAttrs(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	m := ui.NewMultiHandler(h)
	logger := slog.New(m.WithAttrs([]slog.Attr{slog.String("component", "engine")}))

	logger.Info("hello")
	assert.Contains(t, buf.String(), "component=engine")
}

func TestMultiHandler_WithGroup(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	m := ui.NewMultiHandler(h)
	logger := slog.New(m.WithGroup("beam"))

	logger.Info("event", "type", "FileCompleted")

	lines := strings.TrimSpace(buf.String())
	var rec map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines), &rec))

	group, ok := rec["beam"].(map[string]any)
	require.True(t, ok, "expected group 'beam' in JSON output")
	assert.Equal(t, "FileCompleted", group["type"])
}
