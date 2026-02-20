package engine

import (
	"os"
	"sync"
)

// tmpRegistry tracks in-progress temporary files for defense-in-depth cleanup.
var globalTmpRegistry = &tmpRegistry{}

type tmpRegistry struct {
	mu    sync.Mutex
	paths map[string]struct{}
}

// RegisterTmp adds a temporary file path to the global registry.
func RegisterTmp(path string) {
	globalTmpRegistry.mu.Lock()
	defer globalTmpRegistry.mu.Unlock()
	if globalTmpRegistry.paths == nil {
		globalTmpRegistry.paths = make(map[string]struct{})
	}
	globalTmpRegistry.paths[path] = struct{}{}
}

// DeregisterTmp removes a temporary file path from the global registry.
func DeregisterTmp(path string) {
	globalTmpRegistry.mu.Lock()
	defer globalTmpRegistry.mu.Unlock()
	delete(globalTmpRegistry.paths, path)
}

// CleanupTmpFiles removes all registered temporary files.
func CleanupTmpFiles() {
	globalTmpRegistry.mu.Lock()
	paths := make([]string, 0, len(globalTmpRegistry.paths))
	for p := range globalTmpRegistry.paths {
		paths = append(paths, p)
	}
	globalTmpRegistry.paths = nil
	globalTmpRegistry.mu.Unlock()

	for _, p := range paths {
		_ = os.Remove(p)
	}
}
