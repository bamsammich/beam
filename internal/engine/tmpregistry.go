package engine

import (
	"os"
	"sync"
)

// tmpRegistry tracks in-progress temporary files for defense-in-depth cleanup.
type tmpRegistry struct {
	paths map[string]struct{}
	mu    sync.Mutex
}

func newTmpRegistry() *tmpRegistry {
	return &tmpRegistry{paths: make(map[string]struct{})}
}

func (r *tmpRegistry) register(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths[path] = struct{}{}
}

func (r *tmpRegistry) deregister(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.paths, path)
}

func (r *tmpRegistry) cleanup() {
	r.mu.Lock()
	paths := make([]string, 0, len(r.paths))
	for p := range r.paths {
		paths = append(paths, p)
	}
	r.paths = nil
	r.mu.Unlock()

	for _, p := range paths {
		_ = os.Remove(p)
	}
}
