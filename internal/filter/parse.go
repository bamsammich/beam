package filter

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// LoadFile reads filter rules from a file and adds them to the chain.
// Format:
//   - pattern  → exclude
//   + pattern  → include
//   # comment  → skip
//   blank line → skip
//   no prefix  → exclude (rsync default)
func (c *Chain) LoadFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open filter file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip blank lines and comments.
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		include := false
		pattern := line

		if strings.HasPrefix(line, "+ ") {
			include = true
			pattern = strings.TrimSpace(line[2:])
		} else if strings.HasPrefix(line, "- ") {
			include = false
			pattern = strings.TrimSpace(line[2:])
		}
		// else: no prefix, treat as exclude.

		var addErr error
		if include {
			addErr = c.AddInclude(pattern)
		} else {
			addErr = c.AddExclude(pattern)
		}
		if addErr != nil {
			return fmt.Errorf("filter file %s line %d: %w", path, lineNum, addErr)
		}
	}

	return scanner.Err()
}
