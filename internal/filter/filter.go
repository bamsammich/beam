package filter

// Rule represents a single include or exclude filter rule.
type Rule struct {
	Pattern *compiledPattern
	Include bool // true=include, false=exclude
}

// Chain holds an ordered list of filter rules plus size filters.
type Chain struct {
	rules   []Rule
	minSize int64
	maxSize int64
}

// NewChain creates an empty filter chain.
func NewChain() *Chain {
	return &Chain{}
}

// AddExclude adds an exclude rule for the given pattern.
func (c *Chain) AddExclude(pattern string) error {
	cp, err := compilePattern(pattern)
	if err != nil {
		return err
	}
	c.rules = append(c.rules, Rule{Pattern: cp, Include: false})
	return nil
}

// AddInclude adds an include rule for the given pattern.
func (c *Chain) AddInclude(pattern string) error {
	cp, err := compilePattern(pattern)
	if err != nil {
		return err
	}
	c.rules = append(c.rules, Rule{Pattern: cp, Include: true})
	return nil
}

// SetMinSize sets the minimum file size filter.
func (c *Chain) SetMinSize(n int64) {
	c.minSize = n
}

// SetMaxSize sets the maximum file size filter.
func (c *Chain) SetMaxSize(n int64) {
	c.maxSize = n
}

// Empty reports whether the chain has no rules and no size filters.
func (c *Chain) Empty() bool {
	return len(c.rules) == 0 && c.minSize == 0 && c.maxSize == 0
}

// Match returns true if the path should be INCLUDED (not filtered out).
// relPath is relative to the copy root, isDir indicates directories,
// and size is the file size (ignored for directories).
func (c *Chain) Match(relPath string, isDir bool, size int64) bool {
	// Size filters apply only to regular files.
	if !isDir {
		if c.minSize > 0 && size < c.minSize {
			return false
		}
		if c.maxSize > 0 && size > c.maxSize {
			return false
		}
	}

	// Walk rules in order — first match wins.
	for _, rule := range c.rules {
		if rule.Pattern.match(relPath, isDir) {
			return rule.Include
		}
	}

	// No match → include (default).
	return true
}
