package filter

import (
	"regexp"
	"strings"
)

// compiledPattern is a compiled glob pattern that can match paths.
type compiledPattern struct {
	re       *regexp.Regexp
	original string
	anchored bool // pattern starts with /
	dirOnly  bool // pattern ends with /
}

// compilePattern converts a rsync-style glob pattern into a compiled matcher.
func compilePattern(pattern string) (*compiledPattern, error) {
	cp := &compiledPattern{original: pattern}

	// Trailing / means directory-only.
	if strings.HasSuffix(pattern, "/") {
		cp.dirOnly = true
		pattern = strings.TrimSuffix(pattern, "/")
	}

	// Leading / means anchored to root.
	if strings.HasPrefix(pattern, "/") {
		cp.anchored = true
		pattern = strings.TrimPrefix(pattern, "/")
	} else if strings.Contains(pattern, "/") {
		// Contains a / but doesn't start with / — still anchored per rsync rules.
		cp.anchored = true
	}

	// Convert glob to regex.
	reStr := globToRegex(pattern)

	if cp.anchored {
		// Match from the start of the relative path.
		reStr = "^" + reStr + "$"
	} else {
		// Match against basename or any path suffix.
		reStr = "(^|/)" + reStr + "$"
	}

	re, err := regexp.Compile(reStr)
	if err != nil {
		return nil, err
	}
	cp.re = re
	return cp, nil
}

// match tests whether a relative path matches this pattern.
func (cp *compiledPattern) match(relPath string, isDir bool) bool {
	if cp.dirOnly && !isDir {
		return false
	}
	return cp.re.MatchString(relPath)
}

// globToRegex converts a glob pattern to a regex string.
//
//nolint:gocyclo,revive // cognitive-complexity: character-by-character glob parser
func globToRegex(pattern string) string {
	var b strings.Builder
	i := 0
	for i < len(pattern) {
		c := pattern[i]
		switch c {
		case '*':
			if i+1 < len(pattern) && pattern[i+1] == '*' {
				// ** matches anything including /
				if i+2 < len(pattern) && pattern[i+2] == '/' {
					b.WriteString("(.*/)?")
					i += 3
				} else {
					b.WriteString(".*")
					i += 2
				}
			} else {
				// * matches anything except /
				b.WriteString("[^/]*")
				i++
			}
		case '?':
			b.WriteString("[^/]")
			i++
		case '[':
			// Character class — pass through to regex.
			j := i + 1
			if j < len(pattern) && pattern[j] == '!' {
				j++
			}
			if j < len(pattern) && pattern[j] == ']' {
				j++
			}
			for j < len(pattern) && pattern[j] != ']' {
				j++
			}
			if j < len(pattern) {
				cls := pattern[i+1 : j]
				// Convert ! to ^ for negation.
				if strings.HasPrefix(cls, "!") {
					cls = "^" + cls[1:]
				}
				b.WriteString("[" + cls + "]")
				i = j + 1
			} else {
				b.WriteString(regexp.QuoteMeta(string(c)))
				i++
			}
		case '.', '(', ')', '+', '{', '}', '^', '$', '|', '\\':
			b.WriteString(regexp.QuoteMeta(string(c)))
			i++
		default:
			b.WriteByte(c)
			i++
		}
	}
	return b.String()
}
