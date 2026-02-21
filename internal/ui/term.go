package ui

import "golang.org/x/term"

// IsTTY reports whether the given file descriptor refers to a terminal.
func IsTTY(fd uintptr) bool {
	return term.IsTerminal(int(fd))
}

// TermWidth returns the terminal width in columns, or 80 if it cannot be determined.
func TermWidth(fd uintptr) int {
	w, _, err := term.GetSize(int(fd))
	if err != nil || w <= 0 {
		return 80
	}
	return w
}
