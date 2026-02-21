package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"0", 0},
		{"100", 100},
		{"100B", 100},
		{"100b", 100},
		{"100K", 102400},
		{"100k", 102400},
		{"1M", 1048576},
		{"1G", 1073741824},
		{"1T", 1099511627776},
		{"1.5G", 1610612736},
		{"0.5M", 524288},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseSize(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseSizeErrors(t *testing.T) {
	tests := []string{
		"",
		"abc",
		"K",
		"notanumber G",
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := ParseSize(input)
			assert.Error(t, err)
		})
	}
}
