package tui

import (
	"github.com/bamsammich/beam/internal/config"
	"github.com/charmbracelet/lipgloss"
)

// Catppuccin Mocha palette — mutable so config can override.
var (
	ColorGreen  = lipgloss.Color("#a6e3a1")
	ColorBlue   = lipgloss.Color("#89b4fa")
	ColorYellow = lipgloss.Color("#f9e2af")
	ColorRed    = lipgloss.Color("#f38ba8")
	ColorTeal   = lipgloss.Color("#94e2d5")
	ColorMauve  = lipgloss.Color("#cba6f7")
	ColorMuted  = lipgloss.Color("#5a6278")
	ColorDim    = lipgloss.Color("#3a4055")
	ColorBright = lipgloss.Color("#cdd6f4")
)

// Pre-built styles — rebuilt by rebuildStyles() after color changes.
var (
	styleHeader         lipgloss.Style
	styleHeaderLabel    lipgloss.Style
	styleDivider        lipgloss.Style
	styleIconDone       lipgloss.Style
	styleIconFailed     lipgloss.Style
	styleIconSkipped    lipgloss.Style
	styleFilePath       lipgloss.Style
	styleFileDir        lipgloss.Style
	styleFileSize       lipgloss.Style
	styleFileSpeed      lipgloss.Style
	styleInFlight       lipgloss.Style
	styleError          lipgloss.Style
	styleErrorPath      lipgloss.Style
	styleKeybindKey     lipgloss.Style
	styleKeybindLabel   lipgloss.Style
	styleBigNumber      lipgloss.Style
	styleSparkline      lipgloss.Style
	styleWorkerBusy     lipgloss.Style
	styleWorkerIdle     lipgloss.Style
	styleProgressFilled lipgloss.Style
	styleProgressEmpty  lipgloss.Style
	styleStatus         lipgloss.Style
	styleSavePrompt     lipgloss.Style
	styleSaveInput      lipgloss.Style
)

func init() {
	rebuildStyles()
}

// rebuildStyles reconstructs all lipgloss styles from the current color vars.
func rebuildStyles() {
	styleHeader = lipgloss.NewStyle().Bold(true).Foreground(ColorBright)
	styleHeaderLabel = lipgloss.NewStyle().Bold(true).Foreground(ColorMauve)
	styleDivider = lipgloss.NewStyle().Foreground(ColorDim)
	styleIconDone = lipgloss.NewStyle().Foreground(ColorGreen)
	styleIconFailed = lipgloss.NewStyle().Foreground(ColorRed)
	styleIconSkipped = lipgloss.NewStyle().Foreground(ColorMuted)
	styleFilePath = lipgloss.NewStyle().Foreground(ColorBright)
	styleFileDir = lipgloss.NewStyle().Foreground(ColorMuted)
	styleFileSize = lipgloss.NewStyle().Foreground(ColorMuted)
	styleFileSpeed = lipgloss.NewStyle().Foreground(ColorTeal)
	styleInFlight = lipgloss.NewStyle().Foreground(ColorBlue)
	styleError = lipgloss.NewStyle().Foreground(ColorRed)
	styleErrorPath = lipgloss.NewStyle().Foreground(ColorRed).Bold(true)
	styleKeybindKey = lipgloss.NewStyle().Foreground(ColorMauve).Bold(true)
	styleKeybindLabel = lipgloss.NewStyle().Foreground(ColorMuted)
	styleBigNumber = lipgloss.NewStyle().Bold(true).Foreground(ColorGreen)
	styleSparkline = lipgloss.NewStyle().Foreground(ColorBlue)
	styleWorkerBusy = lipgloss.NewStyle().Foreground(ColorBlue)
	styleWorkerIdle = lipgloss.NewStyle().Foreground(ColorDim)
	styleProgressFilled = lipgloss.NewStyle().Foreground(ColorGreen)
	styleProgressEmpty = lipgloss.NewStyle().Foreground(ColorDim)
	styleStatus = lipgloss.NewStyle().Foreground(ColorYellow).Italic(true)
	styleSavePrompt = lipgloss.NewStyle().Foreground(ColorMuted)
	styleSaveInput = lipgloss.NewStyle().Foreground(ColorBright)
}

// ApplyTheme overrides colors from a config ThemeConfig and rebuilds all styles.
func ApplyTheme(tc config.ThemeConfig) {
	if tc.Green != nil {
		ColorGreen = lipgloss.Color(*tc.Green)
	}
	if tc.Blue != nil {
		ColorBlue = lipgloss.Color(*tc.Blue)
	}
	if tc.Yellow != nil {
		ColorYellow = lipgloss.Color(*tc.Yellow)
	}
	if tc.Red != nil {
		ColorRed = lipgloss.Color(*tc.Red)
	}
	if tc.Teal != nil {
		ColorTeal = lipgloss.Color(*tc.Teal)
	}
	if tc.Mauve != nil {
		ColorMauve = lipgloss.Color(*tc.Mauve)
	}
	if tc.Muted != nil {
		ColorMuted = lipgloss.Color(*tc.Muted)
	}
	if tc.Dim != nil {
		ColorDim = lipgloss.Color(*tc.Dim)
	}
	if tc.Bright != nil {
		ColorBright = lipgloss.Color(*tc.Bright)
	}
	rebuildStyles()
}
