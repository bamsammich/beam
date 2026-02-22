package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/cobra/doc"
)

var docsCmd = &cobra.Command{
	Use:    "gen-docs",
	Short:  "Generate documentation for beam",
	Hidden: true,
	RunE:   runGenDocs,
}

func init() {
	docsCmd.Flags().String("dir", "docs", "output directory")
	docsCmd.Flags().String("format", "man", "output format (man or markdown)")
}

func runGenDocs(cmd *cobra.Command, _ []string) error {
	dir, _ := cmd.Flags().GetString("dir")       //nolint:errcheck // flag name is hardcoded
	format, _ := cmd.Flags().GetString("format") //nolint:errcheck // flag name is hardcoded

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	// Use the parent command (root) to generate docs for all commands.
	root := cmd.Root()

	switch format {
	case "man":
		header := &doc.GenManHeader{
			Title:   "BEAM",
			Section: "1",
			Source:  "beam " + version,
		}
		return doc.GenManTree(root, header, dir)
	case "markdown":
		return doc.GenMarkdownTree(root, dir)
	default:
		return fmt.Errorf("unknown format %q (use man or markdown)", format)
	}
}
