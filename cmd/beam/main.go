package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/bamsammich/beam/internal/engine"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	os.Exit(run())
}

func run() int {
	var (
		recursive      bool
		archive        bool
		workers        int
		chunkThreshold int64
		verbose        bool
		quiet          bool
		dryRun         bool
		useIOURing     bool
		showVersion    bool
	)

	rootCmd := &cobra.Command{
		Use:   "beam <source> <destination>",
		Short: "Insanely fast file transfer tool",
		Args: func(cmd *cobra.Command, args []string) error {
			if showVersion {
				return nil
			}
			return cobra.ExactArgs(2)(cmd, args)
		},
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if showVersion {
				fmt.Fprintf(os.Stdout, "beam %s\n", version)
				return nil
			}

			src := args[0]
			dst := args[1]

			// Configure logging.
			logLevel := slog.LevelWarn
			if verbose {
				logLevel = slog.LevelDebug
			} else if !quiet {
				logLevel = slog.LevelInfo
			}
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
				Level: logLevel,
			}))
			slog.SetDefault(logger)

			if dryRun {
				slog.Info("dry run mode")
			}

			// Default workers.
			if workers <= 0 {
				workers = min(runtime.NumCPU()*2, 32)
			}

			// Set up context with signal handling.
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			cfg := engine.Config{
				Src:            src,
				Dst:            dst,
				Recursive:      recursive,
				Archive:        archive,
				Workers:        workers,
				ChunkThreshold: chunkThreshold,
				DryRun:         dryRun,
				Verbose:        verbose,
				Quiet:          quiet,
				UseIOURing:     useIOURing,
			}

			slog.Debug("starting copy",
				"src", src,
				"dst", dst,
				"workers", workers,
				"archive", archive,
				"recursive", recursive,
				"iouring", useIOURing,
			)

			result := engine.Run(ctx, cfg)

			if !quiet {
				printSummary(result.Stats)
			}

			if result.Err != nil {
				slog.Error("copy failed", "error", result.Err)
				if result.Stats.FilesCopied > 0 {
					return &exitError{code: 1} // partial failure
				}
				return &exitError{code: 2} // total failure
			}

			return nil
		},
	}

	// Version flag handled in RunE, but also register the flag.
	rootCmd.Flags().BoolVar(&showVersion, "version", false, "print version and exit")

	rootCmd.Flags().BoolVarP(&recursive, "recursive", "r", false, "copy directories recursively")
	rootCmd.Flags().BoolVarP(&archive, "archive", "a", false, "archive mode (recursive + preserve all)")
	rootCmd.Flags().IntVarP(&workers, "workers", "n", 0, "number of copy workers (default: min(NumCPU*2, 32))")
	rootCmd.Flags().Int64Var(&chunkThreshold, "chunk-threshold", 256*1024*1024, "split files larger than this into chunks (bytes)")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
	rootCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "suppress all output except errors")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "show what would be copied without writing")
	rootCmd.Flags().BoolVar(&useIOURing, "iouring", false, "use io_uring for file copy (Linux only)")

	if err := rootCmd.Execute(); err != nil {
		if exitErr, ok := err.(*exitError); ok {
			return exitErr.code
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 2
	}

	return 0
}

func printSummary(s stats.Snapshot) {
	fmt.Fprintf(os.Stderr, "copied %d files (%s), %d dirs, %d hardlinks",
		s.FilesCopied, stats.FormatBytes(s.BytesCopied), s.DirsCreated, s.HardlinksCreated)
	if s.FilesFailed > 0 {
		fmt.Fprintf(os.Stderr, ", %d failed", s.FilesFailed)
	}
	if s.FilesSkipped > 0 {
		fmt.Fprintf(os.Stderr, ", %d skipped", s.FilesSkipped)
	}
	fmt.Fprintln(os.Stderr)
}

type exitError struct {
	code int
}

func (e *exitError) Error() string {
	return fmt.Sprintf("exit code %d", e.code)
}
