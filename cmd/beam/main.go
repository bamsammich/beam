package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/bamsammich/beam/internal/config"
	"github.com/bamsammich/beam/internal/engine"
	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/ui"
	"github.com/bamsammich/beam/internal/ui/tui"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var version = "dev"

func main() {
	os.Exit(run())
}

// filterFlag is a custom pflag.Value that preserves CLI ordering of
// --exclude and --include rules by appending to a shared filter.Chain.
type filterFlag struct {
	chain   *filter.Chain
	include bool
}

func (f *filterFlag) String() string { return "" }
func (f *filterFlag) Type() string   { return "string" }

func (f *filterFlag) Set(val string) error {
	if f.include {
		return f.chain.AddInclude(val)
	}
	return f.chain.AddExclude(val)
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
		forceFeed      bool
		forceRate      bool
		noProgress     bool
		tuiFlag        bool
		deleteFlag     bool
		verifyFlag     bool
		noTimes        bool
		filterFile     string
		minSizeStr     string
		maxSizeStr     string
	)

	chain := filter.NewChain()

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

			// Load optional config file.
			cfg, err := config.Load()
			if err != nil {
				slog.Warn("failed to load config", "error", err)
			}

			// Apply config defaults for flags not explicitly set on CLI.
			applyConfigDefaults(cmd, cfg.Defaults, &verifyFlag, &workers, &tuiFlag, &archive)

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

			// Load filter file if specified.
			if filterFile != "" {
				if err := chain.LoadFile(filterFile); err != nil {
					return fmt.Errorf("load filter file: %w", err)
				}
			}

			// Parse size filters.
			if minSizeStr != "" {
				n, err := filter.ParseSize(minSizeStr)
				if err != nil {
					return fmt.Errorf("invalid --min-size: %w", err)
				}
				chain.SetMinSize(n)
			}
			if maxSizeStr != "" {
				n, err := filter.ParseSize(maxSizeStr)
				if err != nil {
					return fmt.Errorf("invalid --max-size: %w", err)
				}
				chain.SetMaxSize(n)
			}

			// Set up context with signal handling.
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			// Create stats collector.
			collector := stats.NewCollector()

			// Create events channel.
			events := make(chan event.Event, 256)

			// Create worker throttle.
			workerLimit := &atomic.Int32{}
			workerLimit.Store(int32(workers))

			// Create presenter.
			isTTY := ui.IsTTY(os.Stderr.Fd())
			var presenter ui.Presenter
			if tuiFlag && isTTY {
				presenter = tui.NewPresenter(tui.Config{
					Stats:       collector,
					Workers:     workers,
					DstRoot:     dst,
					SrcRoot:     src,
					Theme:       cfg.Theme,
					WorkerLimit: workerLimit,
				})
			} else {
				if tuiFlag {
					slog.Warn("--tui requires a terminal, falling back to inline output")
				}
				presenter = ui.NewPresenter(ui.Config{
					Writer:     os.Stdout,
					ErrWriter:  os.Stderr,
					IsTTY:      isTTY,
					Quiet:      quiet,
					Verbose:    verbose,
					ForceFeed:  forceFeed,
					ForceRate:  forceRate,
					NoProgress: noProgress,
					Stats:      collector,
					Workers:    workers,
					DstRoot:    dst,
				})
			}

			engineCfg := engine.Config{
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
				Events:         events,
				Stats:          collector,
				Delete:         deleteFlag,
				Verify:         verifyFlag,
				NoTimes:        noTimes,
				WorkerLimit:    workerLimit,
			}

			// Only set filter if it has rules/size constraints.
			if !chain.Empty() {
				engineCfg.Filter = chain
			}

			slog.Debug("starting copy",
				"src", src,
				"dst", dst,
				"workers", workers,
				"archive", archive,
				"recursive", recursive,
				"iouring", useIOURing,
			)

			useTUI := tuiFlag && isTTY
			var result engine.Result

			if useTUI {
				// TUI mode: run engine in background, TUI in foreground.
				// Bubble Tea needs the foreground to capture stdin properly.
				engineCtx, engineCancel := context.WithCancel(ctx)
				defer engineCancel()

				var engineWg sync.WaitGroup
				engineWg.Add(1)
				go func() {
					defer engineWg.Done()
					result = engine.Run(engineCtx, engineCfg)
					close(events)
				}()

				// TUI runs in foreground — blocks until user quits.
				_ = presenter.Run(events)

				// User quit the TUI — cancel engine if still running.
				engineCancel()
				engineWg.Wait()
				stop()
			} else {
				// Inline mode: run presenter in background, engine in foreground.
				var presenterWg sync.WaitGroup
				presenterWg.Add(1)
				go func() {
					defer presenterWg.Done()
					_ = presenter.Run(events)
				}()

				result = engine.Run(ctx, engineCfg)
				stop()
				close(events)
				presenterWg.Wait()
			}

			if !quiet {
				summary := presenter.Summary()
				if summary != "" {
					fmt.Fprintln(os.Stderr, summary)
				}
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
	rootCmd.Flags().BoolVar(&forceFeed, "feed", false, "force feed mode (one line per file)")
	rootCmd.Flags().BoolVar(&forceRate, "rate", false, "force rate mode (sparkline + throughput)")
	rootCmd.Flags().BoolVar(&noProgress, "no-progress", false, "disable progress display")
	rootCmd.Flags().BoolVar(&tuiFlag, "tui", false, "full-screen TUI (Bubble Tea) for large transfers")

	// Filter flags — use custom pflag.Value to preserve CLI ordering.
	rootCmd.Flags().VarP(&filterFlag{chain: chain, include: false}, "exclude", "", "exclude files matching PATTERN (repeatable)")
	rootCmd.Flags().VarP(&filterFlag{chain: chain, include: true}, "include", "", "include files matching PATTERN (repeatable)")
	rootCmd.Flags().StringVar(&filterFile, "filter", "", "read filter rules from FILE")
	rootCmd.Flags().BoolVar(&deleteFlag, "delete", false, "delete extraneous files from destination")
	rootCmd.Flags().BoolVar(&verifyFlag, "verify", false, "verify checksums after copy (BLAKE3)")
	rootCmd.Flags().BoolVar(&noTimes, "no-times", false, "don't preserve mtime (disables skip detection on re-runs)")
	rootCmd.Flags().StringVar(&minSizeStr, "min-size", "", "skip files smaller than SIZE (e.g. 1M, 100K)")
	rootCmd.Flags().StringVar(&maxSizeStr, "max-size", "", "skip files larger than SIZE (e.g. 1G, 500M)")

	// Mark --exclude and --include as allowing repeated use.
	rootCmd.Flags().SetAnnotation("exclude", "cobra_annotation_one_required", nil)
	rootCmd.Flags().SetAnnotation("include", "cobra_annotation_one_required", nil)
	rootCmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Name == "exclude" || f.Name == "include" {
			f.NoOptDefVal = ""
		}
	})

	if err := rootCmd.Execute(); err != nil {
		if exitErr, ok := err.(*exitError); ok {
			return exitErr.code
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 2
	}

	return 0
}

// applyConfigDefaults applies config file defaults for flags not explicitly set on the CLI.
func applyConfigDefaults(cmd *cobra.Command, defaults config.DefaultsConfig, verify *bool, workers *int, tuiFlag *bool, archive *bool) {
	if !cmd.Flags().Changed("verify") && defaults.Verify != nil {
		*verify = *defaults.Verify
	}
	if !cmd.Flags().Changed("workers") && defaults.Workers != nil {
		*workers = *defaults.Workers
	}
	if !cmd.Flags().Changed("tui") && defaults.TUI != nil {
		*tuiFlag = *defaults.TUI
	}
	if !cmd.Flags().Changed("archive") && defaults.Archive != nil {
		*archive = *defaults.Archive
	}
}

type exitError struct {
	code int
}

func (e *exitError) Error() string {
	return fmt.Sprintf("exit code %d", e.code)
}
