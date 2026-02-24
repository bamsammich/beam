package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/bamsammich/beam/internal/config"
	"github.com/bamsammich/beam/internal/engine"
	"github.com/bamsammich/beam/internal/event"
	"github.com/bamsammich/beam/internal/filter"
	"github.com/bamsammich/beam/internal/stats"
	"github.com/bamsammich/beam/internal/transport"
	"github.com/bamsammich/beam/internal/transport/beam"
	"github.com/bamsammich/beam/internal/transport/proto"
	"github.com/bamsammich/beam/internal/ui"
	"github.com/bamsammich/beam/internal/ui/tui"
)

// transportFor creates a Transport for the given location.
//
//nolint:ireturn // factory returns interface by design
func transportFor(
	loc transport.Location,
	authOpts proto.AuthOpts,
	sshKeyFile string,
	sshPort int,
	noBeamSSH bool,
) (transport.Transport, error) {
	if loc.IsBeam() {
		return beam.NewTransport(beamAddr(loc), authOpts, proto.ClientTLSConfig()), nil
	}
	if loc.IsRemote() {
		return beam.NewSSHTransport(beam.SSHTransportOpts{
			AuthOpts: authOpts,
			Host:     loc.Host,
			User:     loc.User,
			SSHOpts: transport.SSHOpts{
				Port:    sshPort,
				KeyFile: sshKeyFile,
			},
			NoBeam: noBeamSSH,
		}), nil
	}
	return transport.NewLocalTransport(), nil
}

func beamAddr(loc transport.Location) string {
	port := loc.Port
	if port == 0 {
		port = transport.DefaultBeamPort
	}
	return fmt.Sprintf("%s:%d", loc.Host, port)
}

var version = "dev"

func main() {
	// Worker mode: re-exec'd child process for fork-per-connection.
	// Must be checked before cobra to avoid flag conflicts.
	if len(os.Args) == 2 && os.Args[1] == proto.WorkerModeFlag {
		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
		slog.SetDefault(logger)

		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		err := proto.RunWorker(ctx)
		stop()

		if err != nil {
			slog.Error("worker failed", "error", err)
			os.Exit(1) //nolint:gocritic // exitAfterDefer: no defers active at this point
		}
		return
	}

	os.Exit(run())
}

// filterFlag is a custom pflag.Value that preserves CLI ordering of
// --exclude and --include rules by appending to a shared filter.Chain.
type filterFlag struct {
	chain   *filter.Chain
	include bool
}

func (*filterFlag) String() string { return "" }
func (*filterFlag) Type() string   { return "string" }

func (f *filterFlag) Set(val string) error {
	if f.include {
		return f.chain.AddInclude(val)
	}
	return f.chain.AddExclude(val)
}

//nolint:gocyclo,revive // cyclomatic,cognitive-complexity: main CLI entry point orchestrates all flag parsing and mode selection
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
		sshKeyFile     string
		sshPort        int
		deltaFlag      bool
		noDelta        bool
		identityFile   string
		fingerprint    string
		bwLimitStr     string
		logFile        string
		benchmarkFlag  bool
		noBeamSSH      bool
	)

	chain := filter.NewChain()

	rootCmd := &cobra.Command{
		Use:   "beam [flags] <source>... <destination>",
		Short: "Fast, parallel file copy with delta sync and a beautiful CLI",
		Args: func(cmd *cobra.Command, args []string) error {
			if showVersion {
				return nil
			}
			return cobra.MinimumNArgs(2)(cmd, args)
		},
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if showVersion {
				fmt.Fprintf(os.Stdout, "beam %s\n", version)
				return nil
			}

			rawSources := args[:len(args)-1]
			rawDst := args[len(args)-1]

			// Parse locations for remote detection.
			dstLoc := transport.ParseLocation(rawDst)
			var srcLocs []transport.Location
			for _, s := range rawSources {
				srcLocs = append(srcLocs, transport.ParseLocation(s))
			}

			// Validate: at most one side can be remote.
			srcRemote := false
			for _, loc := range srcLocs {
				if loc.IsRemote() {
					srcRemote = true
					break
				}
			}
			if srcRemote && dstLoc.IsRemote() {
				return errors.New(
					"remote-to-remote transfers are not supported; one side must be local",
				)
			}
			if srcRemote && len(srcLocs) > 1 {
				return errors.New("multiple remote sources are not supported")
			}

			// Build local path lists (engine.Config.Sources and .Dst use local paths).
			sources := make([]string, len(rawSources))
			for i, loc := range srcLocs {
				sources[i] = loc.Path
			}
			dst := dstLoc.Path

			// Build auth opts for beam connections (SSH pubkey auth).
			needsBeamAuth := srcLocs[0].IsBeam() || srcLocs[0].IsRemote() ||
				dstLoc.IsBeam() || dstLoc.IsRemote()
			var authOpts proto.AuthOpts
			if needsBeamAuth {
				var authErr error
				authOpts, authErr = proto.LoadClientAuth("", identityFile)
				if authErr != nil {
					slog.Debug("beam auth not available", "error", authErr)
				}
				authOpts.Fingerprint = fingerprint
			}

			// Create connectors for source and destination.
			srcConn, err := transportFor(srcLocs[0], authOpts, sshKeyFile, sshPort, noBeamSSH)
			if err != nil {
				return fmt.Errorf("source %s: %w", srcLocs[0], err)
			}
			defer srcConn.Close()

			dstConn, err := transportFor(dstLoc, authOpts, sshKeyFile, sshPort, noBeamSSH)
			if err != nil {
				return fmt.Errorf("destination %s: %w", dstLoc, err)
			}
			defer dstConn.Close()

			isRemote := srcConn.Protocol() != transport.ProtocolLocal ||
				dstConn.Protocol() != transport.ProtocolLocal
			useDelta := deltaFlag && !noDelta && isRemote

			// Load optional config file.
			cfg, err := config.Load()
			if err != nil {
				slog.Warn("failed to load config", "error", err)
			}

			// Apply config defaults for flags not explicitly set on CLI.
			applyConfigDefaults(cmd, cfg.Defaults, &verifyFlag, &workers, &tuiFlag, &archive)

			// Apply bwlimit from config if not set on CLI.
			if !cmd.Flags().Changed("bwlimit") && cfg.Defaults.BWLimit != nil {
				bwLimitStr = *cfg.Defaults.BWLimit
			}

			// Parse bandwidth limit.
			var bwLimit int64
			if bwLimitStr != "" {
				bwLimit, err = filter.ParseSize(bwLimitStr)
				if err != nil {
					return fmt.Errorf("invalid --bwlimit: %w", err)
				}
			}

			// Configure logging.
			logLevel := slog.LevelWarn
			if verbose {
				logLevel = slog.LevelDebug
			} else if !quiet {
				logLevel = slog.LevelInfo
			}
			textHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
				Level: logLevel,
			})
			var logHandler slog.Handler = textHandler
			if logFile != "" {
				lf, lfErr := os.Create(logFile)
				if lfErr != nil {
					return fmt.Errorf("open log file: %w", lfErr)
				}
				defer lf.Close()
				jsonHandler := slog.NewJSONHandler(lf, &slog.HandlerOptions{
					Level: slog.LevelDebug,
				})
				logHandler = ui.NewMultiHandler(textHandler, jsonHandler)
			}
			logger := slog.New(logHandler)
			slog.SetDefault(logger)

			if dryRun {
				slog.Info("dry run mode")
			}

			// Default workers.
			workersExplicit := cmd.Flags().Changed("workers")
			if workers <= 0 {
				workers = min(runtime.NumCPU()*2, 32)
			}

			// Benchmark mode: measure throughput and auto-tune workers.
			if benchmarkFlag {
				benchResult, benchErr := engine.RunBenchmark(context.Background(), sources[0], dst)
				if benchErr != nil {
					slog.Warn("benchmark failed", "error", benchErr)
				} else {
					fmt.Fprintln(os.Stderr, engine.FormatBenchmark(benchResult))
					if !workersExplicit {
						workers = benchResult.SuggestedWorkers
					}
				}
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

			// When --log is set, tee events through a logging goroutine
			// that writes structured records before forwarding to the presenter.
			presenterEvents := (<-chan event.Event)(events)
			if logFile != "" {
				teed := make(chan event.Event, 256)
				go func() {
					for ev := range events {
						attrs := []slog.Attr{
							slog.String("type", ev.Type.String()),
							slog.String("path", ev.Path),
							slog.Int64("size", ev.Size),
							slog.Int("worker", ev.WorkerID),
						}
						if ev.Error != nil {
							attrs = append(attrs, slog.String("error", ev.Error.Error()))
						}
						slog.LogAttrs(context.Background(), slog.LevelInfo, "beam.event", attrs...)
						teed <- ev
					}
					close(teed)
				}()
				presenterEvents = teed
			}

			// Create worker throttle.
			workerLimit := &atomic.Int32{}
			wk := int32(workers) //nolint:gosec // G115: workers bounded by min(NumCPU*2, 32)
			workerLimit.Store(wk)

			// Format source display string for presenters.
			srcDisplay := rawSources[0]
			if len(rawSources) > 1 {
				srcDisplay = fmt.Sprintf("%s (+%d more)", rawSources[0], len(rawSources)-1)
			}

			// Create presenter.
			isTTY := ui.IsTTY(os.Stderr.Fd())
			var presenter ui.Presenter
			if tuiFlag && isTTY {
				presenter = tui.NewPresenter(tui.Config{
					Stats:       collector,
					Workers:     workers,
					DstRoot:     rawDst,
					SrcRoot:     srcDisplay,
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
					DstRoot:    rawDst,
				})
			}

			engineCfg := engine.Config{
				Sources:        sources,
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
				SrcTransport:   srcConn,
				DstTransport:   dstConn,
				Delta:          useDelta,
				BWLimit:        bwLimit,
			}

			// Only set filter if it has rules/size constraints.
			if !chain.Empty() {
				engineCfg.Filter = chain
			}

			slog.Debug("starting copy",
				"sources", sources,
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
				_ = presenter.Run(presenterEvents) //nolint:errcheck // presenter error is non-fatal

				// User quit the TUI — cancel engine if still running.
				engineCancel()
				engineWg.Wait()
				stop()
			} else {
				// Inline mode: run presenter in background, engine in foreground.
				var presenterErr error
				var presenterWg sync.WaitGroup
				presenterWg.Add(1)
				go func() {
					defer presenterWg.Done()
					presenterErr = presenter.Run(presenterEvents)
				}()

				result = engine.Run(ctx, engineCfg)
				stop()
				close(events)
				presenterWg.Wait()
				if presenterErr != nil {
					fmt.Fprintf(os.Stderr, "presenter: %v\n", presenterErr)
				}
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
	rootCmd.Flags().
		BoolVarP(&archive, "archive", "a", false, "archive mode (recursive + preserve all)")
	rootCmd.Flags().
		IntVarP(&workers, "workers", "n", 0, "number of copy workers (default: min(NumCPU*2, 32))")
	rootCmd.Flags().
		Int64Var(&chunkThreshold, "chunk-threshold", 256*1024*1024, "split files larger than this into chunks (bytes)")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose output")
	rootCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "suppress all output except errors")
	rootCmd.Flags().BoolVar(&dryRun, "dry-run", false, "show what would be copied without writing")
	rootCmd.Flags().
		BoolVar(&useIOURing, "iouring", false, "use io_uring for file copy (Linux only)")
	rootCmd.Flags().BoolVar(&forceFeed, "feed", false, "force feed mode (one line per file)")
	rootCmd.Flags().BoolVar(&forceRate, "rate", false, "force rate mode (sparkline + throughput)")
	rootCmd.Flags().BoolVar(&noProgress, "no-progress", false, "disable progress display")
	rootCmd.Flags().
		BoolVar(&tuiFlag, "tui", false, "full-screen TUI (Bubble Tea) for large transfers")

	// Filter flags — use custom pflag.Value to preserve CLI ordering.
	rootCmd.Flags().
		VarP(&filterFlag{chain: chain, include: false}, "exclude", "", "exclude files matching PATTERN (repeatable)")
	rootCmd.Flags().
		VarP(&filterFlag{chain: chain, include: true}, "include", "", "include files matching PATTERN (repeatable)")
	rootCmd.Flags().StringVar(&filterFile, "filter", "", "read filter rules from FILE")
	rootCmd.Flags().
		BoolVar(&deleteFlag, "delete", false, "delete extraneous files from destination")
	rootCmd.Flags().BoolVar(&verifyFlag, "verify", false, "verify checksums after copy (BLAKE3)")
	rootCmd.Flags().
		BoolVar(&noTimes, "no-times", false, "don't preserve mtime (disables skip detection on re-runs)")
	rootCmd.Flags().
		StringVar(&minSizeStr, "min-size", "", "skip files smaller than SIZE (e.g. 1M, 100K)")
	rootCmd.Flags().
		StringVar(&maxSizeStr, "max-size", "", "skip files larger than SIZE (e.g. 1G, 500M)")
	rootCmd.Flags().
		StringVar(&sshKeyFile, "ssh-key", "", "SSH private key file (default: auto-detect)")
	rootCmd.Flags().IntVar(&sshPort, "ssh-port", 22, "SSH port")
	rootCmd.Flags().BoolVar(&deltaFlag, "delta", true, "use delta transfer for remote copies")
	rootCmd.Flags().BoolVar(&noDelta, "no-delta", false, "disable delta transfer")
	rootCmd.Flags().
		StringVarP(&identityFile, "identity", "i", "", "SSH private key file for beam auth (like ssh -i)")
	rootCmd.Flags().
		StringVar(&fingerprint, "fingerprint", "", "expected TLS fingerprint for beam:// (SHA256:...)")
	rootCmd.Flags().
		StringVar(&bwLimitStr, "bwlimit", "", "bandwidth limit (e.g. 100M, 1G)")
	rootCmd.Flags().
		StringVar(&logFile, "log", "", "write structured JSON log to FILE")
	rootCmd.Flags().
		BoolVar(&benchmarkFlag, "benchmark", false, "measure throughput before copy and auto-tune workers")
	rootCmd.Flags().
		BoolVar(&noBeamSSH, "no-beam-ssh", false, "disable beam daemon auto-detection over SSH (force SFTP)")

	// Register subcommands.
	rootCmd.AddCommand(daemonCmd)
	rootCmd.AddCommand(docsCmd)

	// Mark --exclude and --include as allowing repeated use.
	if err := rootCmd.Flags().
		SetAnnotation("exclude", "cobra_annotation_one_required", nil); err != nil {
		panic(fmt.Sprintf("set flag annotation: %v", err))
	}
	if err := rootCmd.Flags().
		SetAnnotation("include", "cobra_annotation_one_required", nil); err != nil {
		panic(fmt.Sprintf("set flag annotation: %v", err))
	}
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
func applyConfigDefaults(
	cmd *cobra.Command,
	defaults config.DefaultsConfig,
	verify *bool,
	workers *int,
	tuiFlag *bool,
	archive *bool,
) {
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
