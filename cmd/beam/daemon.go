package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/bamsammich/beam/internal/config"
	"github.com/bamsammich/beam/internal/transport/proto"
)

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Run a beam protocol daemon",
	Long: `Run a beam protocol daemon that serves files over the beam binary protocol.

The daemon listens for TLS connections and authenticates clients using SSH
public key challenge/response. Each client presents their SSH public key and
proves ownership by signing a server-generated nonce. The daemon verifies the
key against the user's ~/.ssh/authorized_keys.

On successful authentication, the daemon forks a child process running as the
authenticated user (requires root or CAP_SETUID). Use --no-fork for single-user
mode (runs as the daemon's own user).

Connection info (port + TLS fingerprint) is written to /etc/beam/daemon.toml so
that beam clients connecting over SSH can discover and authenticate automatically.

The daemon generates a persistent self-signed TLS certificate on first run,
stored at /etc/beam/daemon.{crt,key}. Provide --tls-cert and --tls-key to use
your own certificate.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE:          runDaemon,
}

func init() {
	daemonCmd.Flags().String("listen", ":9876", "listen address (host:port)")
	daemonCmd.Flags().String("root", "/", "root directory to serve")
	daemonCmd.Flags().String("tls-cert", "", "path to TLS certificate file")
	daemonCmd.Flags().String("tls-key", "", "path to TLS private key file")
	daemonCmd.Flags().Bool("no-fork", false, "disable fork-per-connection (single-user mode)")
}

//nolint:revive // cyclomatic: flag parsing + validation + TLS + discovery — irreducible
func runDaemon(cmd *cobra.Command, _ []string) error {
	listenAddr, _ := cmd.Flags().GetString("listen")    //nolint:errcheck // flag name is hardcoded
	root, _ := cmd.Flags().GetString("root")            //nolint:errcheck // flag name is hardcoded
	tlsCertFile, _ := cmd.Flags().GetString("tls-cert") //nolint:errcheck // flag name is hardcoded
	tlsKeyFile, _ := cmd.Flags().GetString("tls-key")   //nolint:errcheck // flag name is hardcoded
	noFork, _ := cmd.Flags().GetBool("no-fork")         //nolint:errcheck // flag name is hardcoded

	// Validate root exists.
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("root directory %q: %w", root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("root %q is not a directory", root)
	}

	// Load or generate persistent TLS certificate.
	var tlsCert tls.Certificate
	if tlsCertFile != "" && tlsKeyFile != "" {
		var tlsErr error
		tlsCert, tlsErr = tls.LoadX509KeyPair(tlsCertFile, tlsKeyFile)
		if tlsErr != nil {
			return fmt.Errorf("load TLS certificate: %w", tlsErr)
		}
	} else {
		// Use persistent cert at default paths (generate on first run).
		var genErr error
		tlsCert, _, genErr = proto.LoadOrGenerateCert("", "")
		if genErr != nil {
			return fmt.Errorf("TLS cert: %w", genErr)
		}
	}

	forkMode := !noFork && os.Getuid() == 0

	cfg := proto.DaemonConfig{
		TLSCert:    &tlsCert,
		ListenAddr: listenAddr,
		Root:       root,
		ForkMode:   forkMode,
	}

	// Configure logging.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	if forkMode {
		slog.Info("fork mode enabled (running as root)")
	} else if !noFork && os.Getuid() != 0 {
		slog.Warn(
			"not running as root; fork-per-connection disabled (all operations run as daemon user)",
		)
	}

	daemon, err := proto.NewDaemon(cfg)
	if err != nil {
		return err
	}

	// Write discovery file so SSH-bootstrapped clients can find us.
	addr := daemon.Addr()
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("unexpected listener address type: %T", addr)
	}
	if err := config.WriteDaemonDiscovery(config.DaemonDiscovery{
		Fingerprint: daemon.Fingerprint(),
		Port:        tcpAddr.Port,
	}); err != nil {
		slog.Warn("failed to write daemon discovery file", "error", err)
	}

	// Set up signal handling.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	defer config.RemoveDaemonDiscovery()

	return daemon.Serve(ctx)
}
