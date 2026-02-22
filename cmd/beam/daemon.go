package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/bamsammich/beam/internal/transport/proto"
)

var daemonCmd = &cobra.Command{
	Use:   "daemon",
	Short: "Run a beam protocol daemon",
	Long: `Run a beam protocol daemon that serves files from a root directory.

Clients connect over TLS with a bearer token and can perform file operations
(read, write, hash, walk) using the beam binary protocol.

The daemon generates a self-signed TLS certificate by default. Provide
--tls-cert and --tls-key to use your own certificate.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE:          runDaemon,
}

func init() {
	daemonCmd.Flags().String("listen", ":7223", "listen address (host:port)")
	daemonCmd.Flags().String("root", "", "root directory to serve (required)")
	daemonCmd.Flags().String("token", "", "authentication token (required)")
	daemonCmd.Flags().String("tls-cert", "", "path to TLS certificate file")
	daemonCmd.Flags().String("tls-key", "", "path to TLS private key file")

	daemonCmd.MarkFlagRequired("root")  //nolint:errcheck // cobra handles this
	daemonCmd.MarkFlagRequired("token") //nolint:errcheck // cobra handles this
}

func runDaemon(cmd *cobra.Command, _ []string) error {
	listenAddr, _ := cmd.Flags().GetString("listen")    //nolint:errcheck // flag name is hardcoded
	root, _ := cmd.Flags().GetString("root")            //nolint:errcheck // flag name is hardcoded
	token, _ := cmd.Flags().GetString("token")          //nolint:errcheck // flag name is hardcoded
	tlsCertFile, _ := cmd.Flags().GetString("tls-cert") //nolint:errcheck // flag name is hardcoded
	tlsKeyFile, _ := cmd.Flags().GetString("tls-key")   //nolint:errcheck // flag name is hardcoded

	// Validate root exists.
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("root directory %q: %w", root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("root %q is not a directory", root)
	}

	cfg := proto.DaemonConfig{
		ListenAddr: listenAddr,
		Root:       root,
		AuthToken:  token,
	}

	// Load TLS certificate if provided.
	if tlsCertFile != "" && tlsKeyFile != "" {
		cert, tlsErr := tls.LoadX509KeyPair(tlsCertFile, tlsKeyFile)
		if tlsErr != nil {
			return fmt.Errorf("load TLS certificate: %w", tlsErr)
		}
		cfg.TLSCert = &cert
	}

	// Configure logging.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	daemon, err := proto.NewDaemon(cfg)
	if err != nil {
		return err
	}

	// Set up signal handling.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	return daemon.Serve(ctx)
}
