package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
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

The daemon listens for TLS connections and authenticates clients using a bearer
token. By default it generates a random token and writes it (along with the
listen port) to /etc/beam/daemon.toml so that beam clients connecting over SSH
can discover and authenticate automatically.

The daemon serves the entire filesystem visible to the running user. Use --root
to restrict access to a specific directory.

The daemon generates a self-signed TLS certificate by default. Provide
--tls-cert and --tls-key to use your own certificate.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE:          runDaemon,
}

func init() {
	daemonCmd.Flags().String("listen", ":9876", "listen address (host:port)")
	daemonCmd.Flags().String("root", "/", "root directory to serve")
	daemonCmd.Flags().String("token", "", "authentication token (auto-generated if not set)")
	daemonCmd.Flags().String("tls-cert", "", "path to TLS certificate file")
	daemonCmd.Flags().String("tls-key", "", "path to TLS private key file")
}

//nolint:revive // cyclomatic: flag parsing + validation + token gen + TLS + discovery — irreducible
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

	// Auto-generate token if not provided.
	if token == "" {
		tokenBytes := make([]byte, 32)
		if _, randErr := rand.Read(tokenBytes); randErr != nil {
			return fmt.Errorf("generate auth token: %w", randErr)
		}
		token = hex.EncodeToString(tokenBytes)
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

	// Write discovery file so SSH-bootstrapped clients can find us.
	addr := daemon.Addr()
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		return fmt.Errorf("unexpected listener address type: %T", addr)
	}
	if err := config.WriteDaemonDiscovery(config.DaemonDiscovery{
		Token: token,
		Port:  tcpAddr.Port,
	}); err != nil {
		slog.Warn("failed to write daemon discovery file", "error", err)
	}

	// Set up signal handling.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	defer config.RemoveDaemonDiscovery()

	return daemon.Serve(ctx)
}
