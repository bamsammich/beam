package transport

// Compile-time interface check.
var _ Connector = (*LocalConnector)(nil)

// LocalConnector creates local filesystem endpoints.
// Each ConnectRead/ConnectWrite call returns a fresh endpoint rooted at the
// given path — this is critical for multi-source transfers where the engine
// calls ConnectRead with different source paths.
type LocalConnector struct{}

// NewLocalConnector returns a new LocalConnector.
func NewLocalConnector() *LocalConnector {
	return &LocalConnector{}
}

//nolint:ireturn // implements Connector interface
func (*LocalConnector) ConnectRead(path string) (ReadEndpoint, error) {
	return NewLocalReadEndpoint(path), nil
}

//nolint:ireturn // implements Connector interface
func (*LocalConnector) ConnectWrite(path string) (WriteEndpoint, error) {
	return NewLocalWriteEndpoint(path), nil
}

func (*LocalConnector) Protocol() Protocol { return ProtocolLocal }
func (*LocalConnector) Close() error       { return nil }
