package transport

// Compile-time interface check.
var _ Transport = (*LocalTransport)(nil)

// LocalTransport creates local filesystem endpoints.
// Each ReaderAt/ReadWriterAt call returns a fresh endpoint rooted at the
// given path — this is critical for multi-source transfers where the engine
// calls ReaderAt with different source paths.
type LocalTransport struct{}

// NewLocalTransport returns a new LocalTransport.
func NewLocalTransport() *LocalTransport {
	return &LocalTransport{}
}

//nolint:ireturn // implements Transport interface
func (*LocalTransport) ReaderAt(path string) (Reader, error) {
	return NewLocalReader(path), nil
}

//nolint:ireturn // implements Transport interface
func (*LocalTransport) ReadWriterAt(path string) (ReadWriter, error) {
	return NewLocalWriter(path), nil
}

func (*LocalTransport) Protocol() Protocol { return ProtocolLocal }
func (*LocalTransport) Close() error       { return nil }
