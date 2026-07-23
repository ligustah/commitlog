package commitlog

import "os"

// segmentBacking is the byte store behind a segment's log file. The default is a
// local file (localBacking); a tiered/remote store implements the same
// interface so an offloaded, sealed segment reads through transparently. Only
// the local, active segment uses Write — remote backings are read-only, since a
// segment is offloaded only after it is sealed.
//
// This is the seam tiered storage builds on: it decouples "where a segment's
// bytes live" from the segment's block/index/scan logic above it, which reads
// exclusively through ReadAt and appends exclusively through Write.
type segmentBacking interface {
	// ReadAt reads len(p) bytes at off, with io.ReaderAt semantics.
	ReadAt(p []byte, off int64) (int, error)
	// Write appends p to the backing (active, local segments only).
	Write(p []byte) (int, error)
	// Size returns the current byte length of the backing.
	Size() (int64, error)
	// Sync flushes buffered writes to durable storage (no-op for read-only
	// backings).
	Sync() error
	// Close releases the backing's handle.
	Close() error
	// Name returns a human-readable identifier (the file path for a local
	// backing), used in diagnostics and delete.
	Name() string
}

// localBacking is a segmentBacking over a local *os.File — the default and, for
// the active segment, the only writable backing.
type localBacking struct {
	f *os.File
}

// openLocalBacking opens (creating if needed) an append-mode local file backing.
func openLocalBacking(path string) (*localBacking, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &localBacking{f: f}, nil
}

func (l *localBacking) ReadAt(p []byte, off int64) (int, error) { return l.f.ReadAt(p, off) }
func (l *localBacking) Write(p []byte) (int, error)             { return l.f.Write(p) }
func (l *localBacking) Sync() error                             { return l.f.Sync() }
func (l *localBacking) Close() error                            { return l.f.Close() }
func (l *localBacking) Name() string                            { return l.f.Name() }

func (l *localBacking) Size() (int64, error) {
	fi, err := l.f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
