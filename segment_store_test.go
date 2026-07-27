package commitlog

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// A FileSegmentStore round-trips an object and reports its size.
func TestFileSegmentStore_PutSizeReadDelete(t *testing.T) {
	store, err := NewFileSegmentStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("abcdefgh"), 4096) // 32 KiB
	if err := store.Put("seg1", bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	if sz, err := store.Size("seg1"); err != nil || sz != int64(len(data)) {
		t.Fatalf("size=%d err=%v want %d", sz, err, len(data))
	}
	keys, err := store.List()
	if err != nil || len(keys) != 1 || keys[0] != "seg1" {
		t.Fatalf("list=%v err=%v", keys, err)
	}
	got := make([]byte, 100)
	n, err := store.ReadAt("seg1", got, 8000)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:n], data[8000:8000+n]) {
		t.Fatal("range read mismatch")
	}
	if err := store.Delete("seg1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Size("seg1"); err == nil {
		t.Fatal("expected error after delete")
	}
	// Deleting an absent key is a no-op.
	if err := store.Delete("seg1"); err != nil {
		t.Fatalf("delete absent should be nil, got %v", err)
	}
}

// storeBacking reads through the store transparently and correctly across the
// prefetch-window boundary, matching io.ReaderAt semantics.
func TestStoreBacking_ReadThroughAcrossPrefetch(t *testing.T) {
	store, err := NewFileSegmentStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// Larger than one prefetch window so reads cross the boundary.
	size := prefetchSize*2 + 1234
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i * 31)
	}
	if err := store.Put("seg", bytes.NewReader(data), int64(size)); err != nil {
		t.Fatal(err)
	}

	b, err := newStoreBacking(store, "seg")
	if err != nil {
		t.Fatal(err)
	}
	if sz, _ := b.Size(); sz != int64(size) {
		t.Fatalf("backing size %d want %d", sz, size)
	}

	// Reads at assorted offsets/lengths, including ones spanning the 1 MiB
	// prefetch boundary and the tail.
	for _, tc := range []struct{ off, n int64 }{
		{0, 10},
		{prefetchSize - 5, 20},    // straddles the first window
		{prefetchSize, 4096},      // exactly at a window edge
		{2*prefetchSize - 1, 500}, // straddles the second window
		{int64(size) - 100, 100},  // exact tail
		{int64(size) - 10, 50},    // past-tail short read -> EOF
	} {
		p := make([]byte, tc.n)
		n, err := b.ReadAt(p, tc.off)
		end := tc.off + int64(n)
		if err != nil && err != io.EOF {
			t.Fatalf("off=%d n=%d: %v", tc.off, tc.n, err)
		}
		if !bytes.Equal(p[:n], data[tc.off:end]) {
			t.Fatalf("off=%d: content mismatch", tc.off)
		}
	}

	// Reading entirely past the end is io.EOF.
	if _, err := b.ReadAt(make([]byte, 8), int64(size)); err != io.EOF {
		t.Fatalf("read past end: got %v want EOF", err)
	}

	// A full sequential scan through the backing reconstructs the object.
	var got bytes.Buffer
	buf := make([]byte, 7000) // odd size, not aligned to the window
	off := int64(0)
	for off < int64(size) {
		n, err := b.ReadAt(buf, off)
		got.Write(buf[:n])
		off += int64(n)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got.Bytes(), data) {
		t.Fatal("sequential scan reconstruction mismatch")
	}

	// Writes are rejected on a read-only backing.
	if _, err := b.Write([]byte("x")); err == nil {
		t.Fatal("expected write to offloaded backing to fail")
	}
}

// A restore-required store surfaces ErrRestoreRequired when opening a backing.
func TestStoreBacking_RestoreRequired(t *testing.T) {
	rr := &restoreRequiredStore{}
	if _, err := newStoreBacking(rr, "seg"); !errors.Is(err, ErrRestoreRequired) {
		t.Fatalf("got %v want ErrRestoreRequired", err)
	}
}

type restoreRequiredStore struct{}

func (restoreRequiredStore) Put(string, io.Reader, int64) error        { return nil }
func (restoreRequiredStore) ReadAt(string, []byte, int64) (int, error) { return 0, ErrRestoreRequired }
func (restoreRequiredStore) Stream(string, int64) (io.ReadCloser, error) {
	return nil, ErrRestoreRequired
}
func (restoreRequiredStore) Size(string) (int64, error) { return 0, ErrRestoreRequired }
func (restoreRequiredStore) List() ([]string, error)    { return nil, nil }
func (restoreRequiredStore) Delete(string) error        { return nil }
func (restoreRequiredStore) LiveRead() bool             { return false }
