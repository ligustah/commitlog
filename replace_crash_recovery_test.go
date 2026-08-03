package commitlog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// Installing a rewritten segment is TWO renames, and a machine can stop between
// them.
//
//	os.Rename(s.logPath(), old.logPath())    // compacted data over the source
//	os.Rename(s.indexPath(), old.indexPath())
//
// Lose power after the first and the directory holds the new log paired with
// the OLD index — an index whose positions were computed against a file that no
// longer exists and that was strictly larger, since a rewrite only ever drops
// records. Every position in it now points somewhere else: into the middle of a
// record, or past the end.
//
// Nothing about the crash is unusual, and nothing on disk marks it. The pair is
// individually well-formed; only their relationship is wrong. So the question
// is what a reopened log does with them, and the only unacceptable answer is to
// serve the mismatch as data.
//
// Run over every storage format, because the mismatch is detected by comparing
// index positions against the log, and a block-compressed segment's positions
// are not the same kind of number: an entry is a sparse anchor into the
// decompressed stream, not an offset into the file. The check has a separate
// branch for it, and a branch no test enters is a branch that only ever looked
// right.
func TestAReplaceInterruptedBetweenItsTwoRenames(t *testing.T) {
	for _, codec := range []compress.Codec{compress.None, compress.Snappy, compress.Zstd} {
		t.Run(fmt.Sprintf("codec=%d", codec), func(t *testing.T) {
			replaceInterruptedBetweenItsTwoRenames(t, codec)
		})
	}
}

func replaceInterruptedBetweenItsTwoRenames(t *testing.T, codec compress.Codec) {
	dir := tempDir(t)

	// A log worth compacting whose segments still SURVIVE the pass. Four keys
	// rewritten 400 times collapses the whole history into the active segment,
	// and then there is no rewritten sealed segment left to hold a stale index.
	// Mostly-unique keys with a hot one mixed in gives every sealed segment
	// something to drop — so each is rewritten, and each ends up smaller than
	// the index that described it — while leaving them all in the log.
	l, err := New(Options{
		Path:            dir,
		MaxSegmentBytes: 4096,
		Compact:         true,
		Compression:     codec,
	})
	require.NoError(t, err)

	const records = 800
	for i := range records {
		key := fmt.Sprintf("k:%08d", i) // written once, survives the pass
		if i%5 == 0 {
			key = "k:hot" // rewritten constantly, all but the last dropped
		}
		offs, err := l.Append([]*Message{{
			Key:   []byte(key),
			Value: []byte(fmt.Sprintf("v:%08d:%s", i, strings.Repeat("x", 48))),
		}})
		require.NoError(t, err)
		l.SetHighWatermark(offs[0])
	}

	// Snapshot the indexes as they are BEFORE the pass. These are the files a
	// crash between the renames leaves behind.
	before := snapshotIndexes(t, dir)
	require.NotEmpty(t, before, "no sealed segment to compact")

	require.NoError(t, l.Clean())
	require.NoError(t, l.Close())

	// The crash: the log files are the pass's output, the index files are the
	// source's. Only where the pass actually rewrote a segment — an index that
	// did not change is not a mismatch.
	after := snapshotIndexes(t, dir)
	mismatched := 0
	for name, old := range before {
		now, ok := after[name]
		if !ok || string(now) == string(old) {
			continue
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), old, 0o644))
		mismatched++
	}
	require.Positive(t, mismatched,
		"the pass rewrote no segment, so there is no crash to simulate")
	t.Logf("restored %d stale index file(s) over a compacted log", mismatched)

	// Reopen. Refusing is a fine answer; recovering is a better one. Serving
	// the mismatch as data is not an answer.
	reopened, err := New(Options{
		Path:            dir,
		MaxSegmentBytes: 4096,
		Compact:         true,
		Compression:     codec,
	})
	if err != nil {
		t.Logf("reopen refused, which is a lawful answer: %v", err)
		return
	}
	defer reopened.Close()

	rd, err := reopened.NewReader(From(reopened.OldestOffset()))
	if err != nil {
		t.Logf("opening a reader refused, which is a lawful answer: %v", err)
		return
	}

	ctx := context.Background()
	headers := make([]byte, HeaderBufferLen)
	seen := 0
	prev := int64(-1)
	for {
		msg, offset, _, _, err := rd.ReadMessage(ctx, headers)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// An error is the log noticing. That is the acceptable outcome.
			t.Logf("read stopped after %d record(s), which is a lawful answer: %v",
				seen, err)
			return
		}
		require.Greater(t, offset, prev,
			"a reopened log served offsets out of order — the stale index was "+
				"believed")
		prev = offset

		// Every record this log ever held has this shape. Anything else is a
		// position landing inside a record rather than at its start, served as
		// though it were one.
		v := string(msg.Value())
		require.True(t, strings.HasPrefix(v, "v:"),
			"a reopened log served a record whose value is not one that was "+
				"ever written: offset %d, value %q", offset, v)
		require.Len(t, v, len("v:00000000:")+48,
			"a reopened log served a record of the wrong length: offset %d, "+
				"value %q", offset, v)
		require.True(t, strings.HasPrefix(string(msg.Key()), "k:"),
			"a reopened log served a record whose key is not one that was ever "+
				"written: offset %d, key %q", offset, msg.Key())
		seen++
	}
	t.Logf("reopened cleanly and served %d record(s) over [%d]", seen, prev)

	// The scan above starts at a segment's first record, so it can walk from
	// position zero and never ask the index anything. A read that starts in the
	// MIDDLE has to look the offset up — which is the file the crash left stale.
	// Which offsets actually hold a record. Anything the forward scan returned
	// must answer a direct read too — an offset the log just handed out cannot
	// then be unreachable, and lumping "compaction dropped it" together with
	// "the index cannot find it" would hide exactly the failure being looked
	// for.
	live := map[int64]bool{}
	rd2, err := reopened.NewReader(From(reopened.OldestOffset()))
	require.NoError(t, err)
	for {
		_, offset, _, _, err := rd2.ReadMessage(ctx, headers)
		if err != nil {
			break
		}
		live[offset] = true
	}

	oldest, newest := reopened.OldestOffset(), reopened.NewestOffset()
	probed, answered := 0, 0
	for off := oldest; off <= newest; off++ {
		probed++
		rd, err := reopened.NewReader(From(off))
		if err != nil {
			require.False(t, live[off],
				"offset %d was returned by a forward scan but a read starting "+
					"there cannot be opened: %v", off, err)
			continue // compaction dropped it
		}
		msg, got, _, _, err := rd.ReadMessage(ctx, headers)
		if err != nil {
			require.False(t, live[off],
				"offset %d was returned by a forward scan but reading it "+
					"directly fails: %v", off, err)
			continue
		}
		answered++
		require.GreaterOrEqual(t, got, off,
			"a read from %d was answered with an EARLIER record, %d — the "+
				"stale index was believed", off, got)
		v := string(msg.Value())
		require.True(t, strings.HasPrefix(v, "v:") && len(v) == len("v:00000000:")+48,
			"a read from offset %d landed inside a record rather than at one: "+
				"offset %d, value %q", off, got, v)
	}
	require.Positive(t, answered,
		"no offset in the log answered a read, so nothing was actually probed")
	t.Logf("probed %d offset(s), %d answered, none from a stale position",
		probed, answered)
}

// snapshotIndexes reads every .index file in a log directory, by name.
func snapshotIndexes(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	out := map[string][]byte{}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), indexFileSuffix) {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		out[e.Name()] = b
	}
	return out
}
