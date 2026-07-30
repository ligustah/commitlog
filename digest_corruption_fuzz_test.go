package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A damaged key digest must never change the ANSWER, only the cost.
//
// The digest is a sidecar that says which offsets hold matching keys, and a
// KeyPrefix read trusts it to decide what to read — it is the whole reason that
// path can skip segments without opening them. That trust is precisely the shape
// both of this session's defects had: the prefix path served records without
// checking them because a digest had planned the read, as if planning were a
// warrant for the contents.
//
// So this asks what a CORRUPT digest can do, and the risk it targets is
// different from damaged record bytes. A digest that has lost or altered entries
// does not produce wrong bytes — it produces a read that quietly returns FEWER
// records than the log holds, with no error anywhere. Silent omission, which a
// caller cannot distinguish from "there was nothing else there".
//
// The invariant, therefore, is agreement rather than integrity:
//
//	a filtered read returns exactly what a full scan returns, or it fails.
//
// Digests are rebuilt from a segment scan when missing or invalid, so this
// should hold by construction: loadKeyDigest checks the sidecar's own CRC and
// returns nil on mismatch. "Should hold by construction" is what was believed
// about the read path too.
func FuzzCorruptDigestNeverChangesTheAnswer(f *testing.F) {
	// Seeds: (which sidecar, byte within it, xor mask).
	f.Add([]byte{0, 16, 0xFF})
	f.Add([]byte{1, 4, 0x01})
	f.Add([]byte{2, 40, 0x80})
	f.Add([]byte{0, 0, 0xAA}) // the header, where magic and version live
	f.Add([]byte{3, 96, 0x0F})
	// Fuzzer-found, and kept because the hand-picked seeds above have NO teeth:
	// with loadKeyDigest's sidecar CRC check deleted, every one of them still
	// passed, and this one failed in 2.4 seconds. It damages a byte the keyed
	// section actually depends on, which is what turns a corrupt digest into a
	// filtered read that disagrees with a full scan.
	f.Add([]byte("000"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 3 {
			t.Skip()
		}
		var (
			pick = int(data[0])
			at   = int(data[1])
			mask = data[2]
		)
		if mask == 0 {
			t.Skip()
		}

		dir := tempDir(t)
		opts := Options{
			Path:                 dir,
			MaxSegmentBytes:      256,
			Compact:              true,
			DisableAutoClean:     true,
			HWCheckpointInterval: time.Hour,
			CleanerInterval:      time.Hour,
		}
		l, err := New(opts)
		require.NoError(t, err)
		cl := l.(*commitLog)
		defer cl.Close() // nolint: errcheck

		const records = 30
		var last int64
		for i := 0; i < records; i++ {
			offs, aerr := cl.Append([]*Message{{
				Key:   []byte(fmt.Sprintf("want:%03d", i)),
				Value: []byte(fmt.Sprintf("payload-%03d-xxxxxxxx", i)),
			}})
			require.NoError(t, aerr)
			last = offs[0]
		}
		// Records under a different prefix, so a filter has something to exclude
		// and "returned everything" cannot pass for "filtered correctly".
		for i := 0; i < 6; i++ {
			offs, aerr := cl.Append([]*Message{{
				Key:   []byte(fmt.Sprintf("other:%03d", i)),
				Value: []byte("excluded"),
			}})
			require.NoError(t, aerr)
			last = offs[0]
		}
		cl.SetHighWatermark(last)

		// A pass persists the sidecars this test then damages.
		hw := cl.HighWatermark()
		_, cerr := cl.CleanWithSpec(CleanSpec{Ceiling: hw + 1})
		require.NoError(t, cerr)

		bound := cl.ActiveSegmentBase() - 1
		spec, rerr := cl.resolve([]ReadOption{KeyPrefix([]byte("want:")), Until(bound)})
		require.NoError(t, rerr)
		// The truth, read by walking every record rather than planning from a
		// digest. Captured BEFORE the damage.
		want := scanFiltered(t, cl, spec)
		require.NotEmpty(t, want)

		sidecars, gerr := filepath.Glob(filepath.Join(dir, "*"+keysSuffix))
		require.NoError(t, gerr)
		if len(sidecars) == 0 {
			t.Skip() // nothing persisted a digest; nothing to damage
		}
		path := sidecars[pick%len(sidecars)]
		raw, rderr := os.ReadFile(path)
		require.NoError(t, rderr)
		if len(raw) == 0 {
			t.Skip()
		}
		raw[at%len(raw)] ^= mask
		require.NoError(t, os.WriteFile(path, raw, 0666))

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		r, nerr := cl.NewReader(KeyPrefix([]byte("want:")), Until(bound))
		if nerr != nil {
			return // refusing to start is a fine answer
		}
		got := make([]readRec, 0, len(want))
		hdr := make([]byte, HeaderBufferLen)
		for i := 0; i < records+16; i++ {
			msg, off, _, _, readErr := r.ReadMessage(ctx, hdr)
			if readErr != nil {
				// Failing is allowed. Answering WRONG is not, so whatever was
				// served before the failure still has to agree with the scan.
				requirePrefixOf(t, want, got)
				return
			}
			got = append(got, readRec{
				off: off, msg: append(SerializedMessage(nil), msg...),
			})
		}
		requireRecsEq(t, want, got,
			"a damaged digest changed the ANSWER: a filtered read disagreed with a full scan")
	})
}

// requirePrefixOf checks that a read which failed part-way had, up to that
// point, returned exactly what the scan says — same records, same order. A read
// is allowed to stop early; it is not allowed to have been wrong before it did.
func requirePrefixOf(t *testing.T, want, got []readRec) {
	t.Helper()
	require.LessOrEqual(t, len(got), len(want),
		"a filtered read returned MORE records than a full scan")
	requireRecsEq(t, want[:len(got)], got,
		"a damaged digest changed the records served before the read failed")
}
