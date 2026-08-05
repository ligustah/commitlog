package commitlog

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// Stripping headers must not disturb a single byte of the VALUE.
//
// This path is the one place compaction re-frames a record instead of copying
// it: stripFrame re-encodes the message and, in doing so, recomputes its CRC.
// Everywhere else the cleaner appends the original frame verbatim, so a value
// damaged after it was written cannot survive its own checksum. Here it can —
// whatever bytes stripFrame is handed become the record, and the new CRC then
// certifies them.
//
// That makes this the one compaction path on which "the record passes its CRC"
// does NOT establish that the record is what the writer wrote, which is exactly
// the inference a corruption hunt reaches for. So the preservation deserves a
// direct test rather than an inference from the strip tests that already exist:
// those assert values like "v" and "committed", one to nine bytes, where a
// corruption of a leading field would be invisible.
//
// Sizes and byte patterns are varied deliberately. The failure this is looking
// for — a value whose HEAD is wrong while its tail and length are right — cannot
// be seen in a value too short to have a head and a tail.
func TestStrippingHeadersPreservesValuesExactly(t *testing.T) {
	l, app := specLog(t)

	type record struct {
		off  int64
		key  string
		want []byte
	}
	txHeaders := func() map[string][]byte {
		return map[string][]byte{
			"pid":   {0, 0, 0, 0, 0, 0, 0, 7},
			"epoch": {0, 0, 0, 1},
			"seq":   {0, 0, 0, 0, 0, 0, 0, 3},
			"app":   []byte("keep-me"),
		}
	}

	// A structured value in the shape the corruption reports describe: a leading
	// count byte, then fields. If a head byte is disturbed, the count reads wrong
	// while the rest still looks plausible.
	row := func(arity byte, n int) []byte {
		v := make([]byte, 0, n)
		v = append(v, arity)
		for i := 0; len(v) < n; i++ {
			v = append(v, byte('A'+(i%26)))
		}
		return v
	}

	values := [][]byte{
		{},                             // empty, but present
		{0x03},                         // a lone count byte
		row(0x03, 16),                  // small structured row
		row(0x03, 300),                 // spans a block boundary at this segment size
		row(0x03, 4096),                // large
		bytes.Repeat([]byte{0x00}, 64), // all zeros: a truncation reads as valid data
		[]byte("\xff\xfe\x00\x01value with high bytes"),
	}

	var records []record
	for i, v := range values {
		key := fmt.Sprintf("k:%02d", i)
		off := app(&Message{Key: []byte(key), Value: v, Headers: txHeaders()})
		records = append(records, record{off: off, key: key, want: v})
	}
	// A tombstone: a NIL value is not an empty one, and re-encoding must not
	// confuse the two — an empty value would resurrect a deleted key.
	tombOff := app(&Message{
		Key: []byte("tomb"), Value: nil, Attributes: AttrTombstone, Headers: txHeaders(),
	})
	app(&Message{Key: []byte("pad"), Value: []byte("pad")})

	hw := l.HighWatermark()
	requireCleanOK(t, l, CleanSpec{
		Ceiling:      At(hw),
		StripBelow:   hw,
		StripHeaders: []string{"pid", "epoch", "seq"},
	})

	got := readAllMsgs(t, l)
	for _, rec := range records {
		msg, ok := got[rec.off]
		require.True(t, ok, "record %s was lost by the strip", rec.key)

		// The strip must have happened, or this proves nothing about the
		// re-framing path — an untouched frame is copied verbatim.
		hdrs := msg.Headers()
		require.NotContains(t, hdrs, "pid", "%s: headers were not stripped", rec.key)
		require.Equal(t, []byte("keep-me"), hdrs["app"],
			"%s: a header outside StripHeaders was dropped", rec.key)

		require.Equal(t, rec.want, msg.Value(),
			"%s: the value changed across a re-framing strip (len want %d, got %d)",
			rec.key, len(rec.want), len(msg.Value()))
		require.Equal(t, rec.key, string(msg.Key()), "%s: the key changed", rec.key)
	}

	tomb, ok := got[tombOff]
	require.True(t, ok, "the tombstone was lost")
	require.NotZero(t, tomb.Attributes()&AttrTombstone,
		"a stripped tombstone stopped being a tombstone")
	require.Nil(t, tomb.Value(),
		"a nil value came back non-nil: an empty value resurrects a deleted key")
}
