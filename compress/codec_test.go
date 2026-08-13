package compress

import (
	"bytes"
	"strings"
	"testing"
)

// Every codec Valid() admits round-trips both its data and its name.
//
// The codec set is written out five times in codec.go — Compress,
// DecompressInto, Valid, String and Parse — and the two tests below enumerate
// it a sixth and seventh, from literals. So a fifth codec is covered by none of
// them, and adding one means getting five switches right with nothing checking.
//
// The cases here come from Valid() instead, because Valid is what DEFINES the
// set: commitlog.New refuses a codec Valid rejects, which is the only reason
// Compress is allowed to have a silent default arm at all. A codec added by
// widening Valid's bound is therefore covered without anyone remembering to add
// it — and a codec added WITHOUT widening it is inert, which is the safe half.
//
// The three failures this reaches, worst first:
//
//   - in Valid and DecompressInto but not Compress: the default arm stores the
//     block RAW while the header records the codec, so the read decompresses
//     raw bytes as though they were compressed. Silent corruption of data the
//     write path accepted, which is the failure the doc on Compress is about.
//   - in Valid but not String or Parse: a descriptor renders "codec(4)" and the
//     next open cannot parse it back. The log stops opening.
//   - in Valid but not DecompressInto: loud, at least — "unknown codec".
func TestEveryValidCodecRoundTripsItsDataAndItsName(t *testing.T) {
	payload := bytes.Repeat([]byte("repeated schema block "), 500)
	found := 0
	for i := range 256 {
		c := Codec(i)
		if !c.Valid() {
			continue
		}
		found++

		name := c.String()
		if strings.HasPrefix(name, "codec(") {
			t.Errorf("Codec(%d) is Valid but String has no name for it (%q), so a "+
				"descriptor written with it renders a name Parse cannot read back", i, name)
			continue
		}
		if back, err := Parse(name); err != nil || back != c {
			t.Errorf("Parse(%q) = %v, %v; want Codec(%d). A valid codec whose name does "+
				"not round-trip makes a log that cannot reopen", name, back, err, i)
		}

		out, err := c.Decompress(c.Compress(payload))
		if err != nil {
			t.Errorf("%s: decompress after compress: %v", name, err)
			continue
		}
		if !bytes.Equal(out, payload) {
			t.Errorf("%s: the round trip changed the data (%d bytes in, %d out). The "+
				"likeliest cause is a codec added to Valid and DecompressInto but not "+
				"Compress, which stores the block raw under a header that says otherwise",
				name, len(payload), len(out))
		}
	}
	// Valid() is a bound, so a mistake in it can shrink the set to nothing and
	// leave the loop above passing over zero codecs.
	if found < 4 {
		t.Fatalf("Valid admitted only %d codecs; this test derives its cases from Valid "+
			"and says nothing about a set that small", found)
	}
}

func TestRoundtripAllCodecs(t *testing.T) {
	inputs := [][]byte{
		nil,
		{},
		[]byte("a"),
		[]byte("the quick brown fox jumps over the lazy dog"),
		bytes.Repeat([]byte("repeated schema block "), 5000),
		sampleMessageSet(1000),
	}
	for _, c := range []Codec{None, Snappy, S2, Zstd} {
		for i, in := range inputs {
			comp := c.Compress(in)
			out, err := c.Decompress(comp)
			if err != nil {
				t.Fatalf("%s input %d: decompress: %v", c, i, err)
			}
			if !bytes.Equal(out, in) {
				t.Fatalf("%s input %d: roundtrip mismatch (%d → %d → %d bytes)", c, i, len(in), len(comp), len(out))
			}
		}
	}
}

func TestCodecParse(t *testing.T) {
	for name, want := range map[string]Codec{"": None, "none": None, "snappy": Snappy, "s2": S2, "zstd": Zstd} {
		got, err := Parse(name)
		if err != nil || got != want {
			t.Errorf("Parse(%q) = %v, %v; want %v", name, got, err, want)
		}
	}
	if _, err := Parse("brotli"); err == nil {
		t.Error("Parse(brotli) should error")
	}
}
