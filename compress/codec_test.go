package compress

import (
	"bytes"
	"testing"
)

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
