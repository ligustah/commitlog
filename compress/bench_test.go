package compress

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	gsnappy "github.com/golang/snappy"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
)

// sampleMessageSet builds a realistic gocdc-JSON view-record batch: n records
// each carrying an identical embedded schema (the repeated part that compresses
// away) plus varying after-values — the shape the durable output actually stores.
func sampleMessageSet(n int) []byte {
	var buf bytes.Buffer
	schema := `[{"key":true,"name":"product_id","type":{"name":"integer"}},{"key":false,"name":"product","type":{"name":"string"}},{"key":false,"name":"price_cents","type":{"name":"integer"}},{"key":false,"name":"seller","type":{"name":"string"}},{"key":false,"name":"tier","type":{"name":"string"}}]`
	for i := 0; i < n; i++ {
		rec := fmt.Sprintf(`{"operation":"insert","timestamp":"2026-07-07T13:45:41Z","relation":"catalog","source":null,"schema":%s,"after":{"product_id":{"value":%d},"product":{"value":"P0-%d"},"price_cents":{"value":%d},"seller":{"value":"Seller 0"},"tier":{"value":"standard"}}}`,
			schema, i, i, 100+i)
		// Mimic the message framing (length prefix + a few header bytes).
		var hdr [8]byte
		binary.BigEndian.PutUint32(hdr[:4], uint32(len(rec)))
		buf.Write(hdr[:])
		buf.WriteString(rec)
	}
	return buf.Bytes()
}

type codec struct {
	name       string
	compress   func([]byte) []byte
	decompress func([]byte) ([]byte, error)
}

func codecs() []codec {
	zw, _ := zstd.NewWriter(nil)
	zwBetter, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBetterCompression))
	zr, _ := zstd.NewReader(nil)
	return []codec{
		{"snappy", func(b []byte) []byte { return gsnappy.Encode(nil, b) }, func(b []byte) ([]byte, error) { return gsnappy.Decode(nil, b) }},
		{"s2", func(b []byte) []byte { return s2.Encode(nil, b) }, func(b []byte) ([]byte, error) { return s2.Decode(nil, b) }},
		{"s2-better", func(b []byte) []byte { return s2.EncodeBetter(nil, b) }, func(b []byte) ([]byte, error) { return s2.Decode(nil, b) }},
		{"zstd", func(b []byte) []byte { return zw.EncodeAll(b, nil) }, func(b []byte) ([]byte, error) { return zr.DecodeAll(b, nil) }},
		{"zstd-better", func(b []byte) []byte { return zwBetter.EncodeAll(b, nil) }, func(b []byte) ([]byte, error) { return zr.DecodeAll(b, nil) }},
		{"flate", flateEncode, flateDecode},
	}
}

func flateEncode(b []byte) []byte {
	var out bytes.Buffer
	w, _ := flate.NewWriter(&out, flate.DefaultCompression)
	_, _ = w.Write(b)
	_ = w.Close()
	return out.Bytes()
}

func flateDecode(b []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(b))
	var out bytes.Buffer
	_, err := out.ReadFrom(r)
	return out.Bytes(), err
}

// TestCodecComparison prints ratio + throughput for each codec across batch
// sizes (the batch-size↔ratio tuning knob). Run with: go test -run CodecComparison -v
func TestCodecComparison(t *testing.T) {
	for _, n := range []int{1, 10, 100, 1000, 10000} {
		data := sampleMessageSet(n)
		t.Logf("\n=== batch of %d records — %d bytes raw (%.0f B/rec) ===", n, len(data), float64(len(data))/float64(n))
		for _, c := range codecs() {
			comp := c.compress(data)
			dec, err := c.decompress(comp)
			if err != nil || !bytes.Equal(dec, data) {
				t.Fatalf("%s: roundtrip failed: err=%v equal=%v", c.name, err, bytes.Equal(dec, data))
			}
			t.Logf("  %-12s %7d B  ratio=%5.1f×", c.name, len(comp), float64(len(data))/float64(len(comp)))
		}
	}
}

func BenchmarkCodecs(b *testing.B) {
	data := sampleMessageSet(1000)
	for _, c := range codecs() {
		comp := c.compress(data)
		b.Run(c.name+"/compress", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				_ = c.compress(data)
			}
		})
		b.Run(c.name+"/decompress", func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				_, _ = c.decompress(comp)
			}
		})
	}
}
