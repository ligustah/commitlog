package commitlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ligustah/commitlog/compress"
	"github.com/stretchr/testify/require"
)

// A crash can leave the active segment's log physically ahead of its index (the
// write path appends the log frame before its index entry, and checkpointHW
// fsyncs only the log). On reopen, reconcileIndexTail must rebuild the missing
// index tail from the log so NewestOffset/NextOffset reflect the true physical
// log — otherwise a seek and a sequential scan disagree on offsets and the next
// append collides with an un-indexed record. Covers raw and block (compressed)
// segments.
func TestReconcileIndexAheadOfLog(t *testing.T) {
	for _, tc := range []struct {
		name  string
		codec compress.Codec
	}{
		{"raw", compress.None},
		{"block", compress.Snappy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := Options{
				Path:                 dir,
				MaxSegmentBytes:      1 << 20, // one segment, many blocks/frames
				Compression:          tc.codec,
				HWCheckpointInterval: time.Hour,
				CleanerInterval:      time.Hour,
			}
			l, err := New(opts)
			require.NoError(t, err)
			cl := l.(*commitLog)
			for i := 0; i < 7; i++ {
				o, err := cl.Append([]*Message{{Key: []byte("k"), Value: []byte(fmt.Sprintf("value-%d", i))}})
				require.NoError(t, err)
				cl.SetHighWatermark(o[0])
			}
			require.NoError(t, cl.SyncAll())
			require.EqualValues(t, 6, cl.NewestOffset())
			require.NoError(t, cl.Close())

			// Drop the last record's/block's index anchor while the log keeps it
			// (a crash between the log-frame write and the index-anchor sync).
			idxFiles, err := filepath.Glob(filepath.Join(dir, "*"+indexFileSuffix))
			require.NoError(t, err)
			require.NotEmpty(t, idxFiles)
			idx := idxFiles[len(idxFiles)-1]
			fi, err := os.Stat(idx)
			require.NoError(t, err)
			require.NoError(t, os.Truncate(idx, fi.Size()-entryWidth))

			l2, err := New(opts)
			require.NoError(t, err)
			defer l2.Close()
			cl2 := l2.(*commitLog)

			// Reconciled: the tail matches the physical log again.
			require.EqualValues(t, 6, cl2.NewestOffset(), "index tail not reconciled from the log")
			require.EqualValues(t, 7, cl2.activeSegment().NextOffset())

			// Sequential read returns all 7 records with the right values.
			got := fzReadAll(t, cl2)
			require.Len(t, got, 7)
			for i := 0; i < 7; i++ {
				require.Equal(t, fmt.Sprintf("value-%d", i), string(got[int64(i)].Value()))
			}

			// Seek to the reconciled offset agrees with the sequential read.
			r, err := cl2.NewReader(6, true)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			msg, off, _, _, err := r.ReadMessage(ctx, make([]byte, msgSetHeaderLen))
			require.NoError(t, err)
			require.EqualValues(t, 6, off)
			require.Equal(t, "value-6", string(msg.Value()))

			// An append lands past the true tail (7), not onto the un-indexed record.
			o, err := cl2.Append([]*Message{{Value: []byte("after")}})
			require.NoError(t, err)
			require.EqualValues(t, 7, o[0])
		})
	}
}
