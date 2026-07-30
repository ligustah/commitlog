package commitlog

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var segmentSizeTests = []struct {
	name        string
	segmentSize int64
}{
	{"6", 6},
	{"60", 60},
	{"600", 600},
	{"6000", 6000},
}

func TestReaderUncommittedStartOffset(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l, cleanup := setupWithOptions(t, Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			})
			defer l.Close()
			defer cleanup()

			numMsgs := 10
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{
					Value:       []byte(strconv.Itoa(i)),
					Timestamp:   int64(i) + 1,
					LeaderEpoch: 42,
				}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			idx := 4
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := l.NewReader(From(int64(idx)), Uncommitted(), Follow())
			require.NoError(t, err)

			headers := make([]byte, HeaderBufferLen)
			msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers)
			require.NoError(t, err)
			require.Equal(t, int64(idx), offset)
			require.Equal(t, int64(idx)+1, timestamp)
			require.Equal(t, uint64(42), leaderEpoch)
			compareMessages(t, msgs[idx], msg)
		})
	}
}

func TestReaderUncommittedBlockCancel(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
	})
	defer l.Close()
	defer cleanup()

	msg := &Message{Value: []byte("hi")}
	_, err := l.Append([]*Message{msg})
	require.NoError(t, err)

	r, err := l.NewReader(From(0), Uncommitted(), Follow())
	require.NoError(t, err)

	headers := make([]byte, HeaderBufferLen)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go cancel()
	_, _, _, _, err = r.ReadMessage(ctx, headers)
	// context.Canceled, NOT io.EOF. This used to assert EOF, which pinned a
	// defect rather than a property: io.EOF is this package's documented
	// end-of-read signal, so reporting a cancellation as EOF told the caller
	// the log had ended when only its own context had. A consumer reading with
	// a per-read deadline would stop tailing and believe it had caught up.
	require.ErrorIs(t, err, context.Canceled)
}

func TestReaderUncommittedBlockForSegmentWrite(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer l.Close()
	defer cleanup()

	msg := &Message{
		Value:       []byte("hi"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}
	_, err := l.Append([]*Message{msg})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := l.NewReader(From(0), Uncommitted(), Follow())
	require.NoError(t, err)
	headers := make([]byte, HeaderBufferLen)
	m, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
	require.Equal(t, int64(1), timestamp)
	require.Equal(t, uint64(42), leaderEpoch)
	compareMessages(t, msg, m)

	msg = &Message{
		Value:       []byte("hello"),
		Timestamp:   2,
		LeaderEpoch: 43,
	}

	done := make(chan struct{})

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*Message{msg})
		require.NoError(t, err)
		close(done)
	}()

	m, offset, timestamp, leaderEpoch, err = r.ReadMessage(ctx, headers)
	require.NoError(t, err)
	require.Equal(t, int64(1), offset)
	require.Equal(t, int64(2), timestamp)
	require.Equal(t, uint64(43), leaderEpoch)
	compareMessages(t, msg, m)

	<-done
}

func TestReaderUncommittedReadError(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer cleanup()

	msg := &Message{Value: []byte("hi")}
	_, err := l.Append([]*Message{msg})
	require.NoError(t, err)

	r, err := l.NewReader(From(0), Uncommitted(), Follow())
	require.NoError(t, err)

	require.NoError(t, l.Close())

	p := make([]byte, 10)
	_, _, _, _, err = r.ReadMessage(context.Background(), p)
	require.Error(t, err)
}

func TestReaderCommittedStartOffset(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l, cleanup := setupWithOptions(t, Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			})
			defer l.Close()
			defer cleanup()

			numMsgs := 10
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{
					Value:       []byte(strconv.Itoa(i)),
					Timestamp:   int64(i) + 1,
					LeaderEpoch: 42,
				}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			l.SetHighWatermark(4)
			idx := 2
			r, err := l.NewReader(From(int64(idx)), Follow())
			require.NoError(t, err)

			headers := make([]byte, HeaderBufferLen)
			msg, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
			require.NoError(t, err)
			require.Equal(t, int64(idx), offset)
			require.Equal(t, int64(idx)+1, timestamp)
			require.Equal(t, uint64(42), leaderEpoch)
			compareMessages(t, msgs[idx], msg)
		})
	}
}

func TestReaderCommittedBlockCancel(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
	})
	defer l.Close()
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)
	go cancel()
	headers := make([]byte, HeaderBufferLen)
	_, _, _, _, err = r.ReadMessage(ctx, headers)
	// context.Canceled, NOT io.EOF. This used to assert EOF, which pinned a
	// defect rather than a property: io.EOF is this package's documented
	// end-of-read signal, so reporting a cancellation as EOF told the caller
	// the log had ended when only its own context had. A consumer reading with
	// a per-read deadline would stop tailing and believe it had caught up.
	require.ErrorIs(t, err, context.Canceled)
}

func TestReaderCommittedReadError(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer cleanup()

	msg := &Message{Value: []byte("hi")}
	_, err := l.Append([]*Message{msg})
	require.NoError(t, err)
	msg = &Message{Value: []byte("hi")}
	_, err = l.Append([]*Message{msg})
	require.NoError(t, err)
	l.SetHighWatermark(0)

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	require.NoError(t, l.Close())

	headers := make([]byte, HeaderBufferLen)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Error(t, err)
}

func TestReaderCommittedWaitOnEmptyLog(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
	})
	defer l.Close()
	defer cleanup()

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	msg := &Message{
		Value:       []byte("hello"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*Message{msg})
		require.NoError(t, err)
		l.SetHighWatermark(0)
	}()

	headers := make([]byte, HeaderBufferLen)
	m, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)
	require.Equal(t, int64(1), timestamp)
	require.Equal(t, uint64(42), leaderEpoch)
	compareMessages(t, msg, m)
}

func TestReaderCommittedWaitOnEmptyLogWithHW(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 1024,
	})
	defer l.Close()
	defer cleanup()

	// Simulate a log that had retention limits applied past a HW.
	l.SetHighWatermark(9)
	l.segments[0].BaseOffset = 10

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	msg := &Message{
		Value:       []byte("hello"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}

	go func() {
		time.Sleep(5 * time.Millisecond)
		_, err := l.Append([]*Message{msg})
		require.NoError(t, err)
		l.SetHighWatermark(10)
	}()

	headers := make([]byte, HeaderBufferLen)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
}

func TestReaderCommittedRead(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l, cleanup := setupWithOptions(t, Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			})
			defer l.Close()
			defer cleanup()

			numMsgs := 10
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{
					Value:       []byte(strconv.Itoa(i)),
					Timestamp:   int64(i) + 1,
					LeaderEpoch: 42,
				}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			l.SetHighWatermark(9)
			r, err := l.NewReader(From(0), Follow())
			require.NoError(t, err)

			headers := make([]byte, HeaderBufferLen)
			for i, msg := range msgs {
				m, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
				require.NoError(t, err)
				require.Equal(t, int64(i), offset)
				require.Equal(t, int64(i)+1, timestamp)
				require.Equal(t, uint64(42), leaderEpoch)
				compareMessages(t, msg, m)
			}
		})
	}
}

func TestReaderCommittedReadToHW(t *testing.T) {
	for _, test := range segmentSizeTests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			l, cleanup := setupWithOptions(t, Options{
				Path:            tempDir(t),
				MaxSegmentBytes: test.segmentSize,
			})
			defer l.Close()
			defer cleanup()

			numMsgs := 10
			msgs := make([]*Message, numMsgs)
			for i := 0; i < numMsgs; i++ {
				msgs[i] = &Message{
					Value:       []byte(strconv.Itoa(i)),
					Timestamp:   int64(i) + 1,
					LeaderEpoch: 42,
				}
			}
			_, err = l.Append(msgs)
			require.NoError(t, err)
			l.SetHighWatermark(4)
			r, err := l.NewReader(From(0), Follow())
			require.NoError(t, err)

			headers := make([]byte, HeaderBufferLen)
			for i, msg := range msgs[:5] {
				m, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
				require.NoError(t, err)
				require.Equal(t, int64(i), offset)
				require.Equal(t, int64(i)+1, timestamp)
				require.Equal(t, uint64(42), leaderEpoch)
				compareMessages(t, msg, m)
			}
		})
	}
}

func TestReaderCommittedWaitForHW(t *testing.T) {
	var err error
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 30,
	})
	defer l.Close()
	defer cleanup()

	numMsgs := 10
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   int64(i) + 1,
			LeaderEpoch: 42,
		}
	}
	_, err = l.Append(msgs)
	require.NoError(t, err)
	l.SetHighWatermark(4)
	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	go func() {
		time.Sleep(5 * time.Millisecond)
		l.SetHighWatermark(9)
	}()

	headers := make([]byte, HeaderBufferLen)
	for i, msg := range msgs {
		m, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
		require.NoError(t, err)
		require.Equal(t, int64(i), offset)
		require.Equal(t, int64(i)+1, timestamp)
		require.Equal(t, uint64(42), leaderEpoch)
		compareMessages(t, msg, m)
	}
}

func TestReaderCommittedCancel(t *testing.T) {
	var err error
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 30,
	})
	defer l.Close()
	defer cleanup()

	numMsgs := 10
	msgs := make([]*Message, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &Message{
			Value:       []byte(strconv.Itoa(i)),
			Timestamp:   int64(i) + 1,
			LeaderEpoch: 42,
		}
	}
	_, err = l.Append(msgs)
	require.NoError(t, err)
	l.SetHighWatermark(4)
	ctx, cancel := context.WithCancel(context.Background())
	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	go func() {
		time.Sleep(5 * time.Millisecond)
		cancel()
	}()

	count := 0
	headers := make([]byte, HeaderBufferLen)
	for i, msg := range msgs {
		m, offset, timestamp, leaderEpoch, err := r.ReadMessage(ctx, headers)
		if count < 5 {
			require.NoError(t, err)
			require.Equal(t, int64(i), offset)
			require.Equal(t, int64(i)+1, timestamp)
			require.Equal(t, uint64(42), leaderEpoch)
			compareMessages(t, msg, m)
			count++
		} else {
			// A cancellation is not end-of-data — see TestReaderCommittedBlockCancel.
			require.ErrorIs(t, err, context.Canceled)
		}
	}
	require.Equal(t, 5, count)
}

// Ensure ReadMessage waits for the next message when the offset exceeds the
// HW.
func TestReaderCommittedCapOffset(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 100,
	})
	defer cleanup()

	msg1 := &Message{
		Value:       []byte("hi"),
		Timestamp:   1,
		LeaderEpoch: 42,
	}
	_, err := l.Append([]*Message{msg1})
	require.NoError(t, err)
	msg2 := &Message{
		Value:       []byte("hi"),
		Timestamp:   2,
		LeaderEpoch: 42,
	}
	_, err = l.Append([]*Message{msg2})
	require.NoError(t, err)
	l.SetHighWatermark(0)

	r, err := l.NewReader(From(5), Follow())
	require.NoError(t, err)

	go l.SetHighWatermark(1)

	headers := make([]byte, HeaderBufferLen)
	m, offset, timestamp, leaderEpoch, err := r.ReadMessage(context.Background(), headers)
	require.NoError(t, err)
	require.Equal(t, int64(1), offset)
	require.Equal(t, int64(2), timestamp)
	require.Equal(t, uint64(42), leaderEpoch)
	compareMessages(t, msg2, m)
}

// Ensure ReadMessage returns ErrCommitLogDeleted when the commit log is
// deleted.
func TestReaderLogDeleted(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
	})
	defer l.Close()
	defer cleanup()

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	// Capture Delete's error on a channel — require.NoError inside a goroutine is
	// unsafe (testify's FailNow must run on the test goroutine).
	delErr := make(chan error, 1)
	go func() { delErr <- l.Delete() }()

	headers := make([]byte, HeaderBufferLen)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Equal(t, ErrCommitLogDeleted, err)
	require.NoError(t, <-delErr)
}

// Ensure ReadMessage returns ErrCommitLogClosed when the commit log is closed.
func TestReaderLogClosed(t *testing.T) {
	l, cleanup := setupWithOptions(t, Options{
		Path:            tempDir(t),
		MaxSegmentBytes: 10,
	})
	defer l.Close()
	defer cleanup()

	r, err := l.NewReader(From(0), Follow())
	require.NoError(t, err)

	go func() {
		require.NoError(t, l.Close())
	}()

	headers := make([]byte, HeaderBufferLen)
	_, _, _, _, err = r.ReadMessage(context.Background(), headers)
	require.Equal(t, ErrCommitLogClosed, err)
}

func compareMessages(t *testing.T, exp *Message, act SerializedMessage) {
	// TODO: check timestamp
	require.Equal(t, exp.MagicByte, act.MagicByte())
	require.Equal(t, exp.Attributes, act.Attributes())
	require.Equal(t, exp.Key, act.Key())
	require.Equal(t, exp.Value, act.Value())
	if exp.Headers == nil || len(exp.Headers) == 0 {
		require.Equal(t, map[string][]byte{}, act.Headers())
	} else {
		require.Equal(t, exp.Headers, act.Headers())
	}
}
