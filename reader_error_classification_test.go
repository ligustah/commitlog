package commitlog

import (
	"fmt"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// What a failed read MEANS, asked directly.
//
// The point of this file is that it needs no goroutines and no compaction pass.
// Both arms it covers used to sit fused inside readOne and ReadMessageMetadata,
// where the only way to reach them was to race a swap in — and even then the
// arm could not be falsified, because the retry it selects makes the failure it
// classified go away before anything observes the classification. That is the
// ceiling docs/sweep-2026-08-13-complexity.md measured: when the assertion is
// "no error appeared", the sole falsifiable element is whatever makes recovery
// stop happening at all.
//
// Deleting any arm below makes exactly one subtest red, naming the wrong answer.
func TestAFailedReadIsClassifiedByWhatItMeans(t *testing.T) {
	t.Run("reresolve", func(t *testing.T) {
		l, cleanup := setup(t)
		defer cleanup()
		r := &Reader{log: l}

		// Wrapped, not bare: this is how the sentinel actually arrives. The arm
		// uses errors.Is rather than a Cause comparison precisely so a `%w`
		// anywhere on the path cannot turn an ordinary compaction swap into a
		// hard read failure for a record already sitting in the replacement.
		wrapped := fmt.Errorf("read segment: %w", ErrSegmentReplaced)
		require.Equal(t, readerReresolve, r.classifyReadError(wrapped),
			"a compaction swap is not a read failure")

		require.Equal(t, readerReresolve, r.classifyReadError(errors.Wrap(ErrSegmentReplaced, "pkg-wrapped")),
			"pkg/errors wrapping counts too: both wrappers implement Unwrap")
	})

	t.Run("fail", func(t *testing.T) {
		l, cleanup := setup(t)
		defer cleanup()
		r := &Reader{log: l}

		// The default arm. Without it every unrecognised error would be treated
		// as one of the named conditions, which is the direction that loses data
		// rather than time.
		require.Equal(t, readerFail, r.classifyReadError(io.EOF))
		require.Equal(t, readerFail, r.classifyReadError(ErrSegmentNotFound))
	})

	t.Run("readonly", func(t *testing.T) {
		l, cleanup := setup(t)
		defer cleanup()
		r := &Reader{log: l}

		// Both halves are required: the sentinel AND the log agreeing it is
		// readonly. A caller that saw the sentinel from somewhere else on a
		// writable log would otherwise be told the log had gone readonly.
		require.Equal(t, readerFail, r.classifyReadError(ErrCommitLogReadonly),
			"the sentinel alone, on a writable log, is not a readonly log")

		l.SetReadonly(true)
		require.Equal(t, readerLogReadonly, r.classifyReadError(ErrCommitLogReadonly))
	})

	t.Run("closed", func(t *testing.T) {
		l, cleanup := setup(t)
		defer cleanup()
		require.NoError(t, l.Close())
		r := &Reader{log: l}

		// The log's own state outranks the error's identity, deliberately and in
		// the same order newSourceReader uses: "the log is gone" explains any
		// error the read produced, and a caller that cannot tell it from a
		// compaction swap has to guess whether retrying is safe. So a swap
		// sentinel on a closed log reports the LOG, not the swap.
		require.Equal(t, readerLogClosed, r.classifyReadError(io.EOF))
		require.Equal(t, readerLogClosed, r.classifyReadError(ErrSegmentReplaced),
			"a closed log outranks the swap sentinel")
	})

	t.Run("deleted", func(t *testing.T) {
		l, cleanup := setup(t)
		defer cleanup()
		require.NoError(t, l.Delete())
		r := &Reader{log: l}

		// Delete closes as part of deleting, so this arm has to be tested BEFORE
		// the closed one to mean anything — which is exactly the order the code
		// uses. Reversed, a deleted log would report itself merely closed and a
		// caller would wait for a handle that is never coming back.
		require.Equal(t, readerLogDeleted, r.classifyReadError(io.EOF))
		require.Equal(t, readerLogDeleted, r.classifyReadError(ErrSegmentReplaced))
	})
}
