package commitlog

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// ErrInvalidSidecarName reports a sidecar name the log refuses to act on: one
// that is not a plain file name, or one that does not carry the reserved
// clientSidecarPrefix.
//
// It is a refusal and not a warning because the name is an ACTION, not a
// description of one — it reaches os.Remove and an atomic write, so the cost
// of a name the caller did not mean is a deleted index or an overwritten
// checkpoint, not a confusing file. The contract used to live only as a
// sentence on the CommitLog interface ("must not collide with the log's own
// files"), which is advice the caller reads and the log cannot enforce.
var ErrInvalidSidecarName = errors.New("commitlog: invalid sidecar name")

// ClientSidecarPrefix is the name prefix reserved for sidecars. Every sidecar
// carries it; the log never writes a file that does.
//
// It replaces what used to be two hand-maintained lists — the exact names the
// log writes, and the suffixes it matches on — which the sidecar check
// consulted to decide whether a client name collided with one of the log's.
// Enumerating one side of a collision is a losing position: the lists describe
// the log as it is TODAY, so the log gaining a file is the log silently taking
// a name a client is already using, and the check that was supposed to prevent
// that is the thing that goes out of date. A reserved prefix closes the set
// from the other side. Neither party has to know what the other calls things:
// the log may add any file it likes as long as it is not about a client, and
// the client may use any name it likes after the prefix.
//
// Exported because the clients that hold sidecar names need to spell it, and
// because a prefix nobody can name is a rule with no affordance for obeying it.
const ClientSidecarPrefix = "client-"

// isClientSidecar reports whether a file in the log directory belongs to the
// log's CLIENT rather than to the log.
//
// The log's own directory scans use this to skip past those files. That is not
// tidiness — it is the half of the reservation that makes it a contract. The
// scans dispatch on SUFFIX, so without this a sidecar named "client-notes.log"
// would be read as a segment and fail the open outright on its non-integer
// stem, and "client-notes.index" would be deleted as an orphaned index.
func isClientSidecar(name string) bool {
	return strings.HasPrefix(name, ClientSidecarPrefix)
}

// checkSidecarName reports whether name is one the log will act on, refusing it
// with ErrInvalidSidecarName otherwise.
//
// It checks rather than sanitises. A caller that passed "../../state" meant a
// file somewhere else; rewriting that to "state" inside the log directory
// answers a question nobody asked, and answers it silently. For the same reason
// it does not PREPEND the prefix to a name that lacks one: the name is what the
// caller will look the file up by later, and a log that quietly stores it
// somewhere else has changed the caller's mind for them.
func checkSidecarName(name string) error {
	// The sidecar arrives from the log's CLIENT, so it can be a path as easily
	// as a name; validBareName says why that is a refusal and not a cleanup.
	// Backslash is refused on every platform rather than only Windows, because
	// the same name has to mean the same thing on both. Checked FIRST, so the
	// prefix test below is reasoning about a plain file name and not about a
	// path that happens to start with the right characters.
	if err := validBareName("sidecar name", name); err != nil {
		return errors.Wrap(ErrInvalidSidecarName, err.Error())
	}
	// The bare prefix is refused along with everything missing it: a file called
	// exactly "client-" is the reservation and no name, and reserving a name for
	// one client is the collision the prefix exists to make impossible.
	if !isClientSidecar(name) || name == ClientSidecarPrefix {
		return errors.Wrapf(ErrInvalidSidecarName,
			"%q must start with %q and have a name after it", name, ClientSidecarPrefix)
	}
	return nil
}

// Sidecars are small named metadata files owned by the log's CLIENT, stored
// in the log directory next to the segments (e.g. durable_streams' recovery
// floor checkpoint). Put writes atomically (temp + rename), so a crash never
// leaves a torn sidecar; Get returns os.ErrNotExist-satisfying errors when
// absent; Remove of an absent sidecar is a no-op. The name must be a plain file
// name carrying ClientSidecarPrefix; anything else is refused, see
// ErrInvalidSidecarName.
func (l *commitLog) PutSidecar(name string, data []byte) error {
	if err := checkSidecarName(name); err != nil {
		return err
	}
	return AtomicWriteFileWithRetry(filepath.Join(l.Path, name), bytes.NewReader(data))
}

func (l *commitLog) GetSidecar(name string) ([]byte, error) {
	if err := checkSidecarName(name); err != nil {
		return nil, err
	}
	// Paired with PutSidecar's AtomicWriteFileWithRetry: a sidecar written with
	// a retry deserves to be read with one, and the documented contract that an
	// absent sidecar satisfies os.ErrNotExist is preserved — ReadFileWithRetry
	// returns that immediately rather than waiting the file out.
	return ReadFileWithRetry(filepath.Join(l.Path, name))
}

// IdentityConflict returns the disagreement found when this log was opened, or
// nil if there was none. See Options.Identity.
//
// It is a property of the OPEN, not of the log: the answer is fixed when New
// returns and never changes for the life of this handle, so a caller checks it
// once and a second call cannot report that the conflict went away.
func (l *commitLog) IdentityConflict() *IdentityConflict {
	return l.identityConflict
}

func (l *commitLog) RemoveSidecar(name string) error {
	if err := checkSidecarName(name); err != nil {
		return err
	}
	err := os.Remove(filepath.Join(l.Path, name))
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return err
}
