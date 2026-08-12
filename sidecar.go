package commitlog

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// ErrInvalidSidecarName reports a sidecar name the log refuses to act on:
// one that is not a plain file name, or one that names a file the log owns.
//
// It is a refusal and not a warning because the name is an ACTION, not a
// description of one — it reaches os.Remove and an atomic write, so the cost
// of a name the caller did not mean is a deleted index or an overwritten
// checkpoint, not a confusing file. The contract used to live only as a
// sentence on the CommitLog interface ("must not collide with the log's own
// files"), which is advice the caller reads and the log cannot enforce.
var ErrInvalidSidecarName = errors.New("commitlog: invalid sidecar name")

// logOwnedFileNames are the exact names the log writes into its own directory.
// Referenced through the constants and never re-spelled, so a rename of one of
// them cannot leave this list quietly describing a file that no longer exists.
var logOwnedFileNames = []string{
	hwFileName,
	leaderEpochFileName,
	descriptorFileName,
	lockFileName,
}

// logOwnedFileSuffixes are the suffixes the log's own files carry. A sidecar
// ending in one of them is refused whatever its stem, because the stem is not
// what the log matches on: openLog scans the directory by SUFFIX, and a file
// ending in .log whose stem is not an integer fails the open outright — a
// sidecar named "notes.log" would not corrupt the log so much as make it
// unopenable, which is the harder failure to read backwards from.
var logOwnedFileSuffixes = []string{
	logFileSuffix,
	indexFileSuffix,
	keysSuffix,
	blocksSuffix,
	cleanedSuffix,
	truncatedSuffix,
	trimmedSuffix,
	tmpSuffix,
}

// checkSidecarName reports whether name is one the log will act on, refusing it
// with ErrInvalidSidecarName otherwise.
//
// It checks rather than sanitises. A caller that passed "../../state" meant a
// file somewhere else; rewriting that to "state" inside the log directory
// answers a question nobody asked, and answers it silently.
func checkSidecarName(name string) error {
	// The sidecar arrives from the log's CLIENT, so it can be a path as easily
	// as a name; validBareName says why that is a refusal and not a cleanup.
	// Backslash is refused on every platform rather than only Windows, because
	// the same name has to mean the same thing on both.
	if err := validBareName("sidecar name", name); err != nil {
		return errors.Wrap(ErrInvalidSidecarName, err.Error())
	}
	for _, owned := range logOwnedFileNames {
		if name == owned {
			return errors.Wrapf(ErrInvalidSidecarName, "%q is one of the log's own files", name)
		}
	}
	for _, suffix := range logOwnedFileSuffixes {
		if strings.HasSuffix(name, suffix) {
			return errors.Wrapf(ErrInvalidSidecarName, "%q ends in %s, which the log owns", name, suffix)
		}
	}
	return nil
}

// Sidecars are small named metadata files owned by the log's CLIENT, stored
// in the log directory next to the segments (e.g. durable_streams' recovery
// floor checkpoint). Put writes atomically (temp + rename), so a crash never
// leaves a torn sidecar; Get returns os.ErrNotExist-satisfying errors when
// absent; Remove of an absent sidecar is a no-op. A name that collides with
// the log's own files is refused: see ErrInvalidSidecarName.
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
