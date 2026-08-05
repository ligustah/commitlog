package commitlog

import (
	"bytes"
	"encoding/json"

	"github.com/pkg/errors"
)

// CopyTier copies a log's entire tier from one SegmentStore to another: every
// object the source manifest names, the descriptor that says what log they
// belong to, and then the manifest itself.
//
// It exists because the ORDER is the whole thing, and the order is a rule of
// this package rather than of the caller. The manifest is the tier's commit
// point: an object no manifest names was never committed, which is what makes a
// half-finished copy a recognisable pile of orphans instead of a tier that
// claims records it does not have. Writing it last is what buys that, and a
// caller doing this by hand has to know the manifest's key to write it last —
// which is why the key is not exported and this is.
//
// The failure mode is deliberate. A copy that dies partway leaves dst with
// objects and no manifest, so dst reads as an EMPTY tier and every object in it
// is unreferenced; UnreferencedObjects will list them and the copy can simply be
// run again. Nothing has to be unwound, because keys are unique per upload and
// Put overwrites, so a second attempt is not a merge — it is the same copy.
//
// What it will not do:
//
//   - Copy objects the manifest does not name. Those are orphans in src, and
//     copying them makes them orphans in dst.
//   - Write into a dst that already holds a manifest or a descriptor. Putting one
//     tier over another is a replace, not a copy: every object already there
//     would be stranded, named by nothing and deleted by no one. Copy into a
//     fresh store.
//   - Paper over an object the manifest names but src does not hold. The source
//     log tolerates that in one narrow window — a crash between a manifest that
//     dropped entries and the delete that removed them — but in that window the
//     current manifest no longer names them, so a missing object HERE is not that
//     case. A handover is a deliberate act and is not the place to carry a
//     dangling reference into a new store.
func CopyTier(src, dst SegmentStore) error {
	if src == nil || dst == nil {
		return errors.New("commitlog: CopyTier needs both a source and a destination store")
	}

	if err := requireAbsent(dst, manifestKey, "manifest"); err != nil {
		return err
	}
	if err := requireAbsent(dst, descriptorKey, "descriptor"); err != nil {
		return err
	}

	// Reading the manifest through readTierManifest rather than copying its
	// bytes is the point at which the source is checked: version, key shapes,
	// and that it parses at all. A tier worth handing to a new owner is one this
	// build can read.
	objs, err := readTierManifest(src)
	if err != nil {
		return errors.Wrap(err, "read source tier manifest")
	}

	// The descriptor is read before anything is copied, because a store with no
	// descriptor is not a log's store. Copying the objects and leaving the new
	// owner unable to say what log they belong to would produce exactly the
	// silent adoption the descriptor exists to prevent: dst would look like a
	// NEW log and record whatever settings its first opener happened to pass.
	desc, err := readStoreDescriptor(src)
	if err != nil {
		return errors.Wrap(err, "read source log descriptor")
	}

	// These three lines are the contract, and their ORDER is the contract. The
	// manifest is published last because it is the commit: until it lands,
	// nothing in dst is claimed by anything, so a copy that stops anywhere above
	// leaves collectable orphans instead of a tier missing its records.
	if err := copyTierObjects(src, dst, objs); err != nil {
		return err
	}
	if err := writeStoreDescriptor(dst, desc); err != nil {
		return err
	}
	return publishCopiedManifest(dst, objs)
}

// copyTierObjects streams across every object the manifest names.
func copyTierObjects(src, dst SegmentStore, objs []TierObject) error {
	for _, o := range objs {
		if err := copyObject(src, dst, o.LogKey); err != nil {
			return errors.Wrapf(err, "copy segment %d log object", o.BaseOffset)
		}
		// The block table is as load-bearing as the log bytes: readTierManifest
		// refuses a block-compressed entry naming no table, so a copy that left
		// it behind would produce a tier its new owner cannot open at all.
		if o.BlocksKey != "" {
			if err := copyObject(src, dst, o.BlocksKey); err != nil {
				return errors.Wrapf(err, "copy segment %d block table", o.BaseOffset)
			}
		}
		if o.IndexKey == "" {
			continue
		}
		if err := copyObject(src, dst, o.IndexKey); err != nil {
			return errors.Wrapf(err, "copy segment %d index object", o.BaseOffset)
		}
	}
	return nil
}

// publishCopiedManifest commits the copy.
func publishCopiedManifest(dst SegmentStore, objs []TierObject) error {
	body, err := json.Marshal(tierManifest{Version: manifestVersion, Segments: objs})
	if err != nil {
		return errors.Wrap(err, "encode tier manifest")
	}
	if err := dst.Put(manifestKey, bytes.NewReader(body), int64(len(body))); err != nil {
		return errors.Wrap(err, "put tier manifest")
	}
	return nil
}

// requireAbsent refuses a destination that already holds the named object.
//
// It treats anything other than a clean ErrObjectNotFound as a refusal, for the
// same reason readTierManifest does: "we could not find out" is not "there is
// nothing there", and here the difference is whether a copy strands another
// log's tier.
func requireAbsent(dst SegmentStore, key, what string) error {
	_, err := dst.Size(key)
	switch {
	case err == nil:
		return errors.Errorf(
			"commitlog: the destination store already holds a %s; CopyTier copies "+
				"into a fresh store rather than over an existing tier", what)
	case errors.Is(err, ErrObjectNotFound):
		return nil
	default:
		return errors.Wrapf(err, "stat destination %s", what)
	}
}

// copyObject streams one object across. Stream rather than ReadAt because this
// reads every byte of the object by definition, which is the distinction
// SegmentStore.Stream exists to express: against an object store the bill is per
// request, and a windowed copy of a 1 GiB segment is a thousand of them.
func copyObject(src, dst SegmentStore, key string) error {
	size, err := src.Size(key)
	if err != nil {
		return errors.Wrapf(err, "stat %s", key)
	}
	r, err := src.Stream(key, 0)
	if err != nil {
		return errors.Wrapf(err, "open %s", key)
	}
	defer r.Close()
	if err := dst.Put(key, r, size); err != nil {
		return errors.Wrapf(err, "put %s", key)
	}
	return nil
}
