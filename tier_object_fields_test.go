package commitlog

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// A fact an offloaded segment knows about itself must reach the manifest, and
// come back from it.
//
// offloadMeta and TierObject carry the same fields — the first is what a
// segment knows about its own store objects, the second is what the published
// manifest says about them — and offloadMeta.tierObject and TierObject.meta
// convert between them by writing every one of them out by hand, twice.
//
// The existing tests cover every field that is there today: removing any one of
// them from either converter turns several of them red, which is how the
// omission would be caught if it were made now. What they cannot cover is a
// field added LATER, because each was written against the field set as it
// stood, and a field the manifest never carries is not a field a test comparing
// manifests will miss. Dropping BlockMode from a published manifest says a
// block-compressed segment is raw, which is a segment that cannot be read
// correctly; that is the size of what goes unnoticed.
//
// So this test fills the struct by reflection rather than by name. A field
// added to offloadMeta is populated here without anyone touching this file, and
// the round trip fails until both converters carry it.
func TestEveryOffloadMetaFieldSurvivesTheManifest(t *testing.T) {
	var meta offloadMeta
	v := reflect.ValueOf(&meta).Elem()

	for i := 0; i < v.NumField(); i++ {
		name := v.Type().Field(i).Name
		f := v.Field(i)
		// Distinct non-zero values, so a converter that assigns the WRONG field
		// fails as loudly as one that assigns none.
		switch f.Kind() {
		case reflect.String:
			f.SetString(fmt.Sprintf("%s-%d", name, i))
		case reflect.Int64:
			f.SetInt(int64(i) + 1)
		case reflect.Bool:
			f.SetBool(true)
		default:
			// Not a skip. A kind this does not know how to fill would be left at
			// its zero value, and a zero value survives a converter that drops
			// the field entirely — the test would pass while covering nothing.
			t.Fatalf("offloadMeta.%s is a %s, which this test cannot fill: add a "+
				"case for it, or it is the one field nothing checks", name, f.Kind())
		}
	}

	require.Equal(t, meta, meta.tierObject(7, "tier-a").meta(),
		"a field of offloadMeta does not survive the round trip through TierObject: "+
			"offloadMeta.tierObject or TierObject.meta is missing it")
}

// TierObject's own fields are the ones it shares with offloadMeta plus the three
// that only the manifest has. Naming them here is what makes the round-trip test
// above complete: it proves offloadMeta's fields survive, and this proves there
// is nothing else on TierObject for them to have been quietly renamed into.
//
// A field added to TierObject fails here, which is the prompt to decide which
// kind it is — a fact about the object, which belongs in offloadMeta and is then
// covered above, or a fact about the manifest's bookkeeping, which belongs in
// this list.
func TestTierObjectAddsOnlyManifestBookkeeping(t *testing.T) {
	manifestOnly := map[string]bool{
		// Which segment these facts describe. offloadMeta never carries it: a
		// segment knows its own base offset.
		"BaseOffset": true,
		// Which store holds the object. Likewise — a segment knows its tier.
		"Tier": true,
		// Set only across an interrupted move's double-claim window, which is a
		// property of the manifest rather than of the object.
		"MovedFrom": true,
	}

	shared := make(map[string]bool)
	mt := reflect.TypeOf(offloadMeta{})
	for i := 0; i < mt.NumField(); i++ {
		shared[mt.Field(i).Name] = true
	}

	ot := reflect.TypeOf(TierObject{})
	for i := 0; i < ot.NumField(); i++ {
		name := ot.Field(i).Name
		require.True(t, shared[name] || manifestOnly[name],
			"TierObject.%s is neither a field of offloadMeta nor listed here as "+
				"manifest bookkeeping: decide which it is, so it is either covered "+
				"by the round-trip test or deliberately outside it", name)
	}
	for name := range manifestOnly {
		_, ok := ot.FieldByName(name)
		require.True(t, ok, "TierObject has no field %s; this list is stale", name)
	}
}
