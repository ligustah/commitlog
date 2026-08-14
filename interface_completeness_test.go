package commitlog

import (
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// The CommitLog interface has to name every exported method on *commitLog,
// because New returns the INTERFACE and commitLog is unexported: a method
// missing from the interface is a method no caller outside this package can
// reach, through the only constructor the package offers.
//
// Nothing enforced that, and the test suite is structurally incapable of
// noticing. setup and setupWithOptions — the helpers behind essentially every
// test here — return *commitLog, the concrete type. So a new exported method is
// callable from every test in the repo while being unreachable from outside it:
// green suite, documented method, no way to call it. The two sets happened to
// agree exactly (40 and 40) when this was written, which is the discipline
// holding rather than anything checking.
//
// Equality rather than containment is deliberate. The other direction — an
// interface method the struct lacks — the compiler already refuses, since New
// returns one as the other. This is the direction with nothing behind it.
//
// If a method ever should be deliberately package-internal despite being
// exported, it does not belong on this list: unexport it, or add it to the
// interface. A third state ("exported, but not part of the log's contract") is
// not one the package has a way to express.
func TestCommitLogInterfaceNamesEveryExportedMethod(t *testing.T) {
	iface := reflect.TypeOf((*CommitLog)(nil)).Elem()
	concrete := reflect.TypeOf(&commitLog{})

	named := make(map[string]bool, iface.NumMethod())
	for i := range iface.NumMethod() {
		named[iface.Method(i).Name] = true
	}

	// NumMethod on a non-interface type reports only exported methods, which is
	// exactly the set that matters: an unexported method was never reachable.
	var unreachable []string
	for i := range concrete.NumMethod() {
		if name := concrete.Method(i).Name; !named[name] {
			unreachable = append(unreachable, name)
		}
	}
	sort.Strings(unreachable)

	require.Empty(t, unreachable,
		"these methods are exported on *commitLog but absent from the CommitLog "+
			"interface, so no caller of New can reach them: %v", unreachable)
	require.Equal(t, concrete.NumMethod(), iface.NumMethod(),
		"the interface names %d methods and *commitLog exports %d; every "+
			"exported method is accounted for, so the difference is an interface "+
			"method no longer backed by the log", iface.NumMethod(), concrete.NumMethod())
}
