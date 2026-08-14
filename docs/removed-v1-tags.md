# The v1.x tags this repository does not own

This repository is a fork of `liftbridge-io/liftbridge`'s commit log, and the
fork inherited that project's release tags along with its history. Fifteen of
them are `v1.x`, and they outrank every version this project has ever cut:

```
$ go get github.com/ligustah/commitlog@latest
go: github.com/ligustah/commitlog@latest (v1.9.0) requires github.com/ligustah/commitlog@v1.9.0: parsing go.mod:
	module declares its path as: github.com/liftbridge-io/liftbridge
	        but was required as: github.com/ligustah/commitlog
```

That is not a warning. `@latest` picks the highest release version, the highest
is `v1.9.0`, and `v1.9.0`'s `go.mod` names a different module — so the resolution
fails outright rather than falling back to the next candidate. Nobody could add
this module without knowing to pin an explicit `v0.x`.

Deleting them was chosen on 2026-08-14, and at the time of writing the deletion
itself is still outstanding — the record below was written first, on purpose, so
that the destructive step is the reversible one. None of the fifteen has a GitHub
release attached and all fifteen are lightweight tags, so each is exactly a name
pointing at a commit. The names and the commits they point at:

| tag | commit |
| --- | --- |
| v1.0.0 | `0346508249b693795b7bf5d894232bcfd51ebdf9` |
| v1.0.0-alpha | `37038c2e2b9ed5e4986a113aa2849fa5f09d4774` |
| v1.0.0-beta | `16222c19709cea4499bee89ca79e006f53add339` |
| v1.1.0 | `192e7da973ff2dd48fa8700e986c8d08dc6cabdf` |
| v1.2.0 | `08e2aa815431b453e77ca43aba356140d1e0f255` |
| v1.3.0 | `05bffa6c779971bc28ad38e67debee34da6d36cd` |
| v1.4.0 | `460abb6a1a87c9eb661add71a045e7b94b266c68` |
| v1.4.1 | `5a639e62aa081f0872f261f0f58898f4feef6d4f` |
| v1.5.0 | `76757fe795e8a562f523cced37434d4feef660f5` |
| v1.5.1 | `b41ad359c7ef5c99f98dd8c16bcce07b5b181fd7` |
| v1.6.0 | `768a567dcca17ac7842542292a4e2a686ecc6279` |
| v1.7.0 | `a4223eaaea6e70754c02a0281dbd92e63de79017` |
| v1.7.1 | `707350a48d51030c9a566388d188e3fa7b1ad853` |
| v1.8.0 | `535c66e33e80d9ecd92d65c88efb0c8a7a392968` |
| v1.9.0 | `22034a850f614e3afc0d9feb948221a164681ee5` |

This table is the whole of what was destroyed, which is why it is written down:
recreating any of these is `git tag <name> <commit> && git push origin <name>`.
The commits themselves are upstream's and are not affected — they are still in
`liftbridge-io/liftbridge`, and the ones on this fork's own history are still
reachable from `master`.

## What deleting them does and does not fix

It fixes the repository, which is the part this project controls: `git ls-remote`
stops offering a `v1.x`, and a resolver reading tags directly — `GOPROXY=direct`,
or a `replace` against a checkout — sees `v0.88.0` as the highest.

It does **not** retroactively fix `proxy.golang.org`. The public proxy is
append-only by design: a version it has already served stays served, so that a
build that once resolved keeps resolving. The fifteen `v1.x` versions are in its
cache, and deleting the tags upstream does not evict them.

So the deletion is necessary and not sufficient, and the remaining half is the
one thing the module system offers for exactly this: a `retract` directive. It
has to be published at a version the proxy will find, which for a `v1.x` range
means a `v1.10.0` — a tag in a major version this project does not use and does
not intend to. That trade is recorded in the CHANGELOG entry for whichever
release makes it, if one ever does.

Until then the supported instruction is an explicit version, which is what every
consumer of this module already does:

```
go get github.com/ligustah/commitlog@v0.88.0
```
