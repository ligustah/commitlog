module github.com/ligustah/commitlog

go 1.26.0

// v0.43.6 reads a log with a torn tail back as EMPTY. Its check for an index
// that cannot describe its log fired on any unclean shutdown mid-append, where
// the index is the sound half, and rebuilt over a segment before the tail
// truncation that makes it consistent. Fixed in v0.43.7.
retract v0.43.6

require (
	github.com/dustin/go-humanize v1.0.1
	github.com/golang/snappy v1.0.0
	github.com/klauspost/compress v1.19.2
	github.com/natefinch/atomic v1.0.1
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.11.1
	github.com/tysonmote/gommap v0.0.3
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
