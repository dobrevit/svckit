// Package buildinfo carries the version metadata a binary was built with.
//
// The values are empty unless the linker sets them, which keeps `go run` and
// `go test` free of build plumbing:
//
//	go build -ldflags "\
//	  -X github.com/dobrevit/svckit/buildinfo.Version=v1.2.3 \
//	  -X github.com/dobrevit/svckit/buildinfo.Commit=$(git rev-parse HEAD) \
//	  -X github.com/dobrevit/svckit/buildinfo.BuildTime=$(date -u +%FT%TZ)"
package buildinfo
