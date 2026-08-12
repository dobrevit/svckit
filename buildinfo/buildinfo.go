package buildinfo

import (
	"fmt"
	"runtime"
)

var (
	// Version is the current version of the application
	// This should be set at build time using -ldflags
	Version = "dev"

	// CommitHash is the git commit hash
	// This should be set at build time using -ldflags
	CommitHash = "unknown"

	// BuildDate is when the binary was built
	// This should be set at build time using -ldflags
	BuildDate = "unknown"

	// BuildBy is who/what built the binary
	// This should be set at build time using -ldflags
	BuildBy = "unknown"
)

// Info contains version information
type Info struct {
	Version    string `json:"version"`
	CommitHash string `json:"commit_hash"`
	BuildDate  string `json:"build_date"`
	BuildBy    string `json:"build_by"`
	GoVersion  string `json:"go_version"`
	Platform   string `json:"platform"`
}

// GetInfo returns version information
func GetInfo() Info {
	return Info{
		Version:    Version,
		CommitHash: CommitHash,
		BuildDate:  BuildDate,
		BuildBy:    BuildBy,
		GoVersion:  runtime.Version(),
		Platform:   fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}

// GetUserAgent returns a properly formatted User-Agent string
func GetUserAgent(serviceName string) string {
	return fmt.Sprintf("%s/%s (%s; %s) Go/%s",
		serviceName,
		Version,
		CommitHash,
		runtime.GOOS,
		runtime.Version())
}

// GetVersionString returns a human-readable version string
func GetVersionString() string {
	if CommitHash != "unknown" && len(CommitHash) > 7 {
		return fmt.Sprintf("%s (%s)", Version, CommitHash[:7])
	}
	return Version
}
