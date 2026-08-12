package buildinfo_test

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"github.com/dobrevit/svckit/buildinfo"
)

// The build variables are package-level and set by the linker, so a test that
// changes them must put them back for the next one.
func withBuildVars(t *testing.T, version, commit, date, by string) {
	t.Helper()
	prev := [4]string{buildinfo.Version, buildinfo.CommitHash, buildinfo.BuildDate, buildinfo.BuildBy}
	t.Cleanup(func() {
		buildinfo.Version, buildinfo.CommitHash = prev[0], prev[1]
		buildinfo.BuildDate, buildinfo.BuildBy = prev[2], prev[3]
	})
	buildinfo.Version, buildinfo.CommitHash, buildinfo.BuildDate, buildinfo.BuildBy = version, commit, date, by
}

// Unset variables must not read as a real build: "dev" and "unknown" are the
// signal that nothing was stamped in.
func TestDefaultsMarkAnUnstampedBuild(t *testing.T) {
	if buildinfo.Version != "dev" {
		t.Errorf("Version = %q, want dev — a build with no ldflags must not look released", buildinfo.Version)
	}
	for name, got := range map[string]string{
		"CommitHash": buildinfo.CommitHash,
		"BuildDate":  buildinfo.BuildDate,
		"BuildBy":    buildinfo.BuildBy,
	} {
		if got != "unknown" {
			t.Errorf("%s = %q, want unknown", name, got)
		}
	}
}

func TestGetInfoReportsTheStampedValuesAndTheRuntime(t *testing.T) {
	withBuildVars(t, "v1.2.3", "abcdef1234567890", "2026-08-12T10:00:00Z", "ci")

	info := buildinfo.GetInfo()

	if info.Version != "v1.2.3" || info.CommitHash != "abcdef1234567890" {
		t.Errorf("stamped values not reported: %+v", info)
	}
	if info.BuildDate != "2026-08-12T10:00:00Z" || info.BuildBy != "ci" {
		t.Errorf("stamped values not reported: %+v", info)
	}

	// The runtime fields are not stamped; they come from the binary itself.
	if info.GoVersion != runtime.Version() {
		t.Errorf("GoVersion = %q, want %q", info.GoVersion, runtime.Version())
	}
	if want := runtime.GOOS + "/" + runtime.GOARCH; info.Platform != want {
		t.Errorf("Platform = %q, want %q", info.Platform, want)
	}
}

// Info is served over HTTP by consumers, so its JSON shape is part of the API.
func TestInfoMarshalsWithSnakeCaseKeys(t *testing.T) {
	withBuildVars(t, "v1.2.3", "abcdef1", "2026-08-12", "ci")

	encoded, err := json.Marshal(buildinfo.GetInfo())
	if err != nil {
		t.Fatalf("marshalling Info: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}

	for _, key := range []string{"version", "commit_hash", "build_date", "build_by", "go_version", "platform"} {
		if _, ok := decoded[key]; !ok {
			t.Errorf("key %q missing from %s", key, encoded)
		}
	}
}

func TestGetVersionStringAbbreviatesTheCommit(t *testing.T) {
	withBuildVars(t, "v1.2.3", "abcdef1234567890", "", "")

	if got, want := buildinfo.GetVersionString(), "v1.2.3 (abcdef1)"; got != want {
		t.Errorf("GetVersionString() = %q, want %q", got, want)
	}
}

// With nothing stamped there is no commit to show, so the version stands
// alone rather than trailing an empty pair of brackets.
func TestGetVersionStringOmitsAnUnknownCommit(t *testing.T) {
	withBuildVars(t, "dev", "unknown", "", "")

	if got := buildinfo.GetVersionString(); got != "dev" {
		t.Errorf("GetVersionString() = %q, want dev", got)
	}
}

// A commit shorter than the abbreviation would panic if sliced blindly.
func TestGetVersionStringHandlesAShortCommit(t *testing.T) {
	withBuildVars(t, "v1.2.3", "abc", "", "")

	if got := buildinfo.GetVersionString(); got != "v1.2.3" {
		t.Errorf("GetVersionString() = %q, want v1.2.3", got)
	}
}

func TestGetUserAgentIdentifiesTheServiceAndBuild(t *testing.T) {
	withBuildVars(t, "v1.2.3", "abcdef1", "", "")

	got := buildinfo.GetUserAgent("orders")

	for _, want := range []string{"orders/v1.2.3", "abcdef1", runtime.GOOS, "Go/" + runtime.Version()} {
		if !strings.Contains(got, want) {
			t.Errorf("User-Agent %q does not contain %q", got, want)
		}
	}
}

// A User-Agent with a newline in it would let a stamped value inject a header.
func TestGetUserAgentIsASingleLine(t *testing.T) {
	withBuildVars(t, "v1.2.3", "abcdef1", "", "")

	if got := buildinfo.GetUserAgent("orders"); strings.ContainsAny(got, "\r\n") {
		t.Errorf("User-Agent %q spans multiple lines", got)
	}
}
