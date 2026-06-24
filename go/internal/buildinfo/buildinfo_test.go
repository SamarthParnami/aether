package buildinfo

import (
	"strings"
	"testing"
)

// Smoke test that proves the Go build + test lane is wired from the first commit.
func TestStringContainsNameAndVersion(t *testing.T) {
	got := String()
	if !strings.Contains(got, Name) {
		t.Errorf("String() = %q, want it to contain Name %q", got, Name)
	}
	if !strings.Contains(got, Version) {
		t.Errorf("String() = %q, want it to contain Version %q", got, Version)
	}
}
