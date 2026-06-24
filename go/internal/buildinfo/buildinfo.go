// Package buildinfo carries build/version metadata for Aether services.
//
// Version is intended to be overridden at release time via the linker, e.g.
// `-ldflags "-X github.com/SamarthParnami/aether/go/internal/buildinfo.Version=v1.2.3"`.
package buildinfo

// Name is the project name.
const Name = "aether"

// Version is the build version; "dev" by default, stamped at release.
var Version = "0.0.0-dev"

// String returns a human-readable build identifier, e.g. "aether 0.0.0-dev".
func String() string {
	return Name + " " + Version
}
