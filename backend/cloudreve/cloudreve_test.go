// Test Cloudreve filesystem interface
package cloudreve

import (
	"testing"

	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs integration tests against the remote
//
// The remote must point at a folder whose storage policy is S3, or at a pool
// containing one: writing anywhere else is refused by design. Do not set
// "location" on the test remote - fstests rebuilds the Fs from f.Root(), which
// already carries the prefix.
//
// Nothing is skipped for names: the server refuses \ " * : < > ? | in a name, and
// the default encoding stores those as their fullwidth equivalents, so a name
// holding one round-trips.
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestCloudreve:",
		NilObject:  (*Object)(nil),
	})
}
