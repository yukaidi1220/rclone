// Test cloud189 filesystem interface
package cloud189

import (
	"os"
	"strings"
	"testing"

	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs the standard rclone fstest suite against a live 天翼云盘
// account. Set the following environment variables to enable:
//
//	RCLONE_CONFIG_TEST_CLOUD189_TYPE=cloud189
//	RCLONE_CONFIG_TEST_CLOUD189_ACCESS_TOKEN=<the 32 character token>
//	RCLONE_CONFIG_TEST_CLOUD189_SPACE=personal   (or "family")
//	RCLONE_CONFIG_TEST_CLOUD189_FAMILY_ID=<id>   (only when space=family)
//
// RCLONE_TEST_CLOUD189_ROOT is required and names an existing sub-directory to run
// inside (create it first: `rclone mkdir TestCloud189:rc-fstest`). The suite purges
// the root of the Fs it is handed, so pointing it at the root of a space that
// already holds data - a family cloud that mirrors a real tree, for instance -
// would delete that data. There is no safe default for that, so the test skips
// rather than guessing.
//
// The suite covers listing, upload, download, update, rename, move, delete,
// partial-read and the chunk-writer path. Modification times are not settable on
// this drive and the backend says so, which is what the suite's mtime subtests key
// off.
func TestIntegration(t *testing.T) {
	root := os.Getenv("RCLONE_TEST_CLOUD189_ROOT")
	if root == "" {
		t.Skip("RCLONE_TEST_CLOUD189_ROOT is unset: the suite purges the root it runs against, so it must be confined to a dedicated sub-directory")
	}
	remote := "TestCloud189:" + strings.Trim(root, "/")
	fstests.Run(t, &fstests.Opt{
		RemoteName: remote,
		NilObject:  (*Object)(nil),
	})
}
