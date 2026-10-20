// Test YUN139 filesystem interface
package yun139

import (
	"os"
	"strings"
	"testing"

	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs the standard rclone fstest suite against a live
// 139 cloud account. Set the following environment variables to enable:
//
//	RCLONE_CONFIG_TEST_YUN139_TYPE=yun139
//	RCLONE_CONFIG_TEST_YUN139_AUTHORIZATION=<base64("pc:account:token|...")>
//	RCLONE_CONFIG_TEST_YUN139_SPACE=personal   (or "family", or "group")
//	RCLONE_CONFIG_TEST_YUN139_FAMILY_ID=<id>  (only when space=family)
//	RCLONE_CONFIG_TEST_YUN139_GROUP_ID=<id>   (only when space=group)
//
// The suite covers listing, upload, download, update, rename, move, delete,
// partial-read, and chunked-copy paths.
//
// Against space=group the server-side Move/Copy/DirMove subtests are skipped,
// not failed: newFs clears those features for the group space because the
// group's native move task is not wired up yet. Everything else in the suite
// runs the same as it does against a family cloud.
//
// RCLONE_TEST_YUN139_ROOT is required and names an existing sub-directory to
// run inside (create it first: `rclone mkdir TestYun139:rc-fstest`). The suite
// purges the root of the Fs it is handed, so pointing it at the root of a space
// that already holds data - a shared group or family cloud that mirrors a real
// tree, for instance - would delete that data. There is no safe default for
// that, so the test skips rather than guessing.
func TestIntegration(t *testing.T) {
	root := os.Getenv("RCLONE_TEST_YUN139_ROOT")
	if root == "" {
		t.Skip("RCLONE_TEST_YUN139_ROOT is unset: the suite purges the root it runs against, so it must be confined to a dedicated sub-directory")
	}
	remote := "TestYun139:" + strings.Trim(root, "/")
	fstests.Run(t, &fstests.Opt{
		RemoteName: remote,
		NilObject:  (*Object)(nil),
	})
}
