// Test YUN139 filesystem interface
package yun139

import (
	"testing"

	"github.com/rclone/rclone/fstest/fstests"
)

// TestIntegration runs the standard rclone fstest suite against a live
// 139 cloud account. Set the following environment variables to enable:
//
//	RCLONE_CONFIG_TEST_YUN139_TYPE=yun139
//	RCLONE_CONFIG_TEST_YUN139_AUTHORIZATION=<base64("pc:account:token|...")>
//	RCLONE_CONFIG_TEST_YUN139_SPACE=personal   (or "family")
//	RCLONE_CONFIG_TEST_YUN139_FAMILY_ID=<id>  (only when space=family)
//
// The suite covers listing, upload, download, update, rename, move, delete,
// partial-read, and chunked-copy paths.
func TestIntegration(t *testing.T) {
	fstests.Run(t, &fstests.Opt{
		RemoteName: "TestYun139:",
		NilObject:  (*Object)(nil),
	})
}
