package cloud189

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/require"
)

// TestProbeBackendReadPath drives the real constructor and the real read path
// against the shared accounts, so what is exercised here is the code rclone
// itself will run rather than a parallel hand-written request.
//
// Everything it does is read-only except one thing it deliberately avoids: it
// mints a download link and reads the first 64 bytes of an existing file. A
// ranged GET costs those 64 bytes, so the byte path is proven without pushing
// an account's transfer allowance.
func TestProbeBackendReadPath(t *testing.T) {
	if os.Getenv("CLOUD189_PROBE") != "1" {
		t.Skip("live probe against a shared account; set CLOUD189_PROBE=1 and CLOUD189_PROBE_SESSION=<fixture.json> to run")
	}
	path := os.Getenv("CLOUD189_PROBE_SESSION")
	require.NotEmpty(t, path)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var f probeFixture
	require.NoError(t, json.Unmarshal(raw, &f))

	ctx := context.Background()
	cases := []struct {
		name     string
		token    string
		space    string
		discover bool // leave family_id blank and let the backend find it
	}{
		{"drive-full/personal", f.Personal.AccessToken, spacePersonal, false},
		{"drive-full/family", f.Personal.AccessToken, spaceFamily, false},
		{"flow-limited/personal", f.SecondAccount.AccessToken, spacePersonal, false},
		{"flow-limited/family", f.SecondAccount.AccessToken, spaceFamily, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.token == "" {
				t.Skip("fixture carries no access token for this account")
			}
			m := configmap.Simple{
				"type":          "cloud189",
				"access_token":  tc.token,
				"space":         tc.space,
				"family_id":     familyIDFor(tc.token, &f, tc.discover),
				"encoding":      cloud189DefaultEncoding.String(),
				"disable_http2": "false",
			}
			remote, err := NewFs(ctx, "probe", "", m)
			if err != nil {
				t.Fatalf("NewFs: %v", err)
			}
			entries, err := remote.List(ctx, "")
			require.NoError(t, err)
			var dirs, files int
			var firstFile fs.Object
			for _, e := range entries {
				if d, ok := e.(fs.Directory); ok {
					dirs++
					_ = d
					continue
				}
				files++
				if firstFile == nil {
					firstFile, _ = e.(fs.Object)
				}
			}
			t.Logf("listed root: entries=%d dirs=%d files=%d precision=%v hashes=%v",
				len(entries), dirs, files, remote.Precision(), remote.Hashes())

			for _, e := range entries {
				name := e.Remote()
				if strings.HasPrefix(name, "probe-") {
					t.Logf("  leftover probe entry: %s (%T)", name, e)
				}
			}

			if firstFile == nil {
				t.Log("no file in this root to read")
				return
			}
			o := firstFile.(*Object)
			rc, err := o.Open(ctx, &fs.RangeOption{Start: 0, End: 63})
			require.NoErrorf(t, err, "opening %s", o.Remote())
			defer func() { _ = rc.Close() }()
			head, err := io.ReadAll(io.LimitReader(rc, 4096))
			require.NoError(t, err)
			t.Logf("ranged read of %s: bytes=%d (asked 64, whole object is %d)", o.Remote(), len(head), o.Size())
			if len(head) != 64 {
				t.Errorf("expected 64 bytes from a 0-63 range, got %d", len(head))
			}
		})
	}
}

// familyIDFor returns the configured family id for a token, when the fixture
// knows one, so the probe does not depend on discovery alone.
func familyIDFor(token string, f *probeFixture, discover bool) string {
	if discover {
		return ""
	}
	switch token {
	case f.Personal.AccessToken:
		return f.Personal.FamilyID
	case f.SecondAccount.AccessToken:
		return f.SecondAccount.FamilyID
	}
	return ""
}
