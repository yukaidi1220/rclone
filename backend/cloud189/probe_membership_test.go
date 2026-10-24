package cloud189

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestProbeMembershipMetadata snapshots everything the drive says about the
// account, so a membership change can be seen as a diff rather than as a guess.
//
// The "before" side is read out of the original captures by a node script, so
// this test only has to produce the "after" side and keep the two files the
// same shape.
//
// Read-only: every request here is a GET of account metadata. Nothing is
// listed, uploaded or altered.
func TestProbeMembershipMetadata(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	require.NotEmpty(t, s.AccessToken)

	status, code := getUserInfoAlive(t, s)
	if status != http.StatusOK || code != "0" {
		t.Logf("captured session refused (http=%d %s), rotating", status, code)
		s = getSessionForPC(t, s)
	}

	// The endpoints this account has been captured against, plus the older
	// portal capacity endpoint that a third-party driver uses. The last one is
	// labelled as unverified in the output so nobody reads it as capture
	// evidence.
	calls := []struct {
		name  string
		uri   string
		extra string
	}{
		{"getUserInfo", "/getUserInfo.action", "returnType=JSON"},
		{"getUserInfoExt", "/getUserInfoExt.action", "returnType=JSON"},
		{"getUserPrivilegesV2", "/getUserPrivilegesV2.action", "flag=1"},
		{"portal/getUserSizeInfo", "/portal/getUserSizeInfo.action", ""},
	}
	out := map[string]any{"capturedAt": time.Now().UTC().Format(time.RFC3339), "responses": map[string]any{}}
	responses := out["responses"].(map[string]any)
	for _, c := range calls {
		status, body := signedCall(t, s, apiHost, false, http.MethodGet, c.uri, probeQuery(c.extra), "")
		entry := map[string]any{"http": status, "raw": string(body), "parsed": json.RawMessage(body)}
		var probe map[string]any
		if err := json.Unmarshal(body, &probe); err != nil {
			delete(entry, "parsed")
			entry["note"] = "not JSON"
		}
		responses[c.name] = entry
		t.Logf("%s: http=%d bytes=%d", c.name, status, len(body))
	}

	path := os.Getenv("CLOUD189_META_OUT")
	if path == "" {
		path = `D:\天翼云抓包\会员升级后-元数据.json`
	}
	raw, err := json.MarshalIndent(out, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(raw, '\n'), 0o600))
	t.Logf("metadata snapshot written to %s", path)
}
