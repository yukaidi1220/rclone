package cloud189

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloud189/api"
	"github.com/stretchr/testify/require"
)

// Session rotation and the daily-transfer ceiling, both on the account whose
// captured session is already dead.

// getSessionForPC trades a long-lived access token for a fresh session key and
// secret. It is the one endpoint that needs no signature - the access token is
// the credential - and it is also the endpoint that rotates the pair, so a
// caller must be sure the previous holder is finished with it.
//
// The device parameters are replayed exactly as the client sent them: that
// account's privileges list numberOfPCDevices=2, so presenting a new-looking
// device is not a detail. modelRaw arrives already percent-encoded from the
// capture and carries a trailing NUL byte; re-escaping it would double the
// percent signs.
func getSessionForPC(t *testing.T, s probeSession) probeSession {
	t.Helper()
	require.NotEmpty(t, s.AccessToken, "the fixture carries no access token to refresh with")
	q := probeQuery(fmt.Sprintf("accessToken=%s&appId=%s&clientSn=%s&jgOpenId=%s&model=%s&returnType=JSON",
		url.QueryEscape(s.AccessToken), url.QueryEscape(s.AppID), url.QueryEscape(s.ClientSn),
		url.QueryEscape(s.JGOpenID), s.ModelRaw))

	req, err := http.NewRequest(http.MethodPost, apiHost+"/getSessionForPC.action?"+q, http.NoBody)
	require.NoError(t, err)
	req.Header.Set("user-agent", desktopUserAgent)
	req.Header.Set("x-key", "abc")
	req.Header.Set("x-request-id", requestID(t))
	res, err := doScrubbedToken(req, s.AccessToken)
	require.NoError(t, err)
	body, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()

	m := decode(t, body)
	// No key value is printed: a probe log gets pasted around, and a session key
	// signs requests. What the probe needs to see is whether the key changed, how
	// long it is and whether the family key still carries its suffix.
	gotKey, gotFamily := field(m, "sessionKey"), field(m, "familySessionKey")
	t.Logf("getSessionForPC http=%d res_code=%s sessionKey=<len=%d changed=%v> secret=<len=%d> familyKey=<len=%d changed=%v endsInFamily=%v> accessTokenInResp=%v",
		res.StatusCode, field(m, "res_code"),
		len(gotKey), gotKey != s.SessionKey,
		len(field(m, "sessionSecret")),
		len(gotFamily), gotFamily != s.FamilySessionKey, strings.HasSuffix(gotFamily, "_family"),
		m["accessToken"] != nil)
	require.Equalf(t, "0", field(m, "res_code"), "refresh refused: %s", trunc(string(body), 240))

	return probeSession{
		SessionKey:       field(m, "sessionKey"),
		SessionSecret:    field(m, "sessionSecret"),
		FamilySessionKey: field(m, "familySessionKey"),
		AccessToken:      s.AccessToken,
		AppID:            s.AppID,
		ClientSn:         s.ClientSn,
		JGOpenID:         s.JGOpenID,
		ModelRaw:         s.ModelRaw,
		FamilyID:         s.FamilyID,
	}
}

// getUserInfoAlive asks the cheapest authenticated question and reports whether
// this session is accepted, as HTTP status plus res_code text.
func getUserInfoAlive(t *testing.T, s probeSession) (int, string) {
	t.Helper()
	status, body := signedCall(t, s, apiHost, false, http.MethodGet, "/getUserInfo.action",
		probeQuery("returnType=JSON"), "")
	m := decode(t, body)
	code := field(m, "res_code")
	if code == "" {
		code = "no-res_code:" + trunc(string(body), 120)
	}
	return status, code
}

// TestProbeSessionRotation answers the question the captures could not: does
// refreshing a session retire the key it replaced? The account's captured
// session is already dead, so rotating it costs nothing anyone else is using.
func TestProbeSessionRotation(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	require.NotEmpty(t, s.AccessToken)

	if status, code := getUserInfoAlive(t, s); status != http.StatusOK || code != "0" {
		t.Logf("baseline: the captured session is not usable (http=%d res_code=%s)", status, code)
	} else {
		t.Logf("baseline: the captured session still answers (http=%d)", status)
	}

	first := getSessionForPC(t, s)
	status, code := getUserInfoAlive(t, first)
	t.Logf("after refresh #1: new session http=%d res_code=%s", status, code)
	require.Equal(t, http.StatusOK, status, "the freshly minted session must be accepted")

	second := getSessionForPC(t, s)
	status, code = getUserInfoAlive(t, second)
	t.Logf("after refresh #2: newest session http=%d res_code=%s", status, code)
	require.Equal(t, http.StatusOK, status, "the second refresh must also hand out a usable session")

	// This is the measurement: does the key from refresh #1 survive refresh #2?
	status, code = getUserInfoAlive(t, first)
	if status == http.StatusOK && code == "0" {
		t.Log("ROTATION: the previous session key STILL ANSWERS after being replaced - sessions coexist")
	} else {
		t.Logf("ROTATION: the previous session key is refused (http=%d res_code=%s) - refreshing evicts the old holder", status, code)
	}

	writeBackSession(t, first, second)
}

// writeBackSession stores the newest pair in the fixture so later probes reuse
// it instead of rotating the session again on every run.
func writeBackSession(t *testing.T, first, second probeSession) {
	t.Helper()
	path := os.Getenv("CLOUD189_PROBE_SESSION")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var f probeFixture
	require.NoError(t, json.Unmarshal(raw, &f))

	// The secondAccount row keeps the identity fields; only the session pair is
	// replaced, and the key handed out first is recorded because its fate is
	// what the test just measured.
	f.SecondAccount.SessionKey = second.SessionKey
	f.SecondAccount.SessionSecret = second.SessionSecret
	f.SecondAccount.FamilySessionKey = second.FamilySessionKey
	f.SecondAccount.IssuedAt = time.Now().UTC().Format(time.RFC3339)
	f.SecondAccount.SourceHar = "rotated-by-probe"
	f.SecondAccount.RotatedOutKey = first.SessionKey

	out, err := json.MarshalIndent(f, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(out, '\n'), 0o600))
	t.Logf("fixture updated: secondAccount now holds the newest pair (sessionKey len=%d)", len(second.SessionKey))
}

// TestProbeDailyTransferCeiling asks which gates initMultiUpload actually
// applies, using the two accounts as a control pair: one has plenty of storage
// but almost no flow left today, the other has plenty of flow but a full
// drive. A claim that passes on both tells us the server is not looking at
// that dimension at all.
//
// The claims are free: initMultiUpload only opens a session, so no byte moves
// and no object is created.
func TestProbeDailyTransferCeiling(t *testing.T) {
	f := loadProbeFixture(t)
	accounts := []struct {
		label string
		s     probeSession
	}{
		{"flow-nearly-out", f.SecondAccount},
		{"drive-full", f.Personal},
	}
	for _, acct := range accounts {
		t.Run(acct.label, func(t *testing.T) {
			s := acct.s
			status, code := getUserInfoAlive(t, s)
			if status != http.StatusOK || code != "0" {
				t.Logf("captured session refused (http=%d %s), rotating once", status, code)
				s = getSessionForPC(t, s)
			}
			reportAccountState(t, s)
			// Four claims: a single-part size, a multi-part size with the
			// joined digest, the same multi-part size with the whole-file
			// digest where the join is expected, and a size above both
			// transFileSize and the day's remaining flow.
			for _, c := range []struct {
				size      int64
				asFileMd5 bool
			}{
				{1 << 20, false},
				{64 << 20, false},
				{64 << 20, true},
				{3 << 30, false},
			} {
				initClaim(t, s, c.size, c.asFileMd5)
			}
		})
	}
}

// reportAccountState logs capacity, available, the derived used figure and the
// day-flow counters, so a claim can be judged against what the drive actually
// has rather than against a mental arithmetic slip.
func reportAccountState(t *testing.T, s probeSession) {
	t.Helper()
	status, body := signedCall(t, s, apiHost, false, http.MethodGet, "/getUserInfo.action", probeQuery("returnType=JSON"), "")
	m := decode(t, body)
	capacity, ok1 := parseFloat(field(m, "capacity"))
	available, ok2 := parseFloat(field(m, "available"))
	if ok1 && ok2 {
		t.Logf("storage: http=%d capacity=%.0f available=%.0f used=%.0f (available is what getUserInfo means by free space)",
			status, capacity, available, capacity-available)
	} else {
		t.Logf("storage: http=%d raw=%s", status, trunc(string(body), 200))
	}

	status, body = signedCall(t, s, apiHost, false, http.MethodGet, "/getUserPrivilegesV2.action", probeQuery("flag=1"), "")
	m = decode(t, body)
	claims := map[string]string{}
	for _, e := range subList(m, "data") {
		row, ok := e.(map[string]any)
		if !ok {
			continue
		}
		name := field(row, "privilegeName")
		if strings.HasPrefix(name, "trans") {
			claims[name] = field(row, "privilegeValue") + "/" + field(row, "privilegeValueUnit")
		}
	}
	ext := subMap(m, "extData")
	if len(claims) == 0 {
		t.Logf("privileges rows not understood, raw: %s", trunc(string(body), 300))
	}
	t.Logf("privileges http=%d: transDayFlow=%s transFileSize=%s transSpeed=%s usedDayFlow=%s",
		status, claims["transDayFlow"], claims["transFileSize"], claims["transSpeed"], field(ext, "usedDayFlow"))
}

func parseFloat(s string) (float64, bool) {
	var v float64
	_, err := fmt.Sscanf(s, "%g", &v)
	return v, err == nil
}

// initClaim opens an upload session for a file of the given size without
// uploading or committing anything, and reports how the server answered.
// wholeFileAsSlice sends the whole-file digest where the multi-part rule
// would expect the digest over the joined part digests.
func initClaim(t *testing.T, s probeSession, fileSize int64, wholeFileAsSlice bool) {
	t.Helper()
	parts := api.PlanParts(fileSize, probeSliceSize)
	digests := make([][]byte, 0, len(parts))
	for _, p := range parts {
		digests = append(digests, api.MD5Raw([]byte(fmt.Sprintf("claim-%d-%d", p.Index, p.Size))))
	}
	fileMd5 := api.MD5Upper([]byte(fmt.Sprintf("claim-file-%d", fileSize)))
	sliceMd5 := api.SliceAggMD5(digests, fileMd5)
	if wholeFileAsSlice {
		sliceMd5 = fileMd5
	}

	params := api.InitParams("-11", "probe-claim.bin", fileMd5, fileSize, sliceMd5, probeSliceSize)
	enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams(params))
	require.NoError(t, err)

	status, body := signedCall(t, s, uploadHost, false, http.MethodPost, "/person/initMultiUpload", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m := decode(t, body)
	data := subMap(m, "data")
	t.Logf("init claim size=%d parts=%d sliceMd5AsFileMd5=%v http=%d code=%q msg=%q fileDataExists=%s uploadFileId=%q",
		fileSize, len(parts), wholeFileAsSlice, status, field(m, "code"), field(m, "msg"), field(data, "fileDataExists"), trunc(field(data, "uploadFileId"), 48))
}
