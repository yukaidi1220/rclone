package cloud189

import (
	"crypto/rand"
	"encoding/hex"
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

// Live probes against the captures' sessions. Gated so that "go test ./..."
// and CI never touch the network: the tickets belong to shared production
// accounts, so a probe runs only when CLOUD189_PROBE=1 and the operator hands
// over a fixture path. The fixture holds a real session secret and lives
// outside the repository.
//
// These probes deliberately never call getSessionForPC.action. That endpoint
// rotates sessionKey and sessionSecret together, and no capture shows whether
// the old pair survives - so refreshing could drop the official client's
// session. Everything here signs with a pair the client already issued.

type probeSession struct {
	SessionKey       string `json:"sessionKey"`
	SessionSecret    string `json:"sessionSecret"`
	FamilySessionKey string `json:"familySessionKey"`
	FamilyID         string `json:"familyId"`
	LoginName        string `json:"loginName"`
	IssuedAt         string `json:"issuedAt"`
	SourceHar        string `json:"sourceHar"`
	// The refresh parameters the client presented, replayed verbatim so a
	// probe is not registered as a new device.
	AccessToken string `json:"accessToken"`
	AppID       string `json:"appId"`
	ClientSn    string `json:"clientSn"`
	JGOpenID    string `json:"jgOpenId"`
	ModelRaw    string `json:"modelRaw"`
	// RotatedOutKey records the key a rotation replaced, whose fate the
	// rotation probe measures.
	RotatedOutKey string `json:"rotatedOutKey"`
}

type probeFixture struct {
	Note          string       `json:"note"`
	Personal      probeSession `json:"personal"`
	SecondAccount probeSession `json:"secondAccount"`
}

// doScrubbed sends one of the probe's own requests. Go's transport errors quote the
// URL they failed on, and every URL a probe sends to is either signed by the storage
// layer or carries the access token, so the quote comes out before the error reaches
// a test log that may be pasted around. The production path does the same thing with
// scrubErr; a probe that skips it is the one place a credential can still reach
// output.
func doScrubbed(req *http.Request) (*http.Response, error) {
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		err = scrubErr(err, req.URL.String())
	}
	return res, err
}

// doScrubbedToken is doScrubbed for the session endpoint, whose URL carries the
// access token both as it stands and percent-encoded.
func doScrubbedToken(req *http.Request, token string) (*http.Response, error) {
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		err = scrubErr(err, req.URL.String(), token, url.QueryEscape(token))
	}
	return res, err
}

// loadProbeFixture reads the fixture named by CLOUD189_PROBE_SESSION.
func loadProbeFixture(t *testing.T) *probeFixture {
	t.Helper()
	if os.Getenv("CLOUD189_PROBE") != "1" {
		t.Skip("live probe against a shared account; set CLOUD189_PROBE=1 and CLOUD189_PROBE_SESSION=<fixture.json> to run")
	}
	path := os.Getenv("CLOUD189_PROBE_SESSION")
	require.NotEmpty(t, path, "CLOUD189_PROBE_SESSION must point at the session fixture")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var f probeFixture
	require.NoError(t, json.Unmarshal(raw, &f))
	require.NotEmpty(t, f.Personal.SessionSecret)
	t.Logf("probe session issued %s from %s (account withheld)", f.Personal.IssuedAt, f.Personal.SourceHar)
	return &f
}

// requestID returns a random UUID v4 for the x-request-id header.
func requestID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	_, err := rand.Read(b[:])
	require.NoError(t, err)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]), hex.EncodeToString(b[10:16]))
}

// hdr is one extra request header, used where a call needs a header the
// standard set does not carry (decodefields on the upload POSTs).
type hdr struct{ name, value string }

// signedCall performs one signed REST request and returns its status and body.
//
// requestURI must be the path alone: the query never enters the signature, and
// passing it in here would sign something the server does not look for.
func signedCall(t *testing.T, s probeSession, host string, family bool, operate, requestURI, rawQuery, body string, extra ...hdr) (int, []byte) {
	t.Helper()
	sessionKey := s.SessionKey
	if family {
		sessionKey = s.FamilySessionKey
		require.NotEmpty(t, sessionKey, "this session carries no family key")
	}
	require.NotEmpty(t, sessionKey)

	full := host + requestURI
	if rawQuery != "" {
		full += "?" + rawQuery
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Second)
		}
		// One date for both the header and the signature: the server signs what
		// it receives, so these must be the same bytes.
		date := time.Now().UTC().Format(http.TimeFormat)
		params := ""
		if body != "" {
			v, err := url.ParseQuery(body)
			require.NoError(t, err)
			params = v.Get("params")
		}
		if params == "" && rawQuery != "" {
			// getMultiUploadUrls is a GET whose params live in the query, and
			// they still enter the signature. Reading them only from the body
			// signs a request the server rejects with InvalidSignature
			// (measured 2026-10-22).
			v, err := url.ParseQuery(rawQuery)
			require.NoError(t, err)
			params = v.Get("params")
		}
		req, err := http.NewRequest(operate, full, operateBody(operate, body))
		require.NoError(t, err)
		req.Header.Set("user-agent", desktopUserAgent)
		req.Header.Set("accept", "application/json;charset=UTF-8")
		req.Header.Set("cache-control", "no-cache")
		req.Header.Set("date", date)
		req.Header.Set("sessionkey", sessionKey)
		req.Header.Set("x-request-id", requestID(t))
		req.Header.Set("signature", api.Sign(s.SessionSecret, sessionKey, operate, requestURI, date, params))
		if operate == http.MethodPost {
			req.Header.Set("content-type", "application/x-www-form-urlencoded")
		}
		for _, h := range extra {
			req.Header.Set(h.name, h.value)
		}
		res, err := doScrubbed(req)
		if err != nil {
			lastErr = err
			continue
		}
		b, err := io.ReadAll(res.Body)
		_ = res.Body.Close()
		require.NoError(t, err)
		return res.StatusCode, b
	}
	t.Fatalf("transport failed after retries: %v", lastErr)
	return 0, nil
}

func operateBody(operate, body string) io.Reader {
	if operate == http.MethodGet {
		return http.NoBody
	}
	return strings.NewReader(body)
}

// decode decodes a 189 JSON response, keeping numbers as their original text.
//
// UseNumber is not cosmetic: file ids are 18 digits (900000000000000008) and
// float64 only represents integers exactly up to 2^53, so decoding into an
// interface{} would silently corrupt every id handed back.
func decode(t *testing.T, body []byte) map[string]any {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	var m map[string]any
	require.NoErrorf(t, dec.Decode(&m), "response is not JSON: %s", trunc(string(body), 200))
	return m
}

// TestProbeCapturedSessionsAlive checks that the two captured session pairs
// still answer, using read-only endpoints only. A dead session says so in the
// body while replying HTTP 200, so the status code alone proves nothing here.
func TestProbeCapturedSessionsAlive(t *testing.T) {
	f := loadProbeFixture(t)

	sessions := []struct {
		name string
		s    probeSession
	}{
		{"write-ops account", f.Personal},
		{"rapid-transfer account", f.SecondAccount},
	}
	for _, ss := range sessions {
		t.Run(ss.name, func(t *testing.T) {
			q := "rand=" + fmt.Sprintf("%d", time.Now().UnixMilli()) + "&" + clientSuffix + "&returnType=JSON"
			status, body := signedCall(t, ss.s, apiHost, false, http.MethodGet, "/getUserInfo.action", q, "")
			m := decode(t, body)
			t.Logf("personal getUserInfo: http=%d res_code=%v capacity=%v available=%v maxFilesize=%v hasFamily=%v",
				status, m["res_code"], m["capacity"], m["available"], m["maxFilesize"], m["hasFamily"])
			if _, ok := m["res_code"]; ok {
				require.Equalf(t, "0", field(m, "res_code"), "session rejected: %s", trunc(string(body), 200))
			} else {
				require.NotContains(t, string(body), "InvalidSessionKey", "session is dead")
			}

			// Listing the root is the other read that needs no arguments beyond
			// the session, and it is what a truncated listing would show up in.
			q = "rand=" + fmt.Sprintf("%d", time.Now().UnixMilli()) + "&" + clientSuffix +
				"&folderId=-11&pageNum=1&pageSize=200&fileType=0&mediaType=0&orderBy=filename&descending=true"
			status, body = signedCall(t, ss.s, apiHost, false, http.MethodGet, "/listFiles.action", q, "")
			m = decode(t, body)
			ao, _ := m["fileListAO"].(map[string]any)
			require.NotNil(t, ao, "listFiles carried no fileListAO: %s", trunc(string(body), 200))
			t.Logf("personal listFiles: http=%d res_code=%v count=%v fileListSize=%v folderListSize=%v",
				status, m["res_code"], ao["count"], lenOf(ao["fileList"]), lenOf(ao["folderList"]))

			if ss.s.FamilySessionKey == "" {
				t.Log("no family key in this fixture, skipping family reads")
				return
			}
			q = "rand=" + fmt.Sprintf("%d", time.Now().UnixMilli()) + "&" + clientSuffix +
				"&familyId=" + ss.s.FamilyID
			status, body = signedCall(t, ss.s, apiHost, true, http.MethodGet, "/family/file/getUserInfo.action", q, "")
			t.Logf("family getUserInfo: http=%d body=%s", status, trunc(string(body), 160))

			q = "rand=" + fmt.Sprintf("%d", time.Now().UnixMilli()) + "&" + clientSuffix +
				"&familyId=" + ss.s.FamilyID + "&pageNum=1&pageSize=200&fileType=0&mediaType=0&orderBy=1&descending=true"
			status, body = signedCall(t, ss.s, apiHost, true, http.MethodGet, "/family/file/listFiles.action", q, "")
			m = decode(t, body)
			ao, _ = m["fileListAO"].(map[string]any)
			require.NotNil(t, ao, "family listFiles carried no fileListAO: %s", trunc(string(body), 200))
			t.Logf("family listFiles: http=%d count=%v fileListSize=%v folderListSize=%v",
				status, ao["count"], lenOf(ao["fileList"]), lenOf(ao["folderList"]))
		})
	}
}

func lenOf(v any) int {
	if a, ok := v.([]any); ok {
		return len(a)
	}
	return -1
}

// trunc clips a response body for an error message, so a probe failure cannot
// dump a whole listing into the log.
func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
