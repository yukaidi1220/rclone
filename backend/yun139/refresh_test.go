package yun139

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/config/configmap"
)

// roundTripFunc adapts a plain func to http.RoundTripper so tests can mock
// the refresh endpoint without a real server.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// makeAuth builds a base64 authorization
// base64("pc:<account>:<token>|1|RCS|<expiryMs>|junk") the way the server
// issues them, so tokenExpiry() can parse a controllable expiry.
func makeAuth(account, token string, expiry time.Time) string {
	raw := fmt.Sprintf("pc:%s:%s|1|RCS|%d|junk", account, token, expiry.UnixMilli())
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// decodedAuth decodes a base64 authorization back to its plaintext form.
func decodedAuth(t *testing.T, auth string) string {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		t.Fatalf("decodedAuth: %v", err)
	}
	return string(b)
}

// refreshFs builds an Fs primed for refreshToken: a token about to expire,
// a known userDomainID (so refreshToken does not need queryFamilyCloud), and
// an injectable http client.
func refreshFs(t *testing.T, rt roundTripFunc, m configmap.Simple) (*Fs, string) {
	t.Helper()
	// The token registry is process-global and keyed by account, so tests that
	// reuse the same account would otherwise share (and pollute) one tokenState.
	tokenRegistry.Lock()
	tokenRegistry.byAccount = map[string]*tokenState{}
	tokenRegistry.Unlock()
	old := makeAuth("17263275626", "OLDTOK", time.Now().Add(24*time.Hour))
	f := &Fs{
		name:         "test",
		tokMu:        &sync.Mutex{},
		httpClient:   &http.Client{Transport: rt},
		m:            m,
		auth:         old,
		account:      "17263275626",
		userDomainID: "1301956522699563527",
	}
	return f, old
}

// successRT returns a RoundTripper that answers the authTokenRefresh call with
// a 200, ERRORCODE=0 and a fresh APP_AUTH token, and records the request body.
//
// The server returns APP_AUTH as base64("<account>:<token>...") WITHOUT the
// "pc:" prefix; the backend must re-add it. outRaw carries the raw APP_AUTH
// value and outNew the value the backend should have persisted (pc: + raw).
func successRT(t *testing.T, captured *string, outRaw, outNew *string) roundTripFunc {
	return func(r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		*captured = string(b)
		raw := base64.StdEncoding.EncodeToString([]byte("17263275626:NEWTOK|1|RCS|9999999999999|junk"))
		if outRaw != nil {
			*outRaw = raw
		}
		if outNew != nil {
			*outNew = base64.StdEncoding.EncodeToString([]byte("pc:17263275626:NEWTOK|1|RCS|9999999999999|junk"))
		}
		return &http.Response{
			StatusCode: 200,
			Header: http.Header{
				"Errorcode": {"0"},
				"App_auth":  {"Basic " + raw},
			},
			Body:          io.NopCloser(strings.NewReader(`{"userId":1301956522699563527}`)),
			ContentLength: -1,
		}, nil
	}
}

// TestRefreshToken_WritesNewToken verifies a successful refresh adopts the
// APP_AUTH token, persists it to the config, and sends userDomainId (not the
// phone number) as the userId.
func TestRefreshToken_WritesNewToken(t *testing.T) {
	m := configmap.Simple{}
	var captured, gotRaw, gotNew string
	f, old := refreshFs(t, successRT(t, &captured, &gotRaw, &gotNew), m)

	if err := f.refreshToken(context.Background()); err != nil {
		t.Fatalf("refreshToken: %v", err)
	}

	// userId must be the userDomainId, not the phone number.
	if !strings.Contains(captured, `"userId":"1301956522699563527"`) {
		t.Errorf("request body %q: expected userId = userDomainId", captured)
	}
	if strings.Contains(captured, `"userId":"17263275626"`) {
		t.Errorf("request body %q: must NOT use the phone number as userId", captured)
	}

	// The stored token must be the pc:-prefixed normalised form (parseAuth /
	// tokenExpiry require the scheme prefix), not the raw APP_AUTH value.
	if f.auth != gotNew {
		t.Errorf("f.auth = %q, want normalised %q", f.auth, gotNew)
	}
	if f.auth == gotRaw {
		t.Error("f.auth must not be the raw APP_AUTH (missing pc: prefix)")
	}
	got, _ := f.m.Get("authorization")
	if got == "" || got == old {
		t.Errorf("config authorization = %q, want the new token", got)
	}
	if got != gotNew {
		t.Errorf("config authorization = %q, want normalised %q", got, gotNew)
	}
}

// TestRefreshToken_NoTokenIsError verifies a 200 with ERRORCODE=0 but no
// APP_AUTH header is treated as a failure (a successful refresh with no new
// token is abnormal), and leaves the token untouched.
func TestRefreshToken_NoTokenIsError(t *testing.T) {
	m := configmap.Simple{}
	rt := func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"ERRORCODE": {"0"}},
			Body:       io.NopCloser(strings.NewReader(`{}`)),
		}, nil
	}
	f, old := refreshFs(t, rt, m)
	if err := f.refreshToken(context.Background()); err == nil {
		t.Fatal("expected error for a refresh with no APP_AUTH, got nil")
	}
	if f.auth != old {
		t.Errorf("f.auth changed without APP_AUTH: %q", f.auth)
	}
	if _, ok := m.Get("authorization"); ok {
		t.Errorf("config authorization written without a new token")
	}
}

// TestRefreshToken_ErrorCode verifies a non-zero ERRORCODE is surfaced as an
// error and does not modify the token.
func TestRefreshToken_ErrorCode(t *testing.T) {
	m := configmap.Simple{}
	rt := func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Errorcode": {"05010003"}},
			Body:       io.NopCloser(strings.NewReader(`{}`)),
		}, nil
	}
	f, old := refreshFs(t, rt, m)
	if err := f.refreshToken(context.Background()); err == nil {
		t.Fatal("expected error for non-zero ERRORCODE, got nil")
	}
	if f.auth != old {
		t.Errorf("f.auth changed on error: %q", f.auth)
	}
}

// TestRefreshToken_AdoptsConcurrentRefresh verifies the oauthutil-style guard:
// if the config already holds a token different from ours (a concurrent rclone
// refreshed it), we adopt it and never hit the network.
func TestRefreshToken_AdoptsConcurrentRefresh(t *testing.T) {
	m := configmap.Simple{}
	new := makeAuth("17263275626", "OTHERTOK", time.Now().Add(30*24*time.Hour))
	m["authorization"] = new
	called := false
	rt := func(r *http.Request) (*http.Response, error) {
		called = true
		return nil, fmt.Errorf("network must not be hit")
	}
	f, old := refreshFs(t, rt, m)
	if err := f.refreshToken(context.Background()); err != nil {
		t.Fatalf("refreshToken: %v", err)
	}
	if called {
		t.Error("refresh network call made even though config had a fresher token")
	}
	if f.auth != new {
		t.Errorf("f.auth = %q, want the config token %q", f.auth, new)
	}
	_ = old
}

// TestRefreshToken_AdoptsConcurrentRefresh skips the refresh when the token
// still has plenty of life left.
func TestRefreshToken_SkipsWhenFresh(t *testing.T) {
	m := configmap.Simple{}
	called := false
	rt := func(r *http.Request) (*http.Response, error) {
		called = true
		return nil, fmt.Errorf("network must not be hit")
	}
	old := makeAuth("17263275626", "FRESHTOK", time.Now().Add(30*24*time.Hour))
	f := &Fs{
		name:       "test",
		tokMu:      &sync.Mutex{},
		httpClient: &http.Client{Transport: roundTripFunc(rt)},
		m:          m,
		auth:       old,
	}
	if err := f.refreshToken(context.Background()); err != nil {
		t.Fatalf("refreshToken: %v", err)
	}
	if called {
		t.Error("refresh network call made for a still-valid token")
	}
}

// TestRefreshToken_SharedState verifies the account-wide tokenState: two Fs of
// one account share a single state, so a refresh on one is visible to the other
// (accessToken returns the new token) and writes back to every registered
// mapper, not just the refreshing Fs's.
func TestRefreshToken_SharedState(t *testing.T) {
	// reset registry for isolation
	tokenRegistry.Lock()
	tokenRegistry.byAccount = map[string]*tokenState{}
	tokenRegistry.Unlock()

	mA := configmap.Simple{}
	mB := configmap.Simple{}
	var captured string
	fA, _ := refreshFs(t, successRT(t, &captured, nil, nil), mA)
	fB := &Fs{
		name:         "remoteB",
		tokMu:        &sync.Mutex{},
		m:            mB,
		auth:         makeAuth("17263275626", "OLDTOK", time.Now().Add(24*time.Hour)),
		account:      "17263275626",
		userDomainID: "1301956522699563527",
	}

	// fA refreshes; fB shares the same account-wide tokenState. Register fB's
	// mapper on the shared state the way NewFs would (via tokenState), so the
	// refresh writes back to fB's config too and fB reads the shared token.
	fB.ts = fB.tokenState()
	if err := fA.refreshToken(context.Background()); err != nil {
		t.Fatalf("fA.refreshToken: %v", err)
	}
	// fA must hold the freshly-refreshed token, not the old one.
	oldB64 := makeAuth("17263275626", "OLDTOK", time.Now().Add(24*time.Hour))
	if fA.accessToken() == oldB64 {
		t.Error("fA.token did not change after refresh")
	}
	// The refreshed value must decode back to a NEWTOK pc:-authorization.
	if !strings.Contains(decodedAuth(t, fA.accessToken()), "NEWTOK") {
		t.Errorf("fA.token after refresh = %q, want NEWTOK token", fA.accessToken())
	}
	// fB (never refreshed itself) must see the same fresh token.
	if fB.accessToken() != fA.accessToken() {
		t.Errorf("fB.accessToken() = %q, want shared %q", fB.accessToken(), fA.accessToken())
	}
	// The refresh must have written back to BOTH mappers.
	newA, _ := mA.Get("authorization")
	newB, _ := mB.Get("authorization")
	if newA == "" || newA != newB {
		t.Errorf("write-back to all mappers failed: A=%q B=%q", newA, newB)
	}
}

// TestRefreshToken_SelfHealsWrongUserDomainID verifies the robustness retry: a
// manually-configured (wrong) userDomainId makes the first refresh fail with
// ERRORCODE 05010003, but refreshToken redisovers the id from queryFamilyCloud
// and retries successfully, self-healing the config instead of blocking.
func TestRefreshToken_SelfHealsWrongUserDomainID(t *testing.T) {
	tokenRegistry.Lock()
	tokenRegistry.byAccount = map[string]*tokenState{}
	tokenRegistry.Unlock()

	correctUID := "1301956522699563527"
	wrongUID := "999999999999999999"
	refreshCalls := 0
	var lastUserID string

	rt := func(r *http.Request) (*http.Response, error) {
		host := r.URL.Host
		// queryFamilyCloud (family cloud) returns the correct accountUserId.
		if host == "group.yun.139.com" {
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Errorcode": {"0"}},
				Body: io.NopCloser(strings.NewReader(
					`{"familyCloudList":[{"commonAccountInfo":{"accountUserId":"` + correctUID + `"}}]}`)),
				ContentLength: -1,
			}, nil
		}
		// authTokenRefresh (note-njs): parse the userId in the body.
		b, _ := io.ReadAll(r.Body)
		var req struct {
			UserID string `json:"userId"`
		}
		_ = json.Unmarshal(b, &req)
		lastUserID = req.UserID
		refreshCalls++
		if req.UserID != correctUID {
			return &http.Response{
				StatusCode: 200,
				Header:     http.Header{"Errorcode": {"05010003"}},
				Body:       io.NopCloser(strings.NewReader(`{}`)),
			}, nil
		}
		return &http.Response{
			StatusCode: 200,
			Header: http.Header{
				"Errorcode": {"0"},
				"App_auth":  {"Basic " + base64.StdEncoding.EncodeToString([]byte("17263275626:NEWTOK|1|RCS|9999999999999|junk"))},
			},
			Body:          io.NopCloser(strings.NewReader(`{}`)),
			ContentLength: -1,
		}, nil
	}

	m := configmap.Simple{}
	f := &Fs{
		name:         "test",
		tokMu:        &sync.Mutex{},
		httpClient:   &http.Client{Transport: roundTripFunc(rt)},
		m:            m,
		auth:         makeAuth("17263275626", "OLDTOK", time.Now().Add(24*time.Hour)),
		account:      "17263275626",
		userDomainID: wrongUID,
		opt:          Options{UserDomainID: wrongUID},
	}

	if err := f.refreshToken(context.Background()); err != nil {
		t.Fatalf("refreshToken with self-heal: %v", err)
	}
	if refreshCalls < 2 {
		t.Errorf("expected a retry after the wrong-userId failure, got %d refresh calls", refreshCalls)
	}
	if lastUserID != correctUID {
		t.Errorf("retry used userId %q, want correct %q", lastUserID, correctUID)
	}
	// The correct id must be persisted back to the config for future runs.
	gotUID, _ := m.Get("user_domain_id")
	if gotUID != correctUID {
		t.Errorf("user_domain_id persisted = %q, want %q", gotUID, correctUID)
	}
	if f.userDomainID != correctUID {
		t.Errorf("f.userDomainID = %q, want self-healed %q", f.userDomainID, correctUID)
	}
}

// TestRefreshToken_NoRefreshSkipsEvenWhenStale verifies that with
// no_refresh=true a stale-but-not-expired token is left alone: no network, no
// userDomainId discovery, no error - the caller just keeps the token as-is.
func TestRefreshToken_NoRefreshSkipsEvenWhenStale(t *testing.T) {
	tokenRegistry.Lock()
	tokenRegistry.byAccount = map[string]*tokenState{}
	tokenRegistry.Unlock()

	rt := func(r *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("network must not be hit (no_refresh)")
	}
	m := configmap.Simple{}
	old := makeAuth("17263275626", "STALETOK", time.Now().Add(12*time.Hour)) // <15 days, would normally refresh
	f := &Fs{
		name:         "test",
		tokMu:        &sync.Mutex{},
		httpClient:   &http.Client{Transport: roundTripFunc(rt)},
		m:            m,
		auth:         old,
		account:      "17263275626",
		userDomainID: "1301956522699563527",
		opt:          Options{NoRefresh: true},
	}
	if err := f.refreshToken(context.Background()); err != nil {
		t.Fatalf("refreshToken with no_refresh: %v", err)
	}
	if f.auth != old {
		t.Errorf("f.auth changed with no_refresh: %q", f.auth)
	}
}

// TestRefreshToken_ExpiredTokenRefreshes verifies that an actually-expired
// token still goes through the refresh path (rather than being treated as
// fresh) and adopts the returned token.
func TestRefreshToken_ExpiredTokenRefreshes(t *testing.T) {
	tokenRegistry.Lock()
	tokenRegistry.byAccount = map[string]*tokenState{}
	tokenRegistry.Unlock()

	m := configmap.Simple{}
	var captured string
	f, _ := refreshFs(t, successRT(t, &captured, nil, nil), m)
	f.auth = makeAuth("17263275626", "EXPIREDTOK", time.Now().Add(-time.Hour)) // already expired
	if err := f.refreshToken(context.Background()); err != nil {
		t.Fatalf("refreshToken with expired token: %v", err)
	}
	if !strings.Contains(captured, `"authToken":"EXPIREDTOK`) {
		t.Errorf("refresh did not use the expired token as authToken: %q", captured)
	}
	if !strings.Contains(decodedAuth(t, f.auth), "NEWTOK") {
		t.Errorf("expired token was not replaced: f.auth = %q, want NEWTOK token", f.auth)
	}
}
