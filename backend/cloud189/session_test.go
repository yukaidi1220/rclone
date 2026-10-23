package cloud189

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloud189/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/require"
)

// The session is shared with the official client and with anything else using the
// same account, so both halves of the recovery matter: a rejected request has to
// be retried on a live session rather than failing, and the goroutines that all
// hit the same dead session have to log in once between them, not once each.

// rotatingDrive is a drive whose session expires: it accepts the key it last
// minted and rejects everything else, which is what InvalidSessionKey looks like
// from the client's side.
type rotatingDrive struct {
	mu           sync.Mutex
	exchanges    int
	currentKey   string
	sessionReqs  int
	listRequests int
}

const staleKey = "stale-session-key"

func (d *rotatingDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/getSessionForPC.action":
		// Wide enough that a test asking for eight concurrent requests cannot have
		// them all arrive after the first exchange finished.
		time.Sleep(20 * time.Millisecond)
		d.mu.Lock()
		d.exchanges++
		d.currentKey = fmt.Sprintf("fresh-key-%d", d.exchanges)
		key := d.currentKey
		d.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"res_code": 0, "sessionKey": key,
			"sessionSecret": strings.Repeat("ab", api.AESKeyLen),
			"loginName":     "fixture-account",
		})

	case "/listFiles.action":
		d.mu.Lock()
		d.listRequests++
		current := d.currentKey
		d.mu.Unlock()
		if got := r.Header.Get("sessionkey"); got != current {
			d.mu.Lock()
			d.sessionReqs++
			d.mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"errorCode":"InvalidSessionKey","errorMsg":"userSessionBO is null or fail to get sessionsecret by sessionkey"}`))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"res_code": 0, "fileListAO": map[string]any{
			"count": 0, "fileList": []any{}, "folderList": []any{},
		}})

	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"code": "NoSuchEndpoint"})
	}
}

func (d *rotatingDrive) counts() (exchanges, listings, rejected int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.exchanges, d.listRequests, d.sessionReqs
}

// newRotatingFs builds an Fs whose session the drive will not accept, pointed at
// the stand-in. No credential is real: the token is unique to the test, so the
// shared session registry is not reachable from here either.
func newRotatingFs(t *testing.T, drive http.Handler) (*Fs, string) {
	t.Helper()
	srv := httptest.NewServer(drive)
	t.Cleanup(srv.Close)

	f := &Fs{name: "test", httpClient: srv.Client()}
	// The exchange builds its own URL from apiBase rather than from the host a
	// call is handed, so both have to point at the stand-in.
	f.apiBase = srv.URL
	f.opt.AccessToken = "fixture-token-" + t.Name()
	f.opt.Enc = cloud189DefaultEncoding
	f.pacer = fs.NewPacer(context.Background(), pacer.NewDefault(
		pacer.MinSleep(time.Microsecond), pacer.MaxSleep(time.Millisecond), pacer.DecayConstant(1)))
	ts := f.session()
	ts.mu.Lock()
	ts.sessionKey = staleKey
	ts.sessionSecret = strings.Repeat("cd", api.AESKeyLen)
	ts.mu.Unlock()
	return f, srv.URL
}

func TestRejectedSessionIsExchangedAndTheRequestRetried(t *testing.T) {
	drive := &rotatingDrive{currentKey: "nobody-yet"}
	f, host := newRotatingFs(t, drive)

	_, err := f.call(context.Background(), host, http.MethodGet, "/listFiles.action", "folderId=-11", "")
	require.NoError(t, err, "a dead session is recovered inside the call, not reported to the user")

	exchanges, listings, rejected := drive.counts()
	require.Equal(t, 1, exchanges, "the token was exchanged once")
	require.Equal(t, 2, listings, "the request went once on the dead session and once on the new one")
	require.Equal(t, 1, rejected, "only the first attempt was refused")
}

// TestConcurrentRejectionsExchangeTheSessionOnce is the reason the request reports
// which key it signed with. Without it, every goroutine that notices the same dead
// session logs in again, against an account that may be shared with the official
// client and with another program.
func TestConcurrentRejectionsExchangeTheSessionOnce(t *testing.T) {
	drive := &rotatingDrive{currentKey: "nobody-yet"}
	f, host := newRotatingFs(t, drive)

	const workers = 8
	var wg sync.WaitGroup
	errs := make([]error, workers)
	start := make(chan struct{})
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = f.call(context.Background(), host, http.MethodGet, "/listFiles.action", "folderId=-11", "")
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "worker %d", i)
	}
	exchanges, listings, rejected := drive.counts()
	require.Equal(t, 1, exchanges, "one login for one dead session, however many requests noticed it")
	require.Equal(t, workers+rejected, listings, "each refused request was retried, and no request was lost")
	require.NotZero(t, rejected, "the test has to have actually met a rejection")
}

// TestRefreshFailureIsSaidAndNotRetried: when even the exchange is refused, the
// answer must say the token is the thing to fix, and must stop the retries.
func TestRefreshFailureIsSaidAndNotRetried(t *testing.T) {
	drive := &refusingExchangeDrive{}
	f, host := newRotatingFs(t, drive)

	_, err := f.call(context.Background(), host, http.MethodGet, "/listFiles.action", "folderId=-11", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "re-exchanging it failed")
	require.Contains(t, err.Error(), "put a current one in the remote", "the user has to be told which credential is at fault")
	require.True(t, fserrors.IsNoRetryError(err))
	require.EqualValues(t, 1, atomic.LoadInt32(&drive.attempts), "a token the login endpoint refuses is not offered again per retry")
}

// refusingExchangeDrive refuses the exchange itself, which is what a retired
// access token looks like: the shape below is the answer a stale token really
// got on 2026-10-22.
type refusingExchangeDrive struct{ attempts int32 }

func (d *refusingExchangeDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/getSessionForPC.action" {
		atomic.AddInt32(&d.attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"res_code":"UserInvalidOpenToken","res_message":"error in login get unifyAccountInfo is null"}`))
		return
	}
	w.WriteHeader(http.StatusBadRequest)
	_, _ = w.Write([]byte(`{"errorCode":"InvalidSessionKey","errorMsg":"session is dead"}`))
}

// TestFamilyTrafficUsesTheFamilySessionAndEndpoints is the space choice made
// visible. Both sessions carry the same secret in every capture, so the signature
// check in the stand-in cannot tell one from the other and a wrong key would go
// unnoticed: what catches it is which key each request was signed with and which
// endpoint it went to.
func TestFamilyTrafficUsesTheFamilySessionAndEndpoints(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	f.opt.Space = spaceFamily
	f.familyID = famID
	f.familyRootID = famRoot
	f.dirCache = dircache.New("", famRoot, f)
	drive.listingFn = newListingScript(map[string][]string{
		famRoot: {emptyListing()},
	}).fn

	_, err := f.List(context.Background(), "")
	require.NoError(t, err)
	link, ttl, err := f.downloadURL(context.Background(), "900000000000000004")
	require.NoError(t, err)
	require.NotEmpty(t, link)
	require.Equal(t, familyURLTTL, ttl, "the family link lives longer, and the cache window follows it")

	require.NotEmpty(t, drive.apiKeys, "the test has to have asked the drive for something")
	var paths []string
	for _, rec := range drive.apiKeys {
		path, key, ok := strings.Cut(rec, " ")
		require.Truef(t, ok, "the record is a path and a key: %q", rec)
		require.Equal(t, fakeKey+"_family", key, "a family request was signed with the wrong session")
		paths = append(paths, path)
	}
	require.Contains(t, paths, "/family/file/listFiles.action")
	require.Contains(t, paths, "/family/file/getFileDownloadUrl.action")
}

// TestPersonalTrafficNeverUsesTheFamilyKey is the same rule from the other side:
// a personal remote must not sign with the family key either, which is what would
// happen if the choice were made on a stale flag rather than the request.
func TestPersonalTrafficNeverUsesTheFamilyKey(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	f.familyID = famID
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
	}).fn

	_, err := f.List(context.Background(), "")
	require.NoError(t, err)
	_, _, err = f.downloadURL(context.Background(), "900000000000000004")
	require.NoError(t, err)

	require.NotEmpty(t, drive.apiKeys)
	var paths []string
	for _, rec := range drive.apiKeys {
		path, key, ok := strings.Cut(rec, " ")
		require.Truef(t, ok, "the record is a path and a key: %q", rec)
		require.Equal(t, fakeKey, key, "a personal request was signed with anything but the personal key")
		paths = append(paths, path)
	}
	require.Contains(t, paths, "/listFiles.action")
	require.Contains(t, paths, "/getFileDownloadUrl.action")
	for _, path := range paths {
		require.NotContains(t, path, "/family/", "a personal remote has no business calling the family endpoints")
	}
}

// rotatingFamilyDrive is the same expiring session, on the family key. The two
// sessions are separate strings, which is the whole point: recovery has to
// notice that the key it was just refused is the family one and not the personal
// one, or a family remote never exchanges again.
type rotatingFamilyDrive struct {
	mu               sync.Mutex
	exchanges        int
	currentKey       string // the personal key it last minted
	currentFKey      string // the family key it last minted
	listRequests     int    // family listings seen
	sessionReqs      int    // family listings refused for a dead session
	personalListings int
	personalRejected int
}

// rotateSilently expires both keys without telling the client, which is what the
// server does when the session's life runs out: the next request is refused and the
// client has to exchange.
func (d *rotatingFamilyDrive) rotateSilently() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.currentKey = fmt.Sprintf("silent-key-%d", d.exchanges+1)
	d.currentFKey = d.currentKey + "_family"
}

const staleFamilyKey = "stale-family-key"

func (d *rotatingFamilyDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/getSessionForPC.action":
		time.Sleep(20 * time.Millisecond)
		d.mu.Lock()
		d.exchanges++
		d.currentKey = fmt.Sprintf("fresh-key-%d", d.exchanges)
		d.currentFKey = d.currentKey + "_family"
		key, famKey := d.currentKey, d.currentFKey
		d.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"res_code": 0, "sessionKey": key, "familySessionKey": famKey,
			"sessionSecret":       strings.Repeat("ab", api.AESKeyLen),
			"familySessionSecret": strings.Repeat("ab", api.AESKeyLen),
			"loginName":           "fixture-account",
		})

	case "/listFiles.action":
		d.mu.Lock()
		d.personalListings++
		current := d.currentKey
		d.mu.Unlock()
		if got := r.Header.Get("sessionkey"); got != current {
			d.mu.Lock()
			d.personalRejected++
			d.mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"errorCode":"InvalidSessionKey","errorMsg":"userSessionBO is null or fail to get sessionsecret by sessionkey"}`))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"res_code": 0, "fileListAO": map[string]any{
			"count": 0, "fileList": []any{}, "folderList": []any{},
		}})

	case "/family/file/listFiles.action":
		d.mu.Lock()
		d.listRequests++
		current := d.currentFKey
		d.mu.Unlock()
		if got := r.Header.Get("sessionkey"); got != current {
			d.mu.Lock()
			d.sessionReqs++
			d.mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"errorCode":"InvalidSessionKey","errorMsg":"userSessionBO is null or fail to get sessionsecret by sessionkey"}`))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"res_code": 0, "familyListAO": map[string]any{
			"count": 0, "fileList": []any{}, "folderList": []any{},
		}})

	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"code": "NoSuchEndpoint"})
	}
}

func (d *rotatingFamilyDrive) counts() (exchanges, listings, rejected int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.exchanges, d.listRequests, d.sessionReqs
}

// personalCounts is the personal side of the same ledger: listings seen and those
// refused for a dead key.
func (d *rotatingFamilyDrive) personalCounts() (listings, rejected int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.personalListings, d.personalRejected
}

// newRotatingFamilyFs is newRotatingFs with a dead family session in the shared
// state as well, which is what a family remote that has outlived its session has.
func newRotatingFamilyFs(t *testing.T, drive http.Handler) (*Fs, string) {
	t.Helper()
	f, host := newRotatingFs(t, drive)
	ts := f.session()
	ts.mu.Lock()
	ts.familySessionKey = staleFamilyKey
	ts.familySessionSecret = strings.Repeat("cd", api.AESKeyLen)
	ts.mu.Unlock()
	f.familyID = famID
	return f, host
}

// TestRejectedFamilySessionIsExchangedAndTheRequestRetried is the family half of
// the recovery. Judging the refresh by the personal key alone makes a refused
// family key look like a session somebody else already replaced, so exchange
// returns without logging in and the retry replays the same dead key.
func TestRejectedFamilySessionIsExchangedAndTheRequestRetried(t *testing.T) {
	drive := &rotatingFamilyDrive{currentKey: "nobody-yet", currentFKey: "nobody-yet-family"}
	f, host := newRotatingFamilyFs(t, drive)

	_, err := f.call(context.Background(), host, http.MethodGet, "/family/file/listFiles.action", "folderId=-11&pageNum=1", "")
	require.NoError(t, err, "a dead family session is recovered inside the call, not reported to the user")

	exchanges, listings, rejected := drive.counts()
	require.Equal(t, 1, exchanges, "the token was exchanged once for the family rejection")
	require.Equal(t, 2, listings, "the family request went once on the dead key and once on the new one")
	require.Equal(t, 1, rejected, "only the first attempt was refused")
}

// TestConcurrentFamilyRejectionsExchangeTheSessionOnce pins that the family
// dedup works the way the personal one does: a goroutine that finds the same dead
// family key takes the session the first goroutine already minted.
func TestConcurrentFamilyRejectionsExchangeTheSessionOnce(t *testing.T) {
	drive := &rotatingFamilyDrive{currentKey: "nobody-yet", currentFKey: "nobody-yet-family"}
	f, host := newRotatingFamilyFs(t, drive)

	const workers = 6
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = f.call(context.Background(), host, http.MethodGet, "/family/file/listFiles.action", "folderId=-11&pageNum=1", "")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoErrorf(t, err, "worker %d", i)
	}

	exchanges, _, _ := drive.counts()
	require.Equal(t, 1, exchanges, "six goroutines that all found the same dead family session log in once between them")
}

// noFamilyKeyDrive answers the session exchange without naming a family session at
// all, which is the shape that would blank a family key the state is holding.
type noFamilyKeyDrive struct {
	t         *testing.T
	exchanges int
}

func (d *noFamilyKeyDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/getSessionForPC.action":
		d.exchanges++
		writeJSON(w, http.StatusOK, map[string]any{
			"res_code": 0, "sessionKey": "rotated-personal-key",
			"sessionSecret": strings.Repeat("ab", api.AESKeyLen),
			"loginName":     "fixture-account",
		})
	case "/family/file/listFiles.action":
		if got := r.Header.Get("sessionkey"); got != staleFamilyKey {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"errorCode":"InvalidSessionKey","errorMsg":"userSessionBO is null"}`))
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"res_code": 0, "familyListAO": map[string]any{
			"count": 0, "fileList": []any{}, "folderList": []any{},
		}})
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"code": "NoSuchEndpoint"})
	}
}

// TestExchangeKeepsAFamilySessionTheAnswerDidNotName: the session state is shared
// per access token, so a personal remote's exchange that carries no family key must
// leave the family session alone rather than blank it for every family remote of the
// account.
func TestExchangeKeepsAFamilySessionTheAnswerDidNotName(t *testing.T) {
	drive := &noFamilyKeyDrive{t: t}
	f, host := newRotatingFamilyFs(t, drive)

	require.NoError(t, f.exchange(context.Background(), ""))

	ts := f.session()
	ts.mu.Lock()
	keptFamily, rotatedPersonal := ts.familySessionKey, ts.sessionKey
	ts.mu.Unlock()
	require.Equal(t, staleFamilyKey, keptFamily, "a family key the answer never mentioned is left in place")
	require.Equal(t, "rotated-personal-key", rotatedPersonal, "while the key it did answer with is taken")

	_, err := f.call(context.Background(), host, http.MethodGet, "/family/file/listFiles.action", "folderId=-11&pageNum=1", "")
	require.NoError(t, err, "and family traffic keeps working on the session that was kept")
}

// TestNoFamilySessionIsNotRetried: an account either has a family drive or it has
// not, so the request that finds no family session must fail at once. Left as an
// ordinary error the pacer reads it as transient and spends --low-level-retries
// requests, then --retries rounds of the whole command, on an answer that cannot
// change.
func TestNoFamilySessionIsNotRetried(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	f.opt.Space = spaceFamily
	f.familyID = famID
	ts := f.session()
	ts.mu.Lock()
	ts.sessionKey, ts.sessionSecret = fakeKey, strings.Repeat("ab", api.AESKeyLen)
	ts.familySessionKey = ""
	ts.mu.Unlock()

	_, err := f.call(context.Background(), "http://fixture.invalid", http.MethodGet, "/family/file/listFiles.action", "folderId=-11", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no family session")
	require.True(t, fserrors.IsNoRetryError(err), "no retry can give the account a family drive")
	require.Empty(t, sequenceOf(drive), "and not one request went out, let alone a round of them")
}

// The session state is shared by every remote that carries the same access token:
// a personal remote and a family remote of one account are two configuration
// sections, not two logins. These three tests are that relationship seen from the
// write-back side.

// TestPersonalExchangeServesTheFamilyRemoteToo: the answer to one exchange carries
// both keys, and the family remote picks up its own from the shared state without a
// second login.
func TestPersonalExchangeServesTheFamilyRemoteToo(t *testing.T) {
	drive := &rotatingFamilyDrive{currentKey: "nobody-yet", currentFKey: "nobody-yet-family"}
	personal, personalHost := newRotatingFs(t, drive)
	family, familyHost := newRotatingFamilyFs(t, drive)
	family.opt.Space = spaceFamily
	require.Equal(t, personal.opt.AccessToken, family.opt.AccessToken, "one account, two remotes")

	_, err := personal.call(context.Background(), personalHost, http.MethodGet, "/listFiles.action", "folderId=-11", "")
	require.NoError(t, err, "the personal remote recovers on its own rejection")

	_, err = family.call(context.Background(), familyHost, http.MethodGet, "/family/file/listFiles.action", "folderId=-11&pageNum=1", "")
	require.NoError(t, err, "and the family remote works on the key that exchange wrote")

	exchanges, familyListings, familyRejected := drive.counts()
	personalListings, personalRejected := drive.personalCounts()
	require.Equal(t, 1, exchanges, "one login served both remotes")
	require.Equal(t, 1, personalRejected, "only the personal request that noticed the dead session was refused")
	require.Equal(t, 2, personalListings, "and it then went out again on the new key")
	require.Equal(t, 1, familyListings, "the family remote asked once")
	require.Equal(t, 0, familyRejected, "and its request never met a dead session")
}

// TestOneFamilyRejectionServesEveryFamilyRemote: several family drives of one
// account sign with the same family key - the answer carries one family session, not
// one per drive - so an exchange made by one of them is picked up by the others.
func TestOneFamilyRejectionServesEveryFamilyRemote(t *testing.T) {
	drive := &rotatingFamilyDrive{currentKey: "nobody-yet", currentFKey: "nobody-yet-family"}
	first, firstHost := newRotatingFamilyFs(t, drive)
	second, secondHost := newRotatingFamilyFs(t, drive)

	_, err := first.call(context.Background(), firstHost, http.MethodGet, "/family/file/listFiles.action", "folderId=-11&pageNum=1", "")
	require.NoError(t, err)
	exchanges, _, _ := drive.counts()
	require.Equal(t, 1, exchanges)

	// The session dies without the client being told.
	drive.rotateSilently()

	_, err = second.call(context.Background(), secondHost, http.MethodGet, "/family/file/listFiles.action", "folderId=-22&pageNum=1", "")
	require.NoError(t, err, "the second family remote recovers from the same rejection")
	exchanges, _, _ = drive.counts()
	require.Equal(t, 2, exchanges)

	_, err = first.call(context.Background(), firstHost, http.MethodGet, "/family/file/listFiles.action", "folderId=-11&pageNum=1", "")
	require.NoError(t, err, "and the first one now signs with the key the second one minted")
	exchanges, _, _ = drive.counts()
	require.Equal(t, 2, exchanges, "no third login")
}

// TestNoRefreshOnARejectedFamilySessionFailsAtOnce: with no_refresh the remote must
// not touch the login, and it must say so rather than look like a transient fault.
func TestNoRefreshOnARejectedFamilySessionFailsAtOnce(t *testing.T) {
	drive := &rotatingFamilyDrive{currentKey: "nobody-yet", currentFKey: "nobody-yet-family"}
	f, host := newRotatingFamilyFs(t, drive)
	f.opt.NoRefresh = true

	_, err := f.call(context.Background(), host, http.MethodGet, "/family/file/listFiles.action", "folderId=-11&pageNum=1", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no_refresh")
	require.True(t, fserrors.IsNoRetryError(err), "asking again would only spend requests on the same refusal")

	exchanges, _, _ := drive.counts()
	require.Zero(t, exchanges, "the login was not touched")
}
