// Tests for what happens when a cached wopan download link stops working.
//
// The download CDN answers 519 for a link that has aged out on its own and 403 for one that was
// revoked, and both are worth exactly one refetch - so both directions are locked here: the
// refetch happens, and it does not become a retry loop, because the link API is rate limited and
// a second broken link is an answer rather than a transient.
package wopan

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// linkTransport plays the two roles one fs.httpClient has here: it answers the API POSTs with a
// fresh download URL (counting them) and the download GET/HEAD with programmed statuses (the last
// one repeating), so a test can see exactly how many times a link was fetched.
type linkTransport struct {
	mu         sync.Mutex
	apiCalls   int
	getCalls   int
	headCalls  int
	getStatus  []int
	headStatus []int
	etag       string
}

func (t *linkTransport) next(which []int, n int) int {
	if len(which) == 0 {
		return http.StatusOK
	}
	if n <= len(which) {
		return which[n-1]
	}
	return which[len(which)-1]
}

func (t *linkTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	switch r.Method {
	case http.MethodPost:
		t.apiCalls++
		return okResp(fmt.Sprintf(`{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"0000","RSP_DESC":"ok",`+
			`"DATA":{"type":1,"list":[{"fid":"f1","downloadUrl":"https://download.example/a?k=%d"}]}}}`, t.apiCalls)), nil
	case http.MethodHead:
		t.headCalls++
		st := t.next(t.headStatus, t.headCalls)
		res := &http.Response{
			StatusCode: st, Status: fmt.Sprintf("%d %s", st, http.StatusText(st)),
			Body: io.NopCloser(strings.NewReader("")), Header: http.Header{},
		}
		if st == http.StatusOK && t.etag != "" {
			res.Header.Set("ETag", t.etag)
		}
		return res, nil
	default:
		t.getCalls++
		st := t.next(t.getStatus, t.getCalls)
		body := "hello"
		if st != http.StatusOK {
			body = ""
		}
		return &http.Response{
			StatusCode: st, Status: fmt.Sprintf("%d %s", st, http.StatusText(st)),
			Body: io.NopCloser(strings.NewReader(body)), ContentLength: int64(len(body)),
			Header: http.Header{},
		}, nil
	}
}

// newLinkObject returns an object with no cached link, so the first download URL is one API call.
func newLinkObject(tp *linkTransport) *Object {
	f := newUnitTestFs(tp)
	return &Object{fs: f, remote: "dir/a.bin", id: "id", fid: "f1", size: 5,
		uploadedAt: time.Now().Add(-time.Hour)}
}

func readAll(t *testing.T, rc io.ReadCloser) string {
	t.Helper()
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	return string(b)
}

// TestUnitOpenRefetchesAgedLink: 519 on the first GET must fetch a new link and succeed, costing
// exactly two API calls.
func TestUnitOpenRefetchesAgedLink(t *testing.T) {
	tp := &linkTransport{getStatus: []int{statusLinkExpired, http.StatusOK}}
	o := newLinkObject(tp)
	rc, err := o.Open(context.Background())
	require.NoError(t, err, "a 519 must be self-healed, not surfaced")
	assert.Equal(t, "hello", readAll(t, rc))
	assert.Equal(t, 2, tp.apiCalls, "one link, then one refetch")
	assert.Equal(t, 2, tp.getCalls)
}

// TestUnitOpenRefetchesOnForbidden keeps the 403 path on the same shape as 519, so the two
// statuses a caller may recover from cannot drift apart into one branch that silently stopped
// refetching.
func TestUnitOpenRefetchesOnForbidden(t *testing.T) {
	tp := &linkTransport{getStatus: []int{http.StatusForbidden, http.StatusOK}}
	rc, err := newLinkObject(tp).Open(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "hello", readAll(t, rc))
	assert.Equal(t, 2, tp.apiCalls)
}

// TestUnitOpenDoesNotLoopOnBrokenLink: if the fresh link is rejected too, one error and no third
// attempt - "expired" cannot be fixed by asking again forever, and the API is rate limited.
func TestUnitOpenDoesNotLoopOnBrokenLink(t *testing.T) {
	tp := &linkTransport{getStatus: []int{statusLinkExpired, statusLinkExpired}}
	_, err := newLinkObject(tp).Open(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "519")
	assert.Equal(t, 2, tp.apiCalls, "exactly one refetch")
	assert.Equal(t, 2, tp.getCalls)
}

// TestUnitOpenDoesNotRefetchOnServerFailure: a 500 says nothing about the link, so refetching
// would double the load on a server that is already unhappy.
func TestUnitOpenDoesNotRefetchOnServerFailure(t *testing.T) {
	tp := &linkTransport{getStatus: []int{http.StatusInternalServerError}}
	_, err := newLinkObject(tp).Open(context.Background())
	require.Error(t, err)
	assert.Equal(t, 1, tp.apiCalls, "a 500 must not be treated as a stale link")
	assert.Equal(t, 1, tp.getCalls)
}

// TestUnitHashRefetchesAgedLink: Hash probes with HEAD, so a stale link there is worth the same
// one refetch - and outside the post-upload grace window a stale link is a real error, because
// verify would otherwise read a broken link as a changed file.
func TestUnitHashRefetchesAgedLink(t *testing.T) {
	tp := &linkTransport{headStatus: []int{statusLinkExpired, http.StatusOK}, etag: `"abc"`}
	o := newLinkObject(tp)
	sum, err := o.Hash(context.Background(), hash.MD5)
	require.NoError(t, err)
	assert.Equal(t, "abc", sum)
	assert.Equal(t, 2, tp.apiCalls, "one link, then one refetch after the stale HEAD")
}

// TestUnitHashGraceStillSwallows: the self-heal must not erase the reason the grace window
// exists - a freshly uploaded file is never allowed to make verify delete it.
func TestUnitHashGraceStillSwallows(t *testing.T) {
	tp := &linkTransport{headStatus: []int{statusLinkExpired, statusLinkExpired}}
	o := newLinkObject(tp)
	o.uploadedAt = time.Now() // inside the grace window
	sum, err := o.Hash(context.Background(), hash.MD5)
	assert.NoError(t, err)
	assert.Empty(t, sum)

	// Past the window the same broken link is an error, not silence.
	o2 := newLinkObject(tp)
	_, err = o2.Hash(context.Background(), hash.MD5)
	assert.Error(t, err)
}

// TestUnitStaleLinkStatus is the truth table for the predicate, including the status that is
// deliberately left out: 518 means the URL we built was tampered with, so refetching the same way
// would retry our own bug and hide it.
func TestUnitStaleLinkStatus(t *testing.T) {
	for _, tc := range []struct {
		code int
		want bool
	}{
		{http.StatusForbidden, true},
		{statusLinkExpired, true},
		{518, false}, // forged link: our fault, not the server's clock
		{http.StatusOK, false},
		{http.StatusInternalServerError, false},
		{http.StatusNotFound, false},
	} {
		assert.Equalf(t, tc.want, staleLinkStatus(tc.code), "status %d", tc.code)
	}
}

// TestUnitHeadETagCarriesTheStaleMarker: Hash's retry keys off errors.Is(err, errStaleLink), so a
// test that only checked "some error" would pass even if the marker were dropped - and the caller
// would then treat an aged-out link as a hard read failure while every status assertion here kept
// passing.
func TestUnitHeadETagCarriesTheStaleMarker(t *testing.T) {
	for _, tc := range []struct {
		code int
		want bool
	}{
		{statusLinkExpired, true},
		{http.StatusForbidden, true},
		{http.StatusInternalServerError, false},
		{http.StatusNotFound, false},
	} {
		f := newUnitTestFs(&linkTransport{headStatus: []int{tc.code}})
		_, err := f.headETag(context.Background(), "https://download.example/a")
		require.Errorf(t, err, "status %d must still be an error", tc.code)
		assert.Equalf(t, tc.want, errors.Is(err, errStaleLink), "stale marker for status %d", tc.code)
	}
}
