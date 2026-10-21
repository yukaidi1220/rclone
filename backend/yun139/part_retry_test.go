package yun139

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/fserrors"
)

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

func httpResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func drainBody(r *http.Request) {
	if r.Body != nil {
		_, _ = io.Copy(io.Discard, r.Body)
	}
}

func readRequestBody(t *testing.T, r *http.Request) []byte {
	t.Helper()
	if r.Body == nil {
		return nil
	}
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read body: %v", err)
	}
	return b
}

func withLowLevelRetries(t *testing.T, n int) context.Context {
	t.Helper()
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = n
	return ctx
}

func stubPartRetryDelay(t *testing.T, d time.Duration) {
	t.Helper()
	old := partRetryDelay
	partRetryDelay = func(int, int) time.Duration { return d }
	t.Cleanup(func() { partRetryDelay = old })
}

func newPartTestFs(t *testing.T, handler http.Handler) *Fs {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return &Fs{
		httpClient:   srv.Client(),
		personalHost: srv.URL,
		hostMu:       &sync.Mutex{},
		tokMu:        &sync.Mutex{},
		auth:         "dG9rZW4=",
		account:      "13800138000",
		opt:          Options{PartSize: 1024, NoMemberCheck: true, UploadConcurrency: 2},
		space:        spacePersonal,
	}
}

// newPartUploadHandler serves the two-part create request and lets failPart
// answer 500 every time. fileId stays empty so a failed upload does not call
// discardUpload, keeping the test focused on the error chain.
func newPartUploadHandler(t *testing.T, failPart int) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/hcy/file/create"):
			drainBody(r)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"code":    "200",
				"data": map[string]any{
					"fileId":   "",
					"fileName": "x.bin",
					"uploadId": "u",
					"partInfos": []map[string]any{
						{"partNumber": 1, "uploadUrl": "http://" + r.Host + "/part/1"},
						{"partNumber": 2, "uploadUrl": "http://" + r.Host + "/part/2"},
					},
				},
			})
		case strings.HasPrefix(r.URL.Path, "/part/"):
			drainBody(r)
			if r.URL.Path == fmt.Sprintf("/part/%d", failPart) {
				http.Error(w, "boom", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/hcy/file/complete"):
			drainBody(r)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"code":    "200",
				"data":    map[string]any{"fileId": ""},
			})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}
}

func TestPutPartRetriesThenSucceeds(t *testing.T) {
	ctx := withLowLevelRetries(t, 3)
	stubPartRetryDelay(t, time.Millisecond)
	const signed = "https://cdn.example/obj?X-Amz-Signature=secret"
	var calls int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		n := atomic.AddInt32(&calls, 1)
		drainBody(r)
		if n == 1 {
			return nil, &neturl.Error{Op: "Put", URL: signed, Err: timeoutErr{}}
		}
		return httpResponse(http.StatusOK, ""), nil
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	if err := f.putPart(ctx, 1, 2, "x.bin", bytes.NewReader([]byte("hello")), 0, 5, signed); err != nil {
		t.Fatalf("putPart: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("requests = %d, want 2", got)
	}
}

func TestPutPartAttemptsFollowLowLevelRetries(t *testing.T) {
	for _, ll := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprintf("ll=%d", ll), func(t *testing.T) {
			ctx := withLowLevelRetries(t, ll)
			stubPartRetryDelay(t, time.Millisecond)
			var calls int32
			rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				atomic.AddInt32(&calls, 1)
				drainBody(r)
				return nil, &neturl.Error{Op: "Put", URL: "https://cdn.example/p", Err: timeoutErr{}}
			})
			f := &Fs{httpClient: &http.Client{Transport: rt}}

			err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, "https://cdn.example/p")
			if err == nil {
				t.Fatal("putPart: want error")
			}
			if got := atomic.LoadInt32(&calls); got != int32(ll) {
				t.Errorf("requests = %d, want %d", got, ll)
			}
			if !fserrors.IsNoLowLevelRetryError(err) {
				t.Errorf("error is not NoLowLevel: %v", err)
			}
			if fserrors.IsNoRetryError(err) {
				t.Errorf("error is NoRetry: %v", err)
			}
			if fserrors.ShouldRetry(err) {
				t.Errorf("ShouldRetry = true for exhausted transport error: %v", err)
			}
		})
	}
}

func TestPutPartStatusClassification(t *testing.T) {
	cases := []struct {
		name           string
		status         int
		body           string
		attempts       int
		wantNoRetry    bool
		wantNoLowLevel bool
	}{
		{"400-permanent", http.StatusBadRequest, "denied", 1, true, true},
		{"400-request-timeout", http.StatusBadRequest, "<Error><Code>RequestTimeout</Code></Error>", 2, false, true},
		{"400-incomplete-body", http.StatusBadRequest, "<Error><Code>IncompleteBody</Code></Error>", 2, false, true},
		{"400-request-expired", http.StatusBadRequest, "<Error><Code>RequestExpired</Code></Error>", 1, false, true},
		{"400-retriable-phrase", http.StatusBadRequest, "transport connection broken", 1, true, true},
		{"401", http.StatusUnauthorized, "denied", 1, false, true},
		{"403", http.StatusForbidden, "denied", 1, false, true},
		{"404", http.StatusNotFound, "denied", 1, false, true},
		{"408", http.StatusRequestTimeout, "denied", 2, false, true},
		{"409", http.StatusConflict, "denied", 1, false, true},
		{"410", http.StatusGone, "denied", 1, false, true},
		{"413", http.StatusRequestEntityTooLarge, "denied", 1, true, true},
		{"429", http.StatusTooManyRequests, "denied", 2, false, true},
		{"500", http.StatusInternalServerError, "denied", 2, false, true},
		{"503", http.StatusServiceUnavailable, "denied", 2, false, true},
		{"302", http.StatusFound, "denied", 1, false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := withLowLevelRetries(t, 2)
			stubPartRetryDelay(t, time.Millisecond)
			var calls int32
			rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
				atomic.AddInt32(&calls, 1)
				drainBody(r)
				resp := httpResponse(c.status, c.body)
				if c.status >= 300 && c.status < 400 {
					// A redirect target must never be followed: 301/302 would
					// turn the PUT into a GET and 307/308 would replay the
					// signature against another host.
					resp.Header.Set("Location", "https://cdn.example/redirected")
				}
				return resp, nil
			})
			f := &Fs{httpClient: &http.Client{Transport: rt}}

			err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, "https://cdn.example/p")
			if err == nil {
				t.Fatal("putPart: want error")
			}
			if got := atomic.LoadInt32(&calls); got != int32(c.attempts) {
				t.Errorf("requests = %d, want %d", got, c.attempts)
			}
			if got := fserrors.IsNoRetryError(err); got != c.wantNoRetry {
				t.Errorf("IsNoRetry = %v, want %v: %v", got, c.wantNoRetry, err)
			}
			if got := fserrors.IsNoLowLevelRetryError(err); got != c.wantNoLowLevel {
				t.Errorf("IsNoLowLevel = %v, want %v: %v", got, c.wantNoLowLevel, err)
			}
			if fserrors.ShouldRetry(err) {
				t.Errorf("ShouldRetry = true: %v", err)
			}
		})
	}
}

func TestPutPartReplaysBodyAtOffset(t *testing.T) {
	ctx := withLowLevelRetries(t, 2)
	stubPartRetryDelay(t, time.Millisecond)
	data := []byte("0123456789ABC")
	var bodies [][]byte
	var lengths []int64
	var calls int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		b := readRequestBody(t, r)
		bodies = append(bodies, b)
		lengths = append(lengths, r.ContentLength)
		if n := atomic.AddInt32(&calls, 1); n == 1 {
			return httpResponse(http.StatusInternalServerError, "boom"), nil
		}
		return httpResponse(http.StatusOK, ""), nil
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	if err := f.putPart(ctx, 3, 3, "x.bin", bytes.NewReader(data), 10, 3, "https://cdn.example/p"); err != nil {
		t.Fatalf("putPart: %v", err)
	}
	if len(bodies) != 2 {
		t.Fatalf("attempts = %d, want 2", len(bodies))
	}
	for i, b := range bodies {
		if string(b) != "ABC" {
			t.Errorf("attempt %d body = %q, want %q", i+1, b, "ABC")
		}
		if lengths[i] != 3 {
			t.Errorf("attempt %d ContentLength = %d, want 3", i+1, lengths[i])
		}
	}
}

func TestPutPartCancelStopsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(withLowLevelRetries(t, 3))
	defer cancel()
	old := partRetryDelay
	partRetryDelay = func(int, int) time.Duration { return time.Hour }
	t.Cleanup(func() { partRetryDelay = old })

	started := make(chan struct{}, 1)
	var calls int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		select {
		case started <- struct{}{}:
		default:
		}
		return nil, &neturl.Error{Op: "Put", URL: r.URL.String(), Err: timeoutErr{}}
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	done := make(chan error, 1)
	go func() {
		done <- f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, "https://cdn.example/p")
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("putPart never sent the first request")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("putPart kept sleeping after cancel")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("requests = %d, want 1", got)
	}
}

func TestPutPartCanceledContextSendsNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(withLowLevelRetries(t, 3))
	cancel()
	var calls int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return httpResponse(http.StatusOK, ""), nil
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, "https://cdn.example/p")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("requests = %d, want 0", got)
	}
}

func TestPutPartInvalidURLAllowsCommandRetry(t *testing.T) {
	ctx := withLowLevelRetries(t, 3)
	stubPartRetryDelay(t, time.Millisecond)
	var calls int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return httpResponse(http.StatusOK, ""), nil
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, "http://[::1")
	if err == nil {
		t.Fatal("putPart: want error")
	}
	if !fserrors.IsNoLowLevelRetryError(err) {
		t.Errorf("invalid URL error is not NoLowLevel: %v", err)
	}
	if fserrors.IsNoRetryError(err) {
		t.Errorf("invalid URL error is NoRetry: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("requests = %d, want 0", got)
	}
}

func TestPutPartEmptyURLAllowsCommandRetry(t *testing.T) {
	ctx := withLowLevelRetries(t, 3)
	var calls int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return httpResponse(http.StatusOK, ""), nil
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, "")
	if err == nil {
		t.Fatal("putPart: want error")
	}
	if !fserrors.IsNoLowLevelRetryError(err) {
		t.Errorf("empty URL error is not NoLowLevel: %v", err)
	}
	if fserrors.IsNoRetryError(err) {
		t.Errorf("empty URL error is NoRetry: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("requests = %d, want 0", got)
	}
}

func TestPutPartTransportErrorHidesSignedURL(t *testing.T) {
	ctx := withLowLevelRetries(t, 2)
	stubPartRetryDelay(t, time.Millisecond)
	const signed = "https://cdn.example/obj?X-Amz-Signature=deadbeef"
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, &neturl.Error{Op: "Put", URL: signed,
			Err: &neturl.Error{Op: "Put", URL: signed, Err: timeoutErr{}}}
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, signed)
	if err == nil {
		t.Fatal("putPart: want error")
	}
	for _, secret := range []string{"X-Amz-Signature", "deadbeef"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks %q: %v", secret, err)
		}
	}
}

func TestPutPartNoRetryForPermanentTransportError(t *testing.T) {
	ctx := withLowLevelRetries(t, 3)
	stubPartRetryDelay(t, time.Millisecond)
	var calls int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		atomic.AddInt32(&calls, 1)
		return nil, &neturl.Error{Op: "Put", URL: "https://cdn.example/p",
			Err: x509.UnknownAuthorityError{}}
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, "https://cdn.example/p")
	if err == nil {
		t.Fatal("putPart: want error")
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("requests = %d, want 1 (permanent transport errors are not retried)", got)
	}
	if !fserrors.IsNoLowLevelRetryError(err) {
		t.Errorf("error is not NoLowLevel: %v", err)
	}
	if fserrors.ShouldRetry(err) {
		t.Errorf("ShouldRetry = true: %v", err)
	}
}

func TestPutPartCapsErrorBody(t *testing.T) {
	ctx := withLowLevelRetries(t, 1)
	stubPartRetryDelay(t, time.Millisecond)
	big := strings.Repeat("A", 10<<10)
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		drainBody(r)
		return httpResponse(http.StatusInternalServerError, big), nil
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, "https://cdn.example/p")
	if err == nil {
		t.Fatal("putPart: want error")
	}
	if !strings.Contains(err.Error(), strings.Repeat("A", 500)) {
		t.Errorf("error body is not truncated to 500 bytes: %q", err.Error())
	}
	if strings.Contains(err.Error(), strings.Repeat("A", 501)) {
		t.Errorf("error body kept more than 500 bytes")
	}
}

func TestPutPartRedactsSignedURLInErrorBody(t *testing.T) {
	ctx := withLowLevelRetries(t, 2)
	stubPartRetryDelay(t, time.Millisecond)
	const signed = "https://cdn.example/obj?X-Amz-Signature=deadbeef&X-Amz-Credential=secret"
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		drainBody(r)
		return httpResponse(http.StatusInternalServerError,
			"failed "+signed+" and X-Amz-Signature=deadbeef2"+
				" encoded X-Amz-Signature%3Ddeadbeef3%26X-Amz-Credential%3Dsecret3"), nil
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, signed)
	if err == nil {
		t.Fatal("putPart: want error")
	}
	for _, secret := range []string{"deadbeef", "secret", "deadbeef2", "deadbeef3", "secret3"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error body leaks %q: %v", secret, err)
		}
	}
	if !strings.Contains(err.Error(), "<part-url>") || !strings.Contains(err.Error(), "<redacted>") {
		t.Errorf("error body was not redacted: %v", err)
	}
}

func TestPutPartHonorsRetryAfter(t *testing.T) {
	ctx := withLowLevelRetries(t, 3)
	stubPartRetryDelay(t, time.Millisecond)
	var sleeps []time.Duration
	oldSleep := partSleep
	partSleep = func(ctx context.Context, d time.Duration) error {
		sleeps = append(sleeps, d)
		return nil
	}
	t.Cleanup(func() { partSleep = oldSleep })

	var calls int32
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		n := atomic.AddInt32(&calls, 1)
		drainBody(r)
		if n == 1 {
			resp := httpResponse(http.StatusTooManyRequests, "slow down")
			resp.Header.Set("Retry-After", "5")
			return resp, nil
		}
		return httpResponse(http.StatusOK, ""), nil
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	if err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, "https://cdn.example/p"); err != nil {
		t.Fatalf("putPart: %v", err)
	}
	if len(sleeps) != 1 {
		t.Fatalf("sleeps = %v, want one", sleeps)
	}
	if sleeps[0] < 5*time.Second {
		t.Errorf("retry delay = %v, want at least the 5s Retry-After", sleeps[0])
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("requests = %d, want 2", got)
	}
}

func TestPutPartRetryAfterSurvivesToCommandLevel(t *testing.T) {
	ctx := withLowLevelRetries(t, 1)
	stubPartRetryDelay(t, time.Millisecond)
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		drainBody(r)
		resp := httpResponse(http.StatusTooManyRequests, "slow down")
		resp.Header.Set("Retry-After", "120")
		return resp, nil
	})
	f := &Fs{httpClient: &http.Client{Transport: rt}}

	err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, "https://cdn.example/p")
	if err == nil {
		t.Fatal("putPart: want error")
	}
	if !fserrors.IsNoLowLevelRetryError(err) {
		t.Errorf("error is not NoLowLevel: %v", err)
	}
	if fserrors.ShouldRetry(err) {
		t.Errorf("ShouldRetry = true: %v", err)
	}
	if !fserrors.IsRetryAfterError(err) {
		t.Fatalf("final error lost Retry-After: %v", err)
	}
	if d := time.Until(fserrors.RetryAfterErrorTime(err)); d < 100*time.Second {
		t.Errorf("final Retry-After = %v, want about 120s (not capped to 30s)", d)
	}
	stats := accounting.NewStats(ctx)
	_ = stats.Error(err)
	if !stats.HadRetryError() {
		t.Error("Stats.HadRetryError = false; --retries would not wait and re-run")
	}
	if d := time.Until(stats.RetryAfter()); d < 100*time.Second {
		t.Errorf("Stats.RetryAfter = %v, want about 120s", d)
	}
}

func TestParseRetryAfter(t *testing.T) {
	if got := parseRetryAfter("5"); got != 5*time.Second {
		t.Errorf("parseRetryAfter(5) = %v, want 5s", got)
	}
	if got := parseRetryAfter(""); got != 0 {
		t.Errorf("parseRetryAfter(empty) = %v, want 0", got)
	}
	if got := parseRetryAfter("soon"); got != 0 {
		t.Errorf("parseRetryAfter(soon) = %v, want 0", got)
	}
	at := time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat)
	if got := parseRetryAfter(at); got < 5*time.Second || got > 15*time.Second {
		t.Errorf("parseRetryAfter(%q) = %v, want about 10s", at, got)
	}
}

func TestPartTransportErrorNilURL(t *testing.T) {
	f := &Fs{}
	const signed = "https://cdn.example/p?X-Amz-Signature=deadbeef"
	err := f.partTransportError(1, 1, "cdn.example", "", &neturl.Error{Op: "Put", URL: signed})
	if err == nil {
		t.Fatal("partTransportError: want error")
	}
	if !fserrors.IsNoLowLevelRetryError(err) {
		t.Errorf("error is not NoLowLevel: %v", err)
	}
	if strings.Contains(err.Error(), "deadbeef") {
		t.Errorf("error leaks the signed URL: %v", err)
	}
}

// slowReaderAt makes the upload body itself slow. A per-attempt deadline
// covering the body would kill it even though the transfer keeps moving.
type slowReaderAt struct {
	data  []byte
	chunk int
	delay time.Duration
}

func (r *slowReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	time.Sleep(r.delay)
	if len(p) > r.chunk {
		p = p[:r.chunk]
	}
	n := copy(p, r.data[off:])
	return n, nil
}

func TestPutPartSlowBodyIsNotKilled(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.LowLevelRetries = 1
	// --timeout is the transport's response-header timeout. It must not
	// bound the upload body: this part needs ~320ms to send.
	ci.Timeout = fs.Duration(100 * time.Millisecond)

	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	const size = 256 << 10
	slow := &slowReaderAt{data: make([]byte, size), chunk: 4 << 10, delay: 5 * time.Millisecond}
	f := &Fs{httpClient: srv.Client()}
	if err := f.putPart(ctx, 1, 1, "x.bin", slow, 0, size, srv.URL); err != nil {
		t.Fatalf("putPart: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("requests = %d, want 1", got)
	}
}

func TestPutPartErrorBodyTimeout(t *testing.T) {
	ctx := withLowLevelRetries(t, 1)
	stubPartRetryDelay(t, time.Millisecond)
	oldTimeout := partErrorBodyTimeout
	partErrorBodyTimeout = 50 * time.Millisecond
	t.Cleanup(func() { partErrorBodyTimeout = oldTimeout })

	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Headers are out, body stalls. The client must not wait for it.
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
		_, _ = w.Write([]byte("late body"))
	}))
	defer srv.Close()

	f := &Fs{httpClient: srv.Client()}
	start := time.Now()
	err := f.putPart(ctx, 1, 1, "x.bin", bytes.NewReader([]byte("x")), 0, 1, srv.URL)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("putPart: want error")
	}
	if elapsed > time.Second {
		t.Errorf("putPart took %v; the error body timeout did not fire", elapsed)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("requests = %d, want 1", got)
	}
}

func TestUploadFromRandomExhaustedPartIsNoLowLevel(t *testing.T) {
	ctx := withLowLevelRetries(t, 2)
	stubPartRetryDelay(t, time.Millisecond)
	var partCalls int32
	base := newPartUploadHandler(t, 2)
	f := newPartTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/part/") {
			atomic.AddInt32(&partCalls, 1)
		}
		base(w, r)
	}))
	// Serialize the parts so part 1 is guaranteed to run before part 2
	// exhausts its retries and cancels the group.
	f.opt.UploadConcurrency = 1

	data := bytes.Repeat([]byte("x"), 2048)
	_, err := f.uploadFromRandom(ctx, bytes.NewReader(data), "dir", "x.bin", int64(len(data)), "00")
	if err == nil {
		t.Fatal("uploadFromRandom: want error")
	}
	if !fserrors.IsNoLowLevelRetryError(err) {
		t.Errorf("error is not NoLowLevel: %v", err)
	}
	if fserrors.IsNoRetryError(err) {
		t.Errorf("error is NoRetry: %v", err)
	}
	if got := atomic.LoadInt32(&partCalls); got != 3 {
		t.Errorf("part requests = %d, want 3 (part 1 once, part 2 twice)", got)
	}

	stats := accounting.NewStats(ctx)
	_ = stats.Error(err)
	if !stats.HadRetryError() {
		t.Error("Stats.HadRetryError = false; --retries would not re-run the command")
	}
}

func TestUploadFromRandomMissingPartURL(t *testing.T) {
	ctx := withLowLevelRetries(t, 2)
	stubPartRetryDelay(t, time.Millisecond)
	var calls int32
	f := newPartTestFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/hcy/file/create"):
			drainBody(r)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success": true,
				"code":    "200",
				"data": map[string]any{
					"fileId":   "",
					"fileName": "x.bin",
					"uploadId": "u",
					"partInfos": []map[string]any{
						{"partNumber": 1, "uploadUrl": "http://" + r.Host + "/part/1"},
					},
				},
			})
		case strings.HasPrefix(r.URL.Path, "/part/"):
			atomic.AddInt32(&calls, 1)
			drainBody(r)
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	f.opt.UploadConcurrency = 1

	data := bytes.Repeat([]byte("x"), 2048)
	_, err := f.uploadFromRandom(ctx, bytes.NewReader(data), "dir", "x.bin", int64(len(data)), "00")
	if err == nil {
		t.Fatal("uploadFromRandom: want error for the missing part URL")
	}
	if !fserrors.IsNoLowLevelRetryError(err) {
		t.Errorf("error is not NoLowLevel: %v", err)
	}
	if fserrors.IsNoRetryError(err) {
		t.Errorf("error is NoRetry: %v", err)
	}
	if fserrors.ShouldRetry(err) {
		t.Errorf("ShouldRetry = true: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("part requests = %d, want 1", got)
	}
}

func TestChunkWriterCloseExhaustedPartIsNoLowLevel(t *testing.T) {
	ctx := withLowLevelRetries(t, 2)
	stubPartRetryDelay(t, time.Millisecond)
	f := newPartTestFs(t, newPartUploadHandler(t, 2))
	f.opt.UploadConcurrency = 1

	const size = 2048
	data := bytes.Repeat([]byte("y"), size)
	tmp, err := os.CreateTemp(t.TempDir(), "stage-")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tmp.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}
	w := &yun139ChunkWriter{
		f:         f,
		tmp:       tmp,
		path:      tmp.Name(),
		dirID:     "dir",
		leaf:      "x.bin",
		size:      size,
		chunkSize: 1024,
		total:     2,
	}
	w.written.Store(size)

	err = w.Close(ctx)
	if err == nil {
		t.Fatal("Close: want error")
	}
	if !fserrors.IsNoLowLevelRetryError(err) {
		t.Errorf("error is not NoLowLevel: %v", err)
	}
	if fserrors.IsNoRetryError(err) {
		t.Errorf("error is NoRetry: %v", err)
	}
}
