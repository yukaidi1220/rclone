package cloudreve

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloudreve/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/obscure"
	fsobject "github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seen is one request the fake site answered
type seen struct {
	method  string
	path    string
	query   url.Values
	body    string
	header  http.Header
	attempt int
}

// siteHandler answers one request of the fake site
type siteHandler func(s *site, w http.ResponseWriter, r *http.Request, rec seen)

// site is a fake Cloudreve site that records what it was asked
type site struct {
	t        *testing.T
	srv      *httptest.Server
	mu       sync.Mutex
	requests []seen
	handle   siteHandler
	// probePolicy is what the site answers the write permission probe with, and
	// stands for the storage policy of every folder in the test.
	probePolicy string
	probeID     string
	probeName   string
	probeChild  []api.Policy
	probeCode   int // when non-zero, the policy probe answers with this code
	// missing holds the folder URIs the site does not have, for the case of an
	// upload whose path names folders that are not there yet.
	missing map[string]bool
}

// lacks reports whether the site is without the folder at uri
func (s *site) lacks(uri string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.missing[uri]
}

// lack puts the folder at uri out of the site
func (s *site) lack(uri string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.missing == nil {
		s.missing = map[string]bool{}
	}
	s.missing[uri] = true
}

// have puts the folder at uri back into the site
func (s *site) have(uri string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.missing, uri)
}

// recorded returns every request the site answered, in the order they arrived
func (s *site) recorded() []seen {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]seen(nil), s.requests...)
}

// made returns the URIs of the folders the site was asked to create, in order
func (s *site) made() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var uris []string
	for _, r := range s.requests {
		if r.method != "POST" || r.path != "/api/v4/file/create" {
			continue
		}
		request := new(api.CreateFolderRequest)
		if err := json.Unmarshal([]byte(r.body), request); err != nil {
			continue
		}
		uris = append(uris, request.URI)
	}
	return uris
}

// newSite starts a fake Cloudreve site and returns it with an Fs pointed at it
func newSite(t *testing.T, root string, extra configmap.Simple, handle siteHandler) (*Fs, *site) {
	t.Helper()
	s := &site{t: t, handle: handle, probePolicy: PolicyTypeS3, probeID: "POL-S3", probeName: "direct s3"}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err, "reading the request body")
		rec := seen{
			method:  r.Method,
			path:    r.URL.Path,
			query:   r.URL.Query(),
			body:    string(body),
			header:  r.Header.Clone(),
			attempt: s.count(r.Method, r.URL.Path) + 1,
		}
		s.mu.Lock()
		s.requests = append(s.requests, rec)
		s.mu.Unlock()
		// A one-entry listing is how the backend asks which storage policy a
		// folder is on, so the site answers it the same way for every test.
		if rec.method == "GET" && rec.path == "/api/v4/file" && rec.query.Get("page_size") == "1" {
			if s.probeCode != 0 {
				reply(w, s.probeCode, nil)
				return
			}
			if s.lacks(rec.query.Get("uri")) {
				reply(w, api.CodeParentNotExist, nil)
				return
			}
			reply(w, api.CodeSuccess, api.ListResponse{
				// A real listing answers with the folder itself as its parent, ID
				// included - the shape the policy command reads folder_id from.
				Parent:        api.File{Type: api.FileTypeFolder, ID: s.parentID(), Name: "probe"},
				StoragePolicy: &api.Policy{ID: s.probeID, Name: s.probeName, Type: s.probePolicy, Children: s.probeChild},
			})
			return
		}
		s.handle(s, w, r, rec)
	}))
	t.Cleanup(s.srv.Close)

	m := configmap.Simple{
		"url":                s.srv.URL,
		"access_token":       "SEED",
		"refresh_token":      "SEEDR",
		"upload_concurrency": "2",
	}
	for key, value := range extra {
		if value == "" {
			delete(m, key)
			continue
		}
		m[key] = value
	}
	remote, err := NewFs(context.Background(), "cloudreve-test", root, m)
	// A remote rooted at a file comes back with the sentinel rclone uses to
	// learn the root is a file, alongside the Fs.
	require.True(t, err == nil || errors.Is(err, fs.ErrorIsFile), "making the remote: %v", err)
	return remote.(*Fs), s
}

// parentID is the ID the fake names the probed folder with, the way a real site
// puts the folder itself in the parent of its own listing
func (s *site) parentID() string { return "FOLD-" + s.probeID }

// count returns how many recorded requests have this method and path
func (s *site) count(method, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.requests {
		if r.method == method && r.path == path {
			n++
		}
	}
	return n
}

// pathCount returns how many recorded requests have a path starting with prefix
func (s *site) pathCount(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.requests {
		if strings.HasPrefix(r.path, prefix) {
			n++
		}
	}
	return n
}

// link makes an absolute URL on the fake site
func (s *site) link(p string) string { return s.srv.URL + p }

// token returns what the remote left in its config for one option
func token(f *Fs, key string) string {
	value, _ := f.cfg.Get(key)
	return value
}

func testCtx() context.Context { return context.Background() }

// reply writes the envelope every Cloudreve answer is wrapped in
func reply(w http.ResponseWriter, code int, data interface{}) {
	body := map[string]interface{}{"code": code, "msg": fmt.Sprintf("code %d", code)}
	if data != nil {
		body["data"] = data
	}
	raw, err := json.Marshal(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}

// replyRaw answers outside the envelope, which is how the object store replies
func replyRaw(w http.ResponseWriter, status int, body string) {
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// loginStep answers the two requests rclone makes before it posts credentials,
// and reports whether rec was one of them.
func loginStep(w http.ResponseWriter, rec seen, captcha bool, prepared api.PrepareLogin) bool {
	switch rec.path {
	case "/api/v4/site/config/login":
		reply(w, api.CodeSuccess, api.LoginConfig{LoginCaptcha: captcha})
		return true
	case "/api/v4/session/prepare":
		reply(w, api.CodeSuccess, prepared)
		return true
	}
	return false
}

// TestUnitSignInUsesBearerToken checks that a configured account is signed in,
// that the obscured password is recovered before it goes on the wire, and that
// the issued token is what the API calls carry.
func TestUnitSignInUsesBearerToken(t *testing.T) {
	signedIn := map[string]string{}
	f, _ := newSite(t, "", configmap.Simple{
		"email":    "me@example.com",
		"password": obscure.MustObscure("hunter2"),
	}, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		if loginStep(w, rec, false, api.PrepareLogin{PasswordEnabled: true}) {
			return
		}
		switch {
		case rec.method == "POST" && rec.path == "/api/v4/session/token":
			var request api.LoginRequest
			require.NoError(t, json.Unmarshal([]byte(rec.body), &request))
			signedIn["email"] = request.Email
			signedIn["password"] = request.Password
			reply(w, api.CodeSuccess, api.LoginData{Token: api.Token{
				AccessToken:  "AT1",
				RefreshToken: "RT1",
			}})
		case rec.method == "GET" && rec.path == "/api/v4/file":
			assert.Equal(t, "Bearer AT1", rec.header.Get("Authorization"))
			reply(w, api.CodeSuccess, api.ListResponse{})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	assert.Equal(t, map[string]string{"email": "me@example.com", "password": "hunter2"}, signedIn,
		"the plain password is sent, not the stored obscured form")

	_, err := f.List(testCtx(), "")
	require.NoError(t, err)
	assert.Equal(t, "AT1", token(f, "access_token"))
	assert.Equal(t, "RT1", token(f, "refresh_token"))
}

// TestUnitSignInRetriesADroppedConnection checks that the requests which open a
// session are retried like any other: whatever answers in front of the site drops
// connections, and one lost while signing in must not fail the whole remote.
func TestUnitSignInRetriesADroppedConnection(t *testing.T) {
	f, s := newSite(t, "", configmap.Simple{
		"email":    "me@example.com",
		"password": obscure.MustObscure("hunter2"),
	}, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		if rec.path == "/api/v4/site/config/login" && rec.attempt <= 2 {
			panic(http.ErrAbortHandler) // the connection goes, reply never arrives
		}
		if loginStep(w, rec, false, api.PrepareLogin{PasswordEnabled: true}) {
			return
		}
		switch {
		case rec.method == "POST" && rec.path == "/api/v4/session/token":
			reply(w, api.CodeSuccess, api.LoginData{Token: api.Token{
				AccessToken: "AT9", RefreshToken: "RT9",
			}})
		case rec.method == "GET" && rec.path == "/api/v4/file":
			reply(w, api.CodeSuccess, api.ListResponse{})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	require.Equal(t, "AT9", token(f, "access_token"), "the sign-in finished after the drops")
	assert.Equal(t, 3, s.count("GET", "/api/v4/site/config/login"),
		"two dropped connections were come back from")
}

// TestUnitLoginCaptchaIsExplained checks that a site asking for a login captcha
// fails with a way out instead of a bare error code, and that no credentials are
// posted into a challenge rclone cannot answer.
func TestUnitLoginCaptchaIsExplained(t *testing.T) {
	var posted bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v4/site/config/login":
			reply(w, api.CodeSuccess, api.LoginConfig{LoginCaptcha: true})
		case "/api/v4/session/prepare":
			reply(w, api.CodeSuccess, api.PrepareLogin{PasswordEnabled: true})
		case "/api/v4/session/token":
			posted = true
			reply(w, api.CodeSuccess, nil)
		}
	}))
	t.Cleanup(srv.Close)

	_, err := NewFs(testCtx(), "cloudreve-test", "", configmap.Simple{
		"url":                srv.URL,
		"email":              "me@example.com",
		"password":           obscure.MustObscure("hunter2"),
		"upload_concurrency": "4",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "login captcha")
	assert.Contains(t, err.Error(), "access_token")
	assert.False(t, posted, "rclone does not post credentials to a site that asks for a captcha")
}

// TestUnitSSOAccountIsNamed checks that an account without a local password is
// reported as such, since the server otherwise answers with the same code it
// uses for a wrong password.
func TestUnitSSOAccountIsNamed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v4/site/config/login":
			reply(w, api.CodeSuccess, api.LoginConfig{})
		case "/api/v4/session/prepare":
			reply(w, api.CodeSuccess, api.PrepareLogin{PasswordEnabled: false})
		default:
			reply(w, api.CodeInvalidPassword, nil)
		}
	}))
	t.Cleanup(srv.Close)

	_, err := NewFs(testCtx(), "cloudreve-test", "", configmap.Simple{
		"url":                srv.URL,
		"email":              "me@example.com",
		"password":           obscure.MustObscure("hunter2"),
		"upload_concurrency": "4",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no local password")
	assert.Contains(t, err.Error(), "access_token")
}

// TestUnitWrongPasswordIsNotOverExplained checks that a real credential failure
// is reported as a rejected pair rather than as a missing local password.
func TestUnitWrongPasswordIsNotOverExplained(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v4/site/config/login":
			reply(w, api.CodeSuccess, api.LoginConfig{})
		case "/api/v4/session/prepare":
			reply(w, api.CodeSuccess, api.PrepareLogin{PasswordEnabled: true})
		default:
			reply(w, api.CodeInvalidPassword, nil)
		}
	}))
	t.Cleanup(srv.Close)

	_, err := NewFs(testCtx(), "cloudreve-test", "", configmap.Simple{
		"url":                srv.URL,
		"email":              "me@example.com",
		"password":           obscure.MustObscure("wrong"),
		"upload_concurrency": "4",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rejected this email and password pair")
	assert.NotContains(t, err.Error(), "no local password")
}

// TestUnitRenewsSpentSession checks that an expired session is renewed once, and
// that the renewal does not send the dead token.
func TestUnitRenewsSpentSession(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "POST" && rec.path == "/api/v4/session/token/refresh":
			assert.Empty(t, rec.header.Get("Authorization"), "the expired token must not be sent")
			var request api.RefreshRequest
			require.NoError(t, json.Unmarshal([]byte(rec.body), &request))
			assert.Equal(t, "SEEDR", request.RefreshToken)
			reply(w, api.CodeSuccess, api.Token{AccessToken: "AT2", RefreshToken: "RT2"})
		case rec.method == "GET" && rec.path == "/api/v4/file":
			if rec.header.Get("Authorization") != "Bearer AT2" {
				reply(w, api.CodeCheckLogin, nil)
				return
			}
			reply(w, api.CodeSuccess, api.ListResponse{})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	_, err := f.List(testCtx(), "")
	require.NoError(t, err)
	assert.Equal(t, 1, s.count("POST", "/api/v4/session/token/refresh"), "the session is renewed once")
	assert.Equal(t, 2, s.count("GET", "/api/v4/file"), "the call is repeated after renewing")
	assert.Equal(t, "AT2", token(f, "access_token"), "the rotated pair is saved")
	assert.Equal(t, "RT2", token(f, "refresh_token"), "a spent refresh_token is replaced")
}

// TestUnitCannotRenewWithoutCredential checks the message when a hand-set token
// has expired with nothing to renew it.
func TestUnitCannotRenewWithoutCredential(t *testing.T) {
	f, _ := newSite(t, "", configmap.Simple{"refresh_token": ""}, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		reply(w, api.CodeCheckLogin, nil)
	})
	_, err := f.List(testCtx(), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no refresh_token")
}

// TestUnitMapsNotFoundAnswers checks the two not-found sentinels.
func TestUnitMapsNotFoundAnswers(t *testing.T) {
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch rec.path {
		case "/api/v4/file/info":
			reply(w, api.CodeNotFound, nil)
		case "/api/v4/file":
			reply(w, api.CodeParentNotExist, nil)
		default:
			reply(w, api.CodeNotFound, nil)
		}
	})
	_, err := f.NewObject(testCtx(), "missing.txt")
	assert.True(t, errors.Is(err, fs.ErrorObjectNotFound), "a missing file is ErrorObjectNotFound, got %v", err)
	_, err = f.List(testCtx(), "gone")
	assert.True(t, errors.Is(err, fs.ErrorDirNotFound), "a missing folder is ErrorDirNotFound, got %v", err)
}

// TestUnitRmdirAndPurgeOfRemoteRoot checks that a remote rooted at a folder can
// have that folder removed: rclone puts the path in the Fs root and passes an
// empty directory, so refusing an empty directory would refuse the real target.
func TestUnitRmdirAndPurgeOfRemoteRoot(t *testing.T) {
	var deleted []string
	f, _ := newSite(t, "gone", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "GET" && rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFolder, ID: "D9", Name: "gone", Path: "cloudreve://my/gone"})
		case rec.method == "GET" && rec.path == "/api/v4/file":
			reply(w, api.CodeSuccess, api.ListResponse{})
		case rec.method == "DELETE" && rec.path == "/api/v4/file":
			request := new(api.DeleteRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			deleted = append(deleted, request.Uris...)
			reply(w, api.CodeSuccess, nil)
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	require.NoError(t, f.Rmdir(testCtx(), ""))
	require.NoError(t, f.Purge(testCtx(), ""))
	assert.Equal(t, []string{"cloudreve://my/gone", "cloudreve://my/gone"}, deleted)
}

// TestUnitLockConflictFailsFast checks that a lock held by an abandoned upload
// session is reported once with the reason, not retried: the lock lasts until the
// server's session timeout, a day by default, so no run can wait it out.
func TestUnitLockConflictFailsFast(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		reply(w, api.CodeLockConflict, nil)
	})
	_, err := f.List(testCtx(), "locked")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upload_session_timeout", "the error names what is holding the lock")
	assert.Equal(t, 1, s.count("GET", "/api/v4/file"), "a lock conflict is answered, not retried")
}

// TestUnitOtherBusinessCodesAreNotRetried checks that a refusal is an answer,
// not a fault: repeating it only spends requests.
func TestUnitOtherBusinessCodesAreNotRetried(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		reply(w, api.CodeNoPermission, nil)
	})
	_, err := f.List(testCtx(), "forbidden")
	require.Error(t, err)
	assert.Equal(t, 1, s.count("GET", "/api/v4/file"), "a refusal is asked once")
}

// TestUnitLocationPrefixesThePath checks that location is the folder the remote's
// paths sit under, so a path given on the command line stays relative to it.
func TestUnitLocationPrefixesThePath(t *testing.T) {
	f, _ := newSite(t, "given", configmap.Simple{"location": "base"}, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		require.Equal(t, "cloudreve://my/base/given", rec.query.Get("uri"))
		reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFolder, ID: "DL", Name: "given", Path: "cloudreve://my/base/given"})
	})
	assert.Equal(t, "base/given", f.root)
}

// TestUnitEncodingRoundTrip checks that the characters the server refuses with
// 40052 are stored in their encoded form and read back as the name rclone asked
// for.
func TestUnitEncodingRoundTrip(t *testing.T) {
	var asked string
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "GET" && rec.path == "/api/v4/file":
			asked = rec.query.Get("uri")
			reply(w, api.CodeSuccess, api.ListResponse{Files: []api.File{{
				Type: api.FileTypeFile, ID: "FE", Name: `a＂b？c＼d`, Size: 3,
				Path: "cloudreve://my/dir/a%EF%BC%82b%EF%BC%9Fc%EF%BC%BCd",
			}}})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	entries, err := f.List(testCtx(), "dir")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, `dir/a"b?c\d`, entries[0].Remote(), "the stored name maps back to the rclone name")
	assert.Equal(t, "cloudreve://my/dir", asked, "the folder is addressed by its own name")

	// A write into that name is addressed by the stored spelling, which is what
	// the server accepts.
	assert.Equal(t, "cloudreve://my/dir/a%EF%BC%82b%EF%BC%9Fc%EF%BC%BCd", f.uriFor(`dir/a"b?c\d`),
		"the backslash the server refuses is stored as its fullwidth spelling")
}

// TestUnitMissingRootIsTolerated checks the convention rclone needs: a remote
// pointed at a folder that is not there yet still opens, so that mkdir can create
// it, and opening alone creates nothing.
func TestUnitMissingRootIsTolerated(t *testing.T) {
	var created []string
	f, s := newSite(t, "brand/new", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "GET" && rec.path == "/api/v4/file/info":
			reply(w, api.CodeParentNotExist, nil)
		case rec.method == "POST" && rec.path == "/api/v4/file/create":
			request := new(api.CreateFolderRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			created = append(created, request.URI)
			reply(w, api.CodeSuccess, nil)
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	assert.False(t, f.rootExists, "the root was reported as missing")
	assert.Equal(t, 0, s.count("POST", "/api/v4/file/create"), "opening the remote creates nothing")

	require.NoError(t, f.Mkdir(testCtx(), ""))
	assert.Equal(t, []string{"cloudreve://my/brand/new"}, created, "mkdir creates the root it was pointed at")
}

// TestUnitListPagesAndEncodesURI checks pagination and the URI the server is
// asked for.
func TestUnitListPagesAndEncodesURI(t *testing.T) {
	modTime := time.Date(2026, 10, 13, 9, 8, 7, 0, time.UTC)
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		require.Equal(t, "GET", rec.method)
		require.Equal(t, "/api/v4/file", rec.path)
		switch rec.attempt {
		case 1:
			assert.Equal(t, "cloudreve://my/sub%20dir/x%25y", rec.query.Get("uri"))
			assert.Equal(t, "1000", rec.query.Get("page_size"))
			assert.Equal(t, "name", rec.query.Get("order_by"))
			reply(w, api.CodeSuccess, api.ListResponse{
				Files: []api.File{{
					Type: api.FileTypeFile, ID: "F1", Name: "a.bin", Size: 7,
					UpdatedAt: modTime, Path: "cloudreve://my/sub%20dir/x%25y/a.bin",
				}},
				Pagination:    &api.Pagination{NextToken: "PAGE2"},
				StoragePolicy: &api.Policy{Type: PolicyTypeS3},
			})
		case 2:
			assert.Equal(t, "PAGE2", rec.query.Get("next_page_token"))
			reply(w, api.CodeSuccess, api.ListResponse{
				Files: []api.File{{
					Type: api.FileTypeFolder, ID: "D1", Name: "nested",
					UpdatedAt: modTime, Path: "cloudreve://my/sub%20dir/x%25y/nested",
				}},
			})
		default:
			s.t.Fatalf("a third page was asked for")
		}
	})

	entries, err := f.List(testCtx(), "sub dir/x%y")
	require.NoError(t, err)
	require.Len(t, entries, 2)

	o, ok := entries[0].(*Object)
	require.True(t, ok, "the first entry is a file")
	assert.Equal(t, "sub dir/x%y/a.bin", o.Remote())
	assert.Equal(t, int64(7), o.Size())
	assert.True(t, modTime.Equal(o.ModTime(testCtx())), "mod time comes from updated_at")
	assert.Equal(t, "F1", o.ID())

	d, ok := entries[1].(fs.Directory)
	require.True(t, ok, "the second entry is a folder")
	assert.Equal(t, "sub dir/x%y/nested", d.Remote())

	// The policy seen while listing is remembered, so a later write need not ask
	// for it again.
	f.mu.Lock()
	policy, found := f.policies["sub dir/x%y"]
	f.mu.Unlock()
	require.True(t, found)
	assert.Equal(t, PolicyTypeS3, policy.Type)
	assert.Equal(t, 2, s.count("GET", "/api/v4/file"), "two pages and no more")
}

// TestUnitListRejectsUnusableName checks that a name which cannot be one path
// segment is refused instead of silently re-arranging the tree.
func TestUnitListRejectsUnusableName(t *testing.T) {
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		reply(w, api.CodeSuccess, api.ListResponse{Files: []api.File{{
			Name: "bad/name", Type: api.FileTypeFile, Path: "cloudreve://my/bad/name",
		}}})
	})
	_, err := f.List(testCtx(), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unusable name")
}

// TestUnitRefusesNonSPolicy is the scope red line: another policy is not written
// to, and no upload session is created for it.
func TestUnitRefusesNonSPolicy(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		s.t.Fatalf("wrote into a folder on the local policy: %s %s", rec.method, rec.path)
	})
	s.probePolicy = "local"
	src := fsobject.NewStaticObjectInfo("loc/f.txt", time.Now(), 7, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"local"`)
	assert.Equal(t, 0, s.pathCount("/api/v4/file/upload"), "no session is created")
}

// poolChildren is the child set of the pool the tests point folders at.
var poolChildren = []api.Policy{
	{ID: "POL-HK", Name: "silver-hk-08", Type: PolicyTypeS3},
	{ID: "POL-DE", Name: "silver-de-04", Type: PolicyTypeS3},
}

// TestUnitPoolUploadNamesAChild checks that a folder on a load_balance pool is
// written by naming one of its S3 children in the upload session, which is what
// the site's own client does.
func TestUnitPoolUploadNamesAChild(t *testing.T) {
	var named string
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			request := new(api.CreateSessionRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			named = request.PolicyID
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID: "SP1", ChunkSize: 0,
				UploadURLs:     []string{s.link("/store/part?n=1")},
				URI:            request.URI,
				CallbackSecret: "SECP",
				StoragePolicy:  &api.Policy{ID: named, Type: PolicyTypeS3},
			})
		case rec.path == "/store/part":
			w.Header().Set("ETag", `"e1"`)
			replyRaw(w, http.StatusOK, "")
		case rec.method == "GET" && rec.path == "/api/v4/callback/s3/SP1/SECP":
			reply(w, api.CodeSuccess, nil)
		case rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFile, ID: "FP", Size: 4, Path: "cloudreve://my/f.txt"})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	s.probePolicy = policyTypeLoadBalance
	s.probeID = "POOL"
	s.probeName = "随机存储策略"
	s.probeChild = poolChildren

	src := fsobject.NewStaticObjectInfo("f.txt", time.Now(), 4, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("data"), src)
	require.NoError(t, err)
	assert.Contains(t, []string{"POL-HK", "POL-DE"}, named, "an S3 child of the pool is named")
	assert.Equal(t, 1, s.count("GET", "/api/v4/file"), "the pool is read once and remembered")
}

// TestUnitPoolWithoutS3IsRefused checks that a pool with nothing rclone can
// write through is named in the error, children and all.
func TestUnitPoolWithoutS3IsRefused(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		s.t.Fatalf("an upload was attempted through a pool with no S3 child: %s %s", rec.method, rec.path)
	})
	s.probePolicy = policyTypeLoadBalance
	s.probeID = "POOL"
	s.probeName = "随机存储策略"
	s.probeChild = []api.Policy{
		{ID: "POL-COS", Name: "tencent", Type: "cos"},
		{ID: "POL-OSS", Name: "aliyun", Type: "oss"},
	}

	src := fsobject.NewStaticObjectInfo("f.txt", time.Now(), 4, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("data"), src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "随机存储策略")
	assert.Contains(t, err.Error(), "tencent(cos), aliyun(oss)")
}

// TestUnitSessionPolicyOverrideIsRefused checks that when the site lands the
// upload on a policy rclone cannot write through, the session is abandoned
// instead of pushing bytes into it.
func TestUnitSessionPolicyOverrideIsRefused(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID:  "SP2",
				UploadURLs: []string{s.link("/store/part?n=1")},
				URI:        "cloudreve://my/f.txt", CallbackSecret: "SECR",
				StoragePolicy: &api.Policy{ID: "POL-OTHER", Type: "onedrive"},
			})
		case rec.method == "DELETE" && rec.path == "/api/v4/file/upload":
			assert.Contains(t, rec.body, "SP2")
			reply(w, api.CodeSuccess, nil)
		case rec.path == "/store/part":
			s.t.Fatalf("bytes were pushed into a policy rclone cannot write through")
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	s.probePolicy = policyTypeLoadBalance
	s.probeChild = poolChildren

	src := fsobject.NewStaticObjectInfo("f.txt", time.Now(), 4, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("data"), src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `landed on storage policy "onedrive"`)
	assert.Equal(t, 1, s.count("DELETE", "/api/v4/file/upload"), "the session is abandoned")
}

// TestUnitUploadUsesPresignedParts checks the whole S3 upload path: the session
// request, each part's size and lack of credentials, the parts landing out of
// order, the completion document still naming them in order, the callback, and
// the object that comes back.
func TestUnitUploadUsesPresignedParts(t *testing.T) {
	var (
		partsMu   sync.Mutex
		partSizes = map[string]int{}
		partAuth  []string
		landed    []string
		others    int
		completed string
		modTime   = time.Date(2026, 10, 12, 1, 2, 3, 0, time.UTC)
		// The first part is held until the two after it have been answered, so
		// the store sees them finish 2, 3, 1 whatever the scheduler does.
		laterPartsUp = make(chan struct{})
		releaseOnce  sync.Once
	)
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			request := new(api.CreateSessionRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			assert.Equal(t, "cloudreve://my/dir/f.txt", request.URI)
			assert.Equal(t, int64(10), request.Size)
			assert.Equal(t, modTime.UnixMilli(), request.LastModified, "the server reads last_modified as milliseconds")
			assert.Equal(t, "POL-S3", request.PolicyID, "the policy the bytes land on is named")
			assert.Empty(t, request.EntityType, "a new file needs no entity type")
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID:   "S1",
				ChunkSize:   4,
				UploadURLs:  []string{s.link("/store/part?n=1"), s.link("/store/part?n=2"), s.link("/store/part?n=3")},
				CompleteURL: s.link("/store/complete"),
				URI:         request.URI, CallbackSecret: "SEC",
			})
		case rec.path == "/store/part":
			// Parts arrive on the concurrency the backend chooses, so what the
			// site saw has to be kept under a lock.
			number := rec.query.Get("n")
			if number == "1" {
				<-laterPartsUp
			}
			partsMu.Lock()
			partSizes[number] = len(rec.body)
			partAuth = append(partAuth, rec.header.Get("Authorization"))
			// Recorded as the answer goes out, so this is the order the parts
			// finished in rather than the order they were asked for.
			landed = append(landed, number)
			if number != "1" {
				others++
				if others == 2 {
					releaseOnce.Do(func() { close(laterPartsUp) })
				}
			}
			partsMu.Unlock()
			w.Header().Set("ETag", fmt.Sprintf("%q", "etag-"+number))
			replyRaw(w, http.StatusOK, "")
		case rec.method == "POST" && rec.path == "/store/complete":
			assert.Equal(t, "application/xml", rec.header.Get("Content-Type"))
			partsMu.Lock()
			completed = rec.body
			partsMu.Unlock()
			replyRaw(w, http.StatusOK, "<CompleteMultipartUploadOutput/>")
		case rec.method == "GET" && rec.path == "/api/v4/callback/s3/S1/SEC":
			assert.Equal(t, "Bearer SEED", rec.header.Get("Authorization"), "the callback is an API call")
			reply(w, api.CodeSuccess, nil)
		case rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{
				Type: api.FileTypeFile, ID: "F9", Name: "f.txt", Size: 10,
				UpdatedAt: modTime, Path: "cloudreve://my/dir/f.txt",
			})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	src := fsobject.NewStaticObjectInfo("dir/f.txt", modTime, 10, false, nil, nil)
	o, err := f.Put(testCtx(), strings.NewReader("0123456789"), src)
	require.NoError(t, err)
	assert.Equal(t, "F9", o.(*Object).ID())
	assert.Equal(t, int64(10), o.Size())
	assert.True(t, modTime.Equal(o.ModTime(testCtx())))

	// 10 bytes in parts of 4 is 4, 4 and 2.
	partsMu.Lock()
	gotSizes := maps.Clone(partSizes)
	gotAuth := append([]string(nil), partAuth...)
	gotLanded := append([]string(nil), landed...)
	gotCompleted := completed
	partsMu.Unlock()
	assert.Equal(t, map[string]int{"1": 4, "2": 4, "3": 2}, gotSizes)
	require.Len(t, gotLanded, 3)
	assert.Equal(t, "1", gotLanded[2], "the parts finished out of order, so ordering the completion is not luck")
	require.Len(t, gotAuth, 3)
	for _, auth := range gotAuth {
		assert.Empty(t, auth, "a presigned part URL must not carry the session token")
	}
	require.NotEmpty(t, gotCompleted)
	// Read the document back rather than matching on the bytes, so the
	// assertion is about the part list and not about XML escaping.
	var document completeXML
	require.NoError(t, xml.Unmarshal([]byte(gotCompleted), &document))
	assert.Equal(t, []partXML{
		{PartNumber: 1, ETag: `"etag-1"`},
		{PartNumber: 2, ETag: `"etag-2"`},
		{PartNumber: 3, ETag: `"etag-3"`},
	}, document.Parts, "parts reach the store in order, quoted as it answered them")
	assert.Equal(t, 1, s.count("GET", "/api/v4/callback/s3/S1/SEC"), "the site is told the file is ready")
}

// TestUnitUploadOverwriteAsVersion checks that writing over an existing file is
// retried as a version entity, which is how the server accepts an overwrite.
func TestUnitUploadOverwriteAsVersion(t *testing.T) {
	var entityTypes []string
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			request := new(api.CreateSessionRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			entityTypes = append(entityTypes, request.EntityType)
			if request.EntityType == "" {
				reply(w, api.CodeObjectExist, nil)
				return
			}
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID: "S2", ChunkSize: 0,
				UploadURLs: []string{s.link("/store/part?n=1")},
				URI:        request.URI, CallbackSecret: "SEC2",
			})
		case rec.path == "/store/part":
			assert.Equal(t, 7, len(rec.body), "chunk_size 0 means the whole file in one part")
			w.Header().Set("ETag", `"e"`)
			replyRaw(w, http.StatusOK, "")
		case rec.method == "GET" && rec.path == "/api/v4/callback/s3/S2/SEC2":
			reply(w, api.CodeSuccess, nil)
		case rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFile, ID: "F1", Size: 7, Path: "cloudreve://my/dir/f.txt"})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	src := fsobject.NewStaticObjectInfo("dir/f.txt", time.Now(), 7, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.NoError(t, err)
	assert.Equal(t, []string{"", "version"}, entityTypes, "the overwrite is retried once as a version")
	assert.Equal(t, 0, s.count("POST", "/store/complete"), "no complete URL means nothing to finalise at the store")
}

// TestUnitUploadAbandonsFailedSession checks that a part which will not go leaves
// no session behind.
func TestUnitUploadAbandonsFailedSession(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID: "S3", ChunkSize: 4,
				UploadURLs: []string{s.link("/store/part?n=1"), s.link("/store/part?n=2")},
				URI:        "cloudreve://my/dir/f.txt", CallbackSecret: "SEC3",
			})
		case rec.path == "/store/part":
			replyRaw(w, http.StatusBadRequest, "<Error><Code>SignatureDoesNotMatch</Code></Error>")
		case rec.method == "DELETE" && rec.path == "/api/v4/file/upload":
			assert.Contains(t, rec.body, "S3")
			reply(w, api.CodeSuccess, nil)
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	src := fsobject.NewStaticObjectInfo("dir/f.txt", time.Now(), 8, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("0123456789ab"), src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SignatureDoesNotMatch")
	assert.Equal(t, 1, s.count("DELETE", "/api/v4/file/upload"), "the session is abandoned")
}

// TestUnitRejectsStream checks that an upload of unknown size is refused without
// asking the site for anything.
func TestUnitRejectsStream(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		s.t.Fatalf("nothing should be asked of the site: %s %s", rec.method, rec.path)
	})

	stream := fsobject.NewStaticObjectInfo("dir/f.txt", time.Now(), -1, true, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("x"), stream)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "streaming is not supported")
	assert.Equal(t, 0, len(s.requests), "a refused upload asks for nothing")
}

// TestUnitEmptyFileGoesThroughTheSession checks that a zero byte file is uploaded
// like any other, because that is the only path carrying last_modified - the
// create endpoint would stamp the server's own time instead.
func TestUnitEmptyFileGoesThroughTheSession(t *testing.T) {
	modTime := time.Date(2001, 2, 3, 4, 5, 6, 499000000, time.UTC)
	var asked string
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			request := new(api.CreateSessionRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			asked = request.URI
			assert.Equal(t, int64(0), request.Size)
			assert.Equal(t, modTime.UnixMilli(), request.LastModified, "the empty file keeps its modified time")
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID: "SZ", ChunkSize: 157286400,
				UploadURLs: []string{s.link("/store/part?n=1")},
				URI:        request.URI, CallbackSecret: "SECZ",
			})
		case rec.path == "/store/part":
			assert.Equal(t, 0, len(rec.body), "one empty part")
			w.Header().Set("ETag", `"ez"`)
			replyRaw(w, http.StatusOK, "")
		case rec.method == "GET" && rec.path == "/api/v4/callback/s3/SZ/SECZ":
			reply(w, api.CodeSuccess, nil)
		case rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFile, ID: "FZ", Name: "e.txt", Size: 0, UpdatedAt: modTime, Path: "cloudreve://my/dir/e.txt"})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	empty := fsobject.NewStaticObjectInfo("dir/e.txt", modTime, 0, false, nil, nil)
	o, err := f.Put(testCtx(), strings.NewReader(""), empty)
	require.NoError(t, err)
	assert.Equal(t, "cloudreve://my/dir/e.txt", asked)
	assert.Equal(t, int64(0), o.Size())
	assert.Equal(t, 0, s.count("POST", "/api/v4/file/create"), "the create endpoint is not used")
}

// TestUnitDeleteSoftDeleteFlag checks the delete body carries the configured
// choice.
func TestUnitDeleteSoftDeleteFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		skip string
		want bool
	}{{"soft delete by default", "", false}, {"permanent when asked", "true", true}} {
		t.Run(tc.name, func(t *testing.T) {
			var extra configmap.Simple
			if tc.skip != "" {
				extra = configmap.Simple{"skip_soft_delete": tc.skip}
			}
			f, s := newSite(t, "", extra, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
				require.Equal(t, "DELETE", rec.method)
				require.Equal(t, "/api/v4/file", rec.path)
				assert.Equal(t, "Bearer SEED", rec.header.Get("Authorization"), "delete is an API call")
				request := new(api.DeleteRequest)
				require.NoError(t, json.Unmarshal([]byte(rec.body), request))
				assert.Equal(t, []string{"cloudreve://my/dir/f%20x.txt"}, request.Uris)
				assert.Equal(t, tc.want, request.SkipSoftDelete)
				reply(w, api.CodeSuccess, nil)
			})
			o := &Object{fs: f, remote: "dir/f x.txt", id: "F1", size: 3}
			require.NoError(t, o.Remove(testCtx()))
			assert.Equal(t, 1, s.count("DELETE", "/api/v4/file"))
		})
	}
}

// TestUnitDownloadLinkIsCachedAndReminted checks the link cache and the single
// retry when a stored link has stopped working.
func TestUnitDownloadLinkIsCachedAndReminted(t *testing.T) {
	future := time.Now().Add(time.Hour)
	failNext := false
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "POST" && rec.path == "/api/v4/file/url":
			request := new(api.FileURLRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			assert.Equal(t, []string{"cloudreve://my/f.txt"}, request.Uris)
			reply(w, api.CodeSuccess, api.FileURLData{
				URLs:    []api.FileURL{{URL: s.link("/store/content")}},
				Expires: &future,
			})
		case rec.path == "/store/content":
			assert.Empty(t, rec.header.Get("Authorization"), "the store link carries its own credentials")
			if failNext {
				failNext = false
				replyRaw(w, http.StatusForbidden, "expired")
				return
			}
			body := "content"
			if rng := rec.header.Get("Range"); rng != "" {
				assert.Equal(t, "bytes=0-2", rng, "rclone's range options reach the store")
				var start, end int64
				_, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end)
				require.NoError(t, err)
				body = body[start : end+1]
			}
			replyRaw(w, http.StatusOK, body)
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	o := &Object{fs: f, remote: "f.txt", id: "F1", size: 7}

	read := func() string {
		rc, err := o.Open(testCtx(), &fs.RangeOption{Start: 0, End: 2})
		require.NoError(t, err)
		defer func() { _ = rc.Close() }()
		data, err := io.ReadAll(rc)
		require.NoError(t, err)
		return string(data)
	}
	assert.Equal(t, "con", read())
	assert.Equal(t, "con", read())
	assert.Equal(t, 1, s.count("POST", "/api/v4/file/url"), "one link is reused")

	failNext = true
	assert.Equal(t, "con", read())
	assert.Equal(t, 2, s.count("POST", "/api/v4/file/url"), "a refused link is replaced once")
}

// TestUnitUploadOfEmptyFileStatesItsLength checks the one part of a zero-byte
// upload on the wire: the store has to be told the length is zero rather than
// left to read a chunked body, which some object stores answer with HTTP 400
// "Content-Length is nil".
func TestUnitUploadOfEmptyFileStatesItsLength(t *testing.T) {
	var seenLength int64 = -1
	var seenEncoding []string
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID: "S0", ChunkSize: 0,
				UploadURLs: []string{s.link("/store/empty")},
				URI:        driveRoot + "empty.txt", CallbackSecret: "SEC0",
			})
		case rec.path == "/store/empty":
			seenLength = r.ContentLength
			seenEncoding = append([]string(nil), r.TransferEncoding...)
			w.Header().Set("ETag", `"empty"`)
			replyRaw(w, http.StatusOK, "")
		case rec.method == "GET" && rec.path == "/api/v4/callback/s3/S0/SEC0":
			reply(w, api.CodeSuccess, nil)
		case rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{
				Type: api.FileTypeFile, ID: "F0", Name: "empty.txt", Size: 0,
				Path: driveRoot + "empty.txt",
			})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	src := fsobject.NewStaticObjectInfo("empty.txt", time.Now(), 0, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader(""), src)
	require.NoError(t, err)
	assert.Equal(t, int64(0), seenLength, "the store is told the part is empty by length")
	assert.Empty(t, seenEncoding, "an empty part is not sent chunked")
}

// TestUnitReadRetriesADroppedConnection checks that a connection cut before the
// reply is retried instead of failing the read, and that the download link itself
// is not re-minted for it.
func TestUnitReadRetriesADroppedConnection(t *testing.T) {
	future := time.Now().Add(time.Hour)
	drop := true
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "POST" && rec.path == "/api/v4/file/url":
			reply(w, api.CodeSuccess, api.FileURLData{
				URLs: []api.FileURL{{URL: s.link("/store/content")}}, Expires: &future})
		case rec.path == "/store/content":
			if drop {
				drop = false
				// Hanging up mid-request is what the proxy in front of the site does
				// when it drops a connection: the client sees no reply at all.
				panic(http.ErrAbortHandler)
			}
			replyRaw(w, http.StatusOK, "content")
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	o := &Object{fs: f, remote: "f.txt", id: "F1", size: 7}
	rc, err := o.Open(testCtx())
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, "content", string(data))
	assert.Equal(t, 2, s.count("GET", "/store/content"), "the dropped connection was retried")
	assert.Equal(t, 1, s.count("POST", "/api/v4/file/url"), "one link served both attempts")
}

// TestUnitMkdirAcceptsExistingFolder checks that an existing folder of the same
// name is not an error, while a file in the way is.
func TestUnitMkdirAcceptsExistingFolder(t *testing.T) {
	t.Run("folder", func(t *testing.T) {
		f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
			switch rec.path {
			case "/api/v4/file/create":
				reply(w, api.CodeObjectExist, nil)
			case "/api/v4/file/info":
				reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFolder})
			}
		})
		require.NoError(t, f.Mkdir(testCtx(), "here"))
	})
	t.Run("file in the way", func(t *testing.T) {
		f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
			switch rec.path {
			case "/api/v4/file/create":
				reply(w, api.CodeConflict, nil)
			case "/api/v4/file/info":
				reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFile})
			}
		})
		err := f.Mkdir(testCtx(), "here")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "a file is in the way")
	})
}

// TestUnitRmdirRequiresEmptyFolder checks the empty-directory rule.
func TestUnitRmdirRequiresEmptyFolder(t *testing.T) {
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "GET" && rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFolder})
		case rec.method == "GET" && rec.path == "/api/v4/file":
			reply(w, api.CodeSuccess, api.ListResponse{Files: []api.File{{Name: "inside", Type: api.FileTypeFile}}})
		default:
			s.t.Fatalf("a folder with contents was deleted: %s %s", rec.method, rec.path)
		}
	})
	err := f.Rmdir(testCtx(), "dir")
	assert.True(t, errors.Is(err, fs.ErrorDirectoryNotEmpty), "got %v", err)
}

// TestUnitDirMoveUsesTheFolderEndpoint checks the server-side folder move against
// the endpoint that takes only a destination folder and keeps the entry's own
// name, including the extra rename when the destination names it differently.
//
// A file is never moved server-side: the server stamps the entry with the time of
// the move and no API gives the modified time back, so the Fs has no Move or Copy.
func TestUnitDirMoveUsesTheFolderEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		dst      string
		wantName string
	}{
		{"move keeps the name", "other/src", ""},
		{"move to a new name", "other/renamed", "renamed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var moved, renamed []string
			f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
				switch {
				case rec.method == "POST" && rec.path == "/api/v4/file/move":
					moved = append(moved, rec.body)
					reply(w, api.CodeSuccess, nil)
				case rec.method == "POST" && rec.path == "/api/v4/file/rename":
					renamed = append(renamed, rec.body)
					reply(w, api.CodeSuccess, nil)
				case rec.method == "GET" && rec.path == "/api/v4/file/info":
					if rec.attempt == 1 {
						reply(w, api.CodeParentNotExist, nil) // the destination is free
						return
					}
					reply(w, api.CodeSuccess, api.File{
						Type: api.FileTypeFolder, ID: "D1", Name: path.Base(tc.dst),
						Path: "cloudreve://my/" + tc.dst,
					})
				default:
					s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
				}
			})
			require.NoError(t, f.DirMove(testCtx(), f, "src", tc.dst))

			require.Len(t, moved, 1)
			request := new(api.MoveRequest)
			require.NoError(t, json.Unmarshal([]byte(moved[0]), request))
			assert.Equal(t, []string{"cloudreve://my/src"}, request.Uris)
			assert.Equal(t, "cloudreve://my/other", request.Dst, "the server wants the folder it lands in")
			if tc.wantName == "" {
				assert.Empty(t, renamed, "no rename when the name is unchanged")
				return
			}
			require.Len(t, renamed, 1, "the destination name is applied")
			patch := new(api.RenameRequest)
			require.NoError(t, json.Unmarshal([]byte(renamed[0]), patch))
			assert.Equal(t, "cloudreve://my/other/src", patch.URI, "the entry the server kept under its own name")
			assert.Equal(t, tc.wantName, patch.NewName)
		})
	}
}

// TestUnitDirMoveSameFolderAndTaken checks the two cases the caller has to hear
// about: a rename within one folder needs only the rename, and a destination that
// is already there is reported as taken so rclone copies and purges instead.
func TestUnitDirMoveSameFolderAndTaken(t *testing.T) {
	t.Run("rename in place", func(t *testing.T) {
		var moved, renamed []string
		f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
			switch {
			case rec.method == "POST" && rec.path == "/api/v4/file/move":
				moved = append(moved, rec.body)
				reply(w, api.CodeSuccess, nil)
			case rec.method == "POST" && rec.path == "/api/v4/file/rename":
				renamed = append(renamed, rec.body)
				reply(w, api.CodeSuccess, nil)
			case rec.method == "GET" && rec.path == "/api/v4/file/info":
				if rec.attempt == 1 {
					reply(w, api.CodeParentNotExist, nil)
					return
				}
				reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFolder, ID: "D2", Name: "src2", Path: "cloudreve://my/src2"})
			default:
				s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
			}
		})
		require.NoError(t, f.DirMove(testCtx(), f, "src", "src2"))
		assert.Empty(t, moved, "no move request for a rename in place")
		require.Len(t, renamed, 1)
		patch := new(api.RenameRequest)
		require.NoError(t, json.Unmarshal([]byte(renamed[0]), patch))
		assert.Equal(t, "cloudreve://my/src", patch.URI)
		assert.Equal(t, "src2", patch.NewName)
	})
	for _, tc := range []struct {
		name string
		what api.File
	}{
		{"a folder is there", api.File{Type: api.FileTypeFolder, ID: "D3", Name: "dst", Path: "cloudreve://my/dst"}},
		{"a file is there", api.File{Type: api.FileTypeFile, ID: "F3", Name: "dst", Path: "cloudreve://my/dst"}},
	} {
		t.Run("destination taken as "+tc.name, func(t *testing.T) {
			f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
				switch {
				case rec.method == "GET" && rec.path == "/api/v4/file/info":
					reply(w, api.CodeSuccess, tc.what)
				default:
					s.t.Fatalf("the move went ahead onto a taken destination: %s %s", rec.method, rec.path)
				}
			})
			err := f.DirMove(testCtx(), f, "src", "dst")
			assert.True(t, errors.Is(err, fs.ErrorDirExists), "got %v", err)
			assert.Empty(t, s.made(), "no folders were built for a move that never happened")
		})
	}
	t.Run("onto itself", func(t *testing.T) {
		f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
			switch {
			case rec.method == "GET" && rec.path == "/api/v4/file/info":
				reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFolder, ID: "D4", Name: "", Path: "cloudreve://my/"})
			default:
				s.t.Fatalf("the root was moved onto itself: %s %s", rec.method, rec.path)
			}
		})
		err := f.DirMove(testCtx(), f, "", "")
		assert.True(t, errors.Is(err, fs.ErrorDirExists), "moving the drive root onto itself, got %v", err)
	})
}

// TestUnitFileAsRoot checks what a remote pointed at a single file gives back:
// the parent folder as the Fs plus the sentinel that tells rclone it was a file,
// and no per-file state that would later refuse other paths in that folder - the
// fs cache hands this same Fs back when the directory is asked for.
func TestUnitFileAsRoot(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "GET" && r.URL.Path == "/api/v4/file/info":
			if r.URL.Query().Get("uri") == "cloudreve://my/dir/one.txt" {
				reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFile, ID: "F3", Name: "one.txt", Size: 3, Path: "cloudreve://my/dir/one.txt"})
				return
			}
			reply(w, api.CodeNotFound, nil)
		case r.Method == "GET" && r.URL.Path == "/api/v4/file":
			reply(w, api.CodeSuccess, api.ListResponse{Files: []api.File{
				{Type: api.FileTypeFile, ID: "F3", Name: "one.txt", Size: 3, Path: "cloudreve://my/dir/one.txt"},
				{Type: api.FileTypeFile, ID: "F4", Name: "two.txt", Size: 4, Path: "cloudreve://my/dir/two.txt"},
			}})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(srv.Close)

	m := configmap.Simple{
		"url":                srv.URL,
		"access_token":       "SEED",
		"upload_concurrency": "2",
	}
	remote, err := NewFs(testCtx(), "cloudreve-test", "dir/one.txt", m)
	require.ErrorIs(t, err, fs.ErrorIsFile, "the caller learns the remote was a file")
	f := remote.(*Fs)
	assert.Equal(t, "dir", f.root)

	o, err := f.NewObject(testCtx(), "one.txt")
	require.NoError(t, err)
	assert.Equal(t, "F3", o.(*Object).ID())

	entries, err := f.List(testCtx(), "")
	require.NoError(t, err)
	require.Len(t, entries, 2, "the Fs is the folder, so its sibling is reachable too")
	assert.Equal(t, "two.txt", entries[1].Remote())
}

// TestUnitPublicLinkIsMadeAbsolute checks the direct link answer, since the
// server reports it as a path on itself.
func TestUnitPublicLinkIsMadeAbsolute(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		require.Equal(t, "PUT", rec.method)
		require.Equal(t, "/api/v4/file/source", rec.path)
		request := new(api.DirectLinkRequest)
		require.NoError(t, json.Unmarshal([]byte(rec.body), request))
		assert.Equal(t, []string{"cloudreve://my/f.txt"}, request.Uris)
		reply(w, api.CodeSuccess, []api.DirectLinkResponse{{ID: "L1", Link: "/f/AbCdEf/f.txt"}})
	})
	link, err := f.PublicLink(testCtx(), "f.txt", fs.Duration(time.Hour), false)
	require.NoError(t, err)
	assert.Equal(t, s.srv.URL+"/f/AbCdEf/f.txt", link)
	assert.Equal(t, 1, s.count("PUT", "/api/v4/file/source"))
}

// TestUnitUnlinkRemovesStoredLinks checks rclone link --unlink.
func TestUnitUnlinkRemovesStoredLinks(t *testing.T) {
	var deleted []string
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "GET" && rec.path == "/api/v4/file/info":
			assert.Equal(t, "true", rec.query.Get("extended"))
			info := api.FileExtended{}
			info.ID = "F1"
			info.ExtendedInfo.DirectLinks = []api.DirectLink{{ID: "L1"}, {ID: "L2"}}
			reply(w, api.CodeSuccess, info)
		case rec.method == "DELETE" && strings.HasPrefix(rec.path, "/api/v4/file/source/"):
			deleted = append(deleted, strings.TrimPrefix(rec.path, "/api/v4/file/source/"))
			reply(w, api.CodeSuccess, nil)
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	_, err := f.PublicLink(testCtx(), "f.txt", 0, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"L1", "L2"}, deleted)
}

// TestUnitPurgeDeletesTheFolderItself checks that purge relies on the server
// taking the tree with it.
func TestUnitPurgeDeletesTheFolderItself(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "GET" && rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFolder})
		case rec.method == "DELETE" && rec.path == "/api/v4/file":
			request := new(api.DeleteRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			assert.Equal(t, []string{"cloudreve://my/tree"}, request.Uris)
			reply(w, api.CodeSuccess, nil)
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	require.NoError(t, f.Purge(testCtx(), "tree"))
	assert.Equal(t, 1, s.count("DELETE", "/api/v4/file"), "one request for the whole tree")
}

// TestUnitListRDivesIntoFolders checks the recursive listing order.
func TestUnitListRDivesIntoFolders(t *testing.T) {
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		require.Equal(t, "GET", rec.method)
		require.Equal(t, "/api/v4/file", rec.path)
		switch rec.query.Get("uri") {
		case "cloudreve://my/":
			reply(w, api.CodeSuccess, api.ListResponse{Files: []api.File{
				{Name: "a.txt", Type: api.FileTypeFile, Path: "cloudreve://my/a.txt"},
				{Name: "deep", Type: api.FileTypeFolder},
			}})
		case "cloudreve://my/deep":
			reply(w, api.CodeSuccess, api.ListResponse{Files: []api.File{
				{Name: "b.txt", Type: api.FileTypeFile, Path: "cloudreve://my/deep/b.txt"},
			}})
		default:
			s.t.Fatalf("listing a folder that was never created")
		}
	})
	var got []string
	err := f.ListR(testCtx(), "", func(entries fs.DirEntries) error {
		for _, entry := range entries {
			got = append(got, entry.Remote())
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a.txt", "deep", "deep/b.txt"}, got, "folders are reported as well as descended into")
}

// TestUnitAboutReportsTheQuota checks the figures rclone about gets from the site's
// capacity answer.
func TestUnitAboutReportsTheQuota(t *testing.T) {
	for _, tc := range []struct {
		name        string
		total, used int64
		wantFree    *int64
	}{
		{"room left", 10 << 30, 3 << 30, func() *int64 { v := int64(7) << 30; return &v }()},
		{"over quota", 10 << 30, 11 << 30, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
				switch {
				case rec.method == "GET" && rec.path == "/api/v4/user/capacity":
					reply(w, api.CodeSuccess, api.Capacity{Total: tc.total, Used: tc.used})
				default:
					s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
				}
			})
			usage, err := f.About(testCtx())
			require.NoError(t, err)
			require.NotNil(t, usage.Total, "the quota of the account's group")
			assert.Equal(t, tc.total, *usage.Total)
			require.NotNil(t, usage.Used, "what the account holds, trash bin included")
			assert.Equal(t, tc.used, *usage.Used)
			if tc.wantFree == nil {
				assert.Nil(t, usage.Free, "no negative free is reported")
				return
			}
			require.NotNil(t, usage.Free)
			assert.Equal(t, *tc.wantFree, *usage.Free)
		})
	}
}

// TestUnitUserAgentIsSentOnEveryRequest checks that the remote's own User-Agent
// reaches both the API and the storage: fshttp forces that header from the config
// it is handed, so the override has to be on the client rather than on requests.
func TestUnitUserAgentIsSentOnEveryRequest(t *testing.T) {
	const agent = "rclone-test-agent/9.9"
	var apiUA, partUA string
	f, _ := newSite(t, "", configmap.Simple{"user_agent": agent}, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID: "SU", ChunkSize: 0,
				UploadURLs: []string{s.link("/store/part?n=1")},
				URI:        "cloudreve://my/dir/f.txt", CallbackSecret: "SECU",
			})
		case rec.path == "/store/part":
			partUA = rec.header.Get("User-Agent")
			w.Header().Set("ETag", `"e"`)
			replyRaw(w, http.StatusOK, "")
		case rec.method == "GET" && rec.path == "/api/v4/callback/s3/SU/SECU":
			apiUA = rec.header.Get("User-Agent")
			reply(w, api.CodeSuccess, nil)
		case rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{
				Type: api.FileTypeFile, ID: "FU", Name: "f.txt", Size: 7,
				Path: "cloudreve://my/dir/f.txt",
			})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	src := fsobject.NewStaticObjectInfo("dir/f.txt", time.Now(), 7, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.NoError(t, err)
	assert.Equal(t, agent, apiUA, "the API call carries the remote's agent")
	assert.Equal(t, agent, partUA, "and so does the presigned part upload")
}

// TestUnitPolicyCommandReportsThePool checks what the policy command answers for
// a pool folder: the pool itself, the children that may be named, and that
// rclone could write here.
func TestUnitPolicyCommandReportsThePool(t *testing.T) {
	f, s := newSite(t, "bound/here", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "GET" && rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFolder, ID: "D7", Name: "here", Path: "cloudreve://my/bound/here"})
		default:
			s.t.Fatalf("the policy command should only ask about the folder: %s %s", rec.method, rec.path)
		}
	})
	s.probePolicy = policyTypeLoadBalance
	s.probeID, s.probeName = "7lMhZ", "随机存储策略"
	s.probeChild = []api.Policy{{ID: "gJ8s3", Name: "silver-us-15", Type: PolicyTypeS3},
		{ID: "kMzh6", Name: "silver-de-04", Type: PolicyTypeS3}}

	out, err := f.Command(testCtx(), "policy", nil, nil)
	require.NoError(t, err)
	report, ok := out.(policyReport)
	require.True(t, ok, "the answer is a policy report")
	assert.Equal(t, "bound/here", report.Path)
	assert.Equal(t, "FOLD-7lMhZ", report.FolderID, "the answer names the folder the way root_folder_id wants it")
	assert.Equal(t, policyTypeLoadBalance, report.Type)
	assert.Equal(t, "7lMhZ", report.ID)
	assert.True(t, report.Writable, "a pool with an s3 child is one rclone writes through")
	assert.Empty(t, report.Refusal)
	require.Len(t, report.Children, 2, "the pool's children are listed as the IDs to name")
	assert.Equal(t, "kMzh6", report.Children[1].ID)
	assert.Equal(t, PolicyTypeS3, report.Children[1].Type)
}

// TestUnitPolicyCommandReportsARefusal checks that a folder rclone cannot write to
// says so, with the reason, instead of looking writable.
func TestUnitPolicyCommandReportsARefusal(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
	})
	s.probePolicy = "local"
	s.probeID, s.probeName = "PL", "local disk"

	out, err := f.Command(testCtx(), "policy", nil, nil)
	require.NoError(t, err)
	report := out.(policyReport)
	assert.False(t, report.Writable)
	assert.Contains(t, report.Refusal, "can only write through the \"s3\" policy")
}

// TestUnitSetPolicyCommandBindsAndRefetches checks the bind request and that the
// remembered policies are dropped, so the answer it reports came from the site
// rather than from the cache the bind just invalidated.
func TestUnitSetPolicyCommandBindsAndRefetches(t *testing.T) {
	var bound []string
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PATCH" && rec.path == "/api/v4/file/policy":
			request := new(api.BindPolicyRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			bound = append(bound, request.URI+" "+request.PolicyID)
			reply(w, api.CodeSuccess, nil)
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	// Warm the policy cache for this folder.
	_, err := f.Command(testCtx(), "policy", nil, nil)
	require.NoError(t, err)
	probesBefore := s.count("GET", "/api/v4/file")

	_, err = f.Command(testCtx(), "set_policy", []string{"nZ4TN"}, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"cloudreve://my/ nZ4TN"}, bound,
		"the folder is addressed by URI and the policy by ID, both as strings")

	probesAfter := s.count("GET", "/api/v4/file")
	assert.Equal(t, probesBefore+1, probesAfter,
		"the bind cleared the cache, so the report had to ask the site again")
}

// TestUnitCommandArgumentsAreChecked checks the shapes the commands refuse, which
// is what keeps a mistyped rc call from binding the wrong folder.
func TestUnitCommandArgumentsAreChecked(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		s.t.Fatalf("a rejected command should not reach the site: %s %s", rec.method, rec.path)
	})

	_, err := f.Command(testCtx(), "unlink", nil, nil)
	assert.True(t, errors.Is(err, fs.ErrorCommandNotFound), "unknown commands, got %v", err)

	_, err = f.Command(testCtx(), "policy", []string{"some/folder"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "name the folder in the remote")

	for _, args := range [][]string{nil, {"a", "b"}} {
		_, err = f.Command(testCtx(), "set_policy", args, nil)
		require.Error(t, err, "set_policy with %d arguments", len(args))
		assert.Contains(t, err.Error(), "needs one argument")
	}

	_, err = f.Command(testCtx(), "policy", nil, map[string]string{"anything": "1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "takes no options")
	assert.Empty(t, s.made())
}

// TestUnitUploadMakesMissingFolders checks that an upload into folders that are
// not there builds them, from the top down, and then writes the file. rclone's
// own contract has Put make the folders its path names.
func TestUnitUploadMakesMissingFolders(t *testing.T) {
	f, s := newSite(t, "", nil, uploadSite())
	s.lack("cloudreve://my/gone")
	s.lack("cloudreve://my/gone/deep")

	src := fsobject.NewStaticObjectInfo("gone/deep/f.txt", time.Now(), 7, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.NoError(t, err)
	assert.Equal(t, []string{
		"cloudreve://my/gone/deep",
		"cloudreve://my/gone",
		"cloudreve://my/gone/deep",
	}, s.made(), "the missing parent is reported by the server, so the folders are made from the top down")
}

// TestUnitUploadReportsUnmadeFolder checks that a folder rclone cannot make stops
// the upload with the server's own reason instead of a silent retry.
func TestUnitUploadReportsUnmadeFolder(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "POST" && rec.path == "/api/v4/file/create":
			reply(w, api.CodeIllegalObjectName, nil)
		default:
			s.t.Fatalf("the upload went on after its folder was refused: %s %s", rec.method, rec.path)
		}
	})
	s.lack("cloudreve://my/gone")

	src := fsobject.NewStaticObjectInfo("gone/f.txt", time.Now(), 7, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "40052")
	assert.Equal(t, []string{"cloudreve://my/gone"}, s.made(), "the folder is asked for once, then the upload stops")
}

// TestUnitPartRetryResendsTheBytes checks that a part the store refuses once is
// sent again whole: the HTTP transport must not have closed the part's buffer
// after the first attempt, or there would be nothing left to rewind.
func TestUnitPartRetryResendsTheBytes(t *testing.T) {
	var partSizes []int
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID: "SR", ChunkSize: 0,
				UploadURLs: []string{s.link("/store/part?n=1")},
				URI:        "cloudreve://my/dir/f.txt", CallbackSecret: "SECR",
			})
		case rec.path == "/store/part":
			partSizes = append(partSizes, len(rec.body))
			if rec.attempt == 1 {
				replyRaw(w, http.StatusServiceUnavailable, "try again")
				return
			}
			w.Header().Set("ETag", `"e"`)
			replyRaw(w, http.StatusOK, "")
		case rec.method == "GET" && rec.path == "/api/v4/callback/s3/SR/SECR":
			reply(w, api.CodeSuccess, nil)
		case rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{
				Type: api.FileTypeFile, ID: "FR", Name: "f.txt", Size: 7,
				Path: "cloudreve://my/dir/f.txt",
			})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	src := fsobject.NewStaticObjectInfo("dir/f.txt", time.Now(), 7, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.NoError(t, err)
	assert.Equal(t, []int{7, 7}, partSizes, "the retry carries the whole part again")
}

// TestUnitTopLevelFolderCannotBeBuilt checks that an upload whose top-level
// folder the site itself refuses stops there, rather than going looking for a
// parent that rclone cannot make.
func TestUnitTopLevelFolderCannotBeBuilt(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "POST" && rec.path == "/api/v4/file/create":
			reply(w, api.CodeParentNotExist, nil)
		default:
			s.t.Fatalf("the upload went on after its folder was refused: %s %s", rec.method, rec.path)
		}
	})
	s.lack("cloudreve://my/gone")

	src := fsobject.NewStaticObjectInfo("gone/f.txt", time.Now(), 7, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "40016")
	assert.Equal(t, []string{"cloudreve://my/gone"}, s.made(), "the folder is asked for once, not walked into")
}

// uploadSite answers the requests of one whole upload of a 7 byte file into
// folders the site makes on request, so a test about what comes before the
// upload can use it.
func uploadSite() siteHandler {
	return func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "POST" && rec.path == "/api/v4/file/create":
			request := new(api.CreateFolderRequest)
			if err := json.Unmarshal([]byte(rec.body), request); err != nil {
				s.t.Fatalf("bad folder request: %v", err)
			}
			if rest, found := strings.CutPrefix(request.URI, driveRoot); found {
				if parent := path.Dir(rest); parent != "." && s.lacks(driveRoot+parent) {
					reply(w, api.CodeParentNotExist, nil)
					return
				}
			}
			s.have(request.URI)
			reply(w, api.CodeSuccess, nil)
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			request := new(api.CreateSessionRequest)
			if err := json.Unmarshal([]byte(rec.body), request); err != nil {
				s.t.Fatalf("bad session request: %v", err)
			}
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID: "S7", ChunkSize: 0,
				UploadURLs: []string{s.link("/store/part?n=1")},
				URI:        request.URI, CallbackSecret: "SEC7",
			})
		case rec.path == "/store/part":
			w.Header().Set("ETag", `"e"`)
			replyRaw(w, http.StatusOK, "")
		case rec.method == "GET" && rec.path == "/api/v4/callback/s3/S7/SEC7":
			reply(w, api.CodeSuccess, nil)
		case rec.path == "/api/v4/file/info":
			reply(w, api.CodeSuccess, api.File{
				Type: api.FileTypeFile, ID: "F7", Name: "f.txt", Size: 7,
				UpdatedAt: time.Now(), Path: "cloudreve://my/gone/deep/f.txt",
			})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	}
}

// TestUnitServerErrorsAreRetried checks that a failure the site answers with its
// own envelope is retried, which is the case a proxy hiccup arrives as.
func TestUnitServerErrorsAreRetried(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		if rec.attempt < 3 {
			w.WriteHeader(http.StatusBadGateway)
			reply(w, api.CodeUploadFailed, nil)
			return
		}
		reply(w, api.CodeSuccess, api.ListResponse{})
	})
	_, err := f.List(testCtx(), "")
	require.NoError(t, err)
	assert.Equal(t, 3, s.count("GET", "/api/v4/file"), "HTTP 502 was retried until it recovered")
}

// TestUnitNonJSONReplyIsRetried checks that a reply from whatever stands in
// front of the site is treated as a fault worth retrying, not as an answer, and
// that the message still quotes what answered.
func TestUnitNonJSONReplyIsRetried(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		if rec.attempt < 3 {
			w.Header().Set("Content-Type", "text/html")
			replyRaw(w, http.StatusBadGateway, "<html><body>gateway gave up</body></html>")
			return
		}
		reply(w, api.CodeSuccess, api.ListResponse{})
	})
	_, err := f.List(testCtx(), "")
	require.NoError(t, err, "the proxy page was retried past")
	assert.Equal(t, 3, s.count("GET", "/api/v4/file"))
}

// TestUnitNonJSONReplyEventuallySaysWhatAnswered checks the message when the
// front end never recovers.
func TestUnitNonJSONReplyEventuallySaysWhatAnswered(t *testing.T) {
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		w.Header().Set("Content-Type", "text/html")
		replyRaw(w, http.StatusOK, "<html><body>not the api</body></html>")
	})
	_, err := f.List(testCtx(), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not JSON")
	assert.Contains(t, err.Error(), "not the api")
}

// TestUnitRetiredRefreshFallsBackToSigningIn checks the way out when a refresh
// token no longer works: an account that can sign in gets a fresh pair rather
// than a remote stuck on a credential the server has already retired.
func TestUnitRetiredRefreshFallsBackToSigningIn(t *testing.T) {
	f, _ := newSite(t, "", configmap.Simple{
		"email":         "me@example.com",
		"password":      obscure.MustObscure("hunter2"),
		"access_token":  "OLD",
		"refresh_token": "OLDR",
	}, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case loginStep(w, rec, false, api.PrepareLogin{PasswordEnabled: true}):
			return
		case rec.method == "POST" && rec.path == "/api/v4/session/token/refresh":
			reply(w, api.CodeCredentialInvalid, nil) // this pair is retired
		case rec.method == "POST" && rec.path == "/api/v4/session/token":
			reply(w, api.CodeSuccess, api.LoginData{Token: api.Token{
				AccessToken: "AT4", RefreshToken: "RT4",
			}})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	require.NoError(t, f.renewToken(testCtx(), f.tok.currentGeneration()))
	assert.Equal(t, "AT4", token(f, "access_token"), "the account signed in again")
	assert.Equal(t, "RT4", token(f, "refresh_token"))
}

// TestUnitConcurrentRenewalsSpendOneRefresh checks that parallel transfers renew
// the session once: a refresh token is retired by being used, so a second caller
// that spent the same one would leave the remote holding a dead credential.
func TestUnitConcurrentRenewalsSpendOneRefresh(t *testing.T) {
	var refreshes atomic.Int32
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "POST" && rec.path == "/api/v4/session/token/refresh":
			refreshes.Add(1)
			time.Sleep(100 * time.Millisecond) // widen the window the others queue in
			reply(w, api.CodeSuccess, api.Token{AccessToken: "AT2", RefreshToken: "RT2"})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	start := f.tok.currentGeneration()
	errs := make([]error, 4)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = f.renewToken(testCtx(), start)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "renewal %d", i)
	}
	assert.Equal(t, int32(1), refreshes.Load(), "one refresh token is spent, the rest take its result")
	assert.Equal(t, "AT2", token(f, "access_token"))
}

// TestUnitTopUploadProbesTheDriveItself checks that a name at the top of the
// drive is probed as the drive, not as a folder whose name is a single dot.
func TestUnitTopUploadProbesTheDriveItself(t *testing.T) {
	f, s := newSite(t, "", nil, uploadSite())

	src := fsobject.NewStaticObjectInfo("f.txt", time.Now(), 7, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.NoError(t, err)

	var probed []string
	for _, rec := range s.recorded() {
		if rec.path == "/api/v4/file" && rec.query.Get("page_size") == "1" {
			probed = append(probed, rec.query.Get("uri"))
		}
	}
	assert.Equal(t, []string{"cloudreve://my/"}, probed,
		`the folder holding a top level name is the drive, addressed empty, not "."`)
}

// TestUnitMoveToTheDriveRootStaysWellFormed checks the address the rename half of
// a move gets when the destination is the top of the drive: joining an empty
// folder onto the name would leave an empty first segment.
func TestUnitMoveToTheDriveRootStaysWellFormed(t *testing.T) {
	var renamed string
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "POST" && rec.path == "/api/v4/file/move":
			reply(w, api.CodeSuccess, nil)
		case rec.method == "POST" && rec.path == "/api/v4/file/rename":
			renamed = rec.body
			reply(w, api.CodeSuccess, nil)
		case rec.method == "GET" && rec.path == "/api/v4/file/info":
			if rec.attempt == 1 {
				reply(w, api.CodeParentNotExist, nil) // the destination is free
				return
			}
			reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFolder, ID: "DR", Name: "renamed", Path: "cloudreve://my/renamed"})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	require.NoError(t, f.DirMove(testCtx(), f, "sub/old", "renamed"))
	require.NotEmpty(t, renamed, "the rename went out")
	assert.NotContains(t, renamed, "my//", "no empty segment in the addressed path")
	patch := new(api.RenameRequest)
	require.NoError(t, json.Unmarshal([]byte(renamed), patch))
	assert.Equal(t, "cloudreve://my/old", patch.URI, "the server kept the folder under its own name at the top")
	assert.Equal(t, "renamed", patch.NewName)
}

// TestUnitAbandonedSessionWaitsForParts checks that a part still in flight has
// finished before the session that holds it is abandoned.
func TestUnitAbandonedSessionWaitsForParts(t *testing.T) {
	var partDone atomic.Bool
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID:  "SS",
				ChunkSize:  4,
				UploadURLs: []string{s.link("/store/part?n=1"), s.link("/store/part?n=2")},
				URI:        "cloudreve://my/dir/f.txt", CallbackSecret: "SECS",
			})
		case rec.path == "/store/part":
			time.Sleep(200 * time.Millisecond) // slow enough that the old code raced ahead
			partDone.Store(true)
			w.Header().Set("ETag", `"e"`)
			replyRaw(w, http.StatusOK, "")
		case rec.method == "DELETE" && rec.path == "/api/v4/file/upload":
			assert.True(s.t, partDone.Load(),
				"the session is not abandoned while one of its parts is still being sent")
			reply(w, api.CodeSuccess, nil)
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})

	// The session was asked for 8 bytes; the source gives 5, so the second part
	// fails to fill while the first one is still on the wire.
	src := fsobject.NewStaticObjectInfo("dir/f.txt", time.Now(), 8, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.Error(t, err, "a source shorter than its declared size must fail")
	assert.True(t, partDone.Load(), "the part in flight was waited for")
}

// TestUnitSignatureStaysOutWhenTheLinkCannotBeUsed checks that an unusable
// presigned URL does not put its signature in the error: the HTTP layer names the
// whole URL it failed on, and that URL is a working link.
func TestUnitSignatureStaysOutWhenTheLinkCannotBeUsed(t *testing.T) {
	const signature = "X-Amz-Signature=9f3c1e0d7a"
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
			reply(w, api.CodeSuccess, &api.UploadSession{
				SessionID: "SQ", ChunkSize: 0,
				// A space in the host makes the request fail before it is sent,
				// which is how a mangled link from the site reaches us.
				UploadURLs:     []string{"http://store internal/part?" + signature},
				URI:            "cloudreve://my/dir/f.txt",
				CallbackSecret: "SECQ",
			})
		case rec.method == "DELETE" && rec.path == "/api/v4/file/upload":
			reply(w, api.CodeSuccess, nil) // the failed session is abandoned
		default:
			s.t.Fatalf("the upload went on past the bad link: %s %s", rec.method, rec.path)
		}
	})

	src := fsobject.NewStaticObjectInfo("dir/f.txt", time.Now(), 7, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.Error(t, err, "an unusable part link must stop the upload")
	assert.NotContains(t, err.Error(), "9f3c1e0d7a", "the signature is not repeated")
	assert.Contains(t, err.Error(), "part 1", "and the error still says which part failed")
}

// TestUnitSignatureStaysOutOfHTTPError checks the shape the leak actually takes:
// the HTTP layer reports a failure as a url.Error naming the full URL, so wrapping
// it with %w would print a signed link even though the link itself was redacted.
func TestUnitSignatureStaysOutOfHTTPError(t *testing.T) {
	link := "https://store.example/part?X-Amz-Signature=deadbeef0123"
	dial := &url.Error{Op: "Put", URL: link, Err: errors.New("connection refused")}

	err := signedError("PUT", link, dial)
	assert.NotContains(t, err.Error(), "deadbeef0123", "the signature does not come through")
	assert.NotContains(t, err.Error(), "X-Amz-Signature")
	assert.Contains(t, err.Error(), "store.example/part", "the host and path stay, to say what failed")
	assert.Contains(t, err.Error(), "connection refused", "and the reason stays")

	// Wrapping the same error with %w, as this backend used to, does leak it: the
	// assertion below is what the fix is measured against.
	naive := fmt.Errorf("cloudreve: PUT %s: %w", redactLink(link), dial)
	assert.Contains(t, naive.Error(), "deadbeef0123", "proof that url.Error carries the signed URL")
}

// TestUnitSignedURLIsRedacted checks that a signed URL keeps its credentials out
// of the error text.
func TestUnitSignedURLIsRedacted(t *testing.T) {
	redacted := redactLink("https://bucket.s3.example.com/key?X-Amz-Signature=secret&part=1")
	assert.NotContains(t, redacted, "secret")
	assert.NotContains(t, redacted, "X-Amz-Signature")
	assert.Contains(t, redacted, "bucket.s3.example.com/key")
}

// holder builds the lock conflict the site names, as measured on a live site:
// path, token, owning account and the application that took the lock.
func holder(path, token, owner, application string) api.LockConflict {
	held := api.LockConflict{Path: path, Token: token}
	held.Owner.Owner = owner
	held.Owner.Application.Type = application
	return held
}

// TestUnitRetryAfterIsHonouredAsGiven checks that a site which states how long to
// wait is believed for that long: the wait comes from the header rather than from
// rclone's own backoff cap, so lowering that cap cannot turn a rate limit into a
// drumbeat of requests.
func TestUnitRetryAfterIsHonouredAsGiven(t *testing.T) {
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		if rec.method == "GET" && rec.path == "/api/v4/file" {
			w.Header().Set("Retry-After", "300")
			replyRaw(w, http.StatusTooManyRequests, "")
			return
		}
		s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
	})
	_, err := f.request(testCtx(), "GET", "/file", nil, nil, true)
	require.Error(t, err)
	wait, isRetryAfter := pacer.IsRetryAfter(err)
	require.True(t, isRetryAfter, "a rate limit is classed as worth coming back for")
	assert.Equal(t, 5*time.Minute, wait, "as long as the site asked, not as long as our own cap")
}

// TestUnitListHidesUnfinishedUploads checks that the placeholder an interrupted
// upload leaves is neither listed nor openable, so rclone writes the name again
// rather than comparing against an empty phantom it cannot overwrite.
func TestUnitListHidesUnfinishedUploads(t *testing.T) {
	half := api.File{Type: api.FileTypeFile, ID: "PH", Name: "half.txt",
		Metadata: map[string]string{api.MetadataUploadSessionID: "SESS"}}
	f, _ := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "GET" && rec.path == "/api/v4/file":
			reply(w, api.CodeSuccess, api.ListResponse{Files: []api.File{
				{Type: api.FileTypeFile, ID: "OK", Name: "real.txt", Size: 4},
				half,
			}})
		case rec.path == "/api/v4/file/info":
			if rec.query.Get("uri") == driveRoot+"half.txt" {
				reply(w, api.CodeSuccess, half)
				return
			}
			reply(w, api.CodeSuccess, api.File{Type: api.FileTypeFile, ID: "OK", Name: "real.txt", Size: 4})
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	entries, err := f.List(testCtx(), "")
	require.NoError(t, err)
	require.Len(t, entries, 1, "the unfinished upload is not a file")
	assert.Equal(t, "real.txt", entries[0].Remote())

	_, err = f.NewObject(testCtx(), "half.txt")
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound, "and naming it directly says it is not there")
}

// TestUnitUploadReleasesAStaleLock checks the way out of a lock this account left
// behind: the conflict names its token, the token is released, and the upload is
// asked for again.
func TestUnitUploadReleasesAStaleLock(t *testing.T) {
	ours := holder(driveRoot+"dir/f.txt", "TOK1", "ME", "upload")
	foreign := holder(driveRoot+"other.txt", "TOK2", "SOMEONE-ELSE", "upload")
	f, s := newSite(t, "", configmap.Simple{"drop_stale_locks": "true"}, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "GET" && rec.path == "/api/v4/user/me":
			reply(w, api.CodeSuccess, api.CurrentUser{ID: "ME"})
		case rec.method == "PUT" && rec.path == "/api/v4/file/upload" && s.count("PUT", "/api/v4/file/upload") == 1:
			reply(w, api.CodeLockConflict, []api.LockConflict{ours, foreign})
		case rec.method == "DELETE" && rec.path == "/api/v4/file/lock":
			request := new(api.UnlockRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			assert.Equal(t, []string{"TOK1"}, request.Tokens,
				"only this account's upload lock is released, never another account's")
			reply(w, api.CodeSuccess, nil)
		default:
			uploadSite()(s, w, r, rec)
		}
	})
	src := fsobject.NewStaticObjectInfo("dir/f.txt", time.Now(), 7, false, nil, nil)
	_, err := f.Put(testCtx(), strings.NewReader("content"), src)
	require.NoError(t, err, "the upload goes through once the stale lock is gone")
	assert.Equal(t, 2, s.count("PUT", "/api/v4/file/upload"), "refused once, granted on the retry")
	assert.Equal(t, 1, s.count("DELETE", "/api/v4/file/lock"))
}

// TestUnitUploadKeepsALockItShouldNotTouch checks the two ways a lock is left
// alone: it belongs to someone else, or releasing stale locks is switched off.
func TestUnitUploadKeepsALockItShouldNotTouch(t *testing.T) {
	for _, tc := range []struct {
		name   string
		owner  string
		config configmap.Simple
	}{
		{"another account holds it", "SOMEONE-ELSE", configmap.Simple{"drop_stale_locks": "true"}},
		{"this account asked not to drop it", "ME", configmap.Simple{"drop_stale_locks": "false"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, s := newSite(t, "", tc.config, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
				switch {
				case rec.method == "GET" && rec.path == "/api/v4/user/me":
					reply(w, api.CodeSuccess, api.CurrentUser{ID: "ME"})
				case rec.method == "PUT" && rec.path == "/api/v4/file/upload":
					reply(w, api.CodeLockConflict,
						[]api.LockConflict{holder(driveRoot+"dir/f.txt", "TOK1", tc.owner, "upload")})
				default:
					s.t.Fatalf("the lock must survive this run: %s %s", rec.method, rec.path)
				}
			})
			src := fsobject.NewStaticObjectInfo("dir/f.txt", time.Now(), 7, false, nil, nil)
			_, err := f.Put(testCtx(), strings.NewReader("content"), src)
			assert.Equal(t, api.CodeLockConflict, codeOf(err), "the server's own answer is what fails with")
			assert.Equal(t, 1, s.count("PUT", "/api/v4/file/upload"), "and it is not retried")
		})
	}
}

// TestUnitDeleteReleasesAStaleLock checks the other direction: an entry whose name
// a killed upload locked cannot be removed until that lock is gone, so deleting it
// releases this account's own stale lock and asks once more.
func TestUnitDeleteReleasesAStaleLock(t *testing.T) {
	f, s := newSite(t, "", configmap.Simple{"drop_stale_locks": "true"}, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		switch {
		case rec.method == "GET" && rec.path == "/api/v4/user/me":
			reply(w, api.CodeSuccess, api.CurrentUser{ID: "ME"})
		case rec.method == "DELETE" && rec.path == "/api/v4/file" && s.count("DELETE", "/api/v4/file") == 1:
			reply(w, api.CodeLockConflict,
				[]api.LockConflict{holder(driveRoot+"stuck.txt", "TOKLOCK", "ME", "upload")})
		case rec.method == "DELETE" && rec.path == "/api/v4/file/lock":
			request := new(api.UnlockRequest)
			require.NoError(t, json.Unmarshal([]byte(rec.body), request))
			assert.Equal(t, []string{"TOKLOCK"}, request.Tokens)
			reply(w, api.CodeSuccess, nil)
		case rec.method == "DELETE" && rec.path == "/api/v4/file":
			reply(w, api.CodeSuccess, nil)
		default:
			s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
		}
	})
	o := &Object{fs: f, remote: "stuck.txt", id: "F9", size: 0}
	require.NoError(t, o.Remove(testCtx()))
	assert.Equal(t, 2, s.count("DELETE", "/api/v4/file"), "refused once, deleted on the retry")
}

// TestUnitAbandonSurvivesTheCallersCancellation checks the one call that must go
// through after the user has pressed Ctrl-C, since abandoning is what releases the
// name the interrupted upload locked.
func TestUnitAbandonSurvivesTheCallersCancellation(t *testing.T) {
	f, s := newSite(t, "", nil, func(s *site, w http.ResponseWriter, r *http.Request, rec seen) {
		if rec.method == "DELETE" && rec.path == "/api/v4/file/upload" {
			reply(w, api.CodeSuccess, nil)
			return
		}
		s.t.Fatalf("unexpected request %s %s", rec.method, rec.path)
	})
	ctx, cancel := context.WithCancel(testCtx())
	cancel()
	err := f.deleteSession(ctx, &api.UploadSession{SessionID: "S", URI: driveRoot + "f.txt"})
	require.NoError(t, err, "a cancelled caller does not cancel the cleanup")
	assert.Equal(t, 1, s.count("DELETE", "/api/v4/file/upload"))
}
