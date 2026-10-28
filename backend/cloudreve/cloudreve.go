// Package cloudreve implements a backend for Cloudreve, the self-hosted file
// hosting system.
//
// Cloudreve keeps its own database of names and metadata in front of a storage
// policy, and only some policies hand out presigned part URLs to upload
// through. This backend therefore reads any policy but writes only to "s3".
package cloudreve

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/cloudreve/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
)

// PolicyTypeS3 is the only storage policy this backend writes to.
const PolicyTypeS3 = "s3"

// cloudreveDefaultEncoding is the default of the encoding option: rclone's
// Standard set, which covers what no path can hold at all - the slash that
// separates levels, NUL, the other control characters, DEL and a name made only
// of dots - plus the characters the server refuses with 40052 Invalid object
// name, and bytes that are not valid UTF-8.
//
// Spaces, hashes, percent signs and a dot inside a longer name are stored as they
// arrive, so the Standard set leaves them alone and so do we.
const cloudreveDefaultEncoding = encoder.Standard | encoder.EncodeInvalidUtf8 |
	encoder.EncodeBackSlash | encoder.EncodeDoubleQuote | encoder.EncodeAsterisk |
	encoder.EncodeColon | encoder.EncodeLtGt | encoder.EncodeQuestion | encoder.EncodePipe

const (
	apiPrefix = "/api/v4"

	// policyTypeLoadBalance is a pool of policies the server chooses an upload
	// from, unless the client names a child itself.
	policyTypeLoadBalance = "load_balance"

	// driveRoot is the URI scheme of the signed-in user's own drive.
	driveRoot = "cloudreve://my/"

	minSleep = 10 * time.Millisecond
	// maxSleep bounds only rclone's own backoff for faults the server did not
	// quantify: Default.Calculate returns a stated Retry-After as given, so a
	// site that asks for five minutes is still waited out with this at 2 seconds.
	maxSleep      = 2 * time.Second
	decayConstant = 2 // bigger for slower decay, exponential

	defaultPageSize    = 1000
	defaultDeleteBatch = 500
	defaultUploadConc  = 4

	// defaultAbandonTimeout is how long to keep trying after the caller has
	// given up, so an abandoned upload still releases the lock it held.
	defaultAbandonTimeout = time.Minute

	// urlExpiryMargin re-mints a download link this long before the server
	// says it stops working, so a link never expires mid-transfer.
	urlExpiryMargin = 5 * time.Minute

	// maxCachedLinks and maxCachedPolicies bound what a long run over a huge
	// tree keeps.
	maxCachedLinks    = 4096
	maxCachedPolicies = 4096

	// renewMargin renews the session this long before it expires.
	renewMargin = 10 * time.Minute
)

// Options configures the backend
type Options struct {
	URL               string               `config:"url"`
	Email             string               `config:"email"`
	Password          string               `config:"password"`
	AccessToken       string               `config:"access_token"`
	RefreshToken      string               `config:"refresh_token"`
	Location          string               `config:"location"`
	RootFolderID      string               `config:"root_folder_id"`
	UserAgent         string               `config:"user_agent"`
	UploadConcurrency int                  `config:"upload_concurrency"`
	SkipSoftDelete    bool                 `config:"skip_soft_delete"`
	DropStaleLocks    bool                 `config:"drop_stale_locks"`
	StaleLockAfter    fs.Duration          `config:"drop_stale_locks_after"`
	LinkReplace       string               `config:"link_replace"`
	Enc               encoder.MultiEncoder `config:"encoding"`
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "cloudreve",
		Description: "Cloudreve",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:     "url",
			Help:     "URL of the Cloudreve site, e.g. https://drive.example.com.\n\nInclude the scheme. rclone talks to the site's /api/v4 endpoints under this URL.",
			Required: true,
		}, {
			Name: "email",
			Help: "Email of the account, used with password to sign in.\n\n" +
				"Set either this pair or access_token below. Login captcha must be turned off in " +
				"the site's admin settings for rclone to sign in - see the limitations.",
		}, {
			Name:       "password",
			Help:       "Password of the account.\n\nLeave blank when the account has no local password - one created through SSO or OIDC, or one you only sign into with a passkey. rclone cannot answer those; use access_token instead.",
			IsPassword: true,
		}, {
			Name:      "access_token",
			Help:      "Bearer token to use instead of signing in.\n\nSet this when email and password are not usable, or to reuse a token from another client. rclone renews a token it issued itself, so with email and password set this does not need filling.",
			Sensitive: true,
		}, {
			Name:      "refresh_token",
			Help:      "Refresh token matching access_token.\n\nNeeded to renew a token that was not issued by rclone, otherwise the remote stops working when the token expires.",
			Sensitive: true,
		}, {
			Name:    "location",
			Help:    "Folder to use as root, as a path from the top of the drive, e.g. photos/2026.\n\nThe folder does not have to exist: rclone makes it when it first writes there. Leave blank for the whole drive. This cannot be set together with root_folder_id.",
			Default: "",
		}, {
			Name:     "root_folder_id",
			Help:     "ID of the folder to use as root.\n\nThe root is addressed by ID instead of by a path, which helps when a folder name contains characters the URI parser treats specially. `rclone backend policy remote:path/to/folder` reports the ID of a folder you can already reach, as folder_id. This cannot be set together with location.",
			Advanced: true,
		}, {
			Name:     "user_agent",
			Help:     "Override the User-Agent of this remote.\n\nrclone sends its own version by default; a site in front of a proxy or CDN that filters clients may need something else. This wins over the global --user-agent.",
			Advanced: true,
			Default:  "",
		}, {
			Name:     "upload_concurrency",
			Help:     "Number of parts to upload at the same time.\n\nThe server decides how many parts a file is split into, so this only caps how many rclone keeps in flight.",
			Default:  defaultUploadConc,
			Advanced: true,
		}, {
			Name:     "skip_soft_delete",
			Help:     "Delete permanently instead of moving files to the trash bin.\n\nThe server soft-deletes by default and collects the trash bin once the account group's retention period passes, so deleted files keep occupying storage until then. With this set the object goes at once and cannot be recovered.",
			Default:  false,
			Advanced: true,
		}, {
			Name:     "drop_stale_locks",
			Help:     "Release an upload lock this account left behind, instead of failing on it.\n\nAn interrupted upload keeps locking the file's name until the server's upload_session_timeout passes. Deleting goes through this regardless of the age: asking to remove an entry locked by a half-written upload means wanting it gone, and leaving it locked is the deadlock where a folder cannot be purged. Taking the name over with a new upload needs drop_stale_locks_after set, because rclone cannot tell whether that other upload is still running.",
			Default:  true,
			Advanced: true,
		}, {
			Name:     "drop_stale_locks_after",
			Help:     "How long an upload must have been running before a new upload may take its name over.\n\nThe server gives no way to tell a live upload from a dead one: a lock conflict carries no timestamp and the entry it locks exposes only its session id, not an expiry. The age of the newest version of that file is the only evidence there is - it is dated when the running upload started, while the file's own created_at is left alone when a file is overwritten. Taking a younger lock starts a race - both sessions live, whichever callback reaches the server first files the name, the other is refused as a lost lock and its bytes are discarded.\n\n0, the default, never breaks a lock on account of an upload: the name is reported as locked and the transfer fails. Nothing in the server clears such a lock on a timer, so a name left locked by a killed upload stays locked until someone releases it by hand: the conflict names its token, and DELETE /api/v4/file/lock takes them as {\"tokens\":[...]}. Set this above the time the longest upload to this site takes to let rclone reuse such a name itself. Deleting never waits for this: asking to remove an entry clears whatever lock holds it.",
			Default:  fs.Duration(0),
			Advanced: true,
		}, {
			Name: "link_replace",
			Help: "Rewrite the scheme and host of every download link this remote uses.\n\n" +
				"One rule per line (or per comma): from => to, where the left side is the " +
				"scheme and host a link arrives with, and the right side is the scheme and " +
				"host to hand it out with instead. The path and query are kept, and a path " +
				"on the right side goes in front of them, so a proxy served under a " +
				"sub-path works:\n\n" +
				"    https://storage.example.com => https://cdn.example.com/files\n\n" +
				"A rule names the whole site, so the left side takes scheme and host only; " +
				"host and scheme are compared without regard to case, and the first rule " +
				"that matches wins. This applies to the links reads fetch from as well as " +
				"to what rclone link prints, so a rule pointing at a proxy sends the " +
				"transfer traffic there too.",
			Advanced: true,
			Default:  "",
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default:  cloudreveDefaultEncoding,
		}},
		CommandHelp: []fs.CommandHelp{{
			Name:  "policy",
			Short: "Show which storage policy uploads into the folder go through.",
			Long: `Shows the storage policy of the folder the remote points at, as the site
reports it, and whether rclone could write there.

A load_balance policy is a pool: the answer lists its children, and those IDs are
what set_policy can bind the folder to. The answer also carries the folder's own
ID as folder_id, which is what the root_folder_id option wants.

All that is needed is the folder, named in the remote:

    rclone backend policy remote:some/folder

The folder's own policy is a binding the site keeps per folder; without one the
folder inherits the policy of the account's group.`,
		}, {
			Name:  "set_policy",
			Short: "Bind the folder the remote points at to a storage policy.",
			Long: `Binds a folder to a storage policy, so uploads into it go through that policy
instead of the one the account's group gives them. The site accepts only a policy
it knows; run the policy command to see the IDs available under a pool.

    rclone backend set_policy remote:some/folder cde4f

Undoing a binding needs the ID the policy command reported beforehand - the site
has no "inherit again" value. Binding affects where new files land; it does not
move what is already there.`,
		}},
	})
}

// Fs represents a folder within a Cloudreve drive
type Fs struct {
	name       string // name of this remote as given on the command line
	root       string // standard path of the root folder, no surrounding slashes
	rootExists bool   // false when the root names a folder that is not there yet
	features   *fs.Features
	opt        Options
	ci         *fs.ConfigInfo
	cfg        configmap.Mapper

	client *http.Client // the API, carries the bearer token
	bare   *http.Client // storage and site URLs, carries no credentials
	pacer  *fs.Pacer
	tok    tokenState

	mu        sync.Mutex
	policies  map[string]*api.Policy // standard path of a folder -> its policy
	folderIDs map[string]string      // standard path of a folder -> its own ID
	links     map[string]*cachedURL  // URI -> a download link still worth using
	rewrites  []linkRewrite          // link_replace, in the order given
	acctID    string                 // this account's ID, as a lock holder names its owner

	uploadsMu sync.Mutex
	uploads   map[string]map[uint64]bool // server path of a file -> uploads working on it here
	uploadSeq uint64                     // the next ticket handed out above
}

// tokenState holds the session credentials
//
// The access token lives for an hour by default, so it is renewed on demand and
// written back to the config next to its refresh token.
type tokenState struct {
	mu            sync.Mutex
	access        string
	refresh       string
	accessExpires time.Time  // zero when the token came from the config
	canIssue      bool       // set when email and password can mint a new pair
	generation    int64      // counts pairs installed, so waiters need not spend another
	sessionErr    error      // set when the pair in hand turned out to be another account's
	renewMu       sync.Mutex // held for the whole of one renewal
}

// setSessionError stops this run from sending another request
//
// A pair traded from a token left in the config can turn out to belong to another
// account, and there is then nothing to replace it with. Failing the one call that
// noticed is not enough: the pair would stay where the next call finds it, look
// fresh, and never be asked about again.
func (ts *tokenState) setSessionError(err error) {
	ts.mu.Lock()
	if ts.sessionErr == nil {
		ts.sessionErr = err
	}
	ts.mu.Unlock()
}

// sessionError returns why this run must not go on, or nil
func (ts *tokenState) sessionError() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.sessionErr
}

// currentGeneration names the session pair held now
func (ts *tokenState) currentGeneration() int64 {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.generation
}

// accessToken returns the token to send in the Authorization header
func (ts *tokenState) accessToken() string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.access
}

// nearExpiry says whether the session is about to stop working
//
// Renewing before the deadline keeps a long transfer from dying halfway; a token
// taken from the config has no known deadline, so it is renewed only once the
// server refuses it.
func (ts *tokenState) nearExpiry() bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.accessExpires.IsZero() {
		return false
	}
	return time.Until(ts.accessExpires) < renewMargin
}

// setExpiry records when the token now in hand stops working
func (ts *tokenState) setExpiry(at time.Time) {
	ts.mu.Lock()
	ts.accessExpires = at
	ts.mu.Unlock()
}

// tokenExpiry reads the deadline an access token carries
//
// The signature is not checked, which only the site can do: this asks one thing
// of the token, when it dies. A token that answers nothing - hand written, or
// from a server that stopped issuing these - has no deadline, so nothing is
// decided from it: a refresh token beside it is what gets traded, and the token
// itself is used until the server says otherwise. The token never appears in what
// is written out.
func tokenExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return time.Time{}, false
	}
	payload := parts[1]
	if pad := len(payload) % 4; pad != 0 {
		payload += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return time.Time{}, false
	}
	claims := struct {
		Expires int64 `json:"exp"`
	}{}
	if err := json.Unmarshal(raw, &claims); err != nil || claims.Expires <= 0 {
		return time.Time{}, false
	}
	return time.Unix(claims.Expires, 0), true
}

// Object describes a Cloudreve file
type Object struct {
	fs      *Fs
	remote  string
	id      string
	size    int64
	modTime time.Time
}

// envelope wraps every API response
type envelope struct {
	Code          int             `json:"code"`
	Msg           string          `json:"msg"`
	Data          json.RawMessage `json:"data"`
	Error         string          `json:"error"`
	CorrelationID string          `json:"correlation_id"`
}

// apiError is a response whose code is not a success code
type apiError struct {
	code     int
	msg      string
	endpoint string // the call that produced it, when known
	id       string
	data     json.RawMessage // what the server sent alongside the code, when it sent anything
}

func (e *apiError) Error() string {
	where := ""
	if e.endpoint != "" {
		where = " " + e.endpoint
	}
	if e.id != "" {
		return fmt.Sprintf("cloudreve:%s %d %s (correlation id %s)", where, e.code, e.msg, e.id)
	}
	return fmt.Sprintf("cloudreve:%s %d %s", where, e.code, e.msg)
}

// siteRefused says whether the site answered, and the answer was a refusal
//
// A rate limit or a server-side failure arrives as an envelope too, so having a
// code is not enough: those are worth another request, and they say nothing about
// whether the refresh token is still good.
func siteRefused(err error) bool {
	if _, retry := pacer.IsRetryAfter(err); retry {
		return false
	}
	return codeOf(err) != 0
}

// codeOf returns the server's error code, or 0 when err is not an API error
func codeOf(err error) int {
	var e *apiError
	if errors.As(err, &e) {
		return e.code
	}
	return 0
}

// NewFs constructs an Fs from the path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f, err := newFs(ctx, name, root, m)
	if err != nil {
		return nil, err
	}
	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		// Names are told apart by case: two entries differing only in upper or
		// lower case were created and listed side by side on a live site, so this
		// is not declared case insensitive.
		//
		// The type of a file is sent with the upload but the API never reports it
		// back, so rclone is not told to read it.
		WriteMimeType: true,
	}).Fill(ctx, f)
	err = f.findRoot(ctx)
	if errors.Is(err, fs.ErrorIsFile) {
		// The root names a file, so the parent becomes the root and the
		// sentinel goes back to the caller, which is how it learns the remote
		// pointed at a file. Nothing here remembers which one: rclone caches
		// this Fs under the parent's name and hands it back when the directory
		// is asked for, so a per-file guard would refuse unrelated paths.
		dir, _ := path.Split(f.root)
		f.root = strings.Trim(dir, "/")
		f.rootExists = true
		return f, fs.ErrorIsFile
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

// newFs parses the config and builds the machinery, without resolving the root
func newFs(ctx context.Context, name, root string, m configmap.Mapper) (*Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	// A config map that never went through the option defaults - which is how a
	// bare map reaches here, as in the tests - carries no encoding at all, and an
	// empty encoding escapes nothing, leaving the server to refuse the name. The
	// RegInfo default covers the normal path; this covers the rest. An explicit
	// value, including 0, is honoured.
	if _, set := m.Get("encoding"); !set {
		opt.Enc = cloudreveDefaultEncoding
	}
	siteURL, err := url.Parse(opt.URL)
	if err != nil {
		return nil, fmt.Errorf("cloudreve: bad url %q: %w", opt.URL, err)
	}
	if siteURL.Host == "" {
		return nil, fmt.Errorf("cloudreve: url %q has no host - give the full site URL including the scheme, e.g. https://drive.example.com", opt.URL)
	}
	if siteURL.Scheme != "https" && siteURL.Scheme != "http" {
		return nil, fmt.Errorf("cloudreve: url %q must be http or https", opt.URL)
	}
	if opt.UploadConcurrency < 1 {
		return nil, fmt.Errorf("cloudreve: upload_concurrency must be at least 1, got %d", opt.UploadConcurrency)
	}
	if opt.Email == "" && opt.AccessToken == "" {
		return nil, errors.New("cloudreve: need either email and password, or access_token")
	}
	rewrites, err := parseLinkRewrites(opt.LinkReplace)
	if err != nil {
		return nil, err
	}
	if opt.Email != "" && opt.Password == "" && opt.AccessToken == "" {
		return nil, errors.New("cloudreve: email is set but password is not, so rclone cannot sign in")
	}
	if opt.Password != "" {
		// rclone keeps a password option obscured, in the config file and from
		// the command line alike, so the plain form has to be recovered before
		// it goes on the wire.
		revealed, err := obscure.Reveal(opt.Password)
		if err != nil {
			return nil, fmt.Errorf("cloudreve: can't decode the stored password - did you obscure it?: %w", err)
		}
		opt.Password = revealed
	}
	// A remote may name its own User-Agent. fshttp takes it from the config it is
	// handed and sets the header on every request, so the override goes on a copy
	// of the config that only these clients see - the global --user-agent keeps
	// applying to everything else in the run.
	clientCtx := ctx
	if opt.UserAgent != "" {
		var clientConfig *fs.ConfigInfo
		clientCtx, clientConfig = fs.AddConfig(ctx)
		clientConfig.UserAgent = opt.UserAgent
	}
	f := &Fs{
		name:      name,
		root:      strings.Trim(root, "/"),
		opt:       *opt,
		ci:        fs.GetConfig(ctx),
		cfg:       m,
		client:    fshttp.NewClient(clientCtx),
		bare:      fshttp.NewClient(clientCtx),
		policies:  map[string]*api.Policy{},
		folderIDs: map[string]string{},
		links:     map[string]*cachedURL{},
		rewrites:  rewrites,
		uploads:   map[string]map[uint64]bool{},
		tok:       tokenState{access: opt.AccessToken, refresh: opt.RefreshToken},
	}
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))
	// location is the prefix the remote's paths sit under, so a path given on
	// the command line is relative to it.
	if loc := strings.Trim(opt.Location, "/"); loc != "" {
		f.root = strings.Trim(path.Join(loc, f.root), "/")
	}
	if opt.Email != "" && opt.Password != "" {
		f.tok.canIssue = true
	}
	if err := f.openSession(ctx); err != nil {
		return nil, err
	}
	return f, nil
}

// The two ways an account check can come back wrong, kept apart because only the
// first is a reason to throw a working credential away. The second is the site
// answering without naming whose token it is, which is not evidence against it; a
// site that could not be asked at all returns its own error instead.
var (
	errAccountMismatch = errors.New("cloudreve: the stored token is not this account's")
	errAccountUnnamed  = errors.New("cloudreve: the site named no account for this token")
)

// openSession works out what this run has to do to hold a working credential, and
// does the cheapest thing that gets there
//
// Signing in costs three requests and opens a new session on the site that only
// age, a password change or a sign-out closes again, so a pair already in the
// config is kept if it can be kept: a token with its deadline still ahead of it is
// used as it stands, and one that is old or unanswerable about itself is traded for
// a new one in the single request that costs. A pair is given up to signing in only
// when the site says it belongs to another account, when the trade is refused, or
// when nothing about it can be established and there is a password to start over
// with.
//
// A pair that was traded rather than signed in for is asked about before it is
// used: the refresh token left in a config can belong to another account, and the
// pair it trades for works just as well on that account's drive. A pair the answer
// says is somebody else's is never written back to the config, so the run that
// stops over one leaves the file holding only what it was given. The same check
// runs after a renewal in the middle of a transfer, inside the lock that serialises
// renewals: asked outside it, every thread waiting on a renewal would ask the same
// question, and each one that signed in behind an unsatisfying answer would open
// another session on the site.
func (f *Fs) openSession(ctx context.Context) error {
	deadline, dated := tokenExpiry(f.tok.access)
	switch {
	case dated && time.Until(deadline) > 0:
		// Recording the deadline is only worth doing where something can act on
		// it. A pair with no refresh token and no password cannot be renewed, so a
		// deadline written down here would stop a run that still has a working
		// token in hand. How long before the deadline to renew is the request
		// path's, and is not decided twice.
		if f.tok.refresh != "" || f.tok.canIssue {
			f.tok.setExpiry(deadline)
		}
		err := f.confirmAccount(ctx)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, errAccountMismatch):
			if !f.tok.canIssue {
				return err // the one answer that must not be worked around
			}
			fs.Logf(f, "cloudreve: signing in as %s: %v", f.opt.Email, err)
			return f.issueToken(ctx)
		case f.tok.refresh == "" && !f.tok.canIssue:
			fs.Logf(f, "cloudreve: cannot check the stored token and has nothing to replace it with, using it: %v", err)
			return nil
		default:
			fs.Debugf(f, "cloudreve: cannot check the stored token, replacing it: %v", err)
		}
	case f.tok.access != "" && !dated && f.tok.refresh == "" && !f.tok.canIssue:
		// Nothing to trade this pair for and no password to start over with, so it
		// is all the run has. Where an account is named, whose token this is can
		// still be asked for one request, and that is worth it: the alternative is
		// working on somebody else's drive and saying nothing, and the shape of the
		// token is no guide to who it belongs to.
		if f.opt.Email == "" {
			return nil // no account named, so nothing to check it against
		}
		return f.confirmHeld(ctx)
	}
	if f.tok.refresh != "" {
		token, err := f.tradeRefresh(ctx)
		switch {
		case err == nil:
			return f.checkTraded(ctx, token)
		case !f.tok.canIssue:
			// The trade was refused, but nothing has shown the token in hand to be
			// dead: an access token stays good until its own deadline even once the
			// session behind the refresh token is gone. Letting the server be the
			// judge beats closing a remote that works today - and a refused trade
			// says nothing about whose token it is, so the account is still asked.
			fs.Logf(f, "cloudreve: cannot renew the stored token, using what is in hand until the server refuses it: %v", err)
			return f.confirmHeld(ctx)
		}
	}
	if f.tok.canIssue {
		return f.issueToken(ctx)
	}
	// Stale, and nothing to trade it with: let the server be the judge.
	return nil
}

// confirmHeld settles whose the pair in hand is, and lets the config keep it only
// once the site has said it belongs to the account the remote names
//
// Signing in is the way out when the answer says otherwise and there is a password
// to sign in with. Where there is none, a site that will not answer is not evidence
// against the pair, so it is used with a notice; a site that says it is somebody
// else's stops the run.
func (f *Fs) confirmHeld(ctx context.Context) error {
	err := f.confirmAccount(ctx)
	switch {
	case err == nil:
		f.saveTokens()
		return nil
	case errors.Is(err, errAccountMismatch):
		if !f.tok.canIssue {
			// Somebody else's pair, and no way to ask for our own. The one call that
			// noticed cannot be the only thing that stops: the pair stays where the
			// next call finds it, looks fresh, and would never be asked about again.
			f.tok.setSessionError(fmt.Errorf("%w: there is no password to sign in with, so the run stops rather than work on that account's drive", err))
			return err
		}
		fs.Logf(f, "cloudreve: signing in as %s: %v", f.opt.Email, err)
		if signIn := f.issueToken(ctx); signIn != nil {
			// The foreign pair is still installed and nothing replaced it.
			f.tok.setSessionError(fmt.Errorf("%w, and signing in as %s failed too: %v", err, f.opt.Email, signIn))
			return signIn
		}
		return nil
	case !f.tok.canIssue:
		fs.Logf(f, "cloudreve: cannot check the token in hand and has nothing to replace it with, using it: %v", err)
		f.saveTokens()
		return nil
	default:
		fs.Logf(f, "cloudreve: cannot check the token in hand, signing in instead: %v", err)
		if signIn := f.issueToken(ctx); signIn != nil {
			fs.Logf(f, "cloudreve: signing in failed too, so the token in hand is used until the site refuses it: %v", signIn)
			f.saveTokens()
		}
		return nil
	}
}

// tradeRefresh asks the site to swap the refresh token for a new pair, and does
// not fall back to signing in
//
// openSession needs the two apart: a pair that came from a sign-in has just been
// given to us by the account itself and needs no check afterwards, while a pair
// traded from a token left in the config still could belong to somebody else.
func (f *Fs) tradeRefresh(ctx context.Context) (*api.Token, error) {
	f.tok.mu.Lock()
	refresh := f.tok.refresh
	f.tok.mu.Unlock()
	if refresh == "" {
		return nil, errors.New("cloudreve: no refresh_token to trade")
	}
	token := new(api.Token)
	if err := f.plainCall(ctx, "POST", "/session/token/refresh", nil, &api.RefreshRequest{RefreshToken: refresh}, token); err != nil {
		return nil, err
	}
	if token.AccessToken == "" {
		return nil, errors.New("cloudreve: the refresh response carried no access_token")
	}
	return token, nil
}

// confirmAccount asks the site whose token this is, so a pair left in the config
// is kept with the answer in hand rather than taken on trust
//
// Reusing a stored pair skips signing in, so nothing else would notice a remote
// whose email was edited underneath it: the run would read and write somebody
// else's drive and report nothing. The answer also carries the account ID the lock
// rules ask about, which is cached here once the account is settled, so a pair
// turned out to be foreign leaves nothing wrong behind.
func (f *Fs) confirmAccount(ctx context.Context) error {
	return f.confirmToken(ctx, f.tok.accessToken())
}

// confirmToken asks the site whose the given access token is, sending that token
// itself. Nothing is installed here: a pair the site has not been asked about must
// never be the one the next request picks up.
func (f *Fs) confirmToken(ctx context.Context, access string) error {
	if f.opt.Email == "" {
		return nil // nothing to check against: a token with no account named is taken as given
	}
	me := new(api.CurrentUser)
	if err := f.oneCall(ctx, "GET", "/user/me", nil, nil, me, access); err != nil {
		// The site could not be asked, or would not answer this one, which is not
		// evidence that the pair is somebody else's. Refusing here would make a
		// remote that works today fail to open, so the caller decides.
		return err
	}
	if me.Email == "" {
		return fmt.Errorf("%w: the site named no email", errAccountUnnamed)
	}
	if !strings.EqualFold(me.Email, f.opt.Email) {
		return fmt.Errorf("%w: the stored token belongs to %s, not %s", errAccountMismatch, me.Email, f.opt.Email)
	}
	if me.ID != "" {
		f.mu.Lock()
		f.acctID = me.ID
		f.mu.Unlock()
	}
	return nil
}

// issueToken signs in with the configured account
//
// The account is asked how it can prove itself first: an account issued through
// SSO has no local password at all, and the server reports that with the same
// code it uses for a wrong password.
func (f *Fs) issueToken(ctx context.Context) error {
	site := new(api.LoginConfig)
	if err := f.plainCall(ctx, "GET", "/site/config/login", nil, nil, site); err != nil {
		return fmt.Errorf("cloudreve: can't read the site's login settings: %w", err)
	}
	if site.LoginCaptcha {
		return errors.New("cloudreve: the site asks for a login captcha, which rclone cannot answer: turn login captcha off in the admin settings, or set access_token")
	}

	query := url.Values{}
	query.Set("email", f.opt.Email)
	prepared := new(api.PrepareLogin)
	err := f.plainCall(ctx, "GET", "/session/prepare", query, nil, prepared)
	if err != nil {
		if codeOf(err) == api.CodeNotFound {
			return fmt.Errorf("cloudreve: %s has no account with email %q", f.opt.URL, f.opt.Email)
		}
		return fmt.Errorf("cloudreve: can't learn how %q signs in: %w", f.opt.Email, err)
	}
	// The site answers the two ways an account can prove itself separately, and a
	// passkey does not switch the password off: login is refused for a passkey
	// account only when it has no password to check. So the password decides, and a
	// passkey is named as the reason there is no password to post.
	if !prepared.PasswordEnabled {
		if prepared.WebauthnEnabled {
			return fmt.Errorf("cloudreve: the account %q signs in with a passkey and has no local password, so rclone cannot sign in: give it a password in the admin panel, or set access_token", f.opt.Email)
		}
		return fmt.Errorf("cloudreve: the account %q has no local password, so rclone cannot sign in: give it one in the admin panel, or set access_token", f.opt.Email)
	}

	data := new(api.LoginData)
	err = f.plainCall(ctx, "POST", "/session/token", nil, &api.LoginRequest{
		Email:    f.opt.Email,
		Password: f.opt.Password,
	}, data)
	if err != nil {
		switch code := codeOf(err); code {
		case api.CodeInvalidPassword:
			return fmt.Errorf("cloudreve: %s rejected this email and password pair: %w", f.opt.URL, err)
		case api.CodeCaptchaError, api.CodeCaptchaRefreshNeeded:
			return fmt.Errorf("cloudreve: the site started asking for a login captcha, which rclone cannot answer: turn login captcha off in the admin settings, or set access_token: %w", err)
		}
		return fmt.Errorf("cloudreve: can't sign in as %q: %w", f.opt.Email, err)
	}
	if data.Token.AccessToken == "" {
		return errors.New("cloudreve: the login response carried no access_token")
	}
	// A pair the account itself handed over needs no check afterwards, so it is
	// the one thing here that can be saved as it arrives.
	f.setTokens(data.Token.AccessToken, data.Token.RefreshToken, data.Token.AccessExpires)
	f.saveTokens()
	return nil
}

// renewToken trades the refresh token for a new pair
//
// One renewal runs at a time, and a caller whose pair was replaced while it
// queued is done: the token it would spend still works, but it has a newer one
// already, so renewing again would only add a pair nothing will use.
//
// The one that does trade is the one that asks whose pair came back, and writes it
// to the config if the answer is right. Both belong inside that same lock: asking
// outside it would have every queued caller ask the same question once, and a
// sign-in behind each of those answers would open as many sessions as the run has
// threads. The asking cannot renew again, because an identity request is sent
// once and for all.
func (f *Fs) renewToken(ctx context.Context, seen int64) error {
	f.tok.renewMu.Lock()
	defer f.tok.renewMu.Unlock()
	if f.tok.currentGeneration() != seen {
		// Somebody else renewed between the caller's request and this call.
		return nil
	}
	f.tok.mu.Lock()
	refresh := f.tok.refresh
	canIssue := f.tok.canIssue
	f.tok.mu.Unlock()
	if refresh == "" {
		if !canIssue {
			return errors.New("cloudreve: the session expired and there is no refresh_token to renew it: set email and password, or supply a fresh access_token")
		}
		return f.issueToken(ctx)
	}
	token := new(api.Token)
	err := f.plainCall(ctx, "POST", "/session/token/refresh", nil, &api.RefreshRequest{RefreshToken: refresh}, token)
	if err != nil {
		if canIssue && siteRefused(err) {
			// The site answered, and the answer refused the refresh: the whole
			// lineage is gone - expired, the password changed, the session signed
			// out, the account blocked or deleted - and none of that comes back by
			// asking again, so signing in is the way out.
			//
			// An answer that is not the site's is left alone: a dropped connection,
			// a proxy page, a 5xx. None of those say the refresh token is dead, and
			// signing in behind one would open a session for a network hiccup.
			return f.issueToken(ctx)
		}
		return fmt.Errorf("cloudreve: can't renew the session: %w", err)
	}
	if token.AccessToken == "" {
		return errors.New("cloudreve: the refresh response carried no access_token")
	}
	return f.checkTraded(ctx, token)
}

// checkTraded settles whose the pair the site just traded for is, and installs it
// only once that is settled
//
// Installing first and asking afterwards leaves a window in which a transfer
// running beside the renewal sends somebody else's token, and a failure then
// leaves that pair where the next call finds it. So the question is asked with the
// candidate token, and the pair becomes the run's own only if the answer is right.
func (f *Fs) checkTraded(ctx context.Context, token *api.Token) error {
	err := f.confirmToken(ctx, token.AccessToken)
	switch {
	case err == nil:
		f.setTokens(token.AccessToken, token.RefreshToken, token.AccessExpires)
		f.saveTokens()
		return nil
	case errors.Is(err, errAccountMismatch):
		if !f.tok.canIssue {
			// The pair is somebody else's and there is no password for our own. The
			// pair in hand before the trade is the account's own, so the run stops
			// rather than be worked around.
			f.tok.setSessionError(fmt.Errorf("%w: there is no password to sign in with, so the run stops rather than work on that account's drive", err))
			return err
		}
		fs.Logf(f, "cloudreve: signing in as %s: %v", f.opt.Email, err)
		if signIn := f.issueToken(ctx); signIn != nil {
			f.tok.setSessionError(fmt.Errorf("%w, and signing in as %s failed too: %v", err, f.opt.Email, signIn))
			return signIn
		}
		return nil
	case !f.tok.canIssue:
		// The site would not say, and there is nothing else to try: the pair is used
		// and the doubt is said out loud rather than kept quiet.
		fs.Logf(f, "cloudreve: cannot check the traded token and has nothing to replace it with, using it: %v", err)
		f.setTokens(token.AccessToken, token.RefreshToken, token.AccessExpires)
		f.saveTokens()
		return nil
	default:
		fs.Logf(f, "cloudreve: cannot check the traded token, signing in instead: %v", err)
		if signIn := f.issueToken(ctx); signIn != nil {
			fs.Logf(f, "cloudreve: signing in failed too, so the traded token is used until the site refuses it: %v", signIn)
			f.setTokens(token.AccessToken, token.RefreshToken, token.AccessExpires)
			f.saveTokens()
		}
		return nil
	}
}

// setTokens installs a new pair in the run's memory.
//
// What when is settled here for every pair. A site that says nothing about the
// life of the token it just handed over may still have sent one that says. When
// neither the answer nor the token speaks, the deadline is unknown rather than
// the one held before. Leaving that behind would make every request of a long
// run look about to expire, and each of them would spend a renewal.
//
// Nothing is written to the config here. A pair traded from a token left in the
// config is not known to be the account's own until confirmHeld has asked, and
// saving one before then leaves somebody else's credential in the user's file
// even when the run goes on to refuse it.
func (f *Fs) setTokens(access, refresh string, accessExpires time.Time) {
	if accessExpires.IsZero() {
		if at, dated := tokenExpiry(access); dated {
			accessExpires = at
		}
	}
	if !accessExpires.IsZero() && time.Until(accessExpires) < renewMargin {
		// Nothing can be done with a deadline this close: the next request, and
		// every one after it, would treat a freshly installed pair as about to
		// expire and spend a renewal. A site issuing tokens shorter than the margin
		// would then renew once per request. Such a pair is used until the site
		// refuses it instead.
		accessExpires = time.Time{}
	}
	f.tok.mu.Lock()
	f.tok.access = access
	if refresh != "" {
		f.tok.refresh = refresh
	}
	f.tok.accessExpires = accessExpires
	f.tok.generation++
	f.tok.mu.Unlock()
}

// saveTokens puts the pair in hand into the remote's config, which is what keeps
// the stored credential from ageing out between runs.
//
// The site keeps a refresh token working until it expires on its own, so a renewal
// that never reached the config costs the next run the one request that trades it
// again, not a sign-in. Writing is skipped when the config already resolves to this
// pair, so a run that changed nothing does not rewrite the file behind it.
func (f *Fs) saveTokens() {
	if f.cfg == nil {
		return
	}
	f.tok.mu.Lock()
	access, refresh := f.tok.access, f.tok.refresh
	f.tok.mu.Unlock()
	if old, _ := f.cfg.Get("access_token"); old == access {
		if old, _ := f.cfg.Get("refresh_token"); old == refresh {
			return
		}
	}
	f.cfg.Set("access_token", access)
	f.cfg.Set("refresh_token", refresh)
}

// findRoot checks the configured root exists, reporting fs.ErrorIsFile when it
// names a file
func (f *Fs) findRoot(ctx context.Context) error {
	if f.opt.RootFolderID != "" {
		file, err := f.statBy(ctx, map[string]string{"id": f.opt.RootFolderID})
		if err != nil {
			return fmt.Errorf("cloudreve: can't resolve root_folder_id %q: %w", f.opt.RootFolderID, err)
		}
		under, err := f.relativeToDriveRoot(ctx, file)
		if err != nil {
			return err
		}
		if f.root != "" {
			return fmt.Errorf("cloudreve: root_folder_id and a path root cannot both be set: %q and %q", f.opt.RootFolderID, f.root)
		}
		f.root = under
		f.rootExists = true
		if file.Type == api.FileTypeFolder {
			return nil
		}
		return fs.ErrorIsFile
	}
	if f.root == "" {
		// The top of the drive always exists.
		f.rootExists = true
		return nil
	}
	file, err := f.statDrivePath(ctx, f.root)
	if err != nil {
		// A root that is not there yet is not a reason to refuse the remote:
		// rclone builds the destination Fs before it makes the directory, so
		// mkdir of a new path arrives here first. Listing it later still
		// reports fs.ErrorDirNotFound.
		if code := codeOf(err); code == api.CodeNotFound || code == api.CodeParentNotExist {
			return nil
		}
		return err
	}
	f.rootExists = true
	if file.Type == api.FileTypeFolder {
		return nil
	}
	return fs.ErrorIsFile
}

// relativeToDriveRoot turns a server-reported URI into a path under this Fs
func (f *Fs) relativeToDriveRoot(ctx context.Context, file *api.File) (string, error) {
	std, err := f.fromURIPath(file.Path)
	if err != nil {
		return "", err
	}
	if std == "" {
		return "", errors.New("cloudreve: the folder resolves to the top of the drive, which has no path to root a remote at")
	}
	return std, nil
}

// Name of the remote as passed on command line
func (f *Fs) Name() string { return f.name }

// Root of the remote as passed on command line
func (f *Fs) Root() string { return f.root }

// String describes the remote
func (f *Fs) String() string {
	return fmt.Sprintf("cloudreve(%s)", f.root)
}

// Features describes the optional behaviour of this backend
func (f *Fs) Features() *fs.Features { return f.features }

// Precision is one second: the server keeps the last_modified of an upload as a
// whole second, and there is no API to change it afterwards.
func (f *Fs) Precision() time.Duration { return time.Second }

// Hashes is empty: no content hash comes back from the API
func (f *Fs) Hashes() hash.Set { return hash.NewHashSet() }

// drivePath returns the path of remote within the whole drive
//
// A single-dot path, which path.Dir gives for a name at the top of the drive,
// is the drive itself.
func (f *Fs) drivePath(remote string) string {
	if remote == "." {
		remote = ""
	}
	if f.root == "" {
		return strings.Trim(remote, "/")
	}
	if remote == "" {
		return f.root
	}
	return f.root + "/" + remote
}

// uriFor returns the cloudreve:// URI of the standard path remote
func (f *Fs) uriFor(remote string) string {
	return f.uriForDrivePath(f.drivePath(remote))
}

// uriForDrivePath returns the cloudreve:// URI of a path that is already
// relative to the top of the drive
func (f *Fs) uriForDrivePath(drivePath string) string {
	return driveRoot + f.encodeURIPath(drivePath)
}

// encodeURIPath maps each segment through the name encoding this backend uses,
// then percent-encodes it for the URI
func (f *Fs) encodeURIPath(p string) string {
	if p == "" {
		return ""
	}
	segments := strings.Split(p, "/")
	for i, segment := range segments {
		segments[i] = url.PathEscape(f.opt.Enc.FromStandardName(segment))
	}
	return strings.Join(segments, "/")
}

// statDrivePath stats an entry addressed by its path from the top of the drive,
// which is how the root itself is looked up
func (f *Fs) statDrivePath(ctx context.Context, drivePath string) (*api.File, error) {
	return f.statBy(ctx, map[string]string{"uri": f.uriForDrivePath(drivePath)})
}

// fromURIPath turns the URI the server reports into a standard path relative to
// the top of the drive
//
// The server percent-encodes each segment of the path it returns, and rclone may
// have stored the name under its encoded spelling, so each segment is unescaped
// and then mapped back through the name encoding.
func (f *Fs) fromURIPath(uri string) (string, error) {
	rest, found := strings.CutPrefix(uri, driveRoot)
	if !found {
		// The server may answer without the trailing slash for the drive root.
		if rest2, found2 := strings.CutPrefix(strings.TrimSuffix(uri, "/"), strings.TrimSuffix(driveRoot, "/")); found2 {
			rest = rest2
		} else {
			return "", fmt.Errorf("cloudreve: unexpected URI %q", uri)
		}
	}
	segments := strings.Split(rest, "/")
	for i, segment := range segments {
		decoded, err := url.PathUnescape(segment)
		if err != nil {
			return "", fmt.Errorf("cloudreve: can't decode path segment %q: %w", segment, err)
		}
		segments[i] = f.opt.Enc.ToStandardName(decoded)
	}
	std := path.Clean("/" + strings.Join(segments, "/"))
	return std[1:], nil
}

// policyForDrive returns the storage policy in force for the folder at the given
// path from the top of the drive, remembering what it saw
func (f *Fs) policyForDrive(ctx context.Context, drivePath string) (*api.Policy, error) {
	f.mu.Lock()
	policy, found := f.policies[drivePath]
	f.mu.Unlock()
	if found {
		return policy, nil
	}
	list := new(api.ListResponse)
	query := url.Values{}
	query.Set("uri", f.uriForDrivePath(drivePath))
	query.Set("page_size", "1")
	if err := f.call(ctx, "GET", "/file", query, nil, list); err != nil {
		// A folder that is not there is reported with rclone's sentinel, so that
		// the caller's own "directory not found" handling applies rather than a
		// bare server code.
		if code := codeOf(err); code == api.CodeNotFound || code == api.CodeParentNotExist {
			return nil, fs.ErrorDirNotFound
		}
		return nil, err
	}
	if list.StoragePolicy == nil {
		return nil, fmt.Errorf("cloudreve: folder %q reported no storage policy", drivePath)
	}
	f.mu.Lock()
	if len(f.policies) > maxCachedPolicies {
		clear(f.policies)
		clear(f.folderIDs)
	}
	f.policies[drivePath] = list.StoragePolicy
	// The listing answers with the folder itself as its parent, so the folder's
	// ID comes free with the policy - which is what root_folder_id wants.
	if list.Parent.ID != "" {
		f.folderIDs[drivePath] = list.Parent.ID
	}
	f.mu.Unlock()
	return list.StoragePolicy, nil
}

// folderID returns the site's own ID for the folder at the given drive path, as
// learned while resolving its policy, or "" when the drive never named it
func (f *Fs) folderID(drivePath string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.folderIDs[drivePath]
}

// forgetPolicies drops every remembered storage policy, which is what binding a
// folder to a new one calls for
func (f *Fs) forgetPolicies() {
	f.mu.Lock()
	clear(f.policies)
	clear(f.folderIDs)
	f.mu.Unlock()
}

// writableDirPolicy returns the policy writes into the folder at dirPath land
// on, making the folder when the drive has never seen it, and refusing anything
// rclone cannot upload through.
//
// Creating the folder is part of the write contract: rclone expects an upload to
// make the folders its path names, as the FTP and local backends do.
func (f *Fs) writableDirPolicy(ctx context.Context, dirPath string) (*api.Policy, error) {
	policy, err := f.writablePolicyForDir(ctx, dirPath)
	if errors.Is(err, fs.ErrorDirNotFound) && dirPath != "" {
		// The drive's own top is the only thing above a path with no slash in it,
		// and rclone cannot make that.
		if mkErr := f.mkdirDrivePath(ctx, dirPath); mkErr != nil {
			return nil, mkErr
		}
		return f.writablePolicyForDir(ctx, dirPath)
	}
	return policy, err
}

// writablePolicy returns the policy an upload of remote lands on, making the
// folder that is to hold it, and refusing anything that is not S3.
func (f *Fs) writablePolicy(ctx context.Context, remote string) (*api.Policy, error) {
	// path.Dir gives "." for a name at the top of the drive, which is the drive
	// itself: addressed empty, not as a segment spelled ".".
	dir := path.Dir(f.drivePath(remote))
	if dir == "." {
		dir = ""
	}
	return f.writableDirPolicy(ctx, dir)
}

// writablePolicyForDir returns the policy a file written into the folder at the
// given drive path lands on, refusing anything that is not S3.
//
// A load_balance folder is a pool the server chooses from per upload, so the
// child is picked here and named in the upload session: leaving it to the site
// would hand the file to whichever policy the pool favours, and rclone can only
// push bytes through S3.
func (f *Fs) writablePolicyForDir(ctx context.Context, dirPath string) (*api.Policy, error) {
	policy, err := f.policyForDrive(ctx, dirPath)
	if err != nil {
		return nil, err
	}
	effective := policy
	if effective.Type == policyTypeLoadBalance {
		child := chooseS3Child(policy)
		if child == nil {
			return nil, fmt.Errorf("cloudreve: folder %q uses the pool %q, which has no %q child to write through: %s",
				dirPath, policy.Name, PolicyTypeS3, policyList(policy.Children))
		}
		effective = child
	}
	if effective.Type != PolicyTypeS3 {
		return nil, fmt.Errorf("cloudreve: folder %q uses storage policy %q, and rclone can only write through the %q policy",
			dirPath, effective.Type, PolicyTypeS3)
	}
	return effective, nil
}

// chooseS3Child picks at random among the children that write through S3, so
// that a pool still spreads files across its buckets the way the site intends
func chooseS3Child(pool *api.Policy) *api.Policy {
	var candidates []*api.Policy
	for i := range pool.Children {
		if pool.Children[i].Type == PolicyTypeS3 {
			candidates = append(candidates, &pool.Children[i])
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return candidates[rand.Intn(len(candidates))]
}

// policyList names a pool's children, so a refusal says what is available
func policyList(children []api.Policy) string {
	if len(children) == 0 {
		return "no children"
	}
	names := make([]string, 0, len(children))
	for _, child := range children {
		names = append(names, fmt.Sprintf("%s(%s)", child.Name, child.Type))
	}
	return strings.Join(names, ", ")
}

// statBy asks for the metadata of one entry by query parameters
func (f *Fs) statBy(ctx context.Context, params map[string]string) (*api.File, error) {
	file, err := f.entryBy(ctx, params)
	if err != nil {
		return nil, err
	}
	if uploading(file) {
		// The name is claimed by an upload that never finished: the entry is a
		// placeholder with no bytes to open, and rclone has to treat it as
		// absent so the name is written again rather than compared against it.
		return nil, fs.ErrorObjectNotFound
	}
	return file, nil
}

// entryBy is statBy without the placeholder filter: it answers for an entry even
// while an unfinished upload owns it
func (f *Fs) entryBy(ctx context.Context, params map[string]string) (*api.File, error) {
	query := url.Values{}
	for key, value := range params {
		query.Set(key, value)
	}
	file := new(api.File)
	if err := f.call(ctx, "GET", "/file/info", query, nil, file); err != nil {
		return nil, err
	}
	return file, nil
}

// extendedAtServerPath reads one entry together with its versions, which is what
// dates an upload in progress
func (f *Fs) extendedAtServerPath(ctx context.Context, serverPath string) (*api.FileExtended, error) {
	query := url.Values{}
	query.Set("uri", driveRoot+strings.TrimPrefix(serverPath, "/"))
	query.Set("extended", "true")
	file := new(api.FileExtended)
	if err := f.call(ctx, "GET", "/file/info", query, nil, file); err != nil {
		return nil, err
	}
	return file, nil
}

// serverPath is the path the site reports for a remote name, which is the form a
// lock conflict carries: the encoded segments, but not the percent-escaping the
// URI needs for transport
func (f *Fs) serverPath(remote string) string {
	uri := f.uriFor(remote)
	unescaped, err := url.PathUnescape(uri)
	if err == nil {
		uri = unescaped
	}
	return strings.Trim(strings.TrimPrefix(uri, driveRoot), "/")
}

// serverPathsOf converts URIs the caller already holds into the paths a conflict
// names, so they can be matched against each other
func serverPathsOf(uris []string) []string {
	paths := make([]string, 0, len(uris))
	for _, uri := range uris {
		unescaped, err := url.PathUnescape(uri)
		if err != nil {
			unescaped = uri
		}
		paths = append(paths, strings.Trim(strings.TrimPrefix(unescaped, driveRoot), "/"))
	}
	return paths
}

// statFile returns the metadata of the entry at the standard path remote
func (f *Fs) statFile(ctx context.Context, remote string) (*api.File, error) {
	return f.statBy(ctx, map[string]string{"uri": f.uriFor(remote)})
}

// uploading reports whether a listed entry is the placeholder an unfinished
// upload leaves behind rather than a file anyone can read
func uploading(entry *api.File) bool {
	return entry.Metadata[api.MetadataUploadSessionID] != ""
}

// accountID returns this account's own ID, which is what a lock holder gives as
// its owner
func (f *Fs) accountID(ctx context.Context) (string, error) {
	f.mu.Lock()
	id := f.acctID
	f.mu.Unlock()
	if id != "" {
		return id, nil
	}
	me := new(api.CurrentUser)
	if err := f.call(ctx, "GET", "/user/me", nil, nil, me); err != nil {
		return "", err
	}
	if me.ID == "" {
		return "", errors.New("cloudreve: the site named no account ID")
	}
	f.mu.Lock()
	f.acctID = me.ID
	f.mu.Unlock()
	return me.ID, nil
}

// conflictsIn decodes the holders a lock conflict names, which is how a client
// learns the token it has to release
func conflictsIn(data json.RawMessage) []api.LockConflict {
	if len(data) == 0 {
		return nil
	}
	conflicts := []api.LockConflict{}
	if json.Unmarshal(data, &conflicts) != nil {
		return nil
	}
	return conflicts
}

// lockConflicts returns the holders a lock conflict error named
func lockConflicts(err error) []api.LockConflict {
	var e *apiError
	if !errors.As(err, &e) {
		return nil
	}
	return conflictsIn(e.data)
}

// plainCall makes one API request without a bearer token, for the endpoints that
// issue it
func (f *Fs) plainCall(ctx context.Context, method, endpoint string, query url.Values, in, out interface{}) error {
	return f.oneCall(ctx, method, endpoint, query, in, out, "")
}

// tokenToSend returns the token a request should carry, renewing first when there
// is none
func (f *Fs) tokenToSend(ctx context.Context) (string, error) {
	seen := f.tok.currentGeneration()
	token := f.tok.accessToken()
	if token != "" {
		return token, nil
	}
	if err := f.renewToken(ctx, seen); err != nil {
		return "", err
	}
	return f.tok.accessToken(), nil
}

// oneCall sends one request and reads one reply
func (f *Fs) oneCall(ctx context.Context, method, endpoint string, query url.Values, in, out interface{}, token string) error {
	var body []byte
	if in != nil {
		var err error
		body, err = json.Marshal(in)
		if err != nil {
			return fmt.Errorf("cloudreve: can't encode request for %s: %w", endpoint, err)
		}
	}
	// Sign-in and renewal go through the pacer like any other call: what breaks
	// them is whatever answers in front of the site dropping connections, and that
	// clears on its own.
	return f.pacer.Call(func() (bool, error) {
		env, err := f.request(ctx, method, endpoint, query, body, token)
		if err != nil {
			return f.shouldRetry(ctx, err)
		}
		if env.Code != api.CodeSuccess {
			return f.shouldRetry(ctx, f.apiError(method, endpoint, query, env))
		}
		if out != nil && len(env.Data) > 0 {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return false, fmt.Errorf("cloudreve: can't decode response from %s: %w", endpoint, err)
			}
		}
		return false, nil
	})
}

// call makes an authenticated API request, renewing the session once when the
// server reports it has expired
func (f *Fs) call(ctx context.Context, method, endpoint string, query url.Values, in, out interface{}) error {
	attempt, err := f.attempt(ctx, method, endpoint, query, in, out)
	if err != nil {
		return err
	}
	return f.pacer.Call(attempt)
}

// callOnce is call limited to a single delivery, for the endpoints that do
// something once and for all
//
// Filing an upload is one of them: when its reply is lost, repeating the request
// is refused as though the first had never worked, because the server released
// the name as it filed the file. rclone would then read that refusal as a failed
// upload and send the whole file again.
func (f *Fs) callOnce(ctx context.Context, method, endpoint string, query url.Values, in, out interface{}) error {
	attempt, err := f.attempt(ctx, method, endpoint, query, in, out)
	if err != nil {
		return err
	}
	_, err = attempt()
	return err
}

// attempt prepares the closure that sends one request and reads one reply
func (f *Fs) attempt(ctx context.Context, method, endpoint string, query url.Values, in, out interface{}) (func() (bool, error), error) {
	if err := f.tok.sessionError(); err != nil {
		return nil, err // the pair in hand belongs to somebody else: nothing may be sent
	}
	var body []byte
	if in != nil {
		var err error
		body, err = json.Marshal(in)
		if err != nil {
			return nil, fmt.Errorf("cloudreve: can't encode request for %s: %w", endpoint, err)
		}
	}
	renewed := false
	if seen := f.tok.currentGeneration(); f.tok.nearExpiry() {
		if err := f.renewToken(ctx, seen); err != nil {
			return nil, err
		}
		renewed = true
	}
	return func() (bool, error) {
		seen := f.tok.currentGeneration()
		if err := f.tok.sessionError(); err != nil {
			return false, err // the pair turned out to be somebody else's: nothing more goes out
		}
		token, err := f.tokenToSend(ctx)
		if err != nil {
			return false, err
		}
		env, err := f.request(ctx, method, endpoint, query, body, token)
		if err != nil {
			return f.shouldRetry(ctx, err)
		}
		if env.Code == api.CodeSuccess || env.Code == api.CodeNotFullySuccess {
			if out == nil || len(env.Data) == 0 {
				return false, nil
			}
			if err := json.Unmarshal(env.Data, out); err != nil {
				return false, fmt.Errorf("cloudreve: can't decode response from %s: %w", endpoint, err)
			}
			return false, nil
		}
		if env.Code == api.CodeCheckLogin && !renewed {
			renewed = true
			if err := f.renewToken(ctx, seen); err != nil {
				return false, err
			}
			return true, f.apiError(method, endpoint, query, env)
		}
		return f.shouldRetry(ctx, f.apiError(method, endpoint, query, env))
	}, nil
}

// apiError makes an error out of a non-success envelope, naming the call it came
// from because the same code can arrive from several endpoints
func (f *Fs) apiError(method, endpoint string, query url.Values, env envelope) error {
	where := method + " " + endpoint
	if len(query) > 0 {
		// The query is the path being addressed, which is what makes the error
		// actionable; it never carries credentials.
		where += "?" + query.Encode()
	}
	if env.Code == api.CodeLockConflict {
		// Two different answers share this code, and telling them apart is the
		// whole difference between waiting a day and retrying now. With a list of
		// holders the name is still claimed by a session. Without one the server
		// is saying the lock is gone, which is what a callback hears when the file
		// was already filed and filing it released the name.
		if len(conflictsIn(env.Data)) == 0 {
			env.Msg += " - no upload session holds this name now, so the upload it belonged to may already be filed"
		} else {
			env.Msg += " - held by an upload session until the server's upload_session_timeout passes"
		}
	}
	return &apiError{
		code:     env.Code,
		msg:      env.Msg,
		endpoint: where,
		id:       env.CorrelationID,
		data:     env.Data,
	}
}

// request sends one HTTP request and reads the whole reply
func (f *Fs) request(ctx context.Context, method, endpoint string, query url.Values, body []byte, token string) (env envelope, err error) {
	u := f.endpoint(endpoint)
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, reader)
	if err != nil {
		return env, err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return env, fmt.Errorf("cloudreve: %s %s: %w", method, endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return env, pacer.RetryAfterError(fmt.Errorf("cloudreve: reading reply of %s %s: %w", method, endpoint, err), time.Second)
	}
	if err := json.Unmarshal(data, &env); err != nil {
		// Anything that is not the usual envelope came from whatever answers in
		// front of the site - a proxy page, a redirect, an error document. That is
		// transient, so it is worth another request rather than a failure, and
		// such a page states how long to wait in its headers: a rate limit is
		// almost never JSON.
		snippet := strings.TrimSpace(string(data))
		if len(snippet) > 200 {
			snippet = snippet[:200] + "..."
		}
		return env, pacer.RetryAfterError(fmt.Errorf("cloudreve: %s %s: HTTP %d, reply is not JSON: %s", method, endpoint, resp.StatusCode, snippet), retryAfter(resp))
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return env, pacer.RetryAfterError(f.apiError(method, endpoint, query, env), retryAfter(resp))
	}
	if resp.StatusCode >= 500 {
		return env, pacer.RetryAfterError(f.apiError(method, endpoint, query, env), time.Second)
	}
	return env, nil
}

// endpoint builds the full URL of an API path
func (f *Fs) endpoint(endpoint string) string {
	return strings.TrimRight(f.opt.URL, "/") + apiPrefix + endpoint
}

// retryAfter returns how long a rate limit reply asked to wait
func retryAfter(resp *http.Response) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second
}

// shouldRetry decides whether err is worth another request
func (f *Fs) shouldRetry(ctx context.Context, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	if _, isRetryAfter := pacer.IsRetryAfter(err); isRetryAfter {
		// The request layer marked this as worth coming back for - a rate limit,
		// a server-side failure, or a reply that never came from the API.
		return true, err
	}
	if codeOf(err) != 0 {
		// A business error is an answer, not a fault: repeating it costs a
		// request and cannot change the outcome.
		return false, err
	}
	if fserrors.ShouldRetry(err) {
		return true, err
	}
	return false, err
}

// The interfaces this backend implements, so that renaming or dropping a method
// fails the build instead of quietly removing a capability rclone advertises.
var (
	_ fs.Fs           = (*Fs)(nil)
	_ fs.ListRer      = (*Fs)(nil)
	_ fs.Purger       = (*Fs)(nil)
	_ fs.DirMover     = (*Fs)(nil)
	_ fs.PublicLinker = (*Fs)(nil)
	_ fs.Abouter      = (*Fs)(nil)
	_ fs.Commander    = (*Fs)(nil)
	_ fs.Object       = (*Object)(nil)
	_ fs.IDer         = (*Object)(nil)
)
