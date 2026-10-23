// Package cloud189 provides an interface to the China Telecom cloud drive
// (天翼云盘).
package cloud189

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/cloud189/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"golang.org/x/sync/errgroup"
)

// ------------------------------------------------------------ constants ----

const (
	minSleep      = 10 * time.Millisecond
	maxSleep      = 2 * time.Second
	decayConstant = 2

	// listPageSize is the page size the official client asks for. The older web
	// portal endpoint answered 1000 at a time, but this API caps a page below
	// what we ask for, and a page that comes back short is indistinguishable
	// from the last page - which is how a listing gets truncated and sync then
	// deletes files it believes are extra. So ask for what the client asks for.
	listPageSize = 200
	maxListPages = 1000

	// personalRoot is the id the personal drive's top level answers to. The
	// family drive has no such constant: alias 0 works for listing and for a
	// task target, but createFolder refuses it, so the real id is read off a
	// listing when one is available.
	personalRoot    = "-11"
	familyRootAlias = "0"

	// The part size is not the client's choice: the server accepts exactly one
	// part size per file size and refuses any other value with InvalidPartSize.
	// The rule is a ladder anchored on 10MiB - below ~9.75GiB the part size is
	// 10MiB, up to ~19.5GiB it is 20MiB, and above that it is 5 or more times
	// 10MiB - measured 2026-10-22 by declaring sizes without transferring any
	// bytes. chunkPlan implements that rule, so the invariant it enforces is
	// the one the server checks: no more than ~999 parts for small files and
	// ~1999 for big ones.
	defaultChunkSize = fs.SizeSuffix(10 * 1024 * 1024)
	minChunkSize     = int64(10 * 1024 * 1024)

	// maxChunkSize bounds the option so the ladder arithmetic below cannot
	// overflow an int64 while multiplying it; no file rclone can carry needs a
	// base anywhere near it either.
	maxChunkSize = int64(1) << 40

	// Ladder steps, as measured. 1, 2, 4, 5, 8, 16, 32 and 64 MiB are all
	// refused for a small file, so a base below 10MiB is not merely unusual, it
	// can never be valid.
	ladderPartsLow  = 999
	ladderPartsHigh = 1999
	ladderMinScale  = 5

	// Download links are minted per space with a lifetime the client cannot
	// choose: the personal link's expired parameter sits about 5 minutes after
	// it was issued, the family link's about 10 hours after (captured
	// 2026-10-22). The cache windows below are well inside those lives, so a
	// cached URL is still valid when it is handed out.
	personalURLTTL = 3 * time.Minute
	familyURLTTL   = 8 * time.Hour

	// appID is the client application id every request carries.
	appID = "9317140619"

	// desktopUserAgent is what the client sends for its REST calls. The storage
	// layer sees a different one, and rclone replays it there.
	desktopUserAgent = "desktop"
	dartUserAgent    = "Dart/2.19 (dart:io)"

	// clientSuffix is appended to the query of every signed request. The version
	// is the captured client's own build.
	clientSuffix = "clientType=TELEPC&version=7.2.4.0&channelId=web_cloud.189.cn"

	// timeLayout is how the drive writes timestamps: "2026-10-22 19:17:59", in
	// the account's own zone (UTC+8 in every capture).
	timeLayout = "2006-01-02 15:04:05"
)

// apiHost serves the file API; uploadHost serves the multipart upload session.
// The storage hosts inside a part's presigned URL are chosen by the server and
// are never named here.
const (
	apiHost    = "https://api.cloud.189.cn"
	uploadHost = "https://upload.cloud.189.cn"
)

// Server-side refusal codes.
//
// They arrive in three different JSON fields depending on the endpoint, which
// is why classification reads all of them: session refusals use errorCode, the
// REST endpoints use res_code, and the upload session endpoints use code. Every
// one of them is seen with an HTTP status that says the same thing, so the
// status is kept in the error too.
const (
	codeInvalidSessionKey = "InvalidSessionKey" // HTTP 400: the session expired
	codeInvalidArgument   = "InvalidArgument"   // HTTP 400: a parameter this endpoint does not accept
	codeFileAlreadyExists = "FileAlreadyExists" // HTTP 400: a rename onto a name in use
	codeNoSpace           = "InsufficientStorageSpace"
	codeDayFlowOver       = "UserDayFlowOverLimited"
	// codeLocalSizeCeiling is this client's own refusal, not one the drive ever
	// sends: a file larger than the single-file ceiling in force. It is deliberately
	// not one of the two codes above, because it says nothing about the account's
	// state and must not end a run under fatal_if_no_space.
	codeLocalSizeCeiling = "LocalSingleFileCeiling"
	// quotaReAskGap is how long the gates wait before going back to the account
	// for numbers they either never managed to read or had reason to doubt. Both
	// re-reads are per run, not per object: a long run would otherwise pay a
	// metadata pair to every file for an answer that is the same each time.
	quotaReAskGap        = time.Minute
	codeSliceMd5Mismatch = "SliceMd5DoesNotMatch"
	codeInvalidPartSize  = "InvalidPartSize"
	codeInvalidSignature = "InvalidSignature"
)

// ------------------------------------------------------------ options ------

// Space names for the "space" option.
const (
	spacePersonal = "personal"
	spaceFamily   = "family"
)

// Options defines the configuration for this backend
type Options struct {
	AccessToken       string               `config:"access_token"`
	Space             string               `config:"space"`
	FamilyID          string               `config:"family_id"`
	RootFolderID      string               `config:"root_folder_id"`
	NoRefresh         bool                 `config:"no_refresh"`
	HardDelete        bool                 `config:"hard_delete"`
	ChunkSize         fs.SizeSuffix        `config:"chunk_size"`
	UploadConcurrency int                  `config:"upload_concurrency"`
	MaxFileSize       fs.SizeSuffix        `config:"max_file_size"`
	NoQuotaCheck      bool                 `config:"no_quota_check"`
	FatalIfNoSpace    bool                 `config:"fatal_if_no_space"`
	UploadZone        string               `config:"upload_zone"`
	DisableHTTP2      bool                 `config:"disable_http2"`
	Enc               encoder.MultiEncoder `config:"encoding"`
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "cloud189",
		Description: "China Telecom Cloud Drive (天翼云盘)",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name: "access_token",
			Help: "The 189 access token.\n\n" +
				"A 32 character hex token, found in the response of `getSessionForPC.action` when the " +
				"official client or a web login exchanges credentials for a session. It is the long-lived " +
				"credential: exchanging it yields a fresh session without touching the login again, and " +
				"the exchange does not revoke the session it replaces.\n\n" +
				"Capture it from an existing ecloud PC install, or from an OpenList 189pc storage config.",
			Sensitive: true,
		}, {
			Name:    "space",
			Help:    "Which storage space to use.",
			Default: spacePersonal,
			Examples: []fs.OptionExample{{
				Value: spacePersonal,
				Help:  "Personal drive (个人云)",
			}, {
				Value: spaceFamily,
				Help:  "Family drive (家庭云)",
			}},
		}, {
			Name: "family_id",
			Help: "Family drive ID (required when space is family).\n\n" +
				"A numeric id, e.g. 900000001. With exactly one family drive it may be left blank and is " +
				"auto-discovered; with several it must be set, and the backend refuses to guess.",
			Advanced: true,
		}, {
			Name: "root_folder_id",
			Help: "ID of the directory to use as the root of this remote. Leave blank for the top level.\n\n" +
				"The family drive is the case where this matters: a directory there has two ids, the alias " +
				"`0` that listings and task targets accept and the real id that createFolder demands, and " +
				"the real one is normally read off a listing. A brand new empty family drive has nothing to " +
				"read it from, so put it here.",
			Advanced: true,
		}, {
			Name: "no_refresh",
			Help: "Never exchange the access token again once the remote has been opened.\n\n" +
				"By default a request the server refuses with InvalidSessionKey is retried once on a freshly " +
				"exchanged session. That is safe to share: measured 2026-10-22, exchanging again keeps the " +
				"secret and hands out a new session key while the old key keeps working, so the official " +
				"client is not signed out. Set this if you want rclone to fail loudly instead of touching " +
				"the login at all.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "hard_delete",
			Help: "Delete permanently instead of leaving the files in the recycle bin.\n\n" +
				"This drive has no delete parameter that skips the bin, so rclone deletes as usual and then " +
				"clears exactly the entries it just put there from the bin. What is cleared cannot be " +
				"restored, and the space is only released at that point. Left to false, a deleted file " +
				"sits in the bin for the account's retention window and still counts against the " +
				"quota; that window is the recycleDay privilege, measured 10 days on a plain account " +
				"and 60 on the same account after an upgrade.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "chunk_size",
			Help: "Base of the part size ladder used for uploading.\n\n" +
				"The server demands one specific part size per file size and refuses every other value " +
				"with InvalidPartSize, so this is the base of that ladder rather than a free choice: " +
				"under about 9.75 GiB the part size is this value, up to about 19.5 GiB it is twice it, " +
				"and above that it is a multiple of it, all so that the part count stays inside what the " +
				"server accepts (999 parts, 1999 above the second step). Measured 2026-10-22 by declaring " +
				"sizes without transferring them: with the default 10Mi, a 20Mi part size is refused for a " +
				"20Mi and a 2Gi file and accepted for an 11Gi one.\n\n" +
				"Only lower it if the account's rule has changed under you, and expect the server to say " +
				"so. Values below 10Mi were measured to be refused outright (1, 2, 4, 5, 8, 16, 32 and " +
				"64Mi are all InvalidPartSize for a small file), and 1048577 - one byte more than the " +
				"step - is refused too.\n\n" +
				"Note that a file no larger than one part is read into memory whole, and anything " +
				"larger is staged to a temporary file first, so this value also sets the size of " +
				"the block rclone holds in memory for each of the " +
				"--cloud189-upload-concurrency parts that are in flight.",
			Default:  defaultChunkSize,
			Advanced: true,
		}, {
			Name: "upload_concurrency",
			Help: "Concurrency for chunked uploads.\n\n" +
				"This is the number of parts of the same file uploaded at once. The server accepts parts " +
				"out of order within one session and assembles them by part index, and one bad part does " +
				"not poison the session: measured 2026-10-22, a part refused with HTTP 400 can be sent " +
				"again on the same presigned URL and the upload then commits with the right digest.\n\n" +
				"The account does publish a concurrent-transfer privilege (1 for a plain account, 5 for a " +
				"platinum one, read from getUserPrivilegesV2), but nothing measured shows it enforced " +
				"against this number, so raising it is a local choice about memory and connections rather " +
				"than a request the server can grant or refuse.",
			Default:  4,
			Advanced: true,
		}, {
			Name: "max_file_size",
			Help: "Refuse to upload any file larger than this. 0 means read the limit from the account.\n\n" +
				"With the default 0, the account's single-file privilege (transFileSize) is read when the " +
				"remote is opened and files over it are refused before a single byte is read from the " +
				"source. Measured 2026-10-22 on two accounts, that value equals the day's transfer " +
				"allowance (transDayFlow) exactly, so the limit in force is the transfer budget that is " +
				"left, not a per-file size the account keeps - and both are int64 max on an upgraded " +
				"account, which is why nothing here is hardcoded.\n\n" +
				"Setting a number overrides what the account says, which is useful to keep a run inside a " +
				"budget you are managing yourself.",
			Default:  fs.SizeSuffix(0),
			Advanced: true,
		}, {
			Name: "no_quota_check",
			Help: "Skip the quota and space pre-checks and let the server refuse the upload instead.\n\n" +
				"By default the backend reads the account's privileges and free space and refuses, before " +
				"reading the source, what the server is known to refuse: over the day's transfer allowance, " +
				"over the single-file limit, and onto a full drive. Both refusals cost nothing to discover " +
				"on the server side either - it answers 403 at the session-opening call without taking a " +
				"byte - so this option saves the two requests per run (getUserPrivilegesV2 and getUserInfo), " +
				"not the transfer. Set it when those requests themselves are unwanted.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "fatal_if_no_space",
			Help: "Make a full drive or an exhausted day allowance cancel the whole run.\n\n" +
				"Neither condition can clear itself while the run is going: space is only freed by hand and " +
				"the allowance only comes back with the next day, so both refusals stop the retries of the " +
				"file that hit them. That still leaves --retries free to re-run the whole transfer list. " +
				"With this set the first such answer becomes a fatal error and the sync ends at once. " +
				"Default false, so interactive use is unchanged - and note that cancelling early leaves the " +
				"rest of the list untouched rather than failed. A file refused for being over a single-file " +
				"ceiling is not one of these two conditions: it stops that file, whatever is set here.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "upload_zone",
			Help: "Send all upload traffic through this host instead of the one the server names.\n\n" +
				"Leave blank to use the endpoint the session is meant for (recommended). The upload host " +
				"for the session calls and the storage host inside every presigned part URL are both " +
				"rewritten to this origin, so use it to route uploads through your own proxy - which is " +
				"also the way to keep a datacentre that pins the storage node from being reached directly.\n\n" +
				"When set, the session key and the file contents both go to that host, so only point it at " +
				"a server you control, and give it a TLS certificate rclone will accept. The URL must be " +
				"https:// and must name a host.",
			Advanced: true,
		}, {
			Name: "disable_http2",
			Help: "Disable HTTP/2 for all cloud189 traffic.\n\n" +
				"HTTP/2 multiplexes every concurrent part upload onto a single TCP connection, which caps " +
				"aggregate upload throughput at one connection's worth of bandwidth. Set this to use " +
				"HTTP/1.1 keep-alive instead, so the concurrent part uploads spread across parallel TCP " +
				"connections.",
			Default:  false,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			// The set below is the character table measured 2026-10-22: each
			// candidate name was uploaded for real and read back, 54 cases plus
			// the 12 fullwidth substitutes.
			//
			// The server silently deletes `" * : < > ? \ |` in a name and turns
			// a lone `'` into `\'` - corruption either way, with no error and no
			// way to fetch the original, so those characters are escaped. The
			// fullwidth substitutes the encoder emits for them are stored
			// verbatim, which is what makes such a name round-trip at all.
			//
			// EncodeInvalidUtf8 is required: names travel inside an encrypted
			// form string, where Go would replace invalid bytes with U+FFFD and
			// make the name irreversible.
			//
			// Leading and trailing spaces are escaped as well: the drive strips
			// them from file and directory names, so a name that begins or ends
			// with one would arrive as a different name and its install rename
			// would then be refused as already existing.
			//
			// Everything else the table probed is stored exactly as sent, so it
			// is left alone on purpose - leading `.`, `~`, tabs, newlines,
			// `& = % # + , ; @ ( ) [ ] { } $ ! ^`, and a space inside a name,
			// names 300 ASCII characters or 200 Chinese ones long, and `..`.
			// Escaping any of those would invent a restriction this server does
			// not have, and would make rclone rename files the official client
			// stores without complaint.
			Default: cloud189DefaultEncoding,
		}},
	})
}

// cloud189DefaultEncoding is the encoding option's default: exactly the
// characters the measured table shows this server damages, and nothing else.
//
// The boundary spaces are escaped too, and that is newer than the first table:
// measured 2026-10-23 with the integration suite and then directly, the drive
// strips a leading or trailing space from both file and directory names, so a
// name that begins or ends with one is stored as a different name and the rename
// that installs it is then refused as already existing. Escaping the boundary
// space is what makes those names storable at all; a space inside a name is
// stored as sent and is not escaped.
const cloud189DefaultEncoding = encoder.Standard |
	encoder.EncodeDoubleQuote | encoder.EncodeAsterisk |
	encoder.EncodeColon | encoder.EncodeLtGt |
	encoder.EncodeQuestion | encoder.EncodeBackSlash |
	encoder.EncodePipe | encoder.EncodeSingleQuote |
	encoder.EncodeLeftSpace | encoder.EncodeRightSpace |
	encoder.EncodeInvalidUtf8

// maxRuneForName is the widest code point the drive will store in a name.
//
// Anything above the BMP is rejected only at commit time, after every byte of
// the file has been transferred and charged to the day's allowance: the server
// answers 403 EmojiArgumentError (measured 2026-10-22). Refusing locally costs
// nothing and cannot lose a name the drive would have kept, because a name it
// refuses it never stores.
const maxRuneForName = 0xFFFF

// validateName rejects a name the drive cannot store, before anything is read
// from the source or sent to the server.
//
// Length is deliberately not checked: 300 ASCII characters and 200 Chinese ones
// were both stored verbatim, so there is no measured limit to enforce here and
// inventing one would block names the official client accepts.
func validateName(name string) error {
	for _, r := range name {
		if r > maxRuneForName {
			return fserrors.NoRetryError(fmt.Errorf("cloud189: name %q contains %U, which this drive rejects only after the whole file has been uploaded - rename it locally", truncate(name, 40), r))
		}
	}
	return nil
}

// chunkPlan returns the part size the server expects for a file of the given
// size. The rule, measured 2026-10-22 by declaring sizes without transferring
// them, is: one base while the file fits in ladderPartsLow parts of that size,
// two bases up to twice that, and above it the smallest multiple of the base
// (never less than ladderMinScale bases) that keeps the part count inside
// ladderPartsHigh.
//
// The invariant the server is really checking is the part count; sliceSize is
// only how it is spelled on the wire. base comes from the chunk_size option,
// whose default of 10MiB is the unit the server's own rule is anchored on.
func chunkPlan(size, base int64) int64 {
	if size > base*2*ladderPartsLow {
		scale := (size + int64(ladderPartsHigh)*base - 1) / (int64(ladderPartsHigh) * base)
		if scale < ladderMinScale {
			scale = ladderMinScale
		}
		return scale * base
	}
	if size > base*ladderPartsLow {
		return base * 2
	}
	return base
}

// ------------------------------------------------------------ errors -------

// apiError is a refusal from the server, kept with the field that carried the
// code because the three endpoints disagree about where the code goes.
type apiError struct {
	Status  int    // HTTP status
	Field   string // "errorCode", "res_code" or "code"
	Code    string // the refusal itself
	Message string // the server's own words
	Body    string // truncated raw response, for the log
}

func (e *apiError) Error() string {
	return fmt.Sprintf("cloud189: API error %s (HTTP %d, in %s): %s", e.Code, e.Status, e.Field, e.Message)
}

// asAPIError reports whether err is an *apiError, without swallowing a wrapped
// chain.
func asAPIError(err error, target **apiError) bool {
	for err != nil {
		if ae, ok := err.(*apiError); ok {
			*target = ae
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}

// isAuthInvalid reports a refusal that only a fresh session can fix.
func isAuthInvalid(err error) bool {
	var ae *apiError
	return asAPIError(err, &ae) && ae.Code == codeInvalidSessionKey
}

// noSpaceError marks the two refusals that no retry inside this run can clear:
// a full drive is only freed by hand, and the day's transfer allowance only
// comes back with the next day.
func noSpaceError(err error) error {
	if err == nil || fserrors.IsNoRetryError(err) {
		return err
	}
	var ae *apiError
	if asAPIError(err, &ae) && (ae.Code == codeNoSpace || ae.Code == codeDayFlowOver) {
		fs.Errorf(nil, "cloud189: the account cannot accept this transfer (%s): %s", ae.Code, ae.Message)
		return fserrors.NoRetryError(err)
	}
	return err
}

// shouldRetry decides what the pacer may repeat.
//
// A refusal the server phrased in a code is deterministic: the same request
// would be refused the same way, so retrying only spends the allowance. A
// transport failure or a 5xx answer is the opposite, and the caller has no way
// to know which parts landed, so those keep retrying.
func shouldRetry(ctx context.Context, err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if fserrors.IsNoRetryError(err) {
		return false, err
	}
	var ae *apiError
	if asAPIError(err, &ae) {
		return false, noSpaceError(err)
	}
	return true, err
}

// refuse parses whatever shape of refusal this endpoint used.
func refuse(status int, body []byte) error {
	var probe struct {
		ErrorCode string          `json:"errorCode"`
		ErrorMsg  string          `json:"errorMsg"`
		ResCode   json.RawMessage `json:"res_code"`
		ResMsg    string          `json:"res_message"`
		Code      string          `json:"code"`
		Msg       string          `json:"msg"`
		Success   *bool           `json:"success"`
	}
	ae := &apiError{Status: status, Body: truncate(string(body), 500)}
	if err := json.Unmarshal(body, &probe); err == nil {
		switch {
		case probe.ErrorCode != "":
			ae.Field, ae.Code, ae.Message = "errorCode", probe.ErrorCode, probe.ErrorMsg
		case probe.Code != "" && !strings.EqualFold(probe.Code, "SUCCESS"):
			ae.Field, ae.Code, ae.Message = "code", probe.Code, probe.Msg
		case len(probe.ResCode) > 0 && string(probe.ResCode) != "0" && string(probe.ResCode) != "null":
			ae.Field, ae.Message = "res_code", probe.ResMsg
			var asNumber int
			if err := json.Unmarshal(probe.ResCode, &asNumber); err == nil {
				ae.Code = fmt.Sprintf("%d", asNumber)
			} else {
				var asString string
				if err := json.Unmarshal(probe.ResCode, &asString); err == nil {
					ae.Code = asString
				} else {
					ae.Code = string(probe.ResCode)
				}
			}
		}
	}
	if ae.Code == "" {
		return fmt.Errorf("cloud189: unexpected answer (HTTP %d): %s", status, ae.Body)
	}
	return ae
}

// classify turns a response into an error, or returns the body when the call
// succeeded.
//
// Success is decided from the body, not the status: an expired session answers
// 400, but a task that failed answers 200 with res_code 0 and the failure
// buried further in. That second case belongs to the task layer.
func classify(status int, body []byte) ([]byte, error) {
	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		return body, nil
	}
	if status >= http.StatusInternalServerError || status == http.StatusTooManyRequests {
		return body, fmt.Errorf("cloud189: server answered HTTP %d: %s", status, truncate(string(body), 200))
	}
	return body, noSpaceError(refuse(status, body))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// newRequestID returns a random UUID v4 for the x-request-id header, which the
// client sends fresh on every request and the server keys its own logs to.
func newRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// A missing request id is not worth failing a transfer over; the
		// server only uses it to correlate its own logs.
		return ""
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(b[0:4]), hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]), hex.EncodeToString(b[8:10]), hex.EncodeToString(b[10:16]))
}

// ------------------------------------------------------------ session ------

// sessionState is the pair every request to one account signs with, shared by
// all remotes of that account in this process.
//
// The access token is the long-lived credential and never changes here; the
// session key and secret are minted from it on demand. Remotes share the state
// because each exchange is a request, and because a freshly minted session does
// not revoke the one it replaced - so one exchange serves the whole process.
type sessionState struct {
	mu                  sync.Mutex
	sessionKey          string
	sessionSecret       string
	familySessionKey    string
	familySessionSecret string
	loginName           string
}

var sessionRegistry = struct {
	sync.Mutex
	byToken map[string]*sessionState
}{
	byToken: map[string]*sessionState{},
}

// session returns the shared state for this remote's access token.
func (f *Fs) session() *sessionState {
	sessionRegistry.Lock()
	defer sessionRegistry.Unlock()
	ts, ok := sessionRegistry.byToken[f.opt.AccessToken]
	if !ok {
		ts = &sessionState{}
		sessionRegistry.byToken[f.opt.AccessToken] = ts
	}
	return ts
}

// keys returns the pair to sign with, picking the family variant when the path
// belongs to a family drive. The family secret equals the personal one in every
// capture, yet the field is read separately so a server that changes that does
// not silently break family traffic.
func (f *Fs) keys(family bool) (sessionKey, secret string, err error) {
	ts := f.session()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.sessionSecret == "" {
		// No retry can fix this: it describes the state of the process, not of the
		// network.
		return "", "", fserrors.NoRetryError(errors.New("cloud189: no session yet - the access token exchange has not run"))
	}
	if family {
		if ts.familySessionKey == "" {
			// Marked no-retry because nothing a retry does can give the account a
			// family drive: the fix is the remote's space or family_id setting. Left
			// unmarked the pacer reads it as a transient fault and spends
			// --low-level-retries requests on it, then --retries rounds of the whole
			// command on top.
			return "", "", fserrors.NoRetryError(errors.New("cloud189: this account has no family session - check that space matches an existing family drive"))
		}
		if ts.familySessionSecret != "" {
			return ts.familySessionKey, ts.familySessionSecret, nil
		}
		return ts.familySessionKey, ts.sessionSecret, nil
	}
	return ts.sessionKey, ts.sessionSecret, nil
}

// exchange trades the access token for a session. usedKey, when non-empty, is
// the key the server just rejected: if the shared state has already moved past
// it, another goroutine did this work and there is nothing left to do.
func (f *Fs) exchange(ctx context.Context, usedKey string) error {
	ts := f.session()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	// usedKey can come from either session, because a family request is signed
	// with the family key and that is a different string. Comparing it against
	// the personal key alone made every family rejection look as though another
	// goroutine had already refreshed, so a family session was never exchanged
	// at all and every family operation stayed broken for the process.
	if usedKey != "" && ts.sessionKey != usedKey && ts.familySessionKey != usedKey {
		return nil
	}

	q := fmt.Sprintf("rand=%d&%s&accessToken=%s&appId=%s&returnType=JSON",
		time.Now().UnixMilli(), clientSuffix, url.QueryEscape(f.opt.AccessToken), appID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.apiBase+"/getSessionForPC.action?"+q, http.NoBody)
	if err != nil {
		return err
	}
	req.Header.Set("user-agent", desktopUserAgent)
	// This endpoint authenticates with the access token alone: it carries no
	// signature, because a signature needs the session this call is about to
	// hand out. x-key is what the client sends; its captured value is the
	// literal "abc" on a refresh and a uuid on the first exchange.
	req.Header.Set("x-key", "abc")
	res, err := f.httpClient.Do(req)
	if err != nil {
		// The transport quotes the URL it failed on, and this one carries the
		// access token in its query.
		return scrubErr(err, req.URL.String(), f.opt.AccessToken, url.QueryEscape(f.opt.AccessToken))
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if _, err := classify(res.StatusCode, body); err != nil {
		return fmt.Errorf("cloud189: exchanging the access token failed: %w", err)
	}
	var resp struct {
		SessionKey          string `json:"sessionKey"`
		SessionSecret       string `json:"sessionSecret"`
		FamilySessionKey    string `json:"familySessionKey"`
		FamilySessionSecret string `json:"familySessionSecret"`
		LoginName           string `json:"loginName"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("cloud189: decoding the session response: %w", err)
	}
	if resp.SessionKey == "" || len(resp.SessionSecret) < api.AESKeyLen {
		return fmt.Errorf("cloud189: the exchanged session is unusable (key length %d, secret length %d)", len(resp.SessionKey), len(resp.SessionSecret))
	}
	ts.sessionKey, ts.sessionSecret = resp.SessionKey, resp.SessionSecret
	// An answer that names no family session must not blank a live one: this state
	// is shared per access token, so another remote of the same account may be
	// holding a working family session. A family request that then finds its key
	// refused exchanges again, and the answer that carries a family key sets it.
	if resp.FamilySessionKey != "" {
		ts.familySessionKey, ts.familySessionSecret = resp.FamilySessionKey, resp.FamilySessionSecret
	}
	ts.loginName = resp.LoginName
	fs.Debugf(f, "cloud189: session minted for %s", redactLoginName(resp.LoginName))
	return nil
}

// redactLoginName hides the phone number an account logs in with, keeping the last
// two digits so two accounts can still be told apart in a log. An email names the
// account without being a secret, so it is left as it is, and anything without
// enough digits to be a phone number is left alone too.
func redactLoginName(name string) string {
	if strings.Contains(name, "@") {
		return name
	}
	digits := 0
	for _, r := range name {
		if r >= '0' && r <= '9' {
			digits++
		}
	}
	if digits < 7 {
		return name
	}
	out := []rune(name)
	kept := 0
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < '0' || out[i] > '9' {
			continue
		}
		kept++
		if kept > 2 {
			out[i] = '*'
		}
	}
	return string(out)
}

// ------------------------------------------------------------ http ---------

// apiHeader is one extra request header, for the calls that need a header the
// standard set does not carry.
type apiHeader struct{ name, value string }

// callOnce performs one signed request. The signature covers the path and the
// encrypted params only - never the query - so both are passed separately.
//
// The key the request was signed with comes back with it, because the caller that
// has to decide whether a rejection means "the session is dead" or "someone else
// already exchanged it" needs to know which session this request carried.
func (f *Fs) callOnce(ctx context.Context, family bool, host, operate, requestURI, rawQuery, body string, extra []apiHeader) ([]byte, string, error) {
	sessionKey, secret, err := f.keys(family)
	if err != nil {
		return nil, "", err
	}
	// The date must be the same bytes in the header and in the signature, and
	// the server checks neither against the clock, only against itself.
	date := time.Now().UTC().Format(http.TimeFormat)
	params := signedParams(rawQuery, body)
	sig := api.Sign(secret, sessionKey, operate, requestURI, date, params)

	full := host + requestURI
	if rawQuery != "" {
		full += "?" + rawQuery
	}
	var rdr io.Reader = http.NoBody
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, operate, full, rdr)
	if err != nil {
		return nil, sessionKey, err
	}
	req.Header.Set("user-agent", desktopUserAgent)
	req.Header.Set("accept", "application/json;charset=UTF-8")
	req.Header.Set("cache-control", "no-cache")
	req.Header.Set("date", date)
	req.Header.Set("sessionkey", sessionKey)
	req.Header.Set("x-request-id", newRequestID())
	req.Header.Set("signature", sig)
	if body != "" {
		req.Header.Set("content-type", "application/x-www-form-urlencoded")
	}
	for _, h := range extra {
		req.Header.Set(h.name, h.value)
	}

	res, err := f.httpClient.Do(req)
	if err != nil {
		return nil, sessionKey, fmt.Errorf("cloud189: %s %s: %w", operate, requestURI, err)
	}
	defer func() { _ = res.Body.Close() }()
	respBody, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, sessionKey, fmt.Errorf("cloud189: reading %s: %w", requestURI, err)
	}
	data, err := classify(res.StatusCode, respBody)
	return data, sessionKey, err
}

// signedParams returns the encrypted params value a request carries, from the
// form body when it has one and from the query otherwise.
//
// Both halves matter: initMultiUpload and commitMultiUploadFile are POSTs whose
// params sit in the body, while getMultiUploadUrls is a GET whose params sit in
// the query, and both sign them. Reading only the body produces a request the
// upload host refuses with InvalidSignature.
func signedParams(rawQuery, body string) string {
	from := body
	if from == "" {
		from = rawQuery
	}
	v, err := url.ParseQuery(from)
	if err != nil {
		return ""
	}
	return v.Get("params")
}

// call runs a request through the pacer, exchanging the session once if the
// server has stopped accepting it.
//
// The session to sign with is decided from the request path: a path under
// /family is a family operation. That is exactly what every read endpoint needs
// and what the capture shows for the upload endpoints, so callers of call never
// have to think about it. Batch tasks are the exception - their path is
// /batch/... in both drives and only the caller knows which drive the task is
// for - so those go through callWith.
func (f *Fs) call(ctx context.Context, host, operate, requestURI, rawQuery, body string, extra ...apiHeader) ([]byte, error) {
	return f.callWith(ctx, strings.HasPrefix(requestURI, "/family"), host, operate, requestURI, rawQuery, body, extra...)
}

// callWith is call with the session chosen by the caller.
func (f *Fs) callWith(ctx context.Context, family bool, host, operate, requestURI, rawQuery, body string, extra ...apiHeader) ([]byte, error) {
	var out []byte
	err := f.pacer.Call(func() (bool, error) {
		data, usedKey, err := f.callOnce(ctx, family, host, operate, requestURI, rawQuery, body, extra)
		if isAuthInvalid(err) {
			if f.opt.NoRefresh {
				return false, fserrors.NoRetryError(fmt.Errorf("cloud189: the session was rejected and no_refresh is set, so it was not exchanged again - %w", err))
			}
			// One exchange, then one rebuild: the signature covers the session
			// key, so it has to be computed over the new one. The encrypted
			// params travel unchanged, which is sound because exchanging a
			// session for the same access token returns the same secret
			// (measured twice, 2026-10-22). Were that ever to stop being true
			// the server would answer with a refusal it cannot decrypt, not
			// with a wrong file.
			//
			// The key this request was signed with is handed to exchange, which
			// is what lets the second goroutine looking at the same dead session
			// take the one the first already minted instead of logging in a
			// second time.
			fs.Logf(f, "cloud189: the session was rejected, making sure a live one is in place before retrying")
			if rerr := f.exchange(ctx, usedKey); rerr != nil {
				return false, fserrors.NoRetryError(fmt.Errorf("cloud189: the session was rejected and re-exchanging it failed: %w - if the access token itself has been retired, put a current one in the remote", rerr))
			}
			data, _, err = f.callOnce(ctx, family, host, operate, requestURI, rawQuery, body, extra)
		}
		if err != nil {
			return shouldRetry(ctx, err)
		}
		out = data
		return false, nil
	})
	if err != nil {
		return nil, f.fatalIfNoSpace(err)
	}
	return out, nil
}

// fatalIfNoSpace turns the two refusals that cannot clear during a run into a
// fatal error when fatal_if_no_space asks for that.
//
// noSpaceError already stopped the retries, but a stopped file still leaves the
// round to --retries, which on a full account walks the whole list again. Only
// the fatal marking reaches sync's cancel path (fs/sync/sync.go).
func (f *Fs) fatalIfNoSpace(err error) error {
	if err == nil || !f.opt.FatalIfNoSpace {
		return err
	}
	var ae *apiError
	if asAPIError(err, &ae) && (ae.Code == codeNoSpace || ae.Code == codeDayFlowOver) {
		return fserrors.FatalError(err)
	}
	return err
}

// get builds the query for a GET with business parameters.
func (f *Fs) get(extra string) string {
	return fmt.Sprintf("rand=%d&%s&%s", time.Now().UnixMilli(), clientSuffix, extra)
}

// ------------------------------------------------------------ types --------

// Fs represents a remote cloud189 drive
type Fs struct {
	name       string             // name of this remote
	root       string             // the path we are working on
	opt        Options            // parsed options
	m          configmap.Mapper   // config mapper, kept for a credential that rotates
	features   *fs.Features       // optional features
	httpClient *http.Client       // the connection to the server
	partClient *http.Client       // the same transport, refusing to follow a redirect
	pacer      *fs.Pacer          // pacer for API calls
	dirCache   *dircache.DirCache // map of directory path to directory id

	familyID     string      // family drive id, when space is family
	fsRootID     string      // the id the dircache was seeded with, i.e. this remote's own root
	familyRootID string      // the family drive's real root id, once learned
	apiBase      string      // base URL of the file API, which is apiHost for any remote a user configures
	uploadBase   string      // where upload traffic goes: the server's host, or upload_zone
	quota        *quotaState // the account's limits, read at run time
}

// Object describes a cloud189 object
type Object struct {
	fs      *Fs       // what this object is part of
	remote  string    // the remote path
	id      string    // server-side id, kept as text
	size    int64     // bytes
	md5     string    // the digest the listing carries, upper-case hex
	modTime time.Time // when the drive last touched it

	urlMu     *sync.Mutex // pointer so a copied Object stays lock-safe
	url       string      // cached download URL
	urlExpiry time.Time   // when the cached URL stops being valid
}

// ------------------------------------------------------------ interface ----

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string { return f.name }

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string { return f.root }

// String returns a description of the FS
func (f *Fs) String() string {
	if f.opt.Space == spaceFamily {
		return fmt.Sprintf("cloud189 family drive '%s' (%s)", f.name, f.familyID)
	}
	return fmt.Sprintf("cloud189 drive '%s'", f.name)
}

// Precision returns the precision of the ModTimes in this Fs.
//
// The drive stamps createDate and lastOpTime itself and offers no way to write
// either, so reporting a precision would promise a capability rclone does not
// have.
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns the hashes the drive can report: MD5, which every listing
// carries.
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.MD5) }

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features { return f.features }

// cloud189Features is the feature set every cloud189 remote runs with.
//
// CaseInsensitive is deliberately left off: whether the server folds name case
// has never been measured, and claiming it does is what lets two names that
// differ only by case be treated as one.
func cloud189Features(ctx context.Context, f *Fs) *fs.Features {
	return (&fs.Features{
		CanHaveEmptyDirectories: true,
	}).Fill(ctx, f)
}

// newHTTPClient builds the http client, optionally disabling HTTP/2 so
// concurrent part uploads spread across parallel TCP connections.
func newHTTPClient(ctx context.Context, opt *Options) *http.Client {
	return fshttp.NewClientCustom(ctx, func(t *http.Transport) {
		if opt.DisableHTTP2 {
			// Clearing TLSNextProto is what turns HTTP/2 off: with no
			// negotiated protocol map entry, the transport falls back to
			// HTTP/1.1 keep-alive.
			t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		}
	})
}

// parseUploadZone validates the upload_zone option and returns the origin every
// upload request goes to.
//
// Only an origin is accepted, never a path: the signature covers the request
// path alone, and a presigned part URL carries its own path, query and
// signature, so a zone that rewrote anything below the host would produce a
// request this server cannot verify.
func parseUploadZone(zone string) (string, error) {
	if zone == "" {
		return uploadHost, nil
	}
	u, err := url.Parse(zone)
	if err != nil {
		return "", fmt.Errorf("cloud189: upload_zone %q is not a URL: %w", zone, err)
	}
	if u.Scheme != "https" || u.Host == "" {
		return "", fmt.Errorf("cloud189: upload_zone must be an https:// URL naming a host, got %q", zone)
	}
	if u.Path != "" && u.Path != "/" {
		return "", fmt.Errorf("cloud189: upload_zone may only name a host, because the request path is what the signature covers - got path %q", u.Path)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("cloud189: upload_zone may only name a host, got %q", zone)
	}
	return u.Scheme + "://" + u.Host, nil
}

// noRedirectClient returns a client that shares c's transport and settings but
// hands back a redirect instead of following it.
//
// A presigned part PUT carries the storage layer's signature for the host that
// issued it, and Go drops only Authorization and Cookie on a cross-host
// redirect - Content-MD5 and the rest would travel along with whoever the
// redirect names. Refusing the hop keeps file bytes and their credentials on one
// origin.
func noRedirectClient(c *http.Client) *http.Client {
	out := *c
	out.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &out
}

// partURLFor applies upload_zone to a presigned part URL.
//
// Only the origin is swapped. The path, the query and everything the signature
// covers stay exactly as the storage layer wrote them, which is why the option
// takes a host and nothing below one.
func (f *Fs) partURLFor(raw string) (string, error) {
	if !f.usesUploadZone() {
		return raw, nil
	}
	zone, err := url.Parse(f.uploadBase)
	if err != nil {
		return "", fmt.Errorf("cloud189: upload_zone %q cannot be used: %w", f.uploadBase, err)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("cloud189: the server gave an upload URL that cannot be parsed, so upload_zone cannot route it")
	}
	parsed.Scheme = zone.Scheme
	parsed.Host = zone.Host
	return parsed.String(), nil
}

// usesUploadZone reports whether traffic is being routed through a proxy.
func (f *Fs) usesUploadZone() bool {
	return f.uploadBase != uploadHost
}

// newFs partially constructs Fs from the path
//
// It constructs a valid Fs but doesn't attempt to figure out whether it is a
// file or a directory.
func newFs(ctx context.Context, name, root string, m configmap.Mapper) (*Fs, error) {
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	switch opt.Space {
	case spacePersonal, spaceFamily:
	default:
		return nil, fmt.Errorf("cloud189: space must be one of %q, %q, got %q", spacePersonal, spaceFamily, opt.Space)
	}
	if opt.AccessToken == "" {
		return nil, errors.New("cloud189: an access_token is required")
	}
	// The part size the server demands is a function of the file size anchored
	// on 10MiB, so a base below that cannot be what the rule produces for any
	// file. Above it the ladder can still land on a value the server accepts,
	// but a base this large makes the multiplication in chunkPlan meaningless.
	if int64(opt.ChunkSize) < minChunkSize || int64(opt.ChunkSize) > maxChunkSize {
		return nil, fmt.Errorf("cloud189: chunk_size must be between %s and %s - the server's part rule is anchored on 10Mi and refuses any smaller part size, measured with 1, 2, 4, 5, 8, 16, 32 and 64Mi all answered InvalidPartSize - got %s",
			fs.SizeSuffix(minChunkSize), fs.SizeSuffix(maxChunkSize), opt.ChunkSize)
	}
	if opt.UploadConcurrency < 1 {
		return nil, fmt.Errorf("cloud189: upload_concurrency must be 1 or more, got %d", opt.UploadConcurrency)
	}
	uploadBase, err := parseUploadZone(opt.UploadZone)
	if err != nil {
		return nil, err
	}

	f := &Fs{
		name:       name,
		root:       parsePath(root),
		opt:        *opt,
		m:          m,
		httpClient: newHTTPClient(ctx, opt),
		familyID:   opt.FamilyID,
		uploadBase: uploadBase,
		apiBase:    apiHost,
	}
	if opt.RootFolderID != "" {
		// Both drives name the top level by an id, so an explicit one is also
		// the answer to "what is the root": the family drive's real id, which
		// otherwise has to be read off a listing, needs nothing here.
		f.familyRootID = opt.RootFolderID
	}
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))
	f.partClient = noRedirectClient(f.httpClient)
	f.features = cloud189Features(ctx, f)

	if err := f.exchange(ctx, ""); err != nil {
		return nil, err
	}
	f.quota = &quotaState{}
	// The family drive has to be identified before the account is asked about
	// itself, because the family capacity endpoint takes familyId: with family_id
	// left blank that request would go out naming nothing. Measured live on
	// 2026-10-23, the family getUserInfo answered 400 InternalError once in four
	// runs made in exactly that shape.
	if f.opt.Space == spaceFamily {
		if err := f.resolveFamilyID(ctx); err != nil {
			return nil, err
		}
	}
	// The limits are read once, here, and refreshed when a pre-check is about to
	// refuse or when this read never got an answer. Failing to read them is not a
	// reason to stop: the server refuses what it refuses, and the local gate would
	// only save the source read.
	if !opt.NoQuotaCheck {
		if err := f.readQuota(ctx); err != nil {
			fs.Logf(f, "cloud189: the account's transfer limits could not be read, so uploads will not be pre-checked: %v", err)
		}
	}
	return f, nil
}

// NewFs constructs an Fs from the path, remote:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f, err := newFs(ctx, name, root, m)
	if err != nil {
		return nil, err
	}

	rootID, err := f.rootID(ctx)
	if err != nil {
		return nil, err
	}
	f.dirCache = dircache.New(f.root, rootID, f)
	f.fsRootID = rootID

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(f.root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, rootID, &tempF)
		tempF.root = newRoot
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		_, err := tempF.NewObject(ctx, remote)
		if err != nil {
			if err == fs.ErrorObjectNotFound {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		f.features.Fill(ctx, &tempF)
		// The old f is updated rather than tempF returned, because features
		// were filled with functions that close over f.
		// https://github.com/rclone/rclone/issues/2182
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// rootID returns the id the path is anchored at.
//
// An explicit root_folder_id wins, in either drive. Otherwise the personal drive
// has a constant root and the family drive does not: alias 0 serves listings and
// task targets but not createFolder, so the real id is read off a listing. An
// empty family drive has nothing to read it from, and then only the operations
// that accept the alias can work - which is what root_folder_id is for.
func (f *Fs) rootID(ctx context.Context) (string, error) {
	if f.opt.RootFolderID != "" {
		return f.opt.RootFolderID, nil
	}
	if f.opt.Space != spaceFamily {
		return personalRoot, nil
	}
	if f.familyRootID == "" {
		if _, err := f.discoverFamilyRoot(ctx); err != nil {
			fs.Logf(f, "cloud189: could not read the family drive's numeric root id (%v) - a top-level folder will be created without naming its parent, and tasks will name the alias", err)
		}
	}
	// Always the alias: a listing addressed at the numeric id comes back empty,
	// with no error, which reads as an empty drive. The numeric id lives in
	// familyRootID for the request bodies that need it.
	return familyRootAlias, nil
}

// discoverFamilyRoot reads the family drive's real root id off any entry in it.
//
// Every row carries its parent, so one child is enough; the listing that omits
// folderId is the family root itself.
func (f *Fs) discoverFamilyRoot(ctx context.Context) (string, error) {
	// Two ids are in play on a family drive. The listing endpoint answers the `0`
	// alias and answers a row's parentId with an empty page and no error, so every
	// listing is addressed at the alias. Request bodies - the folder a created
	// directory goes into, a task's parent - want the numeric parent id the rows
	// carry, which is what the captured client sends. This reads the numeric id
	// into familyRootID and returns the alias.
	var rootID string
	err := f.listAll(ctx, familyRootAlias, func(row *api.Row) bool {
		if p := row.ParentID.String(); p != "" && p != familyRootAlias {
			rootID = p
			return true
		}
		return false
	})
	if err != nil {
		return "", err
	}
	if rootID == "" {
		return "", errors.New("the family drive lists no entries to read a numeric root id from")
	}
	f.familyRootID = rootID
	return familyRootAlias, nil
}

// realDirID is the id a request body has to name: the numeric root id wherever the
// cache hands back the alias.
func (f *Fs) realDirID(id string) string {
	if id == familyRootAlias && f.opt.Space == spaceFamily && f.familyRootID != "" && f.familyRootID != familyRootAlias {
		return f.familyRootID
	}
	return id
}

// resolveFamilyID fills in family_id when it was left blank.
func (f *Fs) resolveFamilyID(ctx context.Context) error {
	if f.familyID != "" {
		return nil
	}
	q := f.get("")
	body, err := f.call(ctx, f.apiBase, http.MethodGet, "/family/manage/getFamilyList.action", q, "")
	if err != nil {
		return fmt.Errorf("cloud189: finding the family drive: %w", err)
	}
	var resp api.FamilyListResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("cloud189: decoding the family list: %w", err)
	}
	switch len(resp.FamilyInfo) {
	case 0:
		return errors.New("cloud189: this account belongs to no family drive, so space=family cannot be used")
	case 1:
		f.familyID = resp.FamilyInfo[0].FamilyID.String()
		f.opt.FamilyID = f.familyID
		if f.m != nil {
			// Persist what was learned so the next run needs no discovery.
			f.m.Set("family_id", f.familyID)
		}
		fs.Debugf(f, "cloud189: discovered family_id %s", f.familyID)
		return nil
	default:
		names := make([]string, 0, len(resp.FamilyInfo))
		for _, fi := range resp.FamilyInfo {
			names = append(names, fmt.Sprintf("%s(%s)", fi.RemarkName, fi.FamilyID))
		}
		return fmt.Errorf("cloud189: this account is in %d family drives (%s) - set family_id to choose one", len(names), strings.Join(names, ", "))
	}
}

// parsePath parses the remote root, leaving only the path component in the form
// the directory cache expects.
func parsePath(root string) string {
	return strings.Trim(root, "/")
}

// ------------------------------------------------------------ listing ------

// family reports whether this remote points at a family drive.
func (f *Fs) family() bool { return f.opt.Space == spaceFamily }

// listAll walks every entry of dirID, calling fn for each one.
//
// Paging stops only on an empty page. Stopping on "fewer than pageSize" would
// truncate the listing whenever the server caps the page below what was asked
// for, and a truncated listing makes sync delete files it believes are extra.
// Ids are de-duplicated for the same reason, and maxListPages bounds the loop
// against a server that keeps repeating a page.
func (f *Fs) listAll(ctx context.Context, dirID string, fn func(*api.Row) bool) error {
	seen := map[string]struct{}{}
	requestURI := "/listFiles.action"
	if f.family() {
		requestURI = "/family/file/listFiles.action"
	}
	for pageNum := 1; pageNum <= maxListPages; pageNum++ {
		extra := fmt.Sprintf("pageNum=%d&pageSize=%d&fileType=0&mediaType=0&orderBy=filename&descending=true", pageNum, listPageSize)
		if f.family() {
			extra = "familyId=" + url.QueryEscape(f.familyID) + "&" + extra
			// The family endpoint treats an omitted folderId as its root, and
			// the 0 alias is what rootID() falls back to when the real id could
			// not be read.
			if dirID != familyRootAlias {
				extra = "folderId=" + url.QueryEscape(dirID) + "&" + extra
			}
		} else {
			extra = "folderId=" + url.QueryEscape(dirID) + "&" + extra
		}
		body, err := f.call(ctx, f.apiBase, http.MethodGet, requestURI, f.get(extra), "")
		if err != nil {
			return err
		}
		listing, err := api.ParseListing(body)
		if err != nil {
			return err
		}
		stopped := false
		count := 0
		for i := range listing.Folders {
			row := &listing.Folders[i]
			if _, ok := seen[row.ID.String()]; ok {
				continue
			}
			seen[row.ID.String()] = struct{}{}
			count++
			if fn(row) {
				stopped = true
				break
			}
		}
		for i := range listing.Files {
			if stopped {
				break
			}
			row := &listing.Files[i]
			if _, ok := seen[row.ID.String()]; ok {
				continue
			}
			seen[row.ID.String()] = struct{}{}
			count++
			if fn(row) {
				stopped = true
				break
			}
		}
		if stopped {
			return nil
		}
		if count == 0 {
			return nil
		}
	}
	return fmt.Errorf("cloud189: listing of %q did not terminate after %d pages, the server keeps returning data", dirID, maxListPages)
}

// rowToEntry turns a listing row into the entry it describes, or nil when the
// row carries no name at all.
func (f *Fs) rowToEntry(dir string, row *api.Row) fs.DirEntry {
	if row.Name == "" {
		return nil
	}
	name := f.opt.Enc.ToStandardName(row.Name)
	modTime := parseDriveTime(row.LastOpTime, row.CreateDate)
	if rowIsDir(row) {
		return fs.NewDir(path.Join(dir, name), modTime)
	}
	return &Object{
		fs:      f,
		remote:  path.Join(dir, name),
		id:      row.ID.String(),
		size:    row.Size,
		md5:     row.MD5,
		modTime: modTime,
		urlMu:   new(sync.Mutex),
	}
}

// rowIsDir reports which of the two arrays a row came from, falling back to the
// server's own fileCata flag when a caller built the row from a single lookup.
func rowIsDir(row *api.Row) bool { return row.FileCata == 0 }

// List the objects and directories in dir into entries
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	err = f.listAll(ctx, dirID, func(row *api.Row) bool {
		if entry := f.rowToEntry(dir, row); entry != nil {
			entries = append(entries, entry)
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// parseDriveTime reads the drive's own timestamp strings.
//
// They are wall-clock text in the account's zone with no offset attached, so
// they are read as UTC+8 - every capture of this service is a mainland China
// account. lastOpTime wins when both are present because it moves when the file
// does.
func parseDriveTime(candidates ...string) time.Time {
	for _, c := range candidates {
		if c == "" {
			continue
		}
		if t, err := time.ParseInLocation(timeLayout, c, beijing); err == nil {
			return t
		}
	}
	return time.Now()
}

// beijing is the zone the drive writes its timestamps in. Captured 2026-10-22:
// listing times sit 8 hours ahead of the UTC the same server puts in an OSS
// presigned Date header.
var beijing = time.FixedZone("UTC+8", 8*3600)

// ------------------------------------------------------------ dir cache ----

// FindLeaf finds a directory of name leaf in pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	err = f.listAll(ctx, pathID, func(row *api.Row) bool {
		if row.FileCata == 0 && row.Name == f.opt.Enc.FromStandardName(leaf) {
			pathIDOut = row.ID.String()
			found = true
			return true
		}
		return false
	})
	if err != nil {
		return "", false, err
	}
	return pathIDOut, found, nil
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	name := f.opt.Enc.FromStandardName(leaf)
	// Checked on the name that goes on the wire: the encoder does not escape
	// anything above the BMP, so a directory named with an emoji reaches the
	// server either way and the server has no opinion until it is asked to
	// store it.
	if err := validateName(name); err != nil {
		return "", err
	}
	var (
		requestURI, q string
		body          []byte
	)
	if f.family() {
		// The family endpoint is a GET and calls its parent parentId, not
		// parentFolderId. It also refuses the 0 alias, so the numeric root id is
		// what a top-level folder names; when that id could not be read there is
		// nothing to name.
		pathID = f.realDirID(pathID)
		if pathID == familyRootAlias {
			return "", errors.New("cloud189: cannot create a directory in an empty family drive - its root id is not known, so give family_id a value that lets the root be listed")
		}
		q = f.get(fmt.Sprintf("familyId=%s&parentId=%s&folderName=%s", url.QueryEscape(f.familyID), url.QueryEscape(pathID), url.QueryEscape(name)))
		requestURI = "/family/file/createFolder.action"
		body, err = f.call(ctx, f.apiBase, http.MethodGet, requestURI, q, "")
	} else {
		q = f.get(fmt.Sprintf("parentFolderId=%s&folderName=%s", url.QueryEscape(pathID), url.QueryEscape(name)))
		requestURI = "/createFolder.action"
		body, err = f.call(ctx, f.apiBase, http.MethodPost, requestURI, q, "")
	}
	if err != nil {
		return "", err
	}
	var resp struct {
		ID api.FlexID `json:"id"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("cloud189: decoding createFolder: %w", err)
	}
	if resp.ID == "" {
		// An empty id would poison the directory cache: every later operation
		// on this directory would then target the root.
		return "", errors.New("cloud189: createFolder returned no id")
	}
	return resp.ID.String(), nil
}

// Mkdir makes the directory if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir removes the directory if it is empty.
//
// The drive has no way to remove a directory that still has children without
// taking the children with it, so the listing is checked first and a non-empty
// directory is refused rather than deleted recursively.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	if err := f.requireKnownFamilyRoot("a directory removal"); err != nil {
		return err
	}
	if dir == "" {
		return fserrors.NoRetryError(errors.New("cloud189: the drive's own top level cannot be removed, only what is inside it"))
	}
	parentDir, leaf := dircache.SplitPath(dir)
	parentID, err := f.dirCache.FindDir(ctx, parentDir, false)
	if err != nil {
		return err
	}
	// The name is looked up before the directory under it is listed, so a file
	// with that name is answered as the missing directory it is rather than as
	// an empty one, and nothing is deleted beside it.
	row, err := f.findEntry(ctx, parentID, f.opt.Enc.FromStandardName(leaf))
	if err != nil {
		if err == fs.ErrorObjectNotFound {
			return fs.ErrorDirNotFound
		}
		return err
	}
	if !rowIsDir(row) {
		return fs.ErrorDirNotFound
	}
	children, err := f.rowsIn(ctx, row.ID.String())
	if err != nil {
		return err
	}
	if len(children) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	if err := f.deleteRows(ctx, []deleteTarget{{row: row}}); err != nil {
		return err
	}
	// The id is gone from the drive, so it has to go from the cache too: a
	// later Mkdir of the same name must create it again rather than trust the
	// stale id.
	f.dirCache.FlushDir(dir)
	return nil
}

// taskAuthFor decides whether a task can run between this remote's space and
// another one, and how it must be signed.
//
// The three shapes below are what the client was captured doing (2026-10-22). A
// family to family copy across two different families was never captured, so it
// is refused and rclone moves the bytes instead.
func (f *Fs) taskAuthFor(dst *Fs) (taskAuth, bool) {
	switch {
	case f == dst:
		return f.taskAuth(), true
	case f.opt.AccessToken != dst.opt.AccessToken:
		return taskAuth{}, false
	case !f.family() && dst.family():
		// Signed by the personal session, names the destination family in the
		// body, and carries no srcParentId on the task entry.
		return taskAuth{family: false, familyID: dst.familyID, copyType: 1}, true
	case f.family() && !dst.family():
		// The reverse is signed by the family session and aims at the personal
		// root alias.
		return taskAuth{family: true, familyID: f.familyID, copyType: 2}, true
	default:
		return taskAuth{}, false
	}
}

// rowObject builds the Object for a row the drive just described, without
// listing again.
func (f *Fs) rowObject(remote string, row *api.Row) *Object {
	return &Object{
		fs:      f,
		remote:  remote,
		id:      row.ID.String(),
		size:    row.Size,
		md5:     row.MD5,
		modTime: parseDriveTime(row.LastOpTime, row.CreateDate),
		urlMu:   new(sync.Mutex),
	}
}

// occupant returns the row holding a name in a directory, or nil when nothing
// does. A directory that holds the name is returned too, and callers decide
// whether they can do anything with it.
func (f *Fs) occupant(ctx context.Context, dirID, wireName string) (*api.Row, error) {
	row, err := f.findEntry(ctx, dirID, wireName)
	if err == fs.ErrorObjectNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return row, nil
}

// arrivedCopy finds the object a copy task just made: the entry with this name
// that is not the source.
//
// The drive answers a successful COPY with no id at all, so the new object has
// to be recognised from the destination listing. In the one-directory rename case
// that listing holds the source too, under the same name, so matching by name
// alone would hand back the wrong object and every later operation would address
// that one.
func (f *Fs) arrivedCopy(ctx context.Context, dirID, wireName, srcID string) (*api.Row, error) {
	var found *api.Row
	err := f.listAll(ctx, dirID, func(row *api.Row) bool {
		if row.Name == wireName && row.ID.String() != srcID {
			found = row
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, fserrors.NoRetryError(fmt.Errorf("cloud189: the copy finished but no object named %q other than its source appeared in the destination", truncate(wireName, 60)))
	}
	return found, nil
}

// clearName moves whatever holds wireName in dirID out of the way, under a
// unique name, and returns the parked row so the caller can hand it back if the
// operation it was clearing the way for does not happen.
//
// It is needed because a task that meets a name already in use does not overwrite
// and does not rename: it stops in state 2 and waits for the client to decide,
// which this backend does not answer.
func (f *Fs) clearName(ctx context.Context, dirID, wireName string, exceptID string) (*api.Row, error) {
	row, err := f.occupant(ctx, dirID, wireName)
	if err != nil {
		return nil, err
	}
	if row == nil || row.ID.String() == exceptID {
		return nil, nil
	}
	if rowIsDir(row) {
		// A directory is never moved aside for a file: the delete that follows a
		// successful task is recursive, and clearing a name by parking a whole
		// subtree would put that subtree one failed step from being removed.
		return nil, fserrors.NoRetryError(fmt.Errorf("cloud189: %q is a directory on the drive, so nothing can take its name", truncate(wireName, 60)))
	}
	parked := newParkName()
	if err := f.renameObject(ctx, row.ID.String(), rowIsDir(row), parked); err != nil {
		return nil, err
	}
	moved := *row
	moved.Name = parked
	return &moved, nil
}

// restoreName puts a parked row back under the name it was cleared from.
func (f *Fs) restoreName(ctx context.Context, parked *api.Row, name string) error {
	return f.renameObject(ctx, parked.ID.String(), rowIsDir(parked), name)
}

// Copy duplicates an object on the drive, which moves no bytes and costs the
// account's transfer allowance nothing.
//
// The task always arrives under the source's name, so a copy that also renames
// needs a follow-up rename, and both cases have to clear the name they want
// first: the drive refuses to overwrite, and a task that lands on a name in use
// waits for a decision rclone will not give it.
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	auth, ok := srcObj.fs.taskAuthFor(f)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	dstLeaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	wireLeaf := f.opt.Enc.FromStandardName(dstLeaf)
	if err := validateName(wireLeaf); err != nil {
		return nil, err
	}
	// The copy arrives named as its source, spelled the way the SOURCE's remote
	// stores it: two remotes can carry different encoding options, and the name
	// this run has to clear and then read back is the drive's bytes, not this
	// remote's rendering of them. Move does the same thing.
	srcLeaf := srcObj.fs.opt.Enc.FromStandardName(path.Base(srcObj.remote))

	// Whatever the copy arrives as, that name has to be free in the destination.
	cleared, err := f.clearName(ctx, dstDirID, srcLeaf, srcObj.id)
	if err != nil {
		return nil, err
	}
	info, err := srcObj.fs.copyTaskInfo(ctx, srcObj, auth)
	if err != nil {
		f.undoClear(ctx, cleared, srcLeaf)
		return nil, err
	}
	if _, err := srcObj.fs.runBatchTask(ctx, auth, taskCopy, []taskInfo{info}, dstDirID); err != nil {
		f.undoClear(ctx, cleared, srcLeaf)
		return nil, err
	}
	// A successful COPY reports no id for the object it made, so it is read back
	// from the destination listing (captured 2026-10-22). The source itself may
	// be in that same directory under the same name, so the source id is excluded
	// rather than trusting the first row that matches.
	arrived, err := f.arrivedCopy(ctx, dstDirID, srcLeaf, srcObj.id)
	if err != nil {
		f.undoClear(ctx, cleared, srcLeaf)
		return nil, fmt.Errorf("cloud189: the copy finished but the new object cannot be found in the destination: %w", err)
	}
	res := &uploadResult{id: arrived.ID.String(), name: srcLeaf}
	if err := f.installObject(ctx, dstDirID, wireLeaf, res, nil, true); err != nil {
		// The copy itself is gone by now (installObject takes back what it made
		// when it cannot finish), so the only thing left to undo is the name this
		// run cleared for it.
		f.undoClear(ctx, cleared, srcLeaf)
		return nil, err
	}
	if cleared != nil {
		f.dropObject(ctx, cleared, "a server-side copy took its name")
	}
	return f.rowObject(remote, arrived), nil
}

// undoClear puts back a name this run cleared for an operation that then failed.
func (f *Fs) undoClear(ctx context.Context, parked *api.Row, name string) {
	if parked == nil {
		return
	}
	if err := f.restoreName(ctx, parked, name); err != nil {
		fs.Errorf(f, "cloud189: the object parked as %q could not be given back its name %q: %v", parked.Name, name, err)
	}
}

// copyTaskInfo builds the task entry for one object, in the shape the drive
// expects for the direction it is going.
func (f *Fs) copyTaskInfo(ctx context.Context, obj *Object, auth taskAuth) (taskInfo, error) {
	info := taskInfo{
		FileID:   obj.id,
		FileName: f.opt.Enc.FromStandardName(path.Base(obj.remote)),
		IsFolder: 0,
	}
	// The captured personal to family copy names no source parent at all.
	if auth.copyType == 1 {
		return info, nil
	}
	_, srcDirID, err := f.dirCache.FindPath(ctx, obj.remote, false)
	if err != nil {
		return info, err
	}
	info.ParentID, err = taskParentID(f.realDirID(srcDirID))
	return info, err
}

// Move relocates an object inside the drive, or between the two spaces of one
// account when the task for that direction has been captured.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}
	auth, ok := srcObj.fs.taskAuthFor(f)
	if !ok || auth.copyType != 0 {
		// A cross-space move was never captured; only copy plus delete was.
		return nil, fs.ErrorCantMove
	}
	dstLeaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	wireLeaf := f.opt.Enc.FromStandardName(dstLeaf)
	if err := validateName(wireLeaf); err != nil {
		return nil, err
	}
	stdLeaf, srcDirID, err := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
	if err != nil {
		return nil, err
	}
	// The task names the object as the drive stores it.
	srcLeaf := srcObj.fs.opt.Enc.FromStandardName(stdLeaf)
	srcParentID, err := taskParentID(f.realDirID(srcDirID))
	if err != nil {
		return nil, err
	}
	cleared, err := f.clearName(ctx, dstDirID, srcLeaf, srcObj.id)
	if err != nil {
		return nil, err
	}
	info := taskInfo{FileID: srcObj.id, FileName: srcLeaf, IsFolder: 0, ParentID: srcParentID}
	if _, err := srcObj.fs.runBatchTask(ctx, auth, taskMove, []taskInfo{info}, dstDirID); err != nil {
		f.undoClear(ctx, cleared, srcLeaf)
		return nil, err
	}
	// A move keeps the id, so the object is already the one to report.
	res := &uploadResult{id: srcObj.id, name: srcLeaf, md5: srcObj.md5}
	if err := f.installObject(ctx, dstDirID, wireLeaf, res, nil, false); err != nil {
		// The moved object is left where it got to, under the name it moved
		// with, because it is the original. Clearing that name was this run's
		// doing, so put back what was standing there - which only works while
		// nothing else holds it, and says so in the log when it cannot.
		f.undoClear(ctx, cleared, srcLeaf)
		return nil, err
	}
	if cleared != nil {
		f.dropObject(ctx, cleared, "a moved object took its name")
	}
	srcObj.remote = remote
	return srcObj, nil
}

// DirMove moves a directory and everything under it.
//
// One MOVE task on a directory is recursive: measured 2026-10-22 a directory
// with 18 files in it moved in a single task that reported one object done.
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		return fs.ErrorCantDirMove
	}
	auth, ok := srcFs.taskAuthFor(f)
	if !ok || auth.copyType != 0 {
		return fs.ErrorCantDirMove
	}
	// The helper resolves the ids, refuses a move of or onto a root, creates the
	// destination's parent chain and requires the destination not to exist
	// already - which is what the drive's own conflict answer wants.
	srcID, srcDirectoryID, srcLeaf, dstDirID, _, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}
	parentID, err := taskParentID(f.realDirID(srcDirectoryID))
	if err != nil {
		return err
	}
	// Named as the source remote stores it, the way Move and Copy do. Two remotes
	// of one account cannot reach this line with different encodings today -
	// taskAuthFor refuses a server-side move between two remote instances - but
	// the spelling belongs to the source whichever way the guard evolves.
	wireSrc := srcFs.opt.Enc.FromStandardName(srcLeaf)
	cleared, err := f.clearName(ctx, dstDirID, wireSrc, srcID)
	if err != nil {
		return err
	}
	info := taskInfo{FileID: srcID, FileName: wireSrc, IsFolder: 1, ParentID: parentID}
	if _, err := srcFs.runBatchTask(ctx, auth, taskMove, []taskInfo{info}, dstDirID); err != nil {
		f.undoClear(ctx, cleared, wireSrc)
		return err
	}
	srcFs.dirCache.FlushDir(srcRemote)
	if cleared != nil {
		f.dropObject(ctx, cleared, "a moved directory took its name")
	}
	return nil
}

// ------------------------------------------------------------ names --------

// newParkName builds a name no object on the drive can already hold.
//
// It carries nothing of the original name: the swap addresses objects by id, and
// a short name keeps the whole thing inside the range the character table
// measured (a 300 character name plus a suffix was never tested).
func newParkName() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail in practice, and a stamp alone is still
		// unique enough for one process to park two objects.
		return fmt.Sprintf("rclone-park-%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("rclone-park-%d-%s", time.Now().UnixNano(), hex.EncodeToString(b[:]))
}

// fileRow describes an object that is known to be a file, so that a delete or a
// rename can address it without listing it again.
//
// FileCata is set by hand here, and it is the flag rowIsDir reads: leaving it
// zero would make a file read as a directory, and a delete task would then be
// written with isFolder 1.
func fileRow(id, name, parentID string) *api.Row {
	return &api.Row{ID: api.FlexID(id), Name: name, ParentID: api.FlexID(parentID), FileCata: 1}
}

// renameObject gives the object with this id a new name, synchronously.
//
// The id survives the rename (measured 2026-10-22), which is what makes the
// park-and-swap below addressable at all.
//
// The endpoint does not check what kind of object it is given: a family
// renameFile call handed a directory's id renamed the directory and answered
// size 0 (measured 2026-10-22). So the caller has to know whether it is holding
// a file or a directory, and it must get that from the listing's own flag rather
// than from which endpoint it means to use.
func (f *Fs) renameObject(ctx context.Context, id string, isDir bool, newName string) error {
	if err := validateName(newName); err != nil {
		return err
	}
	if f.family() {
		var requestURI, extra string
		if isDir {
			requestURI = "/family/file/renameFolder.action"
			extra = fmt.Sprintf("familyId=%s&folderId=%s&destFolderName=%s",
				url.QueryEscape(f.familyID), url.QueryEscape(id), url.QueryEscape(newName))
		} else {
			requestURI = "/family/file/renameFile.action"
			extra = fmt.Sprintf("familyId=%s&fileId=%s&destFileName=%s",
				url.QueryEscape(f.familyID), url.QueryEscape(id), url.QueryEscape(newName))
		}
		_, err := f.call(ctx, f.apiBase, http.MethodGet, requestURI, f.get(extra), "")
		return err
	}
	requestURI, idKey, nameKey := "/renameFile.action", "fileId", "destFileName"
	if isDir {
		requestURI, idKey, nameKey = "/renameFolder.action", "folderId", "destFolderName"
	}
	extra := fmt.Sprintf("%s=%s&%s=%s", idKey, url.QueryEscape(id), nameKey, url.QueryEscape(newName))
	// The personal drive's renames are POSTs that carry everything in the query
	// and nothing in the body (captured 2026-10-22: content-length 0).
	_, err := f.call(ctx, f.apiBase, http.MethodPost, requestURI, f.get(extra), "")
	return err
}

// ------------------------------------------------------------ tasks ------

// Batch task types, exactly as the server names them (captured 2026-10-22).
const (
	taskDelete       = "DELETE"
	taskCopy         = "COPY"
	taskMove         = "MOVE"
	taskClearRecycle = "CLEAR_RECYCLE"
)

// Task states. 4 ends the task; the other two are states seen while it runs.
//
// 2 is a conflict the server wants the client to decide. This backend never
// starts a task that can reach it, because a same-name collision is resolved by
// renaming before the task is created rather than by answering the server.
// TODO: getConflictTaskInfo and manageBatchTask are captured but not
// implemented; if a task ever lands in state 2 the operation fails with this
// message instead of guessing at a decision.
const (
	taskStateConflict = 2
	taskStateRunning  = 3
	taskStateDone     = 4
)

// Polling is measured at about 1.2s per round trip for a one-object task, and
// the same payload that failed once succeeded on a second attempt 4s later.
// The bound below is not a measured server limit: it is how long rclone waits
// before deciding the task is never coming back.
var (
	taskPollFirst   = 500 * time.Millisecond
	taskPollMax     = 5 * time.Second
	taskPollWaitMax = 10 * time.Minute
)

// taskInfo is one entry of a batch task's taskInfos array.
//
// The shapes are as the client sends them (captured 2026-10-22): fileId quoted,
// srcParentId a bare number, isFolder 1 for a directory. A number is used for
// the parent because that is what the server writes there; fileId stays text
// because an 18 digit id that passes through a float loses its last digits, and
// an id that lost digits deletes the wrong object.
//
// ParentID is a pointer because the one captured cross-space copy leaves it out
// entirely: a personal file copied into a family was sent as
// {"fileId":"…","fileName":"…","isFolder":0} with no parent at all.
type taskInfo struct {
	FileID   string `json:"fileId"`
	FileName string `json:"fileName"`
	IsFolder int    `json:"isFolder"`
	ParentID *int64 `json:"srcParentId,omitempty"`
}

// taskAuth says which session signs a batch task and which family, if any, the
// task is about.
//
// These two are not readable from the request path, because every batch task
// lives at /batch/... in both drives. Measured 2026-10-22: a family task is
// signed with the family session and names its family in the create body and in
// the poll query, while a personal to family copy is signed with the *personal*
// session, names the destination family in the body and puts nothing in the
// poll's query.
type taskAuth struct {
	family   bool
	familyID string
	copyType int // 1 personal to family, 2 family to personal, 0 for either end staying put
}

// taskAuth describes a task that stays inside this remote's own space.
func (f *Fs) taskAuth() taskAuth {
	if f.family() {
		return taskAuth{family: true, familyID: f.familyID}
	}
	return taskAuth{}
}

// createTaskResponse is the answer to createBatchTask: an id and nothing else.
type createTaskResponse struct {
	TaskID api.FlexID `json:"taskId"`
}

// taskStatusResponse is one poll of checkBatchTask.
//
// Only the counts and the state carry information. res_code is 0 and
// res_message reads 成功 on a task that failed every object, so neither is read
// here at all - classify has already refused a real protocol error by then.
type taskStatusResponse struct {
	TaskStatus     int64      `json:"taskStatus"`
	FailedCount    int64      `json:"failedCount"`
	SuccessedCount int64      `json:"successedCount"`
	SkipCount      int64      `json:"skipCount"`
	SubTaskCount   int64      `json:"subTaskCount"`
	Process        int64      `json:"process"`
	ErrorCode      api.FlexID `json:"errorCode"`
	// SuccessedFileIDs names the objects that got through. MOVE, DELETE and
	// CLEAR_RECYCLE answer it; a successful COPY does not, which is why a copy has
	// to look its new object up in the destination listing (captured 2026-10-22).
	SuccessedFileIDs []api.FlexID `json:"successedFileIdList"`
}

// taskParentID turns a directory id into the number a task's srcParentId wants.
func taskParentID(dirID string) (*int64, error) {
	n, err := strconv.ParseInt(dirID, 10, 64)
	if err != nil {
		return nil, fserrors.NoRetryError(fmt.Errorf("cloud189: the drive answers with the directory id %q, which is not the number a task needs: %w", truncate(dirID, 24), err))
	}
	return &n, nil
}

// createBatchTask starts a server-side task.
//
// It is a method of the drive the source lives on, because that is the session the
// capture shows signing a cross-space task.
func (f *Fs) createBatchTask(ctx context.Context, auth taskAuth, taskType string, targets []taskInfo, targetFolderID string) (api.FlexID, error) {
	infos := "[]"
	if len(targets) > 0 {
		encoded, err := json.Marshal(targets)
		if err != nil {
			return "", fmt.Errorf("cloud189: encoding the task list: %w", err)
		}
		infos = string(encoded)
	}
	// The body is a plain form, in the order the client writes it. Only the
	// values are escaped: the JSON of taskInfos contains = and would otherwise
	// read as three fields.
	pairs := []string{"type=" + taskType, "taskInfos=" + url.QueryEscape(infos)}
	if targetFolderID != "" {
		pairs = append(pairs, "targetFolderId="+url.QueryEscape(targetFolderID))
	}
	// A task names the family it concerns in the body of the create, whether it is
	// signed by the family session or not: the captured personal to family copy sent
	// familyId with a personal key. A task that concerns no family sends neither
	// familyId nor copyType (captured 2026-10-22).
	if auth.familyID != "" {
		pairs = append(pairs, "familyId="+url.QueryEscape(auth.familyID))
	}
	if auth.copyType > 0 {
		pairs = append(pairs, fmt.Sprintf("copyType=%d", auth.copyType))
	}
	body, err := f.callWith(ctx, auth.family, f.apiBase, http.MethodPost, "/batch/createBatchTask.action", f.get(""), strings.Join(pairs, "&"))
	if err != nil {
		return "", err
	}
	var resp createTaskResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("cloud189: decoding createBatchTask: %w", err)
	}
	if resp.TaskID == "" {
		return "", errors.New("cloud189: createBatchTask returned no task id")
	}
	return resp.TaskID, nil
}

// checkBatchTask polls a task once.
// checkBatchTask polls a task once. Only a task signed by the family session puts
// its family in the query; the personal to family copy polled with a bare URL
// (captured 2026-10-22).
func (f *Fs) checkBatchTask(ctx context.Context, auth taskAuth, taskID api.FlexID, taskType string) (*taskStatusResponse, error) {
	extra := ""
	if auth.family {
		extra = "familyId=" + url.QueryEscape(auth.familyID)
	}
	body := "taskId=" + url.QueryEscape(taskID.String()) + "&type=" + taskType
	data, err := f.callWith(ctx, auth.family, f.apiBase, http.MethodPost, "/batch/checkBatchTask.action", f.get(extra), body)
	if err != nil {
		return nil, err
	}
	var st taskStatusResponse
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("cloud189: decoding checkBatchTask: %w", err)
	}
	return &st, nil
}

// runBatchTask starts a task and waits for it to end, and returns the counts it
// finished on after judging them.
func (f *Fs) runBatchTask(ctx context.Context, auth taskAuth, taskType string, targets []taskInfo, targetFolderID string) (*taskStatusResponse, error) {
	taskID, err := f.createBatchTask(ctx, auth, taskType, targets, targetFolderID)
	if err != nil {
		return nil, err
	}
	wait := taskPollFirst
	deadline := time.Now().Add(taskPollWaitMax)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		st, err := f.checkBatchTask(ctx, auth, taskID, taskType)
		if err != nil {
			return nil, err
		}
		switch st.TaskStatus {
		case taskStateDone:
			return st, f.taskVerdict(taskType, len(targets), st)
		case taskStateConflict:
			return nil, fserrors.NoRetryError(fmt.Errorf("cloud189: %s task %s stopped at a name conflict the server wants a client to answer; this backend resolves a same-name target by renaming before it starts a task, so this is not expected - check whether two transfers raced", taskType, taskID))
		case taskStateRunning:
		default:
			fs.Debugf(f, "cloud189: %s task %s reported state %d", taskType, taskID, st.TaskStatus)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("cloud189: %s task %s was still running after %v - the drive may still finish it, so check the result before retrying", taskType, taskID, taskPollWaitMax)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
		if wait *= 2; wait > taskPollMax {
			wait = taskPollMax
		}
	}
}

// taskVerdict reads a finished task.
//
// A task that ends with nothing failed and nothing done is a task the server
// accepted and then ignored, which is how a wrong familyId reads. Reporting that
// as success would let sync believe a file was deleted.
func (f *Fs) taskVerdict(taskType string, wanted int, st *taskStatusResponse) error {
	if st.FailedCount > 0 {
		code := string(st.ErrorCode)
		why := code
		if why == "" {
			// Measured 2026-10-22: a family delete failed with failedCount 1,
			// no errorCode and no error message at all, and the objects were
			// still on the drive.
			why = "the server gave no reason"
		}
		if code == codeNoSpace || code == codeDayFlowOver {
			// A task can report the same two refusals the upload endpoints do,
			// and neither clears itself during a run.
			return f.fatalIfNoSpace(fserrors.NoRetryError(&apiError{
				Status:  http.StatusOK,
				Field:   "errorCode",
				Code:    code,
				Message: fmt.Sprintf("%s task failed for %d of %d object(s)", taskType, st.FailedCount, wanted),
			}))
		}
		return fserrors.NoRetryError(fmt.Errorf("cloud189: %s task failed for %d of %d object(s): %s", taskType, st.FailedCount, wanted, why))
	}
	if st.SkipCount > 0 {
		return fserrors.NoRetryError(fmt.Errorf("cloud189: %s task skipped %d of %d object(s), which is what a name conflict answers with", taskType, st.SkipCount, wanted))
	}
	if wanted > 0 && st.SuccessedCount < int64(wanted) {
		return fserrors.NoRetryError(fmt.Errorf("cloud189: %s task reported success for %d of %d object(s)", taskType, st.SuccessedCount, wanted))
	}
	return nil
}

// ------------------------------------------------------------ quota --------

// quotaState is what the account says about its limits, together with what this
// process has since put on the wire.
//
// Every number is read at run time. Measured on two accounts of different
// membership, transFileSize and transDayFlow are 2GiB on an ordinary one and
// int64 max on a platinum one, so nothing here may be hard-coded and the same
// field means something different per account.
//
// Several copy threads read it at once, so it lives behind a pointer: NewFs
// copies the Fs struct when a remote turns out to name a file, and a mutex
// copied that way would be a mutex nobody shares.
type quotaState struct {
	mu            sync.Mutex
	loaded        bool
	spaceKnown    bool
	transDayFlow  int64     // the whole day's transfer allowance
	transFileSize int64     // one file's ceiling, as this account reads it
	usedDayFlow   int64     // what the account said the day had used, when last asked
	sent          int64     // bytes this process has uploaded since then
	limitsAsk     time.Time // when the gates last went back for limits they never got
	overRunAsk    time.Time // when the gates last re-read the day for a file that looked over the line
	capacity      int64
	available     int64
}

// addSent remembers bytes that went up, so the next file is judged against a
// model of the day that moves while the run moves.
func (q *quotaState) addSent(n int64) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.sent += n
	q.mu.Unlock()
}

// privileges reads the account's limits.
//
// The personal endpoint is asked in both spaces: privileges belong to the owner
// of the account, and no family variant of it was ever captured.
func (f *Fs) privileges(ctx context.Context) (*api.PrivilegesResponse, error) {
	data, err := f.call(ctx, f.apiBase, http.MethodGet, "/getUserPrivilegesV2.action", f.get("flag=1"), "")
	if err != nil {
		return nil, err
	}
	var resp api.PrivilegesResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("cloud189: decoding getUserPrivilegesV2: %w", err)
	}
	// A refusal here arrives with HTTP 200 and a res_code, which is how an
	// account that will not answer this endpoint reads. Reading zero privileges
	// would look like an account with no limits, so it is a failure instead.
	if api.RefusedByResCode(resp.ResCode) {
		return nil, refuse(http.StatusOK, data)
	}
	return &resp, nil
}

// capacity reads the space the drive has.
func (f *Fs) capacity(ctx context.Context) (*api.Capacity, error) {
	requestURI, extra := "/getUserInfo.action", "returnType=JSON"
	if f.family() {
		requestURI = "/family/file/getUserInfo.action"
		extra = "familyId=" + url.QueryEscape(f.familyID)
	}
	data, err := f.call(ctx, f.apiBase, http.MethodGet, requestURI, f.get(extra), "")
	if err != nil {
		return nil, err
	}
	var resp api.Capacity
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("cloud189: decoding getUserInfo: %w", err)
	}
	if api.RefusedByResCode(resp.ResCode) {
		return nil, refuse(http.StatusOK, data)
	}
	return &resp, nil
}

// readQuota refreshes the limits and the space from the account.
//
// The account's own usedDayFlow is a live figure, so it is trusted over this
// process's running total and the local count starts again from here.
func (f *Fs) readQuota(ctx context.Context) error {
	priv, err := f.privileges(ctx)
	if err != nil {
		return err
	}
	space, err := f.capacity(ctx)
	if err != nil {
		return err
	}
	q := f.quota
	q.mu.Lock()
	defer q.mu.Unlock()
	if dayFlow, ok := priv.ValueOf("transDayFlow"); ok {
		q.transDayFlow = dayFlow
	}
	if fileSize, ok := priv.ValueOf("transFileSize"); ok {
		q.transFileSize = fileSize
	}
	if used, ok := priv.UsedDayFlow(); ok {
		q.usedDayFlow = used
	}
	q.sent = 0
	// A drive that answers zero for both numbers was not read at all: real
	// accounts answer in GiB or more, and the family endpoint has no res_code to
	// tell an empty answer from a refused one. Refusing uploads because of a
	// number that was never there would be worse than not gating.
	if space.Capacity > 0 {
		q.capacity = space.Capacity
		q.available = space.Available
		q.spaceKnown = true
	}
	q.loaded = true
	return nil
}

// gateUpload refuses, before anything is read or sent, what this account is
// known not to accept.
//
// The server refuses both conditions itself, at the call that opens the session
// and without taking a byte (measured 2026-10-22: 403 UserDayFlowOverLimited and
// 403 InsufficientStorageSpace). The point of asking first is the source: an
// upload that cannot land should not have read the file, written a temp copy or
// spent the bandwidth.
//
// With no_quota_check nothing is read at all and this returns nil, leaving the
// server's own refusal as the only gate.
func (f *Fs) gateUpload(ctx context.Context, name string, size int64) error {
	q := f.quota
	if q == nil || f.opt.NoQuotaCheck {
		return nil
	}
	q.mu.Lock()
	if !q.loaded {
		// The read done when the remote opened can fail for a reason that goes away -
		// a timeout, a session that has since been exchanged. Leaving the gates shut
		// off for the rest of the run on a figure that was never fetched is not
		// acceptable, because the day's allowance is what the gates exist for, so
		// ask again here. An endpoint that keeps refusing is asked once a minute and
		// not once a file: a long run would otherwise pay two metadata requests per
		// object for an answer that never arrives.
		if !q.limitsAsk.IsZero() && time.Since(q.limitsAsk) < quotaReAskGap {
			q.mu.Unlock()
			return nil
		}
		q.limitsAsk = time.Now()
		q.mu.Unlock()
		if err := f.readQuota(ctx); err != nil {
			return nil
		}
		q.mu.Lock()
	}
	limit := int64(f.opt.MaxFileSize)
	if limit <= 0 {
		limit = q.transFileSize
	}
	dayFlow := q.transDayFlow
	used := q.usedDayFlow + q.sent
	free, spaceKnown := q.available, q.spaceKnown
	q.mu.Unlock()

	if limit > 0 && size > limit {
		// This is not one of the two conditions that only the next day or a cleared
		// drive can fix, and the ceiling may be the caller's own option rather than
		// anything the account said. So it stops this file and says where the number
		// came from, without ending the run under fatal_if_no_space.
		whose := "the single-file ceiling this account publishes"
		if f.opt.MaxFileSize > 0 {
			whose = "--cloud189-max_file_size"
		}
		// A plain error, not an apiError: nothing was sent, so printing it as a
		// server answer with an HTTP status would be a lie about where it came from.
		return fserrors.NoRetryError(fmt.Errorf("cloud189: %s is %s and %s is %s (%s)", name, fs.SizeSuffix(size), whose, fs.SizeSuffix(limit), codeLocalSizeCeiling))
	}
	// usedDayFlow moves while a run is going - other programs, the official
	// client, another machine - so a projected overrun is checked against the
	// account once more before it refuses anything. The check is the one the
	// limits branch above gets: a run that has genuinely used up its day would
	// otherwise pay a fresh metadata pair to every remaining file just to be
	// refused again, and between checks this process's own count keeps the model
	// moving, so what it refuses on is at worst one minute behind.
	if dayFlow > 0 && used+size > dayFlow {
		q.mu.Lock()
		recheck := q.overRunAsk.IsZero() || time.Since(q.overRunAsk) >= quotaReAskGap
		if recheck {
			q.overRunAsk = time.Now()
		}
		q.mu.Unlock()
		if recheck {
			if err := f.readQuota(ctx); err == nil {
				q.mu.Lock()
				dayFlow, used = q.transDayFlow, q.usedDayFlow+q.sent
				q.mu.Unlock()
			}
		}
		if dayFlow > 0 && used+size > dayFlow {
			return f.dayFlowRefusal(name, size, dayFlow, used)
		}
	}
	if spaceKnown {
		if free < 0 {
			// Measured: a drive past full reports a negative number, not zero.
			free = 0
		}
		if size > free {
			return f.uploadRefusal(name, codeNoSpace, fmt.Sprintf("%s is %s and the drive has %s free; space is only freed by hand",
				name, fs.SizeSuffix(size), fs.SizeSuffix(free)))
		}
	}
	return nil
}

// uploadRefusal answers in the same shape as the server's own refusal, because
// the marking on it is what decides whether a run retries the file.
func (f *Fs) uploadRefusal(name, code, why string) error {
	err := &apiError{
		Status:  http.StatusForbidden,
		Field:   "local pre-check",
		Code:    code,
		Message: why,
	}
	return f.fatalIfNoSpace(noSpaceError(err))
}

// dayFlowRefusal words the one refusal that a later day can clear.
func (f *Fs) dayFlowRefusal(name string, size, dayFlow, used int64) error {
	return f.uploadRefusal(name, codeDayFlowOver, fmt.Sprintf("%s needs %s and the day's allowance is %s with %s already used; it comes back with the next day",
		name, fs.SizeSuffix(size), fs.SizeSuffix(dayFlow), fs.SizeSuffix(used)))
}

// About reports the space the drive has.
//
// The day's transfer allowance has no place in fs.Usage, so it is put in the log
// as well: it is the limit that actually stops a transfer, and it is the number
// a person needs when a run refuses a file.
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	space, err := f.capacity(ctx)
	if err != nil {
		return nil, err
	}
	free := space.Available
	if free < 0 {
		free = 0
	}
	used := space.Capacity - space.Available
	if used < 0 {
		used = 0
	}
	usage := &fs.Usage{
		Total: fs.NewUsageValue(space.Capacity),
		Free:  fs.NewUsageValue(free),
		Used:  fs.NewUsageValue(used),
	}
	if priv, perr := f.privileges(ctx); perr == nil {
		limit, hasLimit := priv.ValueOf("transDayFlow")
		usedFlow, hasUsed := priv.UsedDayFlow()
		fileLimit, hasFileLimit := priv.ValueOf("transFileSize")
		if hasLimit && hasUsed {
			fs.Logf(f, "cloud189: transfer allowance %s of %s used today; single file ceiling %s",
				fs.SizeSuffix(usedFlow), fs.SizeSuffix(limit), humanSize(fileLimit, hasFileLimit))
		}
	}
	// The account's own copy of these numbers is what the gates read, so refresh
	// them here rather than leaving a stale model behind after an About.
	if q := f.quota; q != nil {
		if err := f.readQuota(ctx); err != nil {
			fs.Debugf(f, "cloud189: refreshing the account limits: %v", err)
		}
	}
	return usage, nil
}

// humanSize words a size that may not have been reported at all.
func humanSize(n int64, known bool) string {
	if !known {
		return "not reported"
	}
	return fs.SizeSuffix(n).String()
}

// ------------------------------------------------------------ upload -------

// stageCopyBuf is how much is moved at a time while staging a source and
// hashing it. It is a transfer buffer only: it has nothing to do with the part
// size the server asks for.
const stageCopyBuf = 1 << 20

// uploadResult is what a committed upload reports back.
type uploadResult struct {
	id   string // userFileId, kept as text
	name string // the name the server says it stored
	md5  string // the digest the server echoed
}

// stagedSource is the bytes to upload, readable at any offset, with every digest
// the protocol needs already computed.
type stagedSource struct {
	readerAt io.ReaderAt
	plans    []api.PartPlan
	partSize int64
	fileMD5  string   // upper-case hex, the whole file
	partMD5s [][]byte // raw digest per part, in part order
	sliceMD5 string
	tmpPath  string // set when the bytes went to disk, so the temp file can be named in a failure
	release  func()
}

// rejectTrailingBytes fails if the source still has anything after the declared
// number of bytes, because the upload would then store a truncated file while
// reporting the size the source claimed.
func rejectTrailingBytes(in io.Reader) error {
	var extra [1]byte
	n, _ := in.Read(extra[:])
	if n != 0 {
		return fserrors.NoRetryError(errors.New("cloud189: the source holds more bytes than it declared - the size is what the upload is built around, so this file cannot be uploaded as it is"))
	}
	return nil
}

// stageSource reads size bytes from in and keeps them where they can be read
// again: in memory while they fit in one part, in a temp file from there up.
//
// Having the whole file before the first request is not a choice this backend
// gets to make. The session is opened with fileMd5, sliceMd5 and sliceSize, and
// every part's URL is issued with that part's MD5 already inside it - the storage
// layer compares the arriving bytes against it and refuses a part whose digest
// does not match (measured 2026-10-22: a part sent with the wrong digest is
// answered with HTTP 400 by the object store, before the drive ever looks at it).
// So the digests come first, and a source that cannot be read twice has to be
// written down once. This pass computes the whole-file digest at the same time,
// so staging costs one disk write and no extra read of the source.
func (f *Fs) stageSource(in io.Reader, size int64) (*stagedSource, error) {
	if size < 0 {
		return nil, fserrors.NoRetryError(errors.New("cloud189: the size of the source is unknown, and this drive needs the whole file and every part of it hashed before it will open an upload session"))
	}
	st := &stagedSource{
		partSize: chunkPlan(size, int64(f.opt.ChunkSize)),
		release:  func() {},
	}
	st.plans = api.PlanParts(size, st.partSize)

	if size <= st.partSize {
		buf := make([]byte, size)
		if _, err := io.ReadFull(in, buf); err != nil {
			return nil, fmt.Errorf("cloud189: reading the source: %w", err)
		}
		if err := rejectTrailingBytes(in); err != nil {
			return nil, err
		}
		st.readerAt = bytes.NewReader(buf)
		st.fileMD5 = api.MD5Upper(buf)
		if len(st.plans) == 1 {
			st.partMD5s = [][]byte{api.MD5Raw(buf)}
		}
		st.sliceMD5 = api.SliceAggMD5(st.partMD5s, st.fileMD5)
		return st, nil
	}

	tmp, err := os.CreateTemp("", "rclone-cloud189-")
	if err != nil {
		return nil, fmt.Errorf("cloud189: opening the temp file: %w", err)
	}
	tmpPath := tmp.Name()
	st.readerAt = tmp
	st.tmpPath = tmpPath
	st.release = func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	// Everything below can fail, and a failure returns no stagedSource at all, so
	// the caller never gets the closure that is the only reference to a temp file
	// as large as the file. Each of those exits releases it here instead.
	fail := func(err error) (*stagedSource, error) {
		st.release()
		return nil, err
	}
	fileHash := md5.New()
	buf := make([]byte, stageCopyBuf)
	for _, p := range st.plans {
		partHash := md5.New()
		n, err := io.CopyBuffer(io.MultiWriter(tmp, fileHash, partHash), io.LimitReader(in, p.Size), buf)
		if err != nil {
			return fail(fmt.Errorf("cloud189: staging %q: %w", truncate(tmpPath, 40), err))
		}
		if n != p.Size {
			return fail(fserrors.NoRetryError(fmt.Errorf("cloud189: the source gave %d bytes for part %d where %d were declared - it is shorter than its own size", n, p.Index, p.Size)))
		}
		st.partMD5s = append(st.partMD5s, partHash.Sum(nil))
	}
	if err := rejectTrailingBytes(in); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(fmt.Errorf("cloud189: flushing the temp file: %w", err))
	}
	st.fileMD5 = strings.ToUpper(hex.EncodeToString(fileHash.Sum(nil)))
	st.sliceMD5 = api.SliceAggMD5(st.partMD5s, st.fileMD5)
	return st, nil
}

// initUploadResponse is the answer to initMultiUpload.
type initUploadResponse struct {
	Code string `json:"code"`
	Data struct {
		UploadFileID   api.FlexID `json:"uploadFileId"`
		FileDataExists api.FlexID `json:"fileDataExists"`
	} `json:"data"`
}

// callEncrypted sends business parameters inside an encrypted params block,
// which is how the upload endpoints take them.
//
// A POST puts params in the form body and declares the field dictionary in the
// decodefields header; a GET puts it in the query. Both sign the ciphertext: the
// signature covers the request path and the params value, so a GET whose params
// are read only from the body is answered InvalidSignature - which is exactly
// how the first version of the probe lost the part URLs (measured 2026-10-22).
func (f *Fs) callEncrypted(ctx context.Context, operate, requestURI string, params []api.Param) ([]byte, error) {
	_, secret, err := f.keys(strings.HasPrefix(requestURI, "/family"))
	if err != nil {
		return nil, err
	}
	enc, err := api.EncryptParams(secret, api.EncodeParams(params))
	if err != nil {
		return nil, err
	}
	if operate == http.MethodPost {
		return f.call(ctx, f.uploadBase, operate, requestURI, f.get(""), "params="+enc,
			apiHeader{name: "decodefields", value: api.DecodeFields})
	}
	return f.call(ctx, f.uploadBase, operate, requestURI, f.get("params="+enc), "")
}

// uploadPath builds an upload endpoint path for the space in use.
func (f *Fs) uploadPath(leaf string) string {
	if f.family() {
		return "/family/" + leaf
	}
	return "/person/" + leaf
}

// beginUpload opens the multipart session for a file whose digests are already
// known, and reports whether the content was already on the drive.
func (f *Fs) beginUpload(ctx context.Context, dirID, name string, st *stagedSource, size int64) (string, bool, error) {
	params := api.InitParams(dirID, name, st.fileMD5, size, st.sliceMD5, st.partSize)
	if f.family() {
		// The family drive names itself inside the encrypted block, where the
		// personal drive leaves it out (captured 2026-10-22).
		params = append(params, api.Param{Key: "familyId", Value: f.familyID})
	}
	data, err := f.callEncrypted(ctx, http.MethodPost, f.uploadPath("initMultiUpload"), params)
	if err != nil {
		return "", false, err
	}
	var resp initUploadResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return "", false, fmt.Errorf("cloud189: decoding initMultiUpload: %w", err)
	}
	if err := uploadAnswerCode(resp.Code); err != nil {
		return "", false, err
	}
	if resp.Data.UploadFileID == "" {
		return "", false, errors.New("cloud189: initMultiUpload returned no uploadFileId")
	}
	// A second, unsolicited answer comes back here: fileDataExists says the
	// content is already stored, which is the rapid-transfer hit. It costs
	// nothing extra because the server reads the digests the init already sent,
	// and no byte is transferred when it lands.
	return resp.Data.UploadFileID.String(), resp.Data.FileDataExists.String() == "1", nil
}

// uploadAnswerCode turns the upload endpoints' own success word into an error.
//
// Those endpoints answer their code inside a body that can arrive with HTTP 200,
// so the status alone is not a verdict.
func uploadAnswerCode(code string) error {
	if code == "" || strings.EqualFold(code, "SUCCESS") {
		return nil
	}
	return &apiError{Status: http.StatusOK, Field: "code", Code: code}
}

// partEndpoint is one part's presigned PUT, exactly as the server issues it.
type partEndpoint struct {
	RequestURL    string `json:"requestURL"`
	RequestHeader string `json:"requestHeader"`
}

// uploadURLsResponse is the answer to getMultiUploadUrls: one entry per part,
// keyed by the part number rather than ordered, so nothing here depends on the
// server's iteration order.
type uploadURLsResponse struct {
	Code       string                  `json:"code"`
	UploadURLs map[string]partEndpoint `json:"uploadUrls"`
}

// partURLs asks for every part's presigned URL in one call.
//
// TODO: whether a family upload needs familyId here as well as in the init is
// not captured; the uploadFileId already binds the session, so nothing is sent.
func (f *Fs) partURLs(ctx context.Context, uploadFileID string, st *stagedSource) ([]partEndpoint, error) {
	params := []api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
		{Key: "partInfo", Value: api.PartInfo(st.partMD5s)},
	}
	if len(st.plans) == 0 {
		return nil, nil
	}
	data, err := f.callEncrypted(ctx, http.MethodGet, f.uploadPath("getMultiUploadUrls"), params)
	if err != nil {
		return nil, err
	}
	var resp uploadURLsResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("cloud189: decoding getMultiUploadUrls: %w", err)
	}
	if err := uploadAnswerCode(resp.Code); err != nil {
		return nil, err
	}
	out := make([]partEndpoint, len(st.plans))
	for i, p := range st.plans {
		ep, ok := resp.UploadURLs[fmt.Sprintf("partNumber_%d", p.Index)]
		if !ok || ep.RequestURL == "" {
			return nil, fserrors.NoRetryError(fmt.Errorf("cloud189: the server gave no upload URL for part %d of %d", p.Index, len(st.plans)))
		}
		out[i] = ep
	}
	return out, nil
}

// partRetryDelay is how long to wait before trying a part again. It is a
// variable so the offline tests can remove the wait.
var partRetryDelay = func(attempt, partIndex int) time.Duration {
	const (
		base     = 500 * time.Millisecond
		maxDelay = 5 * time.Second
	)
	d := base
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	if d > maxDelay {
		d = maxDelay
	}
	// Spread the parts of one file so a flapping storage node does not see all
	// of them arrive at the same instant.
	return d + time.Duration(partIndex%8)*50*time.Millisecond
}

// putParts uploads every part, retrying only the part that failed.
//
// The retry radius is one part: measured 2026-10-22, a part the storage layer
// refused with HTTP 400 could be sent again on the same presigned URL and the
// upload then committed with the right digest, so a bad part does not poison the
// session and the file does not have to start over.
func (f *Fs) putParts(ctx context.Context, name string, st *stagedSource, eps []partEndpoint) error {
	if len(eps) == 0 {
		return nil
	}
	if len(eps) == 1 {
		return f.putPart(ctx, name, 0, 1, st, eps[0])
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(f.opt.UploadConcurrency)
	for i := range eps {
		if gctx.Err() != nil {
			// A part that ran out of attempts cancels the rest. Launching them
			// anyway would only send requests the upload cannot use.
			break
		}
		i := i
		g.Go(func() error { return f.putPart(gctx, name, i, len(eps), st, eps[i]) })
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}

// putPart sends one part, bounded by --low-level-retries.
//
// Exhausting them is marked NoLowLevelRetry: the copy engine then stops
// re-uploading the whole file, while --retries is still free to re-run the
// transfer with a fresh session.
func (f *Fs) putPart(ctx context.Context, name string, idx, total int, st *stagedSource, ep partEndpoint) error {
	headers := api.ParseRequestHeader(ep.RequestHeader)
	target, err := f.partURLFor(ep.RequestURL)
	if err != nil {
		return fserrors.NoLowLevelRetryError(err)
	}
	attempts := max(fs.GetConfig(ctx).LowLevelRetries, 1)
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := f.putPartOnce(ctx, idx, st, target, headers)
		if err == nil {
			return nil
		}
		if fserrors.IsNoRetryError(err) || fserrors.IsNoLowLevelRetryError(err) {
			return err
		}
		lastErr = err
		if attempt == attempts {
			break
		}
		fs.Debugf(f, "cloud189: %s: part %d/%d attempt %d/%d failed, retrying: %v", name, idx+1, total, attempt, attempts, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(partRetryDelay(attempt, idx)):
		}
	}
	return fserrors.NoLowLevelRetryError(fmt.Errorf("cloud189: %s: part %d/%d failed after %d attempts: %w", name, idx+1, total, attempts, lastErr))
}

// putPartOnce issues one presigned PUT for one part.
func (f *Fs) putPartOnce(ctx context.Context, idx int, st *stagedSource, target string, headers map[string]string) error {
	part := st.plans[idx]
	// A fresh reader for every attempt: the transport consumes the body of an
	// attempt that fails.
	r := io.NewSectionReader(st.readerAt, int64(idx)*st.partSize, part.Size)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, r)
	if err != nil {
		// Do not echo the URL: its query is the storage layer's signature.
		return fserrors.NoLowLevelRetryError(fmt.Errorf("cloud189: the server gave an upload URL this request cannot be built from"))
	}
	// The headers the server issued for this part are replayed unchanged, in
	// particular its Content-MD5 and Authorization: they are what the signature
	// in the URL covers, so recomputing or dropping either is a refusal.
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("user-agent", dartUserAgent)
	req.ContentLength = part.Size
	res, err := f.partClient.Do(req)
	if err != nil {
		// The transport quotes the URL it failed on, and that URL is a live
		// signature for this part.
		return fmt.Errorf("cloud189: part %d: %w", idx+1, scrubErr(err, target))
	}
	defer func() { _ = res.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
	if res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	return fmt.Errorf("cloud189: part %d refused by the storage layer with HTTP %d: %s", idx+1, res.StatusCode,
		truncate(redactQueryValues(redactURLInText(string(body), target), target), 300))
}

// commitUpload closes the session and reports what landed.
func (f *Fs) commitUpload(ctx context.Context, uploadFileID string) (*uploadResult, error) {
	params := []api.Param{{Key: "uploadFileId", Value: uploadFileID}}
	data, err := f.callEncrypted(ctx, http.MethodPost, f.uploadPath("commitMultiUploadFile"), params)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Code string `json:"code"`
		File struct {
			UserFileID api.FlexID `json:"userFileId"`
			FileName   string     `json:"fileName"`
			FileMD5    string     `json:"fileMd5"`
		} `json:"file"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("cloud189: decoding commitMultiUploadFile: %w", err)
	}
	if err := uploadAnswerCode(resp.Code); err != nil {
		return nil, err
	}
	if resp.File.UserFileID == "" {
		return nil, errors.New("cloud189: commitMultiUploadFile returned no userFileId")
	}
	return &uploadResult{id: resp.File.UserFileID.String(), name: resp.File.FileName, md5: resp.File.FileMD5}, nil
}

// upload runs the whole multipart protocol for one file.
func (f *Fs) upload(ctx context.Context, in io.Reader, dirID, name string, size int64) (*uploadResult, error) {
	if err := f.gateUpload(ctx, name, size); err != nil {
		return nil, err
	}
	st, err := f.stageSource(in, size)
	if err != nil {
		return nil, err
	}
	defer st.release()
	res, err := f.uploadStaged(ctx, st, dirID, name, size)
	if err != nil {
		return nil, err
	}
	if res.md5 != "" && !strings.EqualFold(res.md5, st.fileMD5) {
		// The drive had better be reporting the same digest, because that is what
		// rclone will compare on the next run. The value above is not a guess: the
		// storage layer checked every part against it on the way in, and the
		// commit refuses outright if the whole-file value does not match the bytes
		// (measured 2026-10-22, where a wrong fileMd5 came back
		// SliceMd5DoesNotMatch and left no object). So a commit that reports
		// something else means the drive stored a different file than the one that
		// was sent, and saying so is the only safe answer.
		return nil, fserrors.NoRetryError(fmt.Errorf("cloud189: %s: the drive reports the stored content as %s, but the bytes uploaded hash to %s - the object on the drive is not this file", name, res.md5, st.fileMD5))
	}
	res.md5 = st.fileMD5
	return res, nil
}

// uploadStaged is the protocol itself, for bytes that are already staged and
// hashed.
func (f *Fs) uploadStaged(ctx context.Context, st *stagedSource, dirID, name string, size int64) (*uploadResult, error) {
	uploadFileID, rapid, err := f.beginUpload(ctx, dirID, name, st, size)
	if err != nil {
		return nil, err
	}
	if !rapid {
		eps, err := f.partURLs(ctx, uploadFileID, st)
		if err != nil {
			return nil, err
		}
		if err := f.putParts(ctx, name, st, eps); err != nil {
			return nil, err
		}
	}
	res, err := f.commitUpload(ctx, uploadFileID)
	if err != nil {
		return nil, err
	}
	if !rapid {
		// The account counts bytes as they arrive. Keeping a local count of what
		// this process has sent is what makes the next file's pre-check honest
		// between refreshes; a rapid hit moved nothing.
		f.quota.addSent(size)
	}
	return res, nil
}

// dropObject removes one object by id, reporting but never masking a failure.
//
// It exists for the copies this backend makes and then does not need: an upload
// that landed beside the name it wanted, or the object that name was holding.
func (f *Fs) dropObject(ctx context.Context, row *api.Row, why string) {
	if err := f.deleteRows(ctx, []deleteTarget{{row: row}}); err != nil {
		fs.Errorf(f, "cloud189: could not remove %q (%s): %v - it is still on the drive and still costs quota", row.Name, why, err)
	}
}

// / discard drops the object an operation made for itself, when dropping it is the
// right answer to a failure. An object that was already on the drive is never
// passed here.
func (f *Fs) discard(ctx context.Context, row *api.Row, isNew bool, why string) {
	if !isNew {
		return
	}
	f.dropObject(ctx, row, why)
}

// installObject makes the object an upload just created be the one held under
// name, and takes the object that was holding it off the drive.
//
// The drive never overwrites. A rename onto a name that is in use is refused
// with res_code FileAlreadyExists, and an upload onto a name that is in use is
// stored beside it under a name of the server's own choosing. So replacing a
// file is three steps, and the window in which the name holds nothing at all is
// one rename:
//
//  1. park whatever holds the name, under a unique name,
//  2. rename the new object into the name,
//  3. delete the parked object.
//
// Every step addresses an object by id, which is why none of them depends on how
// the server spelled the name it invented. occupier may be nil, meaning nothing
// held the name when it was last listed.
//
// oursIsNew says what may be done with the object being installed when the swap
// cannot finish. A fresh upload or a fresh copy is a duplicate of something the
// drive already has, so it is deleted rather than left behind under a name nobody
// asked for. A moved object is the original, so it stays where it is and the name
// it was reaching for is put back: a failure handler never removes something that
// was already on the drive before this run.
func (f *Fs) installObject(ctx context.Context, dirID, name string, res *uploadResult, occupier *api.Row, oursIsNew bool) error {
	own := fileRow(res.id, res.name, dirID)
	if occupier == nil && res.name == name {
		// Landed exactly where it was wanted. Nothing to do.
		return nil
	}
	if occupier == nil {
		// The object is under some other name: either the server renamed it to
		// avoid a collision (an upload onto a name that is in use is stored
		// beside it, under a name of the server's own choosing), or it is a
		// server-side copy or move that always arrives named after its source.
		// Whoever holds the wanted name, if anyone, has to move first.
		row, err := f.findEntry(ctx, dirID, name)
		switch {
		case err == fs.ErrorObjectNotFound:
			// Free. The object only needs the rename.
		case err != nil:
			f.discard(ctx, own, oursIsNew, "the name it wanted could not be looked up")
			return err
		case rowIsDir(row):
			f.discard(ctx, own, oursIsNew, "a directory holds that name")
			return fserrors.NoRetryError(fmt.Errorf("cloud189: %q is a directory on the drive, so a file cannot take its name", name))
		default:
			occupier = row
		}
	}
	parked := ""
	if occupier != nil {
		parked = newParkName()
		if err := f.renameObject(ctx, occupier.ID.String(), rowIsDir(occupier), parked); err != nil {
			f.discard(ctx, own, oursIsNew, "the object it was meant to replace could not be moved aside")
			return err
		}
	}
	if err := f.renameObject(ctx, res.id, false, name); err != nil {
		if occupier != nil {
			// Put the original back first: it was there before this run and its
			// data is the one thing that must not go missing.
			if rerr := f.restoreName(ctx, occupier, occupier.Name); rerr != nil {
				fs.Errorf(f, "cloud189: %s: the swap failed and the original could not be restored either - object %s is now named %q: %v", name, occupier.ID, parked, rerr)
			}
		}
		f.discard(ctx, own, oursIsNew, "it could not be given the name")
		return err
	}
	if occupier != nil {
		replaced := *occupier
		replaced.Name = parked
		f.dropObject(ctx, &replaced, "it was replaced by a newer copy")
	}
	return nil
}

// redactURLInText removes a presigned URL's query from text that may have to be
// shown to a user.
// The part URLs carry the storage layer's signature, and an error body from that
// layer repeats the URL back. Nothing here needs it: the path says which object,
// and the query only says who may write to it.
func redactURLInText(text, u string) string {
	if u == "" {
		return text
	}
	return strings.ReplaceAll(text, u, redactPartURL(u))
}

// redactPartURL keeps the scheme, host and path of a presigned URL and drops its
// query.
func redactPartURL(u string) string {
	parsed, err := url.Parse(u)
	if err != nil {
		return "<upload url>"
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

// redactQueryValues removes every value the query of u carries from text.
//
// redactURLInText only catches a server that quotes the URL back in one piece. The
// storage layer also answers with just the path and query it was asked for, and
// with the separators written as \u0026 by its own JSON encoder, and then the full
// URL is not in the text to find. The values are what matter - the signature, the
// expiry, the session - so those are what get replaced, however they arrive.
//
// Values shorter than eight characters are left alone: they are part numbers and
// sizes, and masking them would delete half the text without hiding anything.
func redactQueryValues(text, u string) string {
	parsed, err := url.Parse(u)
	if err != nil || parsed.RawQuery == "" {
		return text
	}
	for _, values := range parsed.Query() {
		for _, v := range values {
			if len(v) >= 8 {
				text = strings.ReplaceAll(text, v, "<redacted>")
				// The storage layer may quote the query back with its separators
				// percent-encoded, which is a different string for a value holding
				// `/ + =`. Masking both forms is what makes "however they arrive"
				// true.
				text = strings.ReplaceAll(text, url.QueryEscape(v), "<redacted>")
			}
		}
	}
	return text
}

// scrubbedError is an error whose printed text has secrets taken out of it while
// the error underneath stays unwrappable.
//
// This is needed because Go's transport errors quote the whole URL they were sent
// to, and on this drive a URL is never only a URL: the session exchange carries
// the access token in its query, and a presigned part or download URL carries the
// storage layer's live signature. A dial timeout on either is an ordinary,
// retryable event, and without this it writes a credential into the log.
type scrubbedError struct {
	msg string
	err error
}

func (e *scrubbedError) Error() string { return e.msg }
func (e *scrubbedError) Unwrap() error { return e.err }

// scrubErr rewrites an error's message with each secret replaced by a marker,
// and returns the original error untouched when none of them appears in it.
func scrubErr(err error, secrets ...string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	out := msg
	for _, s := range secrets {
		if s != "" {
			out = strings.ReplaceAll(out, s, "<redacted>")
		}
	}
	if out == msg {
		return err
	}
	return &scrubbedError{msg: out, err: err}
}

// ------------------------------------------------------------ delete -------

// deleteTarget is one object to take off the drive.
type deleteTarget struct {
	row *api.Row
}

// deleteRows sends one batch task for all of them.
//
// A directory delete is recursive on this drive: one DELETE task for a directory
// removes the whole tree and the recycle bin lists only the directory itself
// (measured 2026-10-22), so Purge does not have to walk first.
//
// With hard_delete the objects are cleared out of the recycle bin afterwards,
// because the drive has no delete that skips it. That second step is permanent:
// nothing rclone deleted that way can be restored, and the space is only freed
// by it.
func (f *Fs) deleteRows(ctx context.Context, targets []deleteTarget) error {
	if len(targets) == 0 {
		return nil
	}
	infos := make([]taskInfo, 0, len(targets))
	for _, t := range targets {
		parent, err := taskParentID(f.realDirID(t.row.ParentID.String()))
		if err != nil {
			return err
		}
		infos = append(infos, taskInfo{
			FileID:   t.row.ID.String(),
			FileName: t.row.Name,
			IsFolder: b2i(rowIsDir(t.row)),
			ParentID: parent,
		})
	}
	if _, err := f.runBatchTask(ctx, f.taskAuth(), taskDelete, infos, ""); err != nil {
		return err
	}
	if !f.opt.HardDelete {
		return nil
	}
	_, err := f.runBatchTask(ctx, f.taskAuth(), taskClearRecycle, infos, "")
	return err
}

// b2i writes the flag the task list wants for a directory.
func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// ------------------------------------------------------------ objects ------

// findEntry looks up one name inside dirID by the name the drive stores.
//
// The caller encodes, because encoding is not idempotent: the lookalike table is
// bijective, so 'a＜b＞c' encodes a second time back to 'a<b>c'. Measured with
// this backend's own encoder on 2026-10-22. A name encoded twice is therefore a
// name that does not exist on the drive, which is how a lookup of a legitimate
// name comes back missing.
func (f *Fs) findEntry(ctx context.Context, dirID, wireLeaf string) (*api.Row, error) {
	var found *api.Row
	err := f.listAll(ctx, dirID, func(row *api.Row) bool {
		if row.Name == wireLeaf {
			found = row
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, fs.ErrorObjectNotFound
	}
	return found, nil
}

// NewObject finds the Object at remote
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	row, err := f.findEntry(ctx, dirID, f.opt.Enc.FromStandardName(leaf))
	if err != nil {
		return nil, err
	}
	if rowIsDir(row) {
		return nil, fs.ErrorIsDir
	}
	return &Object{
		fs:      f,
		remote:  remote,
		id:      row.ID.String(),
		size:    row.Size,
		md5:     row.MD5,
		modTime: parseDriveTime(row.LastOpTime, row.CreateDate),
		urlMu:   new(sync.Mutex),
	}, nil
}

// Put uploads an object, making the directories above it as needed.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() == 0 {
		// TODO: whether a multipart session accepts a file of no bytes has never
		// been measured, and the protocol's first move is to declare a size, so
		// this says it cannot rather than guessing. The sibling drives in this
		// fork answer the same way for the same reason.
		return nil, fs.ErrorCantUploadEmptyFiles
	}
	leaf, dirID, err := f.dirCache.FindPath(ctx, src.Remote(), true)
	if err != nil {
		return nil, err
	}
	wireLeaf := f.opt.Enc.FromStandardName(leaf)
	if err := validateName(wireLeaf); err != nil {
		return nil, err
	}
	res, err := f.upload(ctx, in, dirID, wireLeaf, src.Size())
	if err != nil {
		return nil, err
	}
	if err := f.installObject(ctx, dirID, wireLeaf, res, nil, true); err != nil {
		return nil, err
	}
	return &Object{
		fs:     f,
		remote: src.Remote(),
		id:     res.id,
		size:   src.Size(),
		md5:    strings.ToUpper(res.md5),
		urlMu:  new(sync.Mutex),
		// The drive stamped the moment it took the file and will not tell us what
		// that moment was without another listing, so this reports the time the
		// upload finished - seconds away from the stamp, and the next listing
		// replaces it with the drive's own. A zero time here is not an option: it
		// reads as year 1 to anything that compares modification times.
		modTime: time.Now(),
	}, nil
}

// ------------------------------------------------------------ download -----

// downloadURL returns a link the file can be read from.
//
// The link comes back HTML-escaped - the family endpoint in particular sends
// `&amp;` inside its own query string - so it is unescaped before it is used.
// An escaped link requested as-is is refused.
func (f *Fs) downloadURL(ctx context.Context, id string) (string, time.Duration, error) {
	var requestURI, extra string
	ttl := personalURLTTL
	if f.family() {
		requestURI = "/family/file/getFileDownloadUrl.action"
		extra = fmt.Sprintf("familyId=%s&fileId=%s", url.QueryEscape(f.familyID), url.QueryEscape(id))
		ttl = familyURLTTL
	} else {
		requestURI = "/getFileDownloadUrl.action"
		extra = "fileId=" + url.QueryEscape(id)
	}
	body, err := f.call(ctx, f.apiBase, http.MethodGet, requestURI, f.get(extra), "")
	if err != nil {
		return "", 0, err
	}
	var resp struct {
		FileDownloadURL string `json:"fileDownloadUrl"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", 0, fmt.Errorf("cloud189: decoding the download link: %w", err)
	}
	link := html.UnescapeString(resp.FileDownloadURL)
	if link == "" {
		return "", 0, fmt.Errorf("cloud189: the download link for %s came back empty", id)
	}
	return link, ttl, nil
}

// Open opens the file for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if o.id == "" {
		return nil, errors.New("cloud189: cannot open an object with no id")
	}
	link, err := o.fs.downloadURLCached(ctx, o)
	if err != nil {
		return nil, err
	}
	res, err := o.get(ctx, link, options...)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= http.StatusMultipleChoices {
		// The link may simply have aged out between caching and use. Re-mint
		// once and retry; what an expired link answers with has never been
		// captured, so no status is treated as proof of expiry.
		_ = res.Body.Close()
		o.invalidateURL()
		link, err = o.fs.downloadURLCached(ctx, o)
		if err != nil {
			return nil, err
		}
		if res, err = o.get(ctx, link, options...); err != nil {
			return nil, err
		}
		if res.StatusCode >= http.StatusMultipleChoices {
			defer func() { _ = res.Body.Close() }()
			body, _ := io.ReadAll(io.LimitReader(res.Body, 256))
			return nil, fmt.Errorf("cloud189: download: HTTP %d: %s", res.StatusCode,
				truncate(redactQueryValues(redactURLInText(string(body), link), link), 200))
		}
	}
	return res.Body, nil
}

// get issues the download request, following the redirect to the storage host
// the server chose.
func (o *Object) get(ctx context.Context, link string, options ...fs.OpenOption) (*http.Response, error) {
	fs.FixRangeOption(options, o.size)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, http.NoBody)
	if err != nil {
		return nil, err
	}
	for k, v := range fs.OpenOptionHeaders(options) {
		req.Header.Set(k, v)
	}
	req.Header.Set("user-agent", dartUserAgent)
	res, err := o.fs.httpClient.Do(req)
	if err != nil {
		// The link is a signed URL and the transport quotes whatever it failed
		// on, so the failure is reported without it.
		return nil, fmt.Errorf("cloud189: download: %w", scrubErr(err, link))
	}
	return res, nil
}

// downloadURLCached returns a usable link for the object, minting one when the
// cached link has passed its window.
func (f *Fs) downloadURLCached(ctx context.Context, o *Object) (string, error) {
	o.urlMu.Lock()
	defer o.urlMu.Unlock()
	if o.url != "" && time.Until(o.urlExpiry) > 0 {
		return o.url, nil
	}
	link, ttl, err := f.downloadURL(ctx, o.id)
	if err != nil {
		return "", err
	}
	o.url, o.urlExpiry = link, time.Now().Add(ttl)
	return link, nil
}

// invalidateURL drops the cached download link.
func (o *Object) invalidateURL() {
	o.urlMu.Lock()
	o.url, o.urlExpiry = "", time.Time{}
	o.urlMu.Unlock()
}

// ------------------------------------------------------------ object info --

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info { return o.fs }

// Remote returns the remote path
func (o *Object) Remote() string { return o.remote }

// Size in bytes
func (o *Object) Size() int64 { return o.size }

// ModTime returns the modification time the drive recorded
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }

// String returns a description of the object
func (o *Object) String() string {
	if o == nil {
		return "<nil cloud189 object>"
	}
	return o.remote
}

// Hash returns the MD5 the drive records for this object
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if ty != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	return strings.ToLower(o.md5), nil
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool { return true }

// SetModTime reports that the drive cannot be told a modification time
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	// The sentinel is what rclone's callers look for - operations.Copy and the
	// chunk-writer path both treat ErrorCantSetModTime as "unsupported" rather
	// than as a failed transfer - and the text says why.
	return fmt.Errorf("cloud189: the drive stamps its own times and offers no way to set one: %w", fs.ErrorCantSetModTime)
}

// Update replaces the contents of the object.
//
// The old object is known by id here, so replacing it costs no listing: the new
// bytes land beside it and then take its name.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	if src.Size() == 0 {
		// Same reason as in Put, and the same answer.
		return fs.ErrorCantUploadEmptyFiles
	}
	parentDir, leaf := dircache.SplitPath(o.remote)
	dirID, err := o.fs.dirCache.FindDir(ctx, parentDir, false)
	if err != nil {
		return err
	}
	wireLeaf := o.fs.opt.Enc.FromStandardName(leaf)
	if err := validateName(wireLeaf); err != nil {
		return err
	}
	occupier := fileRow(o.id, wireLeaf, dirID)
	res, err := o.fs.upload(ctx, in, dirID, wireLeaf, src.Size())
	if err != nil {
		return err
	}
	if err := o.fs.installObject(ctx, dirID, wireLeaf, res, occupier, true); err != nil {
		return err
	}
	o.id = res.id
	o.size = src.Size()
	o.md5 = strings.ToUpper(res.md5)
	// Same as after a Put: the drive stamped its own moment and the next listing
	// carries it, so the object reports the time this upload finished rather than
	// the time of the object it replaced.
	o.modTime = time.Now()
	o.invalidateURL()
	return nil
}

// Remove deletes the object.
//
// It goes to the recycle bin unless hard_delete is set, and the drive has no
// delete that skips the bin: with hard_delete rclone deletes and then clears the
// entry it just made, which is permanent.
func (o *Object) Remove(ctx context.Context) error {
	parentDir, leaf := dircache.SplitPath(o.remote)
	dirID, err := o.fs.dirCache.FindDir(ctx, parentDir, false)
	if err != nil {
		return err
	}
	wireLeaf := o.fs.opt.Enc.FromStandardName(leaf)
	return o.fs.deleteRows(ctx, []deleteTarget{{row: fileRow(o.id, wireLeaf, dirID)}})
}

// requireKnownFamilyRoot refuses a destructive operation when this remote does not
// know where its family root is.
//
// The root id is read from a listing of the alias, and that listing can come back
// empty (measured 2026-10-23, with 24 GiB of entries still on the drive). The read
// path tolerates the alias because a listing against it still works, but a purge or
// a directory removal addressed at it may be aimed at something this remote never
// chose - one such run emptied a family drive into its recycle bin. So the
// destructive paths refuse rather than guess.
func (f *Fs) requireKnownFamilyRoot(what string) error {
	if f.opt.Space != spaceFamily || f.familyRootID != "" || f.opt.RootFolderID != "" {
		return nil
	}
	return fserrors.NoRetryError(fmt.Errorf("cloud189: the family root id could not be read, so %s will not be sent - set family_id or root_folder_id", what))
}

// Purge removes a directory and everything under it.
//
// One DELETE task on a directory takes the whole tree with it: measured
// 2026-10-22 a directory holding 18 files went away in a single task that
// reported successedCount 1, and the recycle bin listed only the directory
// itself. So there is no reason to walk the tree first, and a purge of a deep
// directory costs one request rather than one per entry.
func (f *Fs) Purge(ctx context.Context, dir string) error {
	if err := f.requireKnownFamilyRoot("a purge"); err != nil {
		return err
	}
	if dir == "" {
		// The remote's own root cannot be deleted, only emptied. The drive takes
		// several objects in one task, so that is one request whatever the root
		// holds.
		//
		// The id comes from the dircache, which is the root this Fs was opened at,
		// NOT rootID(): that one answers the space's top level, and using it here
		// made `purge` of a remote opened at a subdirectory empty the whole drive.
		// Measured 2026-10-23: the integration suite's teardown purge took a family
		// drive's root with it, twice.
		dirID := f.fsRootID
		if dirID == "" {
			return fserrors.NoRetryError(errors.New("cloud189: this remote has not been opened yet, so its root is unknown"))
		}
		rows, err := f.rowsIn(ctx, dirID)
		if err != nil {
			return err
		}
		targets := make([]deleteTarget, 0, len(rows))
		for _, row := range rows {
			targets = append(targets, deleteTarget{row: row})
		}
		if err := f.deleteRows(ctx, targets); err != nil {
			return err
		}
		// The directories that went are dropped from the cache by their paths,
		// so a later Mkdir of one of them creates it again instead of writing
		// into an id the drive no longer has. The root itself keeps its id, so
		// the cache is not reset wholesale.
		for _, row := range rows {
			if rowIsDir(row) {
				f.dirCache.FlushDir(f.opt.Enc.ToStandardName(row.Name))
			}
		}
		return nil
	}
	parentDir, leaf := dircache.SplitPath(dir)
	parentID, err := f.dirCache.FindDir(ctx, parentDir, false)
	if err != nil {
		return err
	}
	row, err := f.findEntry(ctx, parentID, f.opt.Enc.FromStandardName(leaf))
	if err != nil {
		if err == fs.ErrorObjectNotFound {
			return fs.ErrorDirNotFound
		}
		return err
	}
	if !rowIsDir(row) {
		return fs.ErrorDirNotFound
	}
	if err := f.deleteRows(ctx, []deleteTarget{{row: row}}); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// rowsIn lists every entry of a directory as the drive describes it.
//
// This is the listing List already walks, kept as rows because a task needs the
// id, the parent id and the directory flag, all of which rowToEntry drops on the
// way to an fs.DirEntry.
func (f *Fs) rowsIn(ctx context.Context, dirID string) ([]*api.Row, error) {
	var rows []*api.Row
	err := f.listAll(ctx, dirID, func(row *api.Row) bool {
		rows = append(rows, row)
		return false
	})
	if err != nil {
		return nil, err
	}
	return rows, nil
}

// ------------------------------------------------------------ chunk writer -

// digestsFrom computes the whole-file and per-part MD5s of bytes that are
// already written down, in one forward pass over them.
//
// It is the chunked path's version of what stageSource does while it copies:
// there is no source stream left to read there, only the file the chunks went
// into.
func digestsFrom(ra io.ReaderAt, plans []api.PartPlan, partSize int64) (string, [][]byte, error) {
	fileHash := md5.New()
	partMD5s := make([][]byte, 0, len(plans))
	buf := make([]byte, stageCopyBuf)
	for _, p := range plans {
		partHash := md5.New()
		n, err := io.CopyBuffer(io.MultiWriter(fileHash, partHash), io.NewSectionReader(ra, int64(p.Index-1)*partSize, p.Size), buf)
		if err != nil {
			return "", nil, fmt.Errorf("cloud189: reading back part %d of the staged file: %w", p.Index, err)
		}
		if n != p.Size {
			return "", nil, fserrors.NoRetryError(fmt.Errorf("cloud189: the staged file gave %d bytes for part %d where %d were written", n, p.Index, p.Size))
		}
		partMD5s = append(partMD5s, partHash.Sum(nil))
	}
	return strings.ToUpper(hex.EncodeToString(fileHash.Sum(nil))), partMD5s, nil
}

// chunkWriter takes the parts of a multi-threaded copy into a temp file and
// uploads the whole thing when the last one has landed.
//
// The upload cannot start before the last part arrives, because the session is
// opened with the digest of every part in it. So this writer collects, and the
// protocol runs at Close.
type chunkWriter struct {
	f        *Fs
	tmp      *os.File
	tmpPath  string
	remote   string
	dirID    string
	leaf     string // the name as it goes on the wire
	size     int64
	partSize int64
	plans    []api.PartPlan

	mu   sync.Mutex
	done []bool // one per part, so a chunk that never arrived is a hole and not a shorter sum
}

// WriteChunk stores one part at its offset in the staged file.
//
// Chunks arrive from several goroutines at once, which is why the file is
// opened once and written at absolute offsets rather than appended to.
func (w *chunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if chunkNumber < 0 || chunkNumber >= len(w.plans) {
		return 0, fserrors.NoRetryError(fmt.Errorf("cloud189: %s: chunk %d is past the %d parts this upload was planned around", w.leaf, chunkNumber, len(w.plans)))
	}
	plan := w.plans[chunkNumber]
	n, err := io.Copy(io.NewOffsetWriter(w.tmp, int64(chunkNumber)*w.partSize), io.LimitReader(reader, plan.Size))
	if err != nil {
		return 0, fmt.Errorf("cloud189: %s: staging chunk %d: %w", w.leaf, chunkNumber+1, err)
	}
	if n != plan.Size {
		return 0, fserrors.NoRetryError(fmt.Errorf("cloud189: %s: chunk %d gave %d bytes where %d were due", w.leaf, chunkNumber+1, n, plan.Size))
	}
	w.mu.Lock()
	w.done[chunkNumber] = true
	w.mu.Unlock()
	return n, nil
}

// Close checks that nothing is missing, hashes the staged file and runs the
// upload.
func (w *chunkWriter) Close(ctx context.Context) error {
	defer w.cleanup()
	for i, wasWritten := range w.done {
		if !wasWritten {
			// A hole would upload a file with the right size and the wrong
			// bytes, and the digests would agree with the hole, because they are
			// computed from what is in the file.
			return fserrors.NoRetryError(fmt.Errorf("cloud189: %s: chunk %d of %d never arrived, so the staged file is incomplete", w.leaf, i+1, len(w.plans)))
		}
	}
	if err := w.f.gateUpload(ctx, w.leaf, w.size); err != nil {
		return err
	}
	if err := w.tmp.Sync(); err != nil {
		return fmt.Errorf("cloud189: %s: flushing the staged file: %w", w.leaf, err)
	}
	fileMD5, partMD5s, err := digestsFrom(w.tmp, w.plans, w.partSize)
	if err != nil {
		return err
	}
	st := &stagedSource{
		readerAt: w.tmp,
		plans:    w.plans,
		partSize: w.partSize,
		fileMD5:  fileMD5,
		partMD5s: partMD5s,
	}
	st.sliceMD5 = api.SliceAggMD5(st.partMD5s, st.fileMD5)
	res, err := w.f.uploadStaged(ctx, st, w.dirID, w.leaf, w.size)
	if err != nil {
		return err
	}
	return w.f.installObject(ctx, w.dirID, w.leaf, res, nil, true)
}

// Abort drops the staged file. Nothing was sent to the drive yet, so there is no
// session to close.
func (w *chunkWriter) Abort(ctx context.Context) error {
	w.cleanup()
	return nil
}

func (w *chunkWriter) cleanup() {
	if w.tmp != nil {
		_ = w.tmp.Close()
		_ = os.Remove(w.tmpPath)
		w.tmp = nil
	}
}

// OpenChunkWriter is the upload path a multi-threaded copy uses: the parts are
// written as they arrive and the file goes up once the set is complete.
//
// Without this, --multi-thread-streams buys nothing on this backend, because the
// copy engine falls back to a single stream when there is no chunk writer.
func (f *Fs) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (fs.ChunkWriterInfo, fs.ChunkWriter, error) {
	size := src.Size()
	if size < 0 {
		return fs.ChunkWriterInfo{}, nil, fserrors.NoRetryError(errors.New("cloud189: a multi-threaded upload needs a source of known size"))
	}
	if size == 0 {
		return fs.ChunkWriterInfo{}, nil, fs.ErrorCantUploadEmptyFiles
	}
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return fs.ChunkWriterInfo{}, nil, err
	}
	wireLeaf := f.opt.Enc.FromStandardName(leaf)
	if err := validateName(wireLeaf); err != nil {
		return fs.ChunkWriterInfo{}, nil, err
	}
	// The size is already known here, so the account is asked before a temp file as
	// large as the file is written down. Close asks once more, because what the day
	// has used moves while the parts arrive.
	if err := f.gateUpload(ctx, wireLeaf, size); err != nil {
		return fs.ChunkWriterInfo{}, nil, err
	}
	partSize := chunkPlan(size, int64(f.opt.ChunkSize))
	plans := api.PlanParts(size, partSize)

	tmp, err := os.CreateTemp("", "rclone-cloud189-")
	if err != nil {
		return fs.ChunkWriterInfo{}, nil, fmt.Errorf("cloud189: opening the temp file: %w", err)
	}
	if err := tmp.Truncate(size); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return fs.ChunkWriterInfo{}, nil, fmt.Errorf("cloud189: making room in the temp file: %w", err)
	}
	w := &chunkWriter{
		f:        f,
		tmp:      tmp,
		tmpPath:  tmp.Name(),
		remote:   remote,
		dirID:    dirID,
		leaf:     wireLeaf,
		size:     size,
		partSize: partSize,
		plans:    plans,
		done:     make([]bool, len(plans)),
	}
	return fs.ChunkWriterInfo{ChunkSize: partSize, Concurrency: f.opt.UploadConcurrency}, w, nil
}

// ------------------------------------------------------------ assertions ---

var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.ChunkWriter     = (*chunkWriter)(nil)
	_ fs.OpenChunkWriter = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ dircache.DirCacher = (*Fs)(nil)
)
