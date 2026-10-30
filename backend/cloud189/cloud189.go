// Package cloud189 provides an interface to the China Telecom cloud drive
// (天翼云盘).
package cloud189

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path"
	"regexp"
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
	DownloadChunkSize fs.SizeSuffix        `config:"download_chunk_size"`
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
				"exchanged session. That is safe to share: measured 2026-10-22 and 2026-10-25, exchanging " +
				"again hands out a new session key (the secret usually changes as well) and the key it " +
				"replaces keeps answering, so the official client and other runs are not signed out - six " +
				"exchanges in a row left six keys alive at once. Set this if you want rclone to fail " +
				"loudly instead of touching the login at all.",
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
			Name: "download_chunk_size",
			Help: "Size of each chunk the multi-thread copy engine reads from the source.\n\n" +
				"The upload part size stays the server's ladder value (chunk_size); this option only " +
				"changes how many bytes one stream reads before handing them to the staged file. Bigger " +
				"chunks mean fewer source range requests, fewer buffer allocations and less per-chunk " +
				"staging work - the part digests at Close are still computed once per upload part, so " +
				"this does not cut hashing.\n\n" +
				"Memory: the engine reserves one chunk per stream, plus one more while it waits for a " +
				"slot, and the stream count is the larger of this backend's upload_concurrency and " +
				"--multi-thread-streams, so expect about (streams+1) times this value per file in " +
				"flight. The engine hangs without an error if --max-buffer-memory is set below this " +
				"value.\n\n" +
				"0 means use the upload part size, which is also what every measurement before " +
				"2026-10-31 assumed.",
			Default:  fs.SizeSuffix(0),
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
	ae := &apiError{Status: status, Body: describeBody(body, 500)}
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
		return body, fmt.Errorf("cloud189: server answered HTTP %d: %s", status, describeBody(body, 200))
	}
	return body, noSpaceError(refuse(status, body))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// secretInBody matches a credential-shaped value in whatever the drive answered:
// the key name and its separator, then the value itself.
//
// The key list is what the captures show this drive sending around - sessionKey
// (which it has echoed back inside an error body, measured 2026-10-26), the
// access token, and the signature parameters of the download and upload URLs.
var secretInBody = regexp.MustCompile(`(?i)(session[_-]?key|access[_-]?token|refresh[_-]?token|["a-z_]*token|sk|sign(?:ature)?|cookie)(["']?\s*[:=]\s*["']?)([^"'&,\s}\\]{6,})`)

// describeBody is what an error may quote from a response body.
//
// The body is worth quoting - a Tomcat page or an unexpected shape is otherwise
// undebuggable - but not verbatim: this drive has been measured putting its own
// sessionKey into an error body, and an error string goes to the log. So each
// credential-shaped value is replaced by its length, the key name stays, and the
// fragment is capped. Nothing that can be used to call the drive survives this.
func describeBody(body []byte, n int) string {
	s := truncate(string(body), n)
	var out strings.Builder
	for {
		m := secretInBody.FindStringSubmatchIndex(s)
		if m == nil {
			out.WriteString(s)
			return out.String()
		}
		// groups: 1 the key name, 2 its separator, 3 the value itself
		valueStart, valueEnd := m[6], m[7]
		out.WriteString(s[:valueStart])
		fmt.Fprintf(&out, "<masked len=%d>", valueEnd-valueStart)
		s = s[valueEnd:]
	}
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

	familyID     string // family drive id, when space is family
	fsRootID     string // the id the dircache was seeded with, i.e. this remote's own root
	familyRootID string // the id a request body names as this remote's anchor
	// familySpaceRoot is the drive's own top level, read off a listing by
	// discoverFamilyRoot, and the only id whose listing has to be addressed by the
	// `0` alias. It is kept apart from familyRootID because root_folder_id can name
	// a subdirectory, and that anchor is not the space root.
	familySpaceRoot string      // the family drive's own top level, once discovered
	apiBase         string      // base URL of the file API, which is apiHost for any remote a user configures
	uploadBase      string      // where upload traffic goes: the server's host, or upload_zone
	quota           *quotaState // the account's limits, read at run time

	deleted *deletedRows // ids this process deleted, kept against the listing lag
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
	// download_chunk_size only decides how much one stream reads at a time, so
	// it has no server-side constraint; 0 keeps the old behaviour (one upload
	// part per read). A tiny value is refused because the copy engine allocates
	// one bool per chunk plus one buffer per stream, so 1 byte would ask for as
	// many chunks as the file has bytes.
	if opt.DownloadChunkSize < 0 {
		return nil, fmt.Errorf("cloud189: download_chunk_size must be 0 or more, got %s", opt.DownloadChunkSize)
	}
	if opt.DownloadChunkSize > 0 && opt.DownloadChunkSize < fs.SizeSuffix(1<<20) {
		return nil, fmt.Errorf("cloud189: download_chunk_size must be 0 (use the upload part size) or at least 1Mi, got %s", opt.DownloadChunkSize)
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
		deleted:    &deletedRows{ids: map[string]time.Time{}},
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

	if err := f.resolveRoot(ctx); err != nil {
		if errors.Is(err, fs.ErrorIsFile) {
			return f, err
		}
		return nil, err
	}
	return f, nil
}

// resolveRoot finishes what NewFs started: it resolves the root path, tells a
// file from a directory, and fixes the purge anchor. It returns
// fs.ErrorIsFile - with the Fs re-rooted at the file's directory - when the
// root names a file.
//
// A root that never resolved leaves fsRootID empty: the destructive paths
// refuse rather than guess. Guessing is what the space root here used to mean -
// measured live 2026-10-23, a remote opened at a subdirectory carried it, so
// `rclone purge remote:sub` listed the drive's top level and sent one DELETE
// task naming every entry of it, and a remote opened at a family subdirectory
// emptied the family root the same way before the server happened to refuse
// such a batch.
func (f *Fs) resolveRoot(ctx context.Context) error {
	// The cache NewFs seeded walks from the space root through the whole path
	// this remote was asked for. It is what gets restored below when the path
	// names nothing: a later write has to create the missing chain where the
	// user pointed, not where this walk happened to give up.
	seeded, origRoot := f.dirCache, f.root
	err := f.dirCache.FindRoot(ctx, false)
	if err == nil {
		return f.resolveAnchor(ctx)
	}
	// The walk failed at the leaf, and the directories above it are already in
	// the cache: assume the leaf is a file and re-root there. The re-rooted
	// dircache is seeded with the parent's id, which the failed walk cached, so
	// nothing above the leaf is listed again - a fresh dircache seeded with the
	// space root would walk the whole chain twice, and the Fs copy upstream's
	// shape makes for this carries mutexes with it.
	newRoot, remote := dircache.SplitPath(f.root)
	parentID, cached := f.dirCache.Get(newRoot)
	if !cached {
		// The walk never reached the leaf's parent, so the leaf cannot be a
		// file inside it: the path names nothing.
		f.dirCache.ResetRoot()
		f.fsRootID = ""
		return nil
	}
	f.root = newRoot
	// The re-rooted dircache is seeded with the parent's own id and an empty
	// root path, so nothing above the leaf is listed again. Seeding it with the
	// parent path instead would send the next walk looking for that path inside
	// itself.
	f.dirCache = dircache.New("", parentID, f)
	if _, oerr := f.NewObject(ctx, remote); oerr != nil {
		if oerr == fs.ErrorObjectNotFound {
			// No root and no file: the path names nothing. The remote keeps the
			// path it was asked for and the cache goes back to the one seeded
			// with the space root, so a listing here says "directory not found"
			// and a write makes the missing chain in the place the user named.
			// Leaving the parent-seeded cache in place would make this Fs claim
			// one path while resolving every operation against its parent.
			f.root = origRoot
			f.dirCache = seeded
			f.dirCache.ResetRoot()
			f.fsRootID = ""
			return nil
		}
		return oerr
	}
	// The anchor is the file's directory, which the re-rooted cache now holds.
	if aerr := f.resolveAnchor(ctx); aerr != nil {
		return aerr
	}
	return fs.ErrorIsFile
}

// resolveAnchor reads back the id the dircache resolved for the root this remote
// is opened at. The seed rootID() names the space's top level, which is the true
// anchor only when root is empty; a remote opened at a subdirectory has to carry
// the id the walk found, because Purge("") and the other whole-directory paths
// address that directory.
func (f *Fs) resolveAnchor(ctx context.Context) error {
	id, err := f.dirCache.FindDir(ctx, "", false)
	if err != nil {
		return err
	}
	f.fsRootID = id
	return nil
}

// rootID returns the id the path is anchored at.
//
// An explicit root_folder_id wins, in either drive. Otherwise the personal drive
// has a constant root and the family drive does not: alias 0 serves listings and
// task targets but not createFolder, so the real id is read off a listing. An
// empty family drive has nothing to read it from, and then only the operations
// that accept the alias can work - which is what root_folder_id is for.
func (f *Fs) rootID(ctx context.Context) (string, error) {
	if f.opt.Space == spaceFamily && f.familySpaceRoot == "" {
		// The drive's own top level has to be known even when the user named a
		// root folder, because it may have named that very top level - which is
		// the id the option's own help recommends. A listing addressed at the
		// number answers an empty page with no error (measured 2026-10-22), so an
		// absence check aimed at the number would read a full drive as empty and
		// call a surviving object deleted. Discovery does not change what this
		// remote is opened at; it only makes the translation in listDirID total.
		if _, err := f.discoverFamilyRoot(ctx); err != nil {
			fs.Debugf(f, "cloud189: could not read the family drive's own top level for the listing translation (%v)", err)
		}
	}
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
	f.familySpaceRoot = rootID
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

// listDirID is the id a listing has to name: the alias wherever the family
// drive's own top level appears, because a family listing addressed at that
// numeric id answers an empty page with no error and reads as an empty drive
// (measured 2026-10-22, the reason rootID always answers the alias). Request
// bodies want the numeric id, so this is the inverse of realDirID and the two
// must not be swapped.
//
// Only familySpaceRoot is translated. familyRootID may instead be the anchor the
// user named with root_folder_id, which is an ordinary directory whose listing
// does answer its numeric id - rewriting that one would look in the wrong
// directory and report whatever it holds as gone.
func (f *Fs) listDirID(id string) string {
	if f.opt.Space == spaceFamily && f.familySpaceRoot != "" && id == f.familySpaceRoot {
		return familyRootAlias
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
	_, err := f.walkListing(ctx, dirID, maxListPages, fn)
	return err
}

// listingWalk is what one walk of a directory came to.
//
// total is the number the drive itself gave for the directory: measured
// 2026-10-28 against a directory of 240 objects, `fileListAO.count` answered 240
// on a page that carried 200 rows and again on the page that carried 40, while
// `fileListSize` answered the rows on that page. So count is the directory's
// total, not the page's length, and rows == total with the walk exhausted is proof
// the walk saw everything - which is the one thing an absence answer needs.
type listingWalk struct {
	rows      int
	total     int
	exhausted bool // a page arrived with nothing new: the listing ended
	stopped   bool // the caller's own function ended the walk first
}

// complete reports whether the walk can be read as "everything the directory
// holds was seen".
//
// Two things have to line up: the walk reached its end, and the rows it saw are
// the number the drive itself claimed for the directory. The second half is why a
// total of zero is never accepted - "this directory holds nothing" is exactly what
// the endpoint also answers for an address it does not know (measured 2026-10-22 on
// a family drive: the numeric root id returns an empty page with no error), and an
// absence may not be inferred from a page that could not have said otherwise.
// Concluding that instead of guessing costs nothing when the drive's own
// successedFileIdList already named the object as handled, which is the first
// question the evidence pass asks.
func (w listingWalk) complete() bool {
	return w.exhausted && !w.stopped && w.total > 0 && w.rows == w.total
}

// walkListing reads at most maxPages pages of one directory.
func (f *Fs) walkListing(ctx context.Context, dirID string, maxPages int, fn func(*api.Row) bool) (listingWalk, error) {
	var w listingWalk
	seen := map[string]struct{}{}
	requestURI := "/listFiles.action"
	if f.family() {
		requestURI = "/family/file/listFiles.action"
	}
	for pageNum := 1; pageNum <= maxPages; pageNum++ {
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
			return w, err
		}
		listing, err := api.ParseListing(body)
		if err != nil {
			return w, err
		}
		if listing.Count > w.total {
			// The largest total any page claimed. A directory that is being
			// emptied while the walk runs reports a smaller total later, and
			// holding to the larger number keeps "complete" an answer the walk can
			// only reach by really having seen everything.
			w.total = listing.Count
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
			f.stampParent(row, dirID)
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
			f.stampParent(row, dirID)
			if fn(row) {
				stopped = true
				break
			}
		}
		if stopped {
			w.stopped = true
			w.rows = len(seen)
			return w, nil
		}
		if count == 0 {
			w.exhausted = true
			w.rows = len(seen)
			return w, nil
		}
	}
	return w, fmt.Errorf("cloud189: listing of %q did not terminate after %d pages, the server keeps returning data", dirID, maxPages)
}

// stampParent fills in the parent a task body will need for this row.
//
// The personal endpoint answers file rows without a parentId: captured 2026-10-23,
// only the folderList rows carry one. Tasks address an object by id and its
// parent by number, so a file found by a listing has to take the parent from the
// listing itself - the only place this client knows it from. A row the drive did
// name a parent for is left alone, and the family alias is mapped back to the
// real id by realDirID on the way out.
func (f *Fs) stampParent(row *api.Row, dirID string) {
	if row.ParentID == "" {
		row.ParentID = api.FlexID(dirID)
	}
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
		// parentFolderId. It refuses the 0 alias, so a top-level folder names the
		// numeric root id - and on a drive that has never held anything there is
		// no entry to read that id from. Leaving parentId out is the one shape the
		// server accepts there: the official client's own transfer folder is made
		// exactly that way (OpenList drivers/189pc/utils.go: createFamilyTransferFolder
		// sends only folderName + familyId), and the folder lands at the drive root.
		pathID = f.realDirID(pathID)
		if pathID == familyRootAlias && f.familyRootID == "" {
			q = f.get(fmt.Sprintf("familyId=%s&folderName=%s", url.QueryEscape(f.familyID), url.QueryEscape(name)))
		} else {
			q = f.get(fmt.Sprintf("familyId=%s&parentId=%s&folderName=%s", url.QueryEscape(f.familyID), url.QueryEscape(pathID), url.QueryEscape(name)))
		}
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
	created := resp.ID.String()
	// The id createFolder answers has been measured live to be one no listing
	// can find when the name was deleted moments before - the test suite's
	// rmdir-then-mkdir of the same root, twice - and the drive needs a few
	// seconds to let go of the recycled entry. So the id is verified by listing
	// the directory it names, the window is waited out, and the parent's
	// listing is the fallback. For a name that was not recycled the first
	// verification answers at once, so the common case costs one listing and
	// no sleep.
	for attempt := 0; ; attempt++ {
		_, verr := f.rowsIn(ctx, created)
		if verr == nil {
			return created, nil
		}
		var refused *apiError
		if attempt >= 2 || ctx.Err() != nil || !errors.As(verr, &refused) || refused.Code != "FileNotFound" {
			break
		}
		time.Sleep(time.Second)
	}
	rows, err := f.rowsIn(ctx, pathID)
	if err == nil {
		for _, row := range rows {
			// A row this process just deleted can still be in the listing; its
			// id is one nothing will answer to, so it never names the fresh
			// directory.
			if row.Name == name && rowIsDir(row) && !f.deleted.recent(row.ID.String()) {
				fs.Debugf(f, "cloud189: createFolder answered %s for %q but the parent's listing names it %s, which is the one kept", created, name, row.ID)
				return row.ID.String(), nil
			}
		}
		fs.Debugf(f, "cloud189: createFolder answered %s for %q, which does not list, and the parent's listing does not name it yet", created, name)
	} else {
		fs.Debugf(f, "cloud189: createFolder answered %s for %q, which does not list, and the parent could not be listed: %v", created, name, err)
	}
	return created, nil
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
		// The space root holds every other remote's parent, so removing it is
		// refused. A remote opened below it owns the directory it was opened at:
		// rmdir of that root is what `rclone rmdir remote:sub` means, and the
		// test suite's teardown calls it on the Fs it is handed.
		if f.root == "" {
			return fserrors.NoRetryError(errors.New("cloud189: the drive's own top level cannot be removed, only what is inside it"))
		}
		return f.rmdirRoot(ctx, true)
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
	// A row this process just deleted can still be in the listing - the suite
	// removes a directory's last file and removes the directory in the same
	// breath - and its id is one nothing will answer to. Counting it would refuse
	// a directory that is empty.
	live := 0
	for _, child := range children {
		if !f.deleted.recent(child.ID.String()) {
			live++
		}
	}
	if live > 0 {
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

// rmdirRoot removes the directory this remote was opened at, when that is below
// the space root - rmdir of "" on such a remote is what the test suite's
// teardown and `rclone rmdir remote:sub` both mean.
//
// The row is looked up from the space root downwards, because a dircache rooted
// at the remote cannot name the directories above it, and the delete task
// addresses the object by id and its parent by number: the row found in its
// parent's listing carries both. The walk is the same resolution NewFs did, and
// a root one level down is found in the first listing. checkEmpty refuses a
// directory that still lists children, which is Rmdir's contract; a caller that
// has just emptied the directory itself passes false, because the listing it
// was emptied by is the freshest view there is and the drive may still be
// carrying the deleted children in it.
func (f *Fs) rmdirRoot(ctx context.Context, checkEmpty bool) error {
	dirID, err := f.rootID(ctx)
	if err != nil {
		return err
	}
	row := (*api.Row)(nil)
	for _, leaf := range strings.Split(f.root, "/") {
		r, err := f.findEntry(ctx, dirID, f.opt.Enc.FromStandardName(leaf))
		if err == fs.ErrorObjectNotFound {
			return fs.ErrorDirNotFound
		}
		if err != nil {
			return err
		}
		if !rowIsDir(r) {
			return fs.ErrorDirNotFound
		}
		dirID = r.ID.String()
		row = r
	}
	if checkEmpty {
		children, err := f.rowsIn(ctx, row.ID.String())
		if err != nil {
			return err
		}
		live := 0
		for _, child := range children {
			if !f.deleted.recent(child.ID.String()) {
				live++
			}
		}
		if live > 0 {
			return fs.ErrorDirectoryNotEmpty
		}
	}
	if err := f.deleteRows(ctx, []deleteTarget{{row: row}}); err != nil {
		return err
	}
	// The remote no longer exists on the drive, so nothing in its cache can be
	// trusted - including the root's own id, which a Flush would leave in place:
	// the next mkdir of "" would then come back from the cache holding the
	// deleted id, and everything under it would answer FileNotFound. Measured
	// live 2026-10-23, twice, by the test suite's own teardown. ResetRoot sends
	// the next lookup back to the drive instead.
	f.dirCache.ResetRoot()
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
	case f.opt.Space == dst.opt.Space && (!f.family() || f.familyID == dst.familyID):
		// Two remotes of one account in the same space - which is what the
		// standard suite constructs for DirMove, and what `rclone moveto`
		// across two configured roots of one drive makes - share the session
		// by token, so either instance signs with the same key. The task body
		// is all ids, and every name in it is spelled by the remote that owns
		// that name, so nothing the instances can disagree on reaches it. A
		// family to family copy across two different families was never
		// captured, and stays refused.
		return f.taskAuth(), true
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

// movedObject is how a server-side move reports what it moved: the same id and
// the same bytes, seen through the remote the caller moved it INTO. Returning the
// source's own object would hand rclone an object whose Fs is where it came from,
// and two remotes of one account can carry different roots and different
// encoding options - the path would then be read relative to the wrong remote.
func movedObject(dst *Fs, remote string, src *Object) *Object {
	return &Object{
		fs:      dst,
		remote:  remote,
		id:      src.id,
		size:    src.size,
		md5:     src.md5,
		modTime: src.modTime,
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
// that is neither the source nor the object this run parked out of the way.
//
// The drive answers a successful COPY with no id at all, so the new object has to
// be recognised from the destination listing - under the name it arrived with,
// which is the source's, because the task's fileName is ignored. Two rows can
// carry that name legitimately at once, and one of them is this run's own doing:
// whoever held the arrival name was renamed aside, and a rename is not a delete,
// so the negative cache does not cover it. If the destination listing is still one
// rename behind, the parked row is what matches first, and installing it under the
// wanted name would hand rclone an object it then deletes - the copy staying on
// the drive under a name nobody asked for. So its id is excluded too.
//
// The walk is repeated for the same window a name is waited for elsewhere: the
// parking rename and the task that lands the copy both hit the name inside a
// second, and the first read after them can still answer with the state before
// either.
func (f *Fs) arrivedCopy(ctx context.Context, dirID, wireName, srcID, parkedID string) (*api.Row, error) {
	var lastErr error
	for attempt := 0; attempt < arrivalAttempts; attempt++ {
		row, err := f.findArrivedCopy(ctx, dirID, wireName, srcID, parkedID)
		if err == nil {
			return row, nil
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			return nil, err
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
		// A COPY is the one write that cannot be addressed by id afterwards, so
		// this walk is the only way to find what it made. Reporting failure on
		// the first read would make the caller retry a copy that landed, which
		// leaves a second object beside the first.
		if attempt < arrivalAttempts-1 {
			time.Sleep(arrivalWait)
		}
	}
	return nil, fserrors.NoRetryError(fmt.Errorf("cloud189: the copy finished but no object named %q other than its source appeared in the destination (last read: %v)", truncate(wireName, 60), lastErr))
}

// findArrivedCopy is one walk of the destination, looking for the object the copy
// task made.
func (f *Fs) findArrivedCopy(ctx context.Context, dirID, wireName, srcID, parkedID string) (*api.Row, error) {
	var found *api.Row
	err := f.listAll(ctx, dirID, func(row *api.Row) bool {
		if row.Name != wireName {
			return false
		}
		id := row.ID.String()
		if id == srcID || (parkedID != "" && id == parkedID) {
			return false
		}
		// A directory and a file can carry the same name here, and a directory is
		// never what a file's copy turned into.
		if rowIsDir(row) {
			return false
		}
		if f.deleted.recent(id) {
			return false
		}
		found = row
		return true
	})
	if err != nil {
		return nil, err
	}
	if found == nil {
		return nil, fs.ErrorObjectNotFound
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
// The drive ignores the fileName a COPY entry carries. The copy lands in the
// target folder under its SOURCE's name - measured live 2026-10-24 with the
// command and the readback both on record: a copy of A.txt asked to arrive as
// sub/B.txt came back as sub/A.txt. So the name that has to be free in the
// destination is the source's spelling, and a copy that also renames is one task
// plus one rename by id.
//
// A copy inside the directory that already holds the source cannot be done at
// all: the copy would have to take a name its own source is holding, and the
// drive refuses it (measured the same day as COPY failing with
// FileAlreadyExists, and as a task stopped in the conflict state a client has to
// answer). That case is fs.ErrorCantCopy, which lets rclone transfer instead.
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
	stdLeaf, srcDirID, err := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
	if err != nil {
		return nil, err
	}
	if f.realDirID(dstDirID) == srcObj.fs.realDirID(srcDirID) {
		return nil, fs.ErrorCantCopy
	}
	// The copy arrives named as its source, spelled the way the SOURCE's remote
	// stores it: two remotes can carry different encoding options, and the name
	// this run has to clear and then read back is the drive's bytes, not this
	// remote's rendering of them. Move does the same thing.
	srcLeaf := srcObj.fs.opt.Enc.FromStandardName(stdLeaf)
	cleared, err := f.clearName(ctx, dstDirID, srcLeaf, srcObj.id)
	if err != nil {
		return nil, err
	}
	info, err := srcObj.fs.copyTaskInfo(ctx, srcObj, auth, srcLeaf)
	if err != nil {
		f.undoClear(ctx, cleared, srcLeaf)
		return nil, err
	}
	if _, err := srcObj.fs.runBatchTask(ctx, auth, taskCopy, []taskInfo{info}, dstDirID); err != nil {
		f.undoClear(ctx, cleared, srcLeaf)
		return nil, err
	}
	// A successful COPY reports no id for the object it made, so it is read back
	// from the destination listing (captured 2026-10-22). The source's own id is
	// excluded from the match: a row that is the source would be reported as the
	// copy, and every later operation on the returned object would then address
	// the original. This run refuses a copy that lands in its source's own
	// directory before the task, so the exclusion is not what tells the two rows
	// apart - it is the backstop for a listing that should not have had to hold
	// both, and it costs one comparison.
	parkedID := ""
	if cleared != nil {
		parkedID = cleared.ID.String()
	}
	arrived, err := f.arrivedCopy(ctx, dstDirID, srcLeaf, srcObj.id, parkedID)
	if err != nil {
		f.undoClear(ctx, cleared, srcLeaf)
		return nil, fmt.Errorf("cloud189: the copy finished but the new object cannot be found in the destination: %w", err)
	}
	res := &uploadResult{id: arrived.ID.String(), name: srcLeaf, md5: arrived.MD5}
	if err := f.installObject(ctx, dstDirID, wireLeaf, res, nil, true); err != nil {
		// The copy itself is gone by now (installObject takes back what it made
		// when it cannot finish), so the only thing left to undo is the name this
		// run cleared for it.
		f.undoClear(ctx, cleared, srcLeaf)
		return nil, err
	}
	f.finishClear(ctx, cleared, srcLeaf, wireLeaf, "a server-side copy took its name")
	return f.rowObject(remote, arrived), nil
}

// undoClear puts back a name this run cleared for an operation that then failed.
//
// It runs on a context detached from the caller's: this is the handler for a
// failure, and the failure is most often the user stopping the run. Renaming with
// the cancelled context would leave the object parked under a name nobody asked
// for, which the next sync reads as a file to delete.
func (f *Fs) undoClear(ctx context.Context, parked *api.Row, name string) {
	if parked == nil {
		return
	}
	if err := f.restoreName(context.WithoutCancel(ctx), parked, name); err != nil {
		fs.Errorf(f, "cloud189: the object parked as %q could not be given back its name %q: %v", parked.Name, name, err)
	}
}

// undoArrival gives a parked object its name back after an operation failed once
// the drive had already been told to move this run's object onto that name.
//
// A plain restore cannot work there: the task landed this run's object on the name,
// the drive never overwrites, so the rename collides and the object that was on the
// drive before this run is left wearing a park name - which the next `rclone sync
// --delete` reads as a file to remove. The moved object is handed over first: it is
// renamed aside, which keeps both on the drive and gives the one that was there
// first the name it had. The log says where the moved bytes went.
func (f *Fs) undoArrival(ctx context.Context, dirID string, parked *api.Row, name, movedID string, movedIsDir bool) {
	if parked == nil {
		return
	}
	ctx = context.WithoutCancel(ctx)
	if err := f.restoreName(ctx, parked, name); err == nil {
		// Nothing to hand over: the task either did not land anything or the name
		// was free all along.
		return
	}
	// The rename was refused. That is either the name being taken or the drive not
	// having answered, and only a read can tell them apart. The read waits past the
	// state the park left behind: a listing one rename behind answers with the very
	// object that was moved aside, which says nothing about who holds the name now.
	row, err := f.holderAfterThePark(ctx, dirID, name, parked.ID.String())
	switch {
	case errors.Is(err, fs.ErrorObjectNotFound):
		// Nobody holds it, so the refusal was about the rename itself. One more
		// try; the object that was there before this run is worth the request.
		if rerr := f.restoreName(ctx, parked, name); rerr != nil {
			fs.Errorf(f, "cloud189: the object parked as %q could not be given back its name %q: %v", parked.Name, name, rerr)
		}
		return
	case err != nil:
		fs.Errorf(f, "cloud189: the object parked as %q could not be given back its name %q, and who holds it could not be read: %v", parked.Name, name, err)
		return
	}
	if row.ID.String() != movedID {
		// Somebody else's object. Nothing here may move it: this run only knows that
		// its own object was the one that landed.
		fs.Errorf(f, "cloud189: the object parked as %q could not be given back its name %q: it is held by another object", parked.Name, name)
		return
	}
	park := newParkName()
	if err := f.renameObject(ctx, movedID, movedIsDir, park); err != nil {
		fs.Errorf(f, "cloud189: %q could not be freed for the object parked as %q: %v", name, parked.Name, err)
		return
	}
	if err := f.restoreName(ctx, parked, name); err != nil {
		fs.Errorf(f, "cloud189: the object that was there before this run is left as %q and the moved object as %q: %v", parked.Name, park, err)
		return
	}
	fs.Logf(f, "cloud189: the operation failed after the move landed, so the moved object is left as %q and '%s' is back in place", park, truncate(name, 60))
}

// holderAfterThePark reads back who holds a name, waiting past the state the park
// left behind.
//
// A listing that is one rename behind answers with the object the park moved away,
// still wearing the name it was moved from. That row is not an answer about who
// holds the name now, and acting on it would report a hand-over that cannot happen.
func (f *Fs) holderAfterThePark(ctx context.Context, dirID, wireName, parkedID string) (*api.Row, error) {
	lastErr := fs.ErrorObjectNotFound
	for attempt := 0; attempt < arrivalAttempts; attempt++ {
		row, err := f.findEntry(ctx, dirID, wireName)
		switch {
		case err == nil && row.ID.String() != parkedID:
			return row, nil
		case err == nil:
			lastErr = fs.ErrorObjectNotFound // the pre-park row, not the name's holder
		default:
			lastErr = err
		}
		if attempt < arrivalAttempts-1 {
			time.Sleep(arrivalWait)
		}
	}
	return nil, lastErr
}

// finishClear decides what becomes of the object that was standing where an
// arrival was going to land.
//
// A task lands the object under the name it moves with, so that is the name the
// operation has to clear first. When the operation keeps that name, the object in
// the way is the one rclone asked to replace, and it goes off the drive. When the
// operation also renames, the parked object never held the wanted name at all - it
// only happened to be where the drive put the arrival - so it is given its name
// back. Deleting it there is how a move that renames removed an unrelated file from
// the destination (found live 2026-10-24: the standard suite's own move cascade
// destroyed its earlier step).
func (f *Fs) finishClear(ctx context.Context, cleared *api.Row, arrivalName, wantedName, why string) {
	if cleared == nil {
		return
	}
	if arrivalName == wantedName {
		f.dropObject(ctx, cleared, why)
		return
	}
	f.undoClear(ctx, cleared, arrivalName)
}

// copyTaskInfo builds the task entry for one object, in the shape the drive
// expects for the direction it is going.
func (f *Fs) copyTaskInfo(ctx context.Context, obj *Object, auth taskAuth, wireName string) (taskInfo, error) {
	info := taskInfo{
		FileID:   obj.id,
		FileName: wireName,
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
	// The name the object carries on the drive right now. The task is told this,
	// because a MOVE lands the object under its source name whatever the entry
	// says (measured live 2026-10-24), and the name to clear in the destination
	// is the one the arrival will wear.
	srcLeaf := srcObj.fs.opt.Enc.FromStandardName(stdLeaf)
	if f.realDirID(dstDirID) == srcObj.fs.realDirID(srcDirID) {
		// A move within one directory is a rename, and no task: the drive has
		// been measured to answer a MOVE addressed at the mover's own directory
		// by destroying the object (measured live 2026-10-23, the suite's move
		// of a file to a new name in its own directory left neither the old nor
		// the new name on the drive). The rename goes through the same
		// park-and-swap the cross-directory move lands in.
		cleared, err := f.clearName(ctx, dstDirID, wireLeaf, srcObj.id)
		if err != nil {
			return nil, err
		}
		res := &uploadResult{id: srcObj.id, name: srcLeaf, md5: srcObj.md5}
		if err := f.installObject(ctx, dstDirID, wireLeaf, res, nil, false); err != nil {
			f.undoClear(ctx, cleared, wireLeaf)
			return nil, err
		}
		if cleared != nil {
			f.dropObject(ctx, cleared, "a moved object took its name")
		}
		return movedObject(f, remote, srcObj), nil
	}
	srcParentID, err := taskParentID(srcObj.fs.realDirID(srcDirID))
	if err != nil {
		return nil, err
	}
	cleared, err := f.clearName(ctx, dstDirID, srcLeaf, srcObj.id)
	if err != nil {
		return nil, err
	}
	info := taskInfo{FileID: srcObj.id, FileName: srcLeaf, IsFolder: 0, ParentID: srcParentID}
	if _, err := srcObj.fs.runBatchTask(ctx, auth, taskMove, []taskInfo{info}, dstDirID); err != nil {
		// A task that failed moved nothing, so the same handler covers both: it
		// restores the name when it is free and hands it over when it is not.
		f.undoArrival(ctx, dstDirID, cleared, srcLeaf, srcObj.id, false)
		return nil, err
	}
	// A task's counters are all this backend has to judge it by, and installObject
	// below deletes whatever holds the name rclone asked for. So the destination is
	// read back for the object this run moved before anything there is renamed or
	// removed: a task that reported success without moving anything must not take a
	// file that was on the drive first.
	if err := f.proveArrived(ctx, dstDirID, srcLeaf, srcObj.id); err != nil {
		f.undoArrival(ctx, dstDirID, cleared, srcLeaf, srcObj.id, false)
		return nil, err
	}
	// A move keeps the id, and the task left the object in the destination under
	// the name it moved with. installObject gives it the name rclone asked for, and
	// the rename it ends with is synchronous: a listing read after it would only be
	// able to disagree with the drive about a rename the drive already answered.
	res := &uploadResult{id: srcObj.id, name: srcLeaf, md5: srcObj.md5}
	if err := f.installObject(ctx, dstDirID, wireLeaf, res, nil, false); err != nil {
		f.undoArrival(ctx, dstDirID, cleared, srcLeaf, srcObj.id, false)
		return nil, err
	}
	f.finishClear(ctx, cleared, srcLeaf, wireLeaf, "a moved object took its name")
	return movedObject(f, remote, srcObj), nil
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
	srcID, srcDirectoryID, srcLeaf, dstDirID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}
	parentID, err := taskParentID(srcFs.realDirID(srcDirectoryID))
	if err != nil {
		return err
	}
	// Named as the source remote stores it, the way Move and Copy do: two remotes
	// of one account may carry different encodings, and the name the drive stores
	// is the source's bytes. The destination's spelling belongs to the rename that
	// follows the task.
	wireSrc := srcFs.opt.Enc.FromStandardName(srcLeaf)
	dstWire := f.opt.Enc.FromStandardName(dstLeaf)
	// The directory cache refuses a destination that already holds a DIRECTORY and
	// says nothing about a file standing there: that one is discovered by the rename
	// at the end, after the task has run and something has been parked. Nothing is
	// parked until the name the directory has to take is known to be free.
	if row, ferr := f.findEntry(ctx, dstDirID, dstWire); ferr == nil && !rowIsDir(row) {
		return fmt.Errorf("cloud189: %q is a file on the drive, so the directory cannot take its name", truncate(dstWire, 60))
	} else if ferr != nil && !errors.Is(ferr, fs.ErrorObjectNotFound) {
		return ferr
	}
	cleared, err := f.clearName(ctx, dstDirID, wireSrc, srcID)
	if err != nil {
		return err
	}
	if srcFs.realDirID(srcDirectoryID) == f.realDirID(dstDirID) {
		// A directory renamed inside the directory that already holds it is the
		// shape the drive answers by destroying what the task is addressed at
		// (measured live 2026-10-23 on a file), and a directory carries a whole
		// subtree with it. So this case is a rename, and no task.
		if err := f.renameObject(ctx, srcID, true, dstWire); err != nil {
			f.undoClear(ctx, cleared, wireSrc)
			return err
		}
	} else {
		info := taskInfo{FileID: srcID, FileName: wireSrc, IsFolder: 1, ParentID: parentID}
		if _, err := srcFs.runBatchTask(ctx, auth, taskMove, []taskInfo{info}, dstDirID); err != nil {
			f.undoClear(ctx, cleared, wireSrc)
			return err
		}
		// Read the arrival back before anything parked is deleted: the destination
		// must hold the moved directory under the name the task left it in, and if
		// it does not then this run has not earned removing what was there. The read
		// waits for this run's id, not merely for some row: the park and the task are
		// two mutations of one name inside a second.
		if err := f.proveArrived(ctx, dstDirID, wireSrc, srcID); err != nil {
			f.undoArrival(ctx, dstDirID, cleared, wireSrc, srcID, true)
			return err
		}
		// The task carries no destination name: the directory arrives under the
		// name it moved with. When that is not the name rclone asked for, rename it
		// here - the id is the moved directory's, and the destination name was
		// verified free before the task started. A rename that fails leaves the
		// directory on the drive under the arrival name, which the destination
		// cache must then not claim to know.
		if dstWire != wireSrc {
			if err := f.renameObject(ctx, srcID, true, dstWire); err != nil {
				f.dirCache.FlushDir(dstRemote)
				f.undoArrival(ctx, dstDirID, cleared, wireSrc, srcID, true)
				return err
			}
		}
	}
	srcFs.dirCache.FlushDir(srcRemote)
	f.finishClear(ctx, cleared, wireSrc, dstWire, "a moved directory took its name")
	return nil
}

// awaitRow reads a name back from a directory the drive has just been told to
// change, waiting out the window its listing takes to catch up.
//
// The window is not small: measured 2026-10-24, a name that had a park, a rename
// and a delete cross it inside one second was still unreadable seven seconds
// later, while a directory that saw one mutation answered immediately. A wait
// shorter than that turns a move that worked into a reported failure, and the
// restore on that path then collides with the object that is on its way in.
func (f *Fs) awaitRow(ctx context.Context, dirID, wireName string) (*api.Row, error) {
	var lastErr error
	for attempt := 0; attempt < arrivalAttempts; attempt++ {
		row, err := f.findEntry(ctx, dirID, wireName)
		if err == nil {
			return row, nil
		}
		lastErr = err
		if !errors.Is(err, fs.ErrorObjectNotFound) || ctx.Err() != nil {
			return nil, err
		}
		if attempt < arrivalAttempts-1 {
			time.Sleep(arrivalWait)
		}
	}
	return nil, lastErr
}

// proveArrived reads the wanted name back and requires the drive to be holding
// this run's object there.
//
// It exists because a swap deletes something that was on the drive before this run
// - the object replaced, or one parked out of the arrival's way - and a task
// judged only by counters has been measured to report work it did not do. Without
// this, the drive's own answer decides whether a user's file is removed.
//
// A row that is not this run's is waited out rather than rejected at once: the
// park this operation sent and the task that landed its object are two mutations
// of one name inside a second, and a listing that lags answers with the row that
// was there before them. Treating that as a failure would report a move that
// worked as failed, and the restore that follows would then collide with the
// object on its way in.
func (f *Fs) proveArrived(ctx context.Context, dirID, wireName, wantID string) error {
	var lastErr error
	sawOther := false
	for attempt := 0; attempt < arrivalAttempts; attempt++ {
		row, err := f.findEntry(ctx, dirID, wireName)
		switch {
		case err == nil && row.ID.String() == wantID:
			return nil
		case err == nil:
			// A row that is not this run's, for the whole window. Whatever happens
			// after this point, trying the same operation again will not change it,
			// and the row may be somebody else's object.
			sawOther = true
			lastErr = fserrors.NoRetryError(fmt.Errorf("cloud189: %q is held by something other than the object this run moved there", truncate(wireName, 60)))
		case errors.Is(err, fs.ErrorObjectNotFound):
			if !sawOther {
				lastErr = err
			}
		default:
			return err
		}
		if ctx.Err() != nil {
			break
		}
		if attempt < arrivalAttempts-1 {
			time.Sleep(arrivalWait)
		}
	}
	return lastErr
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

// deleteTaskResends is how many times the outstanding part of a DELETE task that
// ended with failures and no errorCode at all is sent again.
//
// The refusal never works itself out, so something has to be sent. Measured
// 2026-10-28 with the resend switched off and one pass of the run: 17 of 40 objects
// were refused, and the directory still listed 18 of them ninety seconds later with
// nothing else sent, twice over with the same numbers. Waiting is not a remedy.
//
// Two, because one does not reliably finish the job. Six passes of the 40-object
// fixture with a tolerance of one ended rc = 1, 1, 0, 1, 1, 0 - an object still
// refused at the end of the last pass makes the whole command fail - while seven
// passes with a tolerance of two, the same ordering and the same flags
// (--retries 2 --transfers 15 --checkers 8), ended rc = 0 every time, with at most
// one object still refused inside a pass and that one cleared by the pass after it.
// Two runs at the size the migration chain really works at (300 excess objects, the
// sync stage's own --transfers 3 --checkers 4) also ended rc = 0, refusing 2 and 0
// objects in their last pass. Note what this does NOT measure: the six-and-seven
// comparison holds the ordering fixed only for the three passes on each side that
// share it, and no arm has yet varied the tolerance alone in one window. It is a
// number I chose, and nothing in the protocol says two.
//
// What the 63 refused tasks of those six runs do say is that the first resend is
// not wasted - 47 of them were settled by it - and that the last 16 were settled by
// a later pass of rclone's, not by anything the backend did inside the first send.
//
// The refusals do not depend on how many deletes are in flight: the same fixture at
// --transfers 15, 5 and 3 was refused for 9, 10 and 9 to 12 tasks per run, so this
// is not a backend that needs throttling, it is a server that answers some deletes
// with an empty failure.
//
// The spacing between sends is not a sleep: every resend is preceded by the
// evidence pass in deleteStillOutstanding, which costs at least one listing.
// Whether asking the drive first makes the resend itself more likely to succeed has
// not been measured - the two arms that would answer that (same window, same flags,
// resend-now against resend-after-evidence) have not been run - so the evidence
// pass is justified by correctness alone: it is what stops a delete the drive has
// already carried out from being sent back a third time.
//
// One refusal is enough for the case captured 2026-10-22, where a family directory
// delete polled nine times (eight of them taskStatus 3), closed failedCount 1 with
// no code, was still listed right after, and the same payload sent again finished
// successedCount 1 in 1.2s.
const deleteTaskResends = 2

// How the absence check gathers its evidence: up to deleteVerifyLooks looks
// deleteVerifyWait apart, and the answer is only believed once deleteVerifyAgree
// of them agree in a row.
//
// The window the looks span is (deleteVerifyLooks-1)*deleteVerifyWait and it has
// to reach past the lag this file distrusts elsewhere: a rename and a delete that
// crossed inside one second was still unreadable seven seconds later (see
// deletedRowWindow, and the same measurement in the capture of 2026-10-22). Five
// looks two seconds apart span eight, so the last look is taken after that
// window has closed rather than one second inside it. Measured on 2026-10-28 the
// row was gone from the listing at the first look five times out of five, so the
// extra look is headroom, not an expectation.
//
// Both directions need evidence because the two mistakes cost differently. One
// empty page read as "gone" reports a leftover as deleted and cannot be taken
// back; a lingering row read as "still there" only delays. These are vars, not
// consts, so a test can run the logic without paying the wait.
var (
	deleteVerifyLooks = 5
	deleteVerifyAgree = 2
	deleteVerifyWait  = 2 * time.Second
	// deleteVerifyPages bounds how many listing pages one look may read for one
	// parent directory. Ten pages is my choice, not a number the protocol
	// dictates: it puts the worst case for one look at ten requests over the
	// account's own RTT, and a directory larger than that is answered "cannot
	// tell", which costs a resend rather than buying a wrong "gone". The number
	// is bounded per parent, not per target, so a purge of two thousand objects in
	// one directory is still one walk.
	deleteVerifyPages = 10
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
	resends := 0
	for {
		st, err := f.waitBatchTask(ctx, auth, taskID, taskType)
		if err != nil {
			return nil, err
		}
		verdict := f.taskVerdict(taskType, len(targets), st)
		if verdict == nil {
			return st, nil
		}
		// A DELETE task that did not report what was asked for is not taken at its
		// word. Two measured shapes sit behind this: one failed with no reason at
		// all and the same targets sent again finished it (captured 2026-10-22,
		// seen in production 2026-10-27), and one closed as failed after the work
		// was already done. So the drive is asked, in its own words, what is left
		// over - the task's successedFileIdList first, then the listing of each
		// parent - and only what is still outstanding goes back. Whatever the next
		// answer looks like, the same question is asked again: the drive can refuse
		// the repeat of a delete it has already carried out, and that refusal says
		// nothing about whether the object is on the drive. Saying an object was
		// deleted while it is still there is the one answer here that cannot be
		// taken back.
		if taskType == taskDelete {
			still, why, cerr := f.deleteStillOutstanding(ctx, targets, st)
			if cerr != nil {
				fs.Debugf(f, "cloud189: could not fully check what the unfinished %s task %s left behind: %v", taskType, taskID, cerr)
			}
			if len(still) == 0 {
				fs.Logf(f, "cloud189: %s task %s did not report success but nothing of it is outstanding (%s); taking it as done", taskType, taskID, why)
				return st, nil
			}
			// A refusal the drive did give a reason for is not worth sending back:
			// the two that appear here, a full drive and a spent day, are decided by
			// the account and do not clear inside a run.
			if string(st.ErrorCode) == "" && resends < deleteTaskResends {
				resends++
				fs.Logf(f, "cloud189: %s task %s ended %d failed / %d skipped and %s; sending those %d target(s) again", taskType, taskID, st.FailedCount, st.SkipCount, why, len(still))
				targets = still
				taskID, err = f.createBatchTask(ctx, auth, taskType, targets, targetFolderID)
				if err != nil {
					return nil, err
				}
				continue
			}
		}
		return st, verdict
	}
}

// deleteStillOutstanding answers, for a DELETE task that did not report full
// success, which of the targets it was sent still have to be dealt with, and in
// what words the drive itself said so.
//
// Two pieces of evidence are read, in this order, and both come from the drive:
//
//   - successedFileIdList names the objects the task did handle (captured
//     2026-10-22: MOVE, DELETE and CLEAR_RECYCLE all answer it, a successful COPY
//     does not). Those are not sent back - a repeat naming them again is a repeat
//     of work the drive already did, and the first send of a partly-failed purge
//     used to do exactly that.
//   - one listing per parent directory, read to its end inside deleteVerifyPages,
//     says whether the rest are still there. Only a walk that finished may answer
//     "not here": the same endpoint answers a folderId it does not know with an
//     empty page and no error (measured 2026-10-22), which is what a leftover
//     would look like to a walk that stopped early.
//
// Anything the evidence cannot settle stays outstanding. That costs a resend and,
// if the resends run out, an error - both cheaper than guessing "gone".
func (f *Fs) deleteStillOutstanding(ctx context.Context, targets []taskInfo, st *taskStatusResponse) ([]taskInfo, string, error) {
	agree := 0
	var still []taskInfo
	var why string
	var err error
	for look := 0; look < deleteVerifyLooks; look++ {
		if look > 0 {
			select {
			case <-ctx.Done():
				return still, why, ctx.Err()
			case <-time.After(deleteVerifyWait):
			}
		}
		still, why, err = f.deleteEvidenceOnce(ctx, targets, st)
		switch {
		case err != nil && ctx.Err() != nil:
			return still, why, err
		case len(still) > 0:
			// Something is known to be outstanding: that answer needs no second
			// look, because a leftover read as present only costs a resend.
			return still, why, err
		case strings.HasPrefix(why, "the task named"):
			// The drive itself named every target as handled, so this is not an
			// absence inferred from a listing.
			return nil, why, err
		}
		agree++
		if agree >= deleteVerifyAgree {
			return nil, why, err
		}
	}
	// The looks ran out before they agreed. That is not evidence of anything, so
	// the targets stay outstanding: the cost is a resend and, if the resends run
	// out too, an error. Falling out of a loop must never read as "gone" - that is
	// the one answer here that cannot be taken back afterwards.
	return targets, "the drive's answers did not agree twice", err
}

// deleteEvidenceOnce is one pass of the evidence described above. The "why" it
// returns is what the drive said, not a guess, because the resend log line quotes
// it.
func (f *Fs) deleteEvidenceOnce(ctx context.Context, targets []taskInfo, st *taskStatusResponse) ([]taskInfo, string, error) {
	handled := make(map[string]bool, len(st.SuccessedFileIDs))
	for _, id := range st.SuccessedFileIDs {
		handled[id.String()] = true
	}
	unhandled := make([]taskInfo, 0, len(targets))
	for _, t := range targets {
		if !handled[t.FileID] {
			unhandled = append(unhandled, t)
		}
	}
	if len(unhandled) == 0 {
		return nil, "the task named all of them in successedFileIdList", nil
	}

	byParent := make(map[string][]taskInfo)
	for _, t := range unhandled {
		if t.ParentID == nil {
			return unhandled, "the task carried no parent to look under", errors.New("cloud189: a task target carries no parent to look under")
		}
		p := strconv.FormatInt(*t.ParentID, 10)
		byParent[p] = append(byParent[p], t)
	}

	still := make([]taskInfo, 0, len(unhandled))
	unread := 0
	var walkErr error
	for p, group := range byParent {
		found, complete, err := f.idsPresentIn(ctx, p, group)
		if err != nil {
			walkErr = err
		}
		if !complete {
			// The drive was not read to the end, so nothing can be concluded
			// about what is missing from what was read.
			unread++
			still = append(still, group...)
			continue
		}
		for _, t := range group {
			if found[t.FileID] {
				still = append(still, t)
			}
		}
	}
	why := fmt.Sprintf("%d of the %d target(s) are still on the drive", len(still), len(targets))
	if unread > 0 {
		why = fmt.Sprintf("%s and %d parent directory(ies) could not be listed to the end", why, unread)
	}
	return still, why, walkErr
}

// idsPresentIn reads the listing of one parent directory once and reports which of
// the wanted ids it carries, and whether the walk reached the end of the listing.
//
// The walk stops early only when every wanted id has been found - the answer to
// "is it here" is already yes - so the cost is one listing per parent per look,
// whatever the number of targets under it.
func (f *Fs) idsPresentIn(ctx context.Context, parent string, wanted []taskInfo) (map[string]bool, bool, error) {
	want := make(map[string]bool, len(wanted))
	for _, t := range wanted {
		want[t.FileID] = true
	}
	found := make(map[string]bool, len(want))
	remaining := len(want)
	w, err := f.walkListing(ctx, f.listDirID(parent), deleteVerifyPages, func(row *api.Row) bool {
		id := row.ID.String()
		if want[id] && !found[id] {
			found[id] = true
			remaining--
			return remaining == 0
		}
		return false
	})
	return found, w.complete(), err
}

// waitBatchTask polls one task until it reaches a terminal state and returns the
// last answer. Nothing here judges the outcome - runBatchTask does that, because
// a DELETE task can be sent again.
func (f *Fs) waitBatchTask(ctx context.Context, auth taskAuth, taskID api.FlexID, taskType string) (*taskStatusResponse, error) {
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
			return st, nil
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
		// What skipCount means is not measured. It is certainly not the name
		// conflict: captured 2026-10-22 (the ledger's FileAlreadyExists case) a
		// conflict answers failedCount 1, skipCount 0 and errorCode
		// FileAlreadyExists. So this says only what the response says - the drive
		// declined these objects and did not say what became of them - and a
		// DELETE is settled by the evidence in deleteStillOutstanding before this
		// ever reaches the caller.
		return fserrors.NoRetryError(fmt.Errorf("cloud189: %s task declined %d of %d object(s) without saying what became of them", taskType, st.SkipCount, wanted))
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
	// Only a limit this run was given is applied here. The account also publishes
	// transFileSize, but on the accounts measured it is not a limit that bites: it
	// carries the same number as the day's allowance, which is the one that refuses
	// (2026-10-25, checked against the platform's own behaviour). Refusing on it
	// would be inventing a per-file cap the drive does not enforce.
	limit := int64(f.opt.MaxFileSize)
	dayFlow := q.transDayFlow
	used := q.usedDayFlow + q.sent
	free, spaceKnown := q.available, q.spaceKnown
	q.mu.Unlock()

	if limit > 0 && size > limit {
		// This is not one of the two conditions that only the next day or a cleared
		// drive can fix, and it is this run's own option. So it stops this file and
		// says where the number came from, without ending the run under
		// fatal_if_no_space.
		// A plain error, not an apiError: nothing was sent, so printing it as a
		// server answer with an HTTP status would be a lie about where it came from.
		return fserrors.NoRetryError(fmt.Errorf("cloud189: %s is %s and --cloud189-max_file_size is %s (%s)", name, fs.SizeSuffix(size), fs.SizeSuffix(limit), codeLocalSizeCeiling))
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
			fs.Logf(f, "cloud189: transfer allowance %s of %s used today; the account also publishes a single-file number (%s), which is not a limit this backend enforces - it carries the same value as the allowance on the accounts measured",
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

// partURL mints the presigned URL for one part, asked for immediately before
// that part is sent and asked for again on every retry.
//
// Two measured reasons for one part per call rather than the whole file:
//
//   - The signature carries the time it was issued. A URL minted for a whole
//     file and used minutes later is refused by the storage layer with HTTP 403
//     RequestTimeTooSkewed (measured 2026-10-26 on part 12 of a 102-part file),
//     and the PUT replays the Date header from the mint, so the only cure is a
//     newer URL.
//   - The encrypted partInfo rides in the request line, which the gateway
//     refuses over about 4 KiB with HTTP 414 (measured 2026-10-25: 65 parts
//     accepted, 70 refused). One part per call keeps that request tiny whatever
//     the file's part count, so there is no page size to tune and no 414 to hit.
//
// The part number is the part's real index, not its position in a batch: the
// server keys its answer by the number asked for (measured 2026-10-25), and a
// one-part page numbered from 1 every time would keep returning part 1's URL.
//
// TODO: whether a family upload needs familyId here as well as in the init is
// not captured; the uploadFileId already binds the session, so nothing is sent.
func (f *Fs) partURL(ctx context.Context, uploadFileID string, st *stagedSource, idx int) (partEndpoint, error) {
	plan := st.plans[idx]
	params := []api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
		{Key: "partInfo", Value: api.PartInfoFrom(st.partMD5s[idx:idx+1], plan.Index)},
	}
	data, err := f.callEncrypted(ctx, http.MethodGet, f.uploadPath("getMultiUploadUrls"), params)
	if err != nil {
		return partEndpoint{}, err
	}
	var resp uploadURLsResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return partEndpoint{}, fmt.Errorf("cloud189: decoding getMultiUploadUrls: %w", err)
	}
	if err := uploadAnswerCode(resp.Code); err != nil {
		return partEndpoint{}, err
	}
	ep, ok := resp.UploadURLs[fmt.Sprintf("partNumber_%d", plan.Index)]
	if !ok || ep.RequestURL == "" {
		// Deliberately not marked NoRetry. The caller asks once per part, and a
		// file of many parts would otherwise be abandoned by a single unusable
		// answer, with --retries unable to help. The retry asks again, and only
		// exhaustion is marked, so --retries can still re-run the whole file.
		return partEndpoint{}, fmt.Errorf("cloud189: the server gave no upload URL for part %d of %d", plan.Index, len(st.plans))
	}
	// Three things have to agree before this URL is used: the key it came back
	// under, the part number the URL itself addresses, and the digest the
	// storage layer will check the bytes against. The key alone is the server's
	// word; the other two are what the bytes are routed by and checked against,
	// so a misrouted or mislabelled answer is refused here rather than uploaded
	// into the wrong slot.
	u, perr := url.Parse(ep.RequestURL)
	if perr != nil {
		return partEndpoint{}, fmt.Errorf("cloud189: the upload URL for part %d cannot be parsed", plan.Index)
	}
	if got := u.Query().Get("partNumber"); got != strconv.Itoa(plan.Index) {
		return partEndpoint{}, fmt.Errorf("cloud189: the upload URL for part %d is addressed to part %q", plan.Index, got)
	}
	want := base64.StdEncoding.EncodeToString(st.partMD5s[idx])
	if got := headerValue(api.ParseRequestHeader(ep.RequestHeader), "Content-MD5"); got != want {
		return partEndpoint{}, fmt.Errorf("cloud189: the upload URL for part %d was signed for digest %q, not %q", plan.Index, got, want)
	}
	return ep, nil
}

// headerValue reads one header the storage layer issued, ignoring the case it
// spelled the name with: the names are compared case-insensitively everywhere
// else in HTTP, and a casing change must not turn into an upload refusal.
func headerValue(h map[string]string, name string) string {
	for k, v := range h {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return ""
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
// refused with HTTP 400 did not poison the session - the upload still committed
// with the right digest once that part arrived - so one bad part does not make
// the file start over. What a retry does need is a new URL, which is why the
// mint lives inside the attempt loop rather than before it.
func (f *Fs) putParts(ctx context.Context, name, uploadFileID string, st *stagedSource) error {
	n := len(st.plans)
	if n == 0 {
		return nil
	}
	if n == 1 {
		return f.putPart(ctx, name, 0, 1, st, uploadFileID)
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(f.opt.UploadConcurrency)
	for i := 0; i < n; i++ {
		if gctx.Err() != nil {
			// A part that ran out of attempts cancels the rest. Launching them
			// anyway would only send requests the upload cannot use.
			break
		}
		i := i
		g.Go(func() error { return f.putPart(gctx, name, i, n, st, uploadFileID) })
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
func (f *Fs) putPart(ctx context.Context, name string, idx, total int, st *stagedSource, uploadFileID string) error {
	attempts := max(fs.GetConfig(ctx).LowLevelRetries, 1)
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A fresh URL every attempt, not one carried in from outside the loop:
		// the signature is dated, and a retry of a URL that was refused for
		// being too old would be refused for being too old again.
		ep, err := f.partURL(ctx, uploadFileID, st, idx)
		if err == nil {
			var target string
			target, err = f.partURLFor(ep.RequestURL)
			if err != nil {
				// A URL this build cannot turn into a request (a broken
				// upload_zone, say) is the state of the configuration, not of the
				// network: retrying it only buys another mint.
				return fserrors.NoLowLevelRetryError(err)
			}
			if err = f.putPartOnce(ctx, idx, st, target, api.ParseRequestHeader(ep.RequestHeader)); err == nil {
				return nil
			}
		}
		if fserrors.IsNoRetryError(err) || fserrors.IsNoLowLevelRetryError(err) {
			return err
		}
		lastErr = err
		if attempt == attempts {
			break
		}
		fs.Debugf(f, "cloud189: %s: part %d/%d attempt %d/%d failed, retrying with a fresh URL: %v", name, idx+1, total, attempt, attempts, err)
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
	// Which storage node answered, and whether the connection was reused, is
	// what tells a slow part apart from a slow path: one node behind every slow
	// part is a different problem from every part landing somewhere new.
	var peerAddr string
	var reusedConn bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Conn != nil {
				peerAddr = info.Conn.RemoteAddr().String()
			}
			reusedConn = info.Reused
		},
	}))
	host := ""
	if u, perr := url.Parse(target); perr == nil {
		host = u.Host
	}
	t0 := time.Now()
	res, err := f.partClient.Do(req)
	if err != nil {
		// The transport quotes the URL it failed on, and that URL is a live
		// signature for this part.
		return fmt.Errorf("cloud189: part %d via %s -> %s: %w", idx+1, host, peerAddr, scrubErr(err, target))
	}
	defer func() { _ = res.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
	if res.StatusCode >= http.StatusOK && res.StatusCode < http.StatusMultipleChoices {
		elapsed := time.Since(t0)
		fs.Debugf(f, "cloud189: uploaded part %d/%d (%v) via %s -> %s (reused=%v) in %v (%s)",
			idx+1, len(st.plans), fs.SizeSuffix(part.Size), host, peerAddr, reusedConn,
			elapsed.Round(time.Millisecond), fs.SizeSuffix(float64(part.Size)/elapsed.Seconds()).ByteRateUnit())
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
	t0 := time.Now()
	uploadFileID, rapid, err := f.beginUpload(ctx, dirID, name, st, size)
	if err != nil {
		return nil, err
	}
	parts := len(st.plans)
	if rapid {
		// The init carries the digests and the server answers whether it already
		// holds that content. A hit moves no byte, which is the single fact a
		// migration log has to be able to show: without this line a rapid
		// transfer and a real one look identical apart from how long they took.
		fs.Logf(f, "cloud189: %s (%v): rapid transfer - the drive already held this content, no part uploaded, %v", name, fs.SizeSuffix(size), time.Since(t0).Round(time.Millisecond))
	} else {
		fs.Debugf(f, "cloud189: %s (%v): %d parts of %v, uploadFileId accepted in %v",
			name, fs.SizeSuffix(size), parts, fs.SizeSuffix(st.partSize), time.Since(t0).Round(time.Millisecond))
	}
	if !rapid {
		tParts := time.Now()
		// Each part's URL is minted just before that part is sent, so there is no
		// list of URLs to fetch here and nothing that can go stale while the file
		// uploads.
		if err := f.putParts(ctx, name, uploadFileID, st); err != nil {
			return nil, err
		}
		fs.Debugf(f, "cloud189: %s: all %d parts uploaded in %v (%v)", name, parts,
			time.Since(tParts).Round(time.Millisecond), fs.SizeSuffix(float64(size)/time.Since(tParts).Seconds()).ByteRateUnit())
	}
	tCommit := time.Now()
	res, err := f.commitUpload(ctx, uploadFileID)
	if err != nil {
		return nil, err
	}
	fs.Debugf(f, "cloud189: %s: commit returned %q in %v (total %v)", name, res.name, time.Since(tCommit).Round(time.Millisecond), time.Since(t0).Round(time.Millisecond))
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
// cannot finish. A fresh upload is a duplicate of something the drive may already
// have, so it is deleted rather than left behind under a name nobody asked for.
// A moved object is the original, so it stays where it is and the name it was
// reaching for is put back: a failure handler never removes something that was
// already on the drive before this run.
func (f *Fs) installObject(ctx context.Context, dirID, name string, res *uploadResult, occupier *api.Row, oursIsNew bool) error {
	own := fileRow(res.id, res.name, dirID)
	if occupier == nil && res.name == name {
		// Landed exactly where it was wanted. Nothing to do.
		return nil
	}
	if occupier == nil {
		// The object is under some other name: an upload onto a name that is in
		// use is stored beside it, under a name of the server's own choosing, and
		// the same-directory rename hands over the name the object still wears.
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
		case row.ID.String() == res.id:
			// The drive already lists this object under the name it wanted,
			// whatever the commit echo spelled: the object is in place, and the
			// thing standing in the way is itself. Parking it would move the new
			// bytes aside and the delete that finishes the swap would take them
			// off the drive.
			return nil
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
	for _, t := range targets {
		f.deleted.remember(t.row.ID.String())
	}
	if !f.opt.HardDelete {
		return nil
	}
	_, err := f.runBatchTask(ctx, f.taskAuth(), taskClearRecycle, infos, "")
	return err
}

// deletedRows remembers the ids this Fs instance has deleted. It lives behind a
// pointer for the same reason the quota does: NewFs copies the Fs when a remote
// turns out to name a file, and a mutex copied that way is a mutex nobody shares.
// Being per instance rather than per account is a known limit: a second remote of
// the same account opened in the same run has no idea what the first one deleted,
// so it can read a lingering row of a deleted object as a live one.
type deletedRows struct {
	mu  sync.Mutex
	ids map[string]time.Time
}

// deletedRowWindow is how long a deleted object's row is distrusted. The
// drive's listings keep answering a deleted row for about a second after the
// delete task reports done (measured 2026-10-23: the suite's rmdir-then-mkdir
// of the same root found the deleted directory's row again and bound its dead
// id, and every operation through the cached id then answered FileNotFound).
// A fresh object with the same name gets a fresh id, which is never in the set,
// so only the dead rows are distrusted. The window covers the worst lag measured
// on a name that had several mutations inside a second - seven seconds - and it is
// derived from the readback wait rather than written down beside it: a retry loop
// that outlives the window would wait past the distrust and accept the very row it
// was distrusting (found by the copy readback's own test).
const deletedRowWindow = 2 * arrivalAttempts * arrivalWait

// How long a swap waits for the drive's listing to agree before it believes the
// operation did not happen. The measurement behind the numbers is 2026-10-24: a
// name that had three mutations cross it inside a second was still unreadable
// seven seconds on, so anything under that turns successes into failures.
const (
	arrivalWait     = 3 * time.Second
	arrivalAttempts = 6
)

// rememberDeleted notes the ids a finished delete task removed.
func (d *deletedRows) remember(ids ...string) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	for id, when := range d.ids {
		if now.Sub(when) > 2*deletedRowWindow {
			delete(d.ids, id)
		}
	}
	if len(d.ids) >= 1024 {
		return // to the top and stop remembering new keys, like the other tables
	}
	for _, id := range ids {
		d.ids[id] = now
	}
}

// recent reports whether a row with this id was deleted by this process
// recently enough that a listing may still be carrying it.
func (d *deletedRows) recent(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	when, ok := d.ids[id]
	return ok && time.Since(when) < deletedRowWindow
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
			if f.deleted.recent(row.ID.String()) {
				// The listing still carries the row this process deleted, and
				// the id it carries is one nothing will ever answer to again.
				return false
			}
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
	if o.size <= 0 {
		// Without a length there is nothing to compare a short body against, so
		// nothing to continue from either.
		return res.Body, nil
	}
	// A read may have asked for part of the object, and that part is what has to
	// be measured and resumed - not the whole object. FixRangeOption has already
	// turned every way of asking (a range, a seek, a suffix) into one pair of
	// offsets.
	start, end := int64(0), o.size-1
	for _, opt := range options {
		if r, ok := opt.(*fs.RangeOption); ok {
			start, end = r.Start, r.End
			break
		}
	}
	if start != 0 || end != o.size-1 {
		// A read that asked for part of the object is left as the host answered
		// it. Continuing one is a different measurement from continuing the whole
		// object, and nothing has measured the host cutting a ranged read.
		return res.Body, nil
	}
	return &resumableBody{ctx: ctx, o: o, body: res.Body, start: start, end: end}, nil
}

// maxResumes bounds how many times one download may reconnect. Each one is a fresh
// request, and a host that cuts every attempt should be reported rather than
// hammered.
const maxResumes = 4

// resumableBody hands a download to the caller and repairs a body the storage host
// cut short.
//
// The host has been measured to drop a large transfer part way and answer the same
// object in full afterwards (2026-10-24: the standard suite's 21 MiB read failed
// with unexpected EOF on three runs in a row, and the object read back whole
// minutes later). An object cannot change under a reader - it is addressed by id -
// so the rest is asked for from the byte already delivered, and a dropped
// connection becomes a slower download instead of a failed file.
type resumableBody struct {
	ctx     context.Context
	o       *Object
	body    io.ReadCloser
	start   int64 // the first byte this read asked for
	end     int64 // the last one, inclusive
	offset  int64 // bytes handed to the caller so far
	resumes int
	done    bool
}

// want is how many bytes this read asked for.
func (b *resumableBody) want() int64 { return b.end - b.start + 1 }

func (b *resumableBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	n, err := b.body.Read(p)
	b.offset += int64(n)
	if err == nil {
		return n, nil
	}
	if errors.Is(err, io.EOF) && b.offset < b.want() {
		// The body ended before the last byte this read asked for: this is a cut,
		// not the end of it.
		err = io.ErrUnexpectedEOF
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) || b.offset >= b.want() || b.resumes >= maxResumes {
		b.done = true
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return n, fmt.Errorf("cloud189: the download stopped at %d of %d bytes after %d reconnects: %w", b.offset, b.want(), b.resumes, err)
		}
		return n, err
	}
	if rerr := b.reopen(); rerr != nil {
		b.done = true
		return n, fmt.Errorf("cloud189: the download stopped at %d of %d bytes and could not be continued: %w", b.offset, b.o.size, rerr)
	}
	return n, nil
}

// reopen asks for the object from the byte already delivered, on a fresh link.
func (b *resumableBody) reopen() error {
	b.resumes++
	_ = b.body.Close()
	// A link that was being used when the transfer was cut may be the reason it
	// was cut; asking the drive for another one costs a request and removes it as
	// a suspicion.
	b.o.invalidateURL()
	link, err := b.o.fs.downloadURLCached(b.ctx, b.o)
	if err != nil {
		return err
	}
	from := b.start + b.offset
	res, err := b.o.get(b.ctx, link, &fs.RangeOption{Start: from, End: b.end})
	if err != nil {
		return err
	}
	switch res.StatusCode {
	case http.StatusPartialContent:
		if got := res.Header.Get("Content-Range"); got != "" && !strings.HasPrefix(got, fmt.Sprintf("bytes %d-", from)) {
			_ = res.Body.Close()
			return fmt.Errorf("the host answered a range that does not start where the transfer stopped: %s", truncate(got, 60))
		}
	case http.StatusRequestedRangeNotSatisfiable:
		_ = res.Body.Close()
		return errors.New("the host holds fewer bytes than it announced")
	case http.StatusOK:
		// The host ignored the range and started again from the beginning: drop
		// what has already been handed to the caller.
		if _, err := io.CopyN(io.Discard, res.Body, from); err != nil {
			_ = res.Body.Close()
			return fmt.Errorf("the host answered the whole object and it ended before the byte already read: %w", err)
		}
	default:
		_ = res.Body.Close()
		return fmt.Errorf("HTTP %d", res.StatusCode)
	}
	b.body = res.Body
	return nil
}

func (b *resumableBody) Close() error {
	b.done = true
	return b.body.Close()
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

// PublicLink returns a direct download link for the file at remote.
//
// The link is minted fresh and never taken from the download-URL cache, so the
// holder gets the whole lifetime the drive granted, not what is left of a
// window rclone was already part way through.
//
// What comes back is the address the bytes are read from, not the drive's own
// download endpoint. getFileDownloadUrl.action answers with a
// download.cloud.189.cn URL that only redirects: one 302 lands on a presigned
// storage URL whose query carries the real grant (measured 2026-10-27, a ctyun
// OBS host with X-Amz-Expires=300, and that host is what a direct-link rule keys
// on). Following it once costs a single-byte request, so a client that does not
// follow redirects gets a usable answer, and the lifetime the returned link
// really has travels with it rather than being guessed at here. Were that one
// request to fail, the redirecting form is returned with a warning instead: it
// still reads for anything that follows the redirect, and a link part way
// through its life is worth more than a failure.
//
// getFileDownloadUrl.action takes no expiry parameter, so the client cannot
// choose how long a link lives. Measured 2026-10-27 the redirecting form is valid
// for about 5 minutes from issue on the personal space and about 8 hours on the
// family one, and the storage address it names carries a grant of its own. A
// requested expiry is therefore ignored with a warning, and the reader has to
// finish inside a window the drive chose. unlink is ignored too: nothing was ever
// stored on the drive, so there is no share to revoke.
//
// Those are the lifetimes of the redirecting form. The storage grant behind it is
// shorter on both spaces and dramatically so on the family one (measured
// X-Amz-Expires=300, i.e. five minutes against the eight hours above), while the
// redirecting form renews that grant every time it is hit. So a link passed on
// from here lives minutes where the one it replaces would have lived hours. The
// resolved address is still what goes out - a direct-link rule keys on the host,
// which is the whole reason this backend follows the redirect - but the shorter
// lifetime is reported rather than left for the recipient to discover.
//
// Directories cannot be linked and return fs.ErrorCantShareDirectories.
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (string, error) {
	if strings.HasSuffix(remote, "/") {
		return "", fs.ErrorCantShareDirectories
	}
	obj, err := f.NewObject(ctx, remote)
	if err != nil {
		if err == fs.ErrorIsDir {
			return "", fs.ErrorCantShareDirectories
		}
		return "", err
	}
	o := obj.(*Object)
	if expire.IsSet() {
		fs.Logf(f, "Public Link: cloud189 does not let the client choose how long a download link lives, ignoring the requested expiry of %v", expire)
	}
	if unlink {
		// rclone's contract for a backend without unlink is to ignore the flag:
		// the link is minted per request and stored nowhere on the drive, so
		// there is nothing to revoke.
		fs.Debugf(f, "a download link is minted per request, so there is no stored link to unlink")
	}
	// The uncached path: a link handed to someone else should not be shortened
	// by whatever is left of rclone's own window.
	link, _, err := f.downloadURL(ctx, o.id)
	if err != nil {
		return "", err
	}
	resolved, rerr := f.storageURL(ctx, link)
	if rerr != nil {
		var refused *linkRefusedError
		if errors.As(rerr, &refused) {
			// The drive's own endpoint refused, so the link is dead and passing
			// it on would hand the caller something that does not work.
			return "", rerr
		}
		fs.Logf(f, "Public Link: the storage address behind the link could not be read (%v), returning the link that redirects", rerr)
		return link, nil
	}
	// The storage grant is shorter than the redirecting form on both spaces, and
	// on the family one by orders of magnitude. A caller who passes this on is
	// handing out minutes where the link would have lived hours, so it is said
	// out loud rather than left to be discovered by whoever cannot open it.
	if grant, ok := linkValidity(resolved); ok {
		if grant < 30*time.Minute {
			fs.Logf(f, "Public Link: the address handed out carries a %v grant, shorter than the link it redirects from; a recipient has to finish inside that window", grant.Round(time.Second))
		}
	}
	return resolved, nil
}

// linkValidity reports how long a presigned storage URL stays valid, when its
// query says so.
func linkValidity(raw string) (time.Duration, bool) {
	u, err := url.Parse(raw)
	if err != nil {
		return 0, false
	}
	q := u.Query()
	secs, err := strconv.ParseInt(q.Get("X-Amz-Expires"), 10, 64)
	if err != nil || secs <= 0 {
		return 0, false
	}
	// The clock starts when the URL was signed, not when it was read, so the
	// signing time is taken off when it is there.
	if signedAt, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date")); err == nil {
		return time.Until(signedAt.Add(time.Duration(secs) * time.Second)), true
	}
	return time.Duration(secs) * time.Second, true
}

// maxLinkRedirects bounds how far the one redirect is followed. The drive uses
// a single hop; more than a few is a redirect loop, and the chain it would build
// is not worth following.
const maxLinkRedirects = 5

// linkRefusedError says the drive's own link answered badly, rather than the
// storage address behind it.
//
// The two failures mean different things to the caller: the link itself is dead
// in the first case, and still fine for anything that follows redirects in the
// second.
type linkRefusedError struct {
	status int
}

func (e *linkRefusedError) Error() string {
	return fmt.Sprintf("cloud189: the download link answered HTTP %d", e.status)
}

// storageURL follows a download link once and reports the address the bytes
// actually come from. A byte is asked for and the body is never read: the answer
// wanted is the URL the client ended up requesting, which Go leaves on the
// response after following the redirect.
func (f *Fs) storageURL(ctx context.Context, link string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link, http.NoBody)
	if err != nil {
		return "", scrubErr(err, link)
	}
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("user-agent", dartUserAgent)

	// Every address on the way is a secret, and Go quotes the one it failed on.
	// After a redirect that is the last address, not the one asked for: the
	// storage URL with its own signature in the query, which the single-secret
	// form of scrubErr would leave in place. The chain is collected so all of it
	// can be taken out of the message.
	chain := []string{link}
	client := *f.httpClient
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		chain = append(chain, req.URL.String())
		if len(via) >= maxLinkRedirects {
			return fmt.Errorf("cloud189: the download link redirected more than %d times", maxLinkRedirects)
		}
		return nil
	}
	res, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("cloud189: resolving the download link: %w", scrubErr(err, chain...))
	}
	defer func() {
		// A byte was asked for, so the rest is short. It is dropped rather than
		// abandoned so the connection can go back to the pool - capped, because
		// a server that ignores Range would otherwise turn this into the full
		// download the range was there to avoid.
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
		_ = res.Body.Close()
	}()
	if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusPartialContent {
		if len(chain) == 1 {
			// No redirect happened, so it was the drive's own endpoint that
			// refused. The link is dead and handing it back would be worse than
			// the error.
			return "", &linkRefusedError{status: res.StatusCode}
		}
		return "", fmt.Errorf("cloud189: the storage address answered HTTP %d", res.StatusCode)
	}
	return res.Request.URL.String(), nil
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
		// The suite's NilObject is a typed nil and prints "<nil>" for every
		// backend; a custom string here reads as a non-nil object.
		return "<nil>"
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

// SetModTime reports that the drive cannot be told a modification time.
//
// The bare sentinel is what rclone's callers look for - operations.Copy and
// the chunk-writer path both treat ErrorCantSetModTime as "unsupported" rather
// than as a failed transfer - and it is also what the suite compares with ==,
// so it travels unwrapped.
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
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
	// Park the object being replaced out of the name BEFORE the new bytes go up.
	// The drive never overwrites: an upload onto a name already in use is stored
	// beside it under a name the server invents (measured live 2026-10-24, when the
	// new bytes came back as "file name(20261024123855).txt" while the old object
	// kept the wanted name). Into a name this run has just freed, the upload
	// arrives wearing the name it was asked for, so the swap needs no rename of the
	// fresh object at all - and the name is not hit by park, rename and delete
	// inside one second, which is what left the drive's listing too far behind for
	// rclone to read the object back (measured the same day: the name was still
	// unreadable seven seconds after a swap that had in fact completed).
	parkedName := newParkName()
	if err := o.fs.renameObject(ctx, o.id, false, parkedName); err != nil {
		return err
	}
	parked := *fileRow(o.id, parkedName, dirID)
	res, err := o.fs.upload(ctx, in, dirID, wireLeaf, src.Size())
	if err != nil {
		// The bytes that were there are still on the drive, only under a name
		// nobody asked for. Give them back before reporting the failure.
		o.fs.undoClear(ctx, &parked, wireLeaf)
		return err
	}
	// The drive deduplicates by digest, so new bytes equal to the parked object's
	// own can come back as THAT object: the same stored data, and the same file that
	// was moved aside a moment ago. It is then the original being put back, never
	// something to delete - and the commit's echo cannot be trusted to tell the two
	// cases apart, because the drive answers a deduplicated commit with the name the
	// object holds, which is the park name here. So the decision is made from the
	// listing, not from what the upload reported.
	if res.id == o.id {
		if err := o.fs.restoreOrConfirm(ctx, dirID, wireLeaf, &parked); err != nil {
			// The object is not confirmed back under its name, and this branch has
			// no fresh object of its own to leave there: put the original back
			// rather than let it sit under a park name with the name empty.
			o.fs.undoClear(ctx, &parked, wireLeaf)
			return err
		}
		o.size = src.Size()
		o.md5 = strings.ToUpper(res.md5)
		o.modTime = time.Now()
		o.invalidateURL()
		return nil
	}
	if err := o.fs.installObject(ctx, dirID, wireLeaf, res, nil, true); err != nil {
		// installObject took back the fresh upload if it could; the parked object is
		// what was on the drive before this run, so it goes back either way.
		o.fs.undoClear(ctx, &parked, wireLeaf)
		return err
	}
	// Nothing that was on the drive before this run is removed until the drive says
	// the new bytes are standing at the wanted name.
	if err := o.fs.proveArrived(ctx, dirID, wireLeaf, res.id); err != nil {
		o.fs.undoClear(ctx, &parked, wireLeaf)
		return err
	}
	o.fs.dropObject(ctx, &parked, "it was replaced by newer bytes")
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

// DirCacheFlush resets the directory cache - the optional interface rclone's own
// suite calls between listing retries.
//
// It matters here because a directory can be removed under the cache's feet: the
// suite purges and rmdir's directories through a remote opened AT that directory,
// which flushes that remote's cache and not the one the run keeps using above it.
// The mapping left behind names a folder the drive no longer has, and every path
// under it resolves through that dead id until something drops it.
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// restoreOrConfirm puts a parked object back under the name it was cleared from.
//
// The rename is what is tried first, because it is the drive that knows whether
// the object is back - and the listing does not: a parked object read back under
// the name it was parked from is exactly what a listing that is one rename behind
// answers with. Taking that as confirmation would report an object as being in
// place while it is still sitting under a park name, with the name empty.
//
// When the rename is refused, the name is read: an object that already holds the
// name it is asked for is the one case where being refused means being done.
func (f *Fs) restoreOrConfirm(ctx context.Context, dirID, wireName string, parked *api.Row) error {
	err := f.restoreName(ctx, parked, wireName)
	if err == nil {
		return nil
	}
	// Only a refusal about the NAME may be read as "the object already holds it".
	// Any other failure is the drive not having answered, and a listing that is one
	// rename behind answers with the object under the name it was parked from -
	// which would turn "still parked" into a reported success.
	var refused *apiError
	if !errors.As(err, &refused) || refused.Code != codeFileAlreadyExists {
		return err
	}
	row, ferr := f.awaitRow(ctx, dirID, wireName)
	switch {
	case ferr == nil && row.ID.String() == parked.ID.String():
		return nil
	case ferr == nil:
		return fserrors.NoRetryError(fmt.Errorf("cloud189: %q is held by something other than the object this run parked there", truncate(wireName, 60)))
	case errors.Is(ferr, fs.ErrorObjectNotFound):
		// The name is free and the rename still failed, so the failure was the
		// drive's answer about the rename itself rather than a collision.
		return err
	}
	return ferr
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
			// The anchor is read when the remote is opened, and a listing that
			// failed at that moment leaves it empty while every other path keeps
			// working through the directory cache - measured 2026-10-25, the
			// standard suite's teardown purge refused a root it had been using all
			// run. Ask the cache again; a root that really does not resolve still
			// leaves it empty and the purge still refuses.
			if aerr := f.resolveAnchor(ctx); aerr == nil {
				dirID = f.fsRootID
			}
		}
		if dirID == "" {
			return fserrors.NoRetryError(errors.New("cloud189: this remote's root could not be resolved, so a purge will not be sent"))
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
		// A remote opened below the space root owns the directory it was opened
		// at, and a purge of it removes the directory too - that is what
		// `rclone purge remote:sub` means, and what the suite's own teardown
		// checks for. The space root itself is only ever emptied.
		if f.root != "" {
			return f.rmdirRoot(ctx, false)
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
	f         *Fs
	tmp       *os.File
	tmpPath   string
	remote    string
	dirID     string
	leaf      string // the name as it goes on the wire
	size      int64
	partSize  int64 // upload part size: the ladder value the server checks
	chunkSize int64 // download chunk size: what the copy engine hands us per call
	plans     []api.PartPlan

	mu   sync.Mutex
	done []bool // one per download chunk, so a chunk that never arrived is a hole and not a shorter sum
}

// WriteChunk stores one download chunk at its offset in the staged file.
//
// Chunks arrive from several goroutines at once, which is why the file is
// opened once and written at absolute offsets rather than appended to. The
// chunk size here is the engine's (download_chunk_size), not the upload part
// size: the staged file is the same bytes either way, and the part digests are
// computed from it at Close.
func (w *chunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	chunkSize := w.chunkSize
	if chunkSize <= 0 {
		// Writers built before download_chunk_size existed (and their tests)
		// only carry partSize, where one read was one part.
		chunkSize = w.partSize
	}
	if chunkNumber < 0 || chunkNumber >= len(w.done) {
		return 0, fserrors.NoRetryError(fmt.Errorf("cloud189: %s: chunk %d is past the %d chunks this copy was planned around", w.leaf, chunkNumber, len(w.done)))
	}
	start := int64(chunkNumber) * chunkSize
	due := chunkSize
	if remaining := w.size - start; remaining < due {
		due = remaining
	}
	n, err := io.Copy(io.NewOffsetWriter(w.tmp, start), io.LimitReader(reader, due))
	if err != nil {
		return 0, fmt.Errorf("cloud189: %s: staging chunk %d: %w", w.leaf, chunkNumber+1, err)
	}
	if n != due {
		return 0, fserrors.NoRetryError(fmt.Errorf("cloud189: %s: chunk %d gave %d bytes where %d were due", w.leaf, chunkNumber+1, n, due))
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
			return fserrors.NoRetryError(fmt.Errorf("cloud189: %s: chunk %d of %d never arrived, so the staged file is incomplete", w.leaf, i+1, len(w.done)))
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

	// The copy engine reads the source chunk_size bytes at a time; the upload
	// part size above is what the server checks. They are allowed to differ:
	// every chunk lands at its absolute offset in the staged file either way.
	chunkSize := partSize
	if f.opt.DownloadChunkSize > 0 {
		chunkSize = int64(f.opt.DownloadChunkSize)
	}
	if chunkSize > size {
		chunkSize = size
	}

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
		f:         f,
		tmp:       tmp,
		tmpPath:   tmp.Name(),
		remote:    remote,
		dirID:     dirID,
		leaf:      wireLeaf,
		size:      size,
		partSize:  partSize,
		chunkSize: chunkSize,
		plans:     plans,
		done:      make([]bool, (size+chunkSize-1)/chunkSize),
	}
	fs.Debugf(remote, "cloud189: open chunk writer: %d download chunks of %v, %d upload parts of %v", len(w.done), fs.SizeSuffix(chunkSize), len(plans), fs.SizeSuffix(partSize))
	return fs.ChunkWriterInfo{ChunkSize: chunkSize, Concurrency: f.opt.UploadConcurrency}, w, nil
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
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ dircache.DirCacher = (*Fs)(nil)
)
