// Package yun139 provides an interface to China Mobile cloud drive (中国移动云盘).
// (Single-file layout per AGENTS.md: main implementation in yun139.go,
// API types in api/types.go.)
package yun139

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	neturl "net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/backend/yun139/api"
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
	"github.com/rclone/rclone/lib/random"
	"golang.org/x/sync/errgroup"
)

// ------------------------------------------------------------ constants ----

const (
	yunBaseURL        = "https://yun.139.com"
	minSleep          = 10 * time.Millisecond
	maxSleep          = 2 * time.Second
	decayConstant     = 2
	listPageSize      = 100
	maxListPages      = 1000
	defaultRootID     = "/"
	defaultFamilyRoot = "0"

	// personalPartSize is the default part size for personal uploads.
	// The official PC client (8.8.6, captured 2026-09-03) uploads in
	// 5242880-byte (5 MiB) parts with parallelUpload:true.
	personalPartSize = int64(5 * 1024 * 1024)

	// maxPartsPerRequest caps the partInfos sent in /file/create and
	// /file/getUploadUrl (the server accepts up to 100).
	maxPartsPerRequest = 100

	// downloadURLTTL bounds the reuse of a cached download URL. The client
	// asks for expireSec:86400 (24h), so a 23h cache is safe.
	downloadURLTTL = 23 * time.Hour

	// minTokenLifetime is the remaining validity under which the token is
	// refreshed proactively (the official client uses 15 days).
	minTokenLifetime = 15 * 24 * time.Hour

	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

// Space names for the "space" option.
const (
	spacePersonal = "personal"
	spaceFamily   = "family"
)

// svcType values for the x-SvcType header: 1 = personal, 2 = family.
const (
	svcTypePersonal = "1"
	svcTypeFamily   = "2"
)

// ------------------------------------------------------------ options ------

// Options defines the configuration for this backend
type Options struct {
	Authorization     string               `config:"authorization"` // base64("pc:account:token")
	Space             string               `config:"space"`
	FamilyID          string               `config:"family_id"`
	RootFolderID      string               `config:"root_folder_id"`
	UserDomainID      string               `config:"user_domain_id"` // <user-domain-id>-style id, optional
	HardDelete        bool                 `config:"hard_delete"`
	NoRefresh         bool                 `config:"no_refresh"` // never refresh the token
	PartSize          fs.SizeSuffix        `config:"part_size"`
	UploadConcurrency int                  `config:"upload_concurrency"`
	RegionCode        string               `config:"region_code"` // "province:city" 上传调度节点码,如 531:543(江苏无锡);留空不发送
	DisableHTTP2      bool                 `config:"disable_http2"`
	MaxFileSize       fs.SizeSuffix        `config:"max_file_size"` // 单文件上传上限覆盖;0=按会员等级自动
	Enc               encoder.MultiEncoder `config:"encoding"`
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "yun139",
		Description: "China Mobile Cloud Drive (中国移动云盘)",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name: "authorization",
			Help: "The 139 authorization token.\n\n" +
				"Base64 of \"pc:<account>:<token>|...\". Obtain it from an OpenList/alist 139 storage config " +
				"or by logging in to https://yun.139.com and capturing the Authorization header.",
			Sensitive: true,
		}, {
			Name:    "space",
			Help:    "Which storage space to use.",
			Default: spacePersonal,
			Examples: []fs.OptionExample{{
				Value: spacePersonal,
				Help:  "Personal cloud (新个人云)",
			}, {
				Value: spaceFamily,
				Help:  "Family cloud (新家庭云)",
			}},
		}, {
			Name:     "family_id",
			Help:     "Family cloud ID (required when space is family).",
			Advanced: true,
		}, {
			Name:      "root_folder_id",
			Help:      "ID of the root folder. Leave blank for the top level.",
			Advanced:  true,
			Sensitive: true,
		}, {
			Name: "user_domain_id",
			Help: "The <user-domain-id>-style user domain id.\\n\\n" +
				"Found in the official client's request URLs ('u=' query param, " +
				"also returned by user/getUser and queryFamilyCloud). Optional: " +
				"when blank, the phone number is used where the server accepts it.",
			Advanced: true,
		}, {
			Name: "hard_delete",
			Help: "Delete permanently instead of moving files to the recycle bin.\n\n" +
				"Only applies to the personal space.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "no_refresh",
			Help: "Never refresh the token.\n\n" +
				"Use when the same account is shared with another program that manages " +
				"the token itself (e.g. an OpenList/alist instance), to avoid competing " +
				"refreshes rotating the token out from under each other.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "part_size",
			Help: "Part size for uploads.\n\n" +
				"Files larger than this are uploaded in parts of this size. Must be a multiple of 64 bytes. " +
				"The official client uses 100 MiB.",
			Default:  fs.SizeSuffix(personalPartSize),
			Advanced: true,
		}, {
			Name:     "upload_concurrency",
			Help:     "Concurrency for part uploads within a single file.",
			Default:  4,
			Advanced: true,
		}, {
			Name:     "region_code",
			Help:     "userRegion 上传调度节点码,格式 province:city,如 531:543(江苏无锡)。官方 PC 客户端会随 create 发送;设置后 personal 上传也带 userRegion 以就近调度。留空则不发送。",
			Advanced: true,
		}, {
			Name: "disable_http2",
			Help: "Disable HTTP/2 for all yun139 traffic.\n\n" +
				"HTTP/2 multiplexes every concurrent part upload onto a single TCP " +
				"connection, which caps aggregate upload throughput at one connection's " +
				"worth of bandwidth. Set this to use HTTP/1.1 keep-alive instead, so the " +
				"concurrent part uploads spread across multiple parallel TCP connections.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "max_file_size",
			Help: "Override the maximum single-file upload size.\n\n" +
				"0 (default) auto-detects the member tier (vip userIdentity) and uses " +
				"no-member 5G / silver 8G / gold 20G / diamond 500G. Larger files are " +
				"skipped with a NoRetryError instead of wasting a full multi-part upload " +
				"that the server rejects with 04010319 (权益不足).",
			Default:  fs.SizeSuffix(0),
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			// 139 rejects names with leading/trailing whitespace, control
			// chars, leading tilde/period, and invalid UTF-8, with
			// '04000002: 文件名称不符合标准'. Encode them on the wire so
			// every fstests encoding case round-trips.
			Default: encoder.Standard | encoder.EncodeInvalidUtf8 |
				encoder.EncodeLeftSpace | encoder.EncodeLeftTilde |
				encoder.EncodeLeftCrLfHtVt | encoder.EncodeRightSpace |
				encoder.EncodeRightCrLfHtVt | encoder.EncodeLeftPeriod |
				encoder.EncodeRightPeriod,
		}},
	})
}

// ------------------------------------------------------------ errors ------

// shouldRetry reports whether a request error is retryable.
func shouldRetry(ctx context.Context, err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	// Business errors (API rejections) are never retryable; transport errors are.
	var ae *apiError
	if errors.As(err, &ae) {
		return false, err
	}
	return true, err
}

// apiError is a business error returned by the 139 API.
type apiError struct {
	Code    string
	Message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("yun139: API error %s: %s", e.Code, e.Message)
}

// ------------------------------------------------------- member level ------

// 单文件上传上限(字节)。无会员 5G / 白银 8G / 黄金 20G / 钻石 500G。
const (
	memberLimitNoMember int64 = 5 << 30
	memberLimitSilver   int64 = 8 << 30
	memberLimitGold     int64 = 20 << 30
	memberLimitDiamond  int64 = 500 << 30
)

// memberQuotaErrorCode is the server rejection when a file's total size
// exceeds the account's member single-file upload limit: /hcy/file/create
// (and complete) validate the size after all part PUTs succeed and return
// RSP_CODE 04010319 权益不足 (measured 2026-09-20, silver tier 8G limit).
const memberQuotaErrorCode = "04010319"

// memberLevelInfo is a detected member tier and its single-file upload limit.
type memberLevelInfo struct {
	maxFileSize int64  // 0 = unknown/no limit (fail-open)
	typeCode    string // e.g. "1001"(白银) / "1000"(无会员); only for logging
	typeName    string // e.g. "白银会员" / "无会员"; only for logging
}

// memberLevelCache caches the detected per-account member tier so remotes
// sharing one account (accountKey) only probe once and reuse the tier name too.
var memberLevelCache = struct {
	sync.Mutex
	byAccount map[string]memberLevelInfo // accountKey -> tier info
}{byAccount: map[string]memberLevelInfo{}}

// maxFileSizeForLevel maps a detected member tier to the single-file upload
// limit. typeName is preferred (only "白银会员" is live-confirmed; gold/diamond
// names/codes are inferred). An unrecognised tier returns 0 = no limit
// (fail-open), so a probe we do not understand never blocks a valid upload.
func maxFileSizeForLevel(typeCode, typeName string) int64 {
	switch typeName {
	case "白银会员":
		return memberLimitSilver
	case "黄金会员":
		return memberLimitGold
	case "钻石会员":
		return memberLimitDiamond
	}
	// 无会员:data 为空,不进入 maxFileSizeForLevel(queryMemberLevel 返回 typeName="")。
	switch typeCode {
	case "1000":
		return memberLimitNoMember
	case "1001":
		return memberLimitSilver
	case "1002":
		return memberLimitGold
	case "1003":
		return memberLimitDiamond
	}
	return 0 // unknown => no limit
}

// queryMemberLevel calls vip.yun.139.com/m4c/openapi/userIdentity to detect the
// member tier of the current account. It uses f.httpClient directly (not f.call
// / personalCall, whose host/signature wiring targets the personal/family
// hosts); the VIP endpoint only needs an Authorization: Basic header. Returns
// the first member's type/typeName, or ("","",nil) when data is empty (无会员).
func (f *Fs) queryMemberLevel(ctx context.Context) (typeCode, typeName string, err error) {
	body, _ := json.Marshal(map[string]any{"memberTypeList": []any{}})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.MemberLevelURL, bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	req.Header.Set("Authorization", "Basic "+f.accessToken())
	res, err := f.httpClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("yun139: query member level: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("yun139: query member level: http %s", res.Status)
	}
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return "", "", fmt.Errorf("yun139: query member level: read: %w", err)
	}
	var env struct {
		ResultCode string `json:"resultCode"` // 0 = OK; the userIdentity envelope uses resultCode/resultDesc
		ResultDesc string `json:"resultDesc"`
		Data       []struct {
			Type     string `json:"type"`
			TypeName string `json:"typeName"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return "", "", fmt.Errorf("yun139: query member level: decode: %w", err)
	}
	if env.ResultCode != "" && env.ResultCode != "0" {
		return "", "", &apiError{Code: env.ResultCode, Message: env.ResultDesc}
	}
	if len(env.Data) == 0 {
		// 无会员:userIdentity 对无会员号返回 data 空(实测 2026-09-20)。
		// 用 typeCode "1000" 标识无会员,让 maxFileSizeForLevel 落到 5G 上限,
		// 避免 >5G 文件仍整包上传才被 04010319 打回浪费带宽。
		return "1000", "无会员", nil
	}
	return env.Data[0].Type, env.Data[0].TypeName, nil
}

// probeAndCacheMemberLevel detects and caches the account's single-file upload
// limit once. An explicit max_file_size wins; otherwise a cached value is used;
// only then is a live probe issued. Any probe failure is fail-open (记为 0,只记
// Debug 日志), never blocking the mount.
func (f *Fs) probeAndCacheMemberLevel(ctx context.Context) {
	if f.opt.MaxFileSize > 0 {
		f.memberMaxFileSize = int64(f.opt.MaxFileSize)
		return
	}
	key := f.accountKey()
	memberLevelCache.Lock()
	if info, ok := memberLevelCache.byAccount[key]; ok {
		f.memberMaxFileSize = info.maxFileSize
		f.memberLevelType = info.typeCode
		f.memberLevelName = info.typeName
		memberLevelCache.Unlock()
		return
	}
	memberLevelCache.Unlock()
	typ, name := "", ""
	if t, n, err := f.queryMemberLevel(ctx); err == nil {
		typ, name = t, n
	} else {
		fs.Debugf(f, "yun139: member level probe failed, assuming no single-file limit: %v", err)
	}
	v := maxFileSizeForLevel(typ, name)
	info := memberLevelInfo{maxFileSize: v, typeCode: typ, typeName: name}
	memberLevelCache.Lock()
	memberLevelCache.byAccount[key] = info
	memberLevelCache.Unlock()
	f.memberMaxFileSize = v
	f.memberLevelType = typ
	f.memberLevelName = name
	if f.memberMaxFileSize > 0 {
		fs.Debugf(f, "yun139: member level detected %q (type=%s), max single-file upload %d bytes", name, typ, f.memberMaxFileSize)
	}
}

// maxFileSize returns the effective single-file upload limit: an explicit
// max_file_size overrides the detected member limit; 0 means no limit.
func (f *Fs) maxFileSize() int64 {
	if f.opt.MaxFileSize > 0 {
		return int64(f.opt.MaxFileSize)
	}
	return f.memberMaxFileSize
}

// errTooLarge builds a NoRetryError for a file larger than the member limit, so
// copy/sync skip it immediately instead of wasting a full multi-part upload.
func errTooLarge(leaf string, size, limit int64, level string) error {
	return fserrors.NoRetryError(fmt.Errorf(
		"yun139: file %q is %d bytes, exceeding the %s single-file upload limit of %d bytes",
		leaf, size, level, limit))
}

// noRetryOnMemberQuota wraps a server 04010319 (权益不足) rejection in a
// NoRetryError so the low-level/upper retries do not resend the whole large
// file (each retry redoes every part PUT). Other errors pass through unchanged.
func noRetryOnMemberQuota(err error) error {
	if err == nil {
		return nil
	}
	var ae *apiError
	if errors.As(err, &ae) && ae.Code == memberQuotaErrorCode {
		fs.Errorf(nil, "yun139: upload rejected by member quota (%s 权益不足): %v", memberQuotaErrorCode, err)
		return fserrors.NoRetryError(fmt.Errorf("yun139: upload rejected by member quota (%s 权益不足): %w", memberQuotaErrorCode, err))
	}
	return err
}

// ------------------------------------------------------------ names --------

// yun139RejectedRunes holds the characters the 139 server refuses to store in
// a file or directory name, returning RSP_CODE 04000002 "文件名称不符合标准".
// Live probe (2026-09-19, personal space, directory create) shows the server
// enforces Windows filename rules: it rejects the control chars (0x00-0x1F,
// 0x7F) and the eight ASCII "reserved" characters `" * : < > ? \ |`, while
// accepting CJK, accented Latin (é), BMP symbols (© ® ° ± ✓) and non-BMP
// emoji. The backend's default encoder already encodes the control chars,
// leading/trailing space·dot·CR·LF·HT·VT, leading tilde/period and invalid
// UTF-8, so only the eight reserved ASCII characters below reach the server
// raw and trip the rejection.
var yun139RejectedRunes = `"*:<>?\|`

// validateName checks a leaf file name against yun139's storage rules.
//
// It returns an error wrapped with NoRetryError for names containing one of
// yun139RejectedRunes, which the server rejects deterministically with
// RSP_CODE 04000002. The NoRetryError wrapper stops --retries from re-running
// a whole sync round, which is pointless for a name that can never succeed.
//
// sanitize (replacing illegal characters) is intentionally NOT offered: sync
// is bidirectional, and a name transformed on the way up cannot be
// untransformed on the way down.
func validateName(leaf string) error {
	if strings.ContainsAny(leaf, yun139RejectedRunes) {
		return fserrors.NoRetryError(fmt.Errorf(
			"yun139: file or directory name contains a character which yun139 cannot store: %q", leaf))
	}
	return nil
}

// validateName checks the leaf name of remote against yun139's storage rules.
func (f *Fs) validateName(remote string) error {
	return validateName(path.Base(remote))
}

// ------------------------------------------------------------ Fs ----------

// tokenState holds the authorization token shared by every yun139 remote
// that belongs to the same account. Refreshing rotates the token, so two
// remotes of one account that kept independent state could refresh the same
// (now-stale) token and rotate it out from under each other. Sharing one state
// per account and serialising the refresh on a single lock avoids that.
type tokenState struct {
	mu      sync.Mutex
	auth    string
	mappers map[string]configmap.Mapper // section name -> mapper for write back
}

// tokenRegistry indexes the shared token states by account.
//
// The account (phone number) is an invariant of the authorization token, so
// it is a stable registry key across token rotations.
var tokenRegistry = struct {
	sync.Mutex
	byAccount map[string]*tokenState
}{byAccount: map[string]*tokenState{}}

// addMapper registers a remote so that refreshed tokens are written back to it.
func (ts *tokenState) addMapper(name string, m configmap.Mapper) {
	if m == nil {
		return
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.mappers == nil {
		ts.mappers = map[string]configmap.Mapper{}
	}
	ts.mappers[name] = m
}

// writeBackLocked saves the current token into every registered section.
//
// ts.mu must be held. The in-memory value is updated by the caller before this
// runs, so a concurrent request that already picked up the new token keeps
// working while the sections are being written.
func (ts *tokenState) writeBackLocked() {
	for _, m := range ts.mappers {
		if m == nil {
			continue
		}
		m.Set("authorization", ts.auth)
	}
}

// accountKey returns the stable registry key for this Fs's account. If the
// account cannot be parsed the authorization itself is used as a fallback so
// distinct remotes never collide.
func (f *Fs) accountKey() string {
	if f.account != "" {
		return "acct:" + f.account
	}
	return "auth:" + f.auth
}

// tokenState returns the shared token state for this Fs's account, registering
// this remote's mapper so a refresh writes back to its config section.
func (f *Fs) tokenState() *tokenState {
	tokenRegistry.Lock()
	defer tokenRegistry.Unlock()
	key := f.accountKey()
	ts, ok := tokenRegistry.byAccount[key]
	if !ok {
		ts = &tokenState{auth: f.auth, mappers: map[string]configmap.Mapper{}}
		tokenRegistry.byAccount[key] = ts
	}
	ts.addMapper(f.name, f.m)
	return ts
}

// Fs represents a remote yun139
type Fs struct {
	name       string             // name of this remote
	root       string             // the path we are working on
	opt        Options            // parsed options
	m          configmap.Mapper   // config mapper, used to write tokens back
	features   *fs.Features       // optional features
	httpClient *http.Client       // the connection to the server
	pacer      *fs.Pacer          // pacer for API calls
	dirCache   *dircache.DirCache // Map of directory path to directory id
	// srvPathOf caches family dirID -> server "root:/..." path. Held by
	// pointer so copying the Fs struct (dircache's tempFs trick) stays
	// lock-copy-free.
	srvPathOf *srvPathCache

	space   string // spacePersonal or spaceFamily
	svcType string // svcTypePersonal or svcTypeFamily

	tokMu   *sync.Mutex // guards authorization + account (pointer so NewFs's tempF copy stays lock-safe)
	auth    string      // the raw base64 authorization token
	account string      // the phone number
	ts      *tokenState // account-wide shared token state (nil until NewFs sets it)

	hostMu       *sync.Mutex // guards personalHost
	personalHost string      // per-account personal cloud host, discovered once

	familyRootMu *sync.Mutex
	familyRootID string // server-side root catalog ID of the family cloud

	userDomainID string // <user-domain-id>-style domain id, learned from queryFamilyCloud

	memberMaxFileSize int64  // 本账号单文件上传上限;0=未知/无限制(fail-open)
	memberLevelName   string // 探测到的等级名(如"白银会员"),仅用于日志
	memberLevelType   string // 探测到的 type code(如"1001"),仅用于日志
}

// Object describes a yun139 object
type Object struct {
	fs         *Fs       // what this object is part of
	remote     string    // the remote path
	id         string    // ID of the object
	size       int64     // size of the object
	modTime    time.Time // modification time
	isDir      bool      // whether this is a directory
	serverPath string    // family/group: server-side path (root:/...)
	sha256     string    // personal space: contentHash from listing/upload

	hashMu    *sync.Mutex // protects sha256 (concurrent Hash on one object)
	urlMu     *sync.Mutex // protects url / urlExpiry (pointer so Object copy stays lock-safe)
	url       string      // cached download URL
	urlExpiry time.Time   // when the cached URL stops being valid
}

// ------------------------------------------------------------ token -------

// parseAuth decodes the authorization token and extracts the account.
func (f *Fs) parseAuth() (account string, err error) {
	decoded, err := base64.StdEncoding.DecodeString(f.auth)
	if err != nil {
		return "", fmt.Errorf("yun139: authorization is not valid base64: %w", err)
	}
	parts := strings.SplitN(string(decoded), ":", 3)
	if len(parts) < 3 {
		return "", errors.New("yun139: authorization should be base64(\"pc:<account>:<token>\")")
	}
	return parts[1], nil
}

// tokenExpiryOf returns the expiry time encoded in an authorization token, if
// present. Token-expiry is a pure function of the auth string, so callers pass
// the exact token they are inspecting (the shared tokenState value in refresh).
func tokenExpiryOf(auth string) (time.Time, bool) {
	decoded, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return time.Time{}, false
	}
	parts := strings.SplitN(string(decoded), ":", 3)
	if len(parts) < 3 {
		return time.Time{}, false
	}
	strs := strings.Split(parts[2], "|")
	if len(strs) < 4 {
		return time.Time{}, false
	}
	ms, err := strconv.ParseInt(strs[3], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.UnixMilli(ms), true
}

// accessToken returns the current authorization (raw base64).
//
// When the account-wide tokenState has been registered (NewFs sets it), read
// from the shared state so a remote of the same account always sees the freshest
// token even if another remote refreshed it. Falls back to f.auth pre-NewFs.
func (f *Fs) accessToken() string {
	if f.ts != nil {
		f.ts.mu.Lock()
		defer f.ts.mu.Unlock()
		return f.ts.auth
	}
	f.tokMu.Lock()
	defer f.tokMu.Unlock()
	return f.auth
}

// discoverUserDomainID resolves the <user-domain-id>-style id from
// queryFamilyCloud, persists it to the config, and returns it. The official
// client caches this from login; we learn it in one extra request. The phone
// number is never accepted by the refresh endpoint, so this is the only
// reliable source for the userId a refresh must send.
func (f *Fs) discoverUserDomainID(ctx context.Context) (string, error) {
	_, uid, err := f.queryFamilyCloud(ctx)
	if err != nil {
		return "", err
	}
	if uid == "" {
		return "", errors.New("yun139: queryFamilyCloud returned no accountUserId")
	}
	f.userDomainID = uid
	f.opt.UserDomainID = uid
	f.m.Set("user_domain_id", uid)
	return uid, nil
}

// refreshToken refreshes the token via the SSO endpoint when it has less than
// minTokenLifetime left. The new token is written back to the config.
//
// Concurrency: shares one tokenState per account across every remote of that
// account (mirroring wopan's tokenRegistry), so several backends of one account
// refresh under a single account-wide lock and never rotate a stale token out
// from under each other:
//
//   - the refresh is serialised per-account by ts.mu (the shared tokenState's
//     lock, not per-Fs), so two remotes of one account cannot refresh together;
//   - before refreshing, the token is re-read from the shared state: if a
//     concurrent remote already refreshed it, we adopt the fresher token and
//     skip our own refresh instead of using the now-stale old one;
//   - the new token returned in the response APP_AUTH header is written back
//     to every registered mapper via writeBackLocked, only when it differs;
//   - --yun139-no-refresh (NoRefresh) disables refresh entirely for accounts
//     whose token another program manages.
func (f *Fs) refreshToken(ctx context.Context) error {
	if f.opt.NoRefresh {
		return nil
	}
	ts := f.tokenState()
	f.ts = ts

	// Fast path: if the token still has plenty of life left there is nothing to
	// refresh, so return before doing any network or userDomainId discovery.
	// The read is done under the shared lock (cheap) so a concurrent refresh's
	// commit of ts.auth is never read unsynchronised.
	ts.mu.Lock()
	auth := ts.auth
	if auth == "" {
		auth = f.auth
	}
	expiry, ok := tokenExpiryOf(auth)
	if ok && time.Until(expiry) > minTokenLifetime {
		ts.mu.Unlock()
		return nil
	}
	ts.mu.Unlock()

	// Resolve the userDomainId up front, OUTSIDE the shared lock. Discovery
	// calls queryFamilyCloud -> familyCall -> call -> accessToken(), which
	// locks ts.mu; doing it while holding ts.mu would deadlock on the same
	// mutex. The same applies to the self-heal rediscovery on a retry.
	userID := f.userDomainID
	manual := f.opt.UserDomainID != "" // conf provided a value (possibly wrong)
	if userID == "" {
		uid, derr := f.discoverUserDomainID(ctx)
		if derr != nil {
			return fserrors.NoRetryError(fmt.Errorf("yun139: refresh token: could not resolve userDomainId: %w", derr))
		}
		userID = uid
	}

	// doRefresh sends the SSO heartbeat with the given authToken + userId and
	// returns the fresh, pc:-normalised authorization. It does not touch ts.mu.
	// Success or failure is carried in the response header ERRORCODE (0 = ok);
	// the new token comes back in the response header APP_AUTH as
	// "Basic " + base64("<account>:<token>...") WITHOUT the "pc:" scheme prefix
	// that parseAuth / tokenExpiry expect, so we re-add it before persisting.
	doRefresh := func(authToken, userID string) (string, error) {
		reqBody, err := json.Marshal(map[string]string{"authToken": authToken, "userId": userID})
		if err != nil {
			return "", err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.AuthTokenRefreshURL, bytes.NewReader(reqBody))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
		req.Header.Set("User-Agent", pcUserAgentShort)
		req.Header.Set("APP_CP", "pc")
		req.Header.Set("CP_VERSION", pcAppVersion)

		res, err := f.httpClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("yun139: refresh token: %w", err)
		}
		defer func() { _ = res.Body.Close() }()
		if res.StatusCode >= 300 {
			body, _ := io.ReadAll(res.Body)
			return "", fmt.Errorf("yun139: refresh token: http %s: %s", res.Status, truncate(string(body), 200))
		}
		// A 200 with a non-zero ERRORCODE is still a rejection and must not be
		// treated as a successful refresh.
		if code := res.Header.Get("ERRORCODE"); code != "" && code != "0" {
			return "", fserrors.NoRetryError(fmt.Errorf("yun139: refresh token: ERRORCODE %s", code))
		}
		newAuth := strings.TrimPrefix(res.Header.Get("APP_AUTH"), "Basic ")
		newAuth = strings.TrimSpace(newAuth)
		if newAuth == "" {
			return "", errors.New("yun139: refresh token returned no APP_AUTH")
		}
		if decoded, err := base64.StdEncoding.DecodeString(newAuth); err == nil {
			s := string(decoded)
			if !strings.HasPrefix(s, "pc:") {
				newAuth = base64.StdEncoding.EncodeToString([]byte("pc:" + s))
			}
		}
		return newAuth, nil
	}

	// Attempt the refresh, with a single self-heal retry when the configured
	// userDomainId looks wrong. Each attempt reads the latest token under the
	// shared lock, then releases it before any network/discovery work.
	for attempt := 0; attempt < 2; attempt++ {
		ts.mu.Lock()
		auth := ts.auth
		if auth == "" {
			auth = f.auth
		}
		expiry, ok := tokenExpiryOf(auth)
		if !ok {
			ts.mu.Unlock()
			return nil // no expiry: assume valid
		}
		if time.Until(expiry) > minTokenLifetime {
			ts.mu.Unlock()
			return nil
		}
		// A concurrent rclone may have refreshed already; adopt it and stop.
		if v, ok := f.m.Get("authorization"); ok && v != "" && v != auth {
			ts.auth = v
			f.auth = v
			if acct, err := f.parseAuth(); err == nil {
				f.account = acct
			}
			fs.Debugf(f, "yun139: adopted refreshed token from config file")
			ts.mu.Unlock()
			return nil
		}
		decoded, err := base64.StdEncoding.DecodeString(auth)
		if err != nil {
			ts.mu.Unlock()
			return err
		}
		parts := strings.SplitN(string(decoded), ":", 3)
		if len(parts) < 3 {
			ts.mu.Unlock()
			return errors.New("yun139: invalid authorization format")
		}
		authToken := parts[2]
		ts.mu.Unlock()

		// Refresh outside the lock (doRefresh does not take ts.mu).
		newAuth, rerr := doRefresh(authToken, userID)
		if rerr == nil {
			// Commit the fresh token under the lock, writing back to every mapper.
			ts.mu.Lock()
			if newAuth != "" && newAuth != auth {
				ts.auth = newAuth
				f.auth = newAuth
				f.opt.Authorization = newAuth
				ts.writeBackLocked()
				fs.Debugf(f, "yun139: refreshed token in config file")
			}
			ts.mu.Unlock()
			return nil
		}
		// Refresh failed. If a manually-configured userDomainId is in play it is
		// probably wrong (server rejects it with 05010003): rediscover OUTSIDE
		// the lock and retry once so a stale config self-heals.
		if manual && attempt == 0 {
			if uid, derr := f.discoverUserDomainID(ctx); derr == nil && uid != "" && uid != userID {
				userID = uid
				continue
			}
		}
		return rerr
	}
	return nil
}

// ------------------------------------------------------------ headers -----

// pcHeaders returns the header set that the official 139 PC client
// sends. Captured live (2026-09-03, client 8.8.6.20260829):
//
//	Accept: */*
//	Authorization: Basic ***
//	Content-Type: application/json
//	x-yun-api-version: v1
//	x-yun-app-channel: 10200153
//	x-yun-client-info: ||11|8.8.6.20260829|PC|REVTS1RPUC1CUklONDBD|<DEV>|| Windows 11 (10.0.26200.8246)|1024X720|Q2hpbmVzZSAoU2ltcGxpZmllZCk=|||
//	x-yun-device-id: <DEV>
//	x-yun-market-source: 001
//	x-yun-module-type: 100
//	x-yun-op-type: 1
//	x-yun-svc-type: 1
//	Accept-Language: zh-CN,en,*
//	User-Agent: Mozilla/5.0
//
// Notably there are NO mcloud-* headers and no mcloud-sign: the PC
// client does not sign its requests the way the web client does.
func pcHeaders(auth, account, ts, randStr, sign, svcType string) map[string]string {
	deviceID := md5hex(account) + "-ENDIN"
	pcDeviceInfo := "||11|" + pcAppVersion + "|PC|REVTS1RPUC1CUklONDBD|" + deviceID +
		"|| Windows 11 (10.0.26200.8246)|1024X720|Q2hpbmVzZSAoU2ltcGxpZmllZCk=|||"
	return map[string]string{
		"Accept":              "*/*",
		"Authorization":       "Basic " + auth,
		"Content-Type":        "application/json",
		"x-yun-api-version":   "v1",
		"x-yun-app-channel":   pcAppChannel,
		"x-yun-client-info":   pcDeviceInfo,
		"x-yun-device-id":     deviceID,
		"x-yun-market-source": "001",
		"x-yun-module-type":   "100",
		"x-yun-op-type":       "1",
		"x-yun-svc-type":      "1",
		"Accept-Language":     "zh-CN,en,*",
		"User-Agent":          "Mozilla/5.0",
	}
}

const (
	pcAppVersion = "8.8.6.20260829"
	pcAppChannel = "10200153"
	// pcUserAgentFull is the Electron renderer UA, used by the PC
	// client's main API paths (create-folder, trash, list, family).
	pcUserAgentFull = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) mCloud/" + pcAppVersion + " Chrome/108.0.5359.215 Electron/22.3.0 Safari/537.36"
	// pcUserAgentShort is the native upload module's UA, used only on
	// /hcy/file/create with part upload bodies.
	pcUserAgentShort = "Mozilla/5.0"
)

// pcHeadersFull returns the header set the PC client's Electron main
// process sends for general API calls (folder create, trash, list,
// family). Captured live 2026-09-03, client 8.8.6.20260829.
func pcHeadersFull(auth, account, deviceID string) map[string]string {
	pcDeviceInfo := "||11|" + pcAppVersion + "|PC|REVTS1RPUC1CUklONDBD|" + deviceID +
		"|| Windows 11 (10.0.26200.8246)|1024X720|Q2hpbmVzZSAoU2ltcGxpZmllZCk=|||"
	return map[string]string{
		"APP_AUTH":            "Basic " + auth,
		"APP_CP":              "pc",
		"Accept":              "*/*",
		"Accept-Encoding":     "gzip, deflate, br",
		"Accept-Language":     "zh-CN",
		"Authorization":       "Basic " + auth,
		"CP_VERSION":          pcAppVersion,
		"Content-Type":        "application/json;charset=UTF-8",
		"Sec-Fetch-Dest":      "empty",
		"Sec-Fetch-Mode":      "cors",
		"Sec-Fetch-Site":      "cross-site",
		"User-Agent":          pcUserAgentFull,
		"sec-ch-ua":           `"Not?A_Brand";v="8", "Chromium";v="108"`,
		"sec-ch-ua-mobile":    "?0",
		"sec-ch-ua-platform":  `"Windows"`,
		"x-DeviceInfo":        pcDeviceInfo,
		"x-ExpRoute-Code":     "routeCode=" + account + ",type=2",
		"x-yun-api-version":   "v1",
		"x-yun-app-channel":   pcAppChannel,
		"x-yun-client-info":   pcDeviceInfo,
		"x-yun-device-id":     deviceID,
		"x-yun-market-source": "001",
		"x-yun-module-type":   "100",
		"x-yun-op-type":       "1",
		"x-yun-svc-type":      "1",
	}
}

// md5hex returns the lower-case hex MD5 of s. Used to derive a stable
// x-yun-device-id from the account name.
func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// srvPathCache maps a family dirID to its server-side path ("root:/...").
// Lives outside the Fs struct so the dircache code can copy Fs by value
// without tripping vet's lock-copy check.
type srvPathCache struct {
	mu sync.RWMutex
	m  map[string]string
}

func newSrvPathCache() *srvPathCache { return &srvPathCache{m: make(map[string]string)} }

func (c *srvPathCache) put(k, v string) {
	c.mu.Lock()
	c.m[k] = v
	c.mu.Unlock()
}

func (c *srvPathCache) get(k string) (string, bool) {
	c.mu.RLock()
	v, ok := c.m[k]
	c.mu.RUnlock()
	return v, ok
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ------------------------------------------------------------ request -----

// call issues one signed JSON request. personalURL is the full URL for
// PersonalNew endpoints; pathURL is the yun.139.com orchestration path.
// The body is marshalled with sorted keys (Go map iteration is random, and
// the signature must be computed over the exact bytes sent).
func (f *Fs) call(ctx context.Context, url string, body any, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	// The signature is computed over the compact body sent, but Go's map
	// ordering is random, so re-marshal through a sorted-key map to make
	// sign(body) == the bytes on the wire.

	ts := time.Now().Format("2006-01-02 15:04:05")
	randStr := random.String(16)
	sign := api.Sign(string(payload), ts, randStr)

	auth := f.accessToken()

	var headers map[string]string
	host := strings.SplitN(url, "/", 4)[2]
	if host == "group.yun.139.com" {
		// Family space: lean header set, short UA, x-yun-url-type:3 on
		// every call (captured 2026-09-03, family cloud).
		headers = pcHeaders(auth, f.account, ts, randStr, sign, f.svcType)
		headers["x-yun-url-type"] = "3"
		headers["Accept"] = "*/*"
		headers["x-DeviceInfo"] = f.deviceInfoHeader()
	} else {
		// Personal space always speaks as the official PC client
		// (captured 2026-09-03, client 8.8.6.20260829). The native
		// upload module sends a lean header set on /hcy/file/create
		// and /hcy/file/getDownloadUrl; every other call comes from
		// the Electron main process with the full browser-like set.
		p := strings.SplitN(url, "?", 2)[0]
		if (strings.HasSuffix(p, "/hcy/file/create") && strings.Contains(string(payload), `"partInfos"`)) ||
			strings.HasSuffix(p, "/hcy/file/getDownloadUrl") {
			headers = pcHeaders(auth, f.account, ts, randStr, sign, f.svcType)
			if strings.HasSuffix(p, "/hcy/file/getDownloadUrl") {
				headers["x-yun-url-type"] = "3"
			}
		} else {
			headers = pcHeadersFull(auth, f.account, md5hex(f.account)+"-ENDIN")
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	res, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("yun139: http %s: %s", res.Status, truncate(string(raw), 500))
	}

	// Decode the common envelope. Some endpoints return nested business
	// results that must be surfaced as errors.
	var env struct {
		Success bool   `json:"success"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("yun139: decode envelope: %w (body %s)", err, truncate(string(raw), 500))
	}
	// Family adapter endpoints (group.yun.139.com/hcy/family/adapter/*)
	// have no top-level success/code; their envelope is
	// {result:{resultCode,resultDesc}} (captured 2026-09-03).
	var famEnv struct {
		Result struct {
			ResultCode string `json:"resultCode"`
			ResultDesc string `json:"resultDesc"`
		} `json:"result"`
	}
	_ = json.Unmarshal(raw, &famEnv)
	if famEnv.Result.ResultCode != "" && famEnv.Result.ResultCode != "0" {
		return &apiError{Code: famEnv.Result.ResultCode, Message: famEnv.Result.ResultDesc}
	}
	if env.Code != "" && !env.Success {
		// Some endpoints reply success=false but carry a nested
		// resultCode=0 payload that actually succeeded (family move);
		// surface the message but keep it non-retryable.
		return &apiError{Code: env.Code, Message: env.Message}
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("yun139: decode response: %w (body %s)", err, truncate(string(raw), 500))
		}
	}
	return nil
}

// personalCall issues a PersonalNew request against the per-account host.
func (f *Fs) personalCall(ctx context.Context, pathname string, body any, out any) error {
	host, err := f.personalCloudHost(ctx)
	if err != nil {
		return err
	}
	return f.call(ctx, host+pathname, body, out)
}

// familyCall issues a family-cloud orchestration request.
func (f *Fs) familyCall(ctx context.Context, pathname string, body any, out any) error {
	return f.call(ctx, api.FamilyBaseURL+pathname, body, out)
}

// deviceInfoHeader returns the x-DeviceInfo value the official client
// posts on every /hcy/group/dynamic/* call. The string has the shape
//
//	"||<clientInfo>|<osInfo>|<screen>|<localeBase64>|||<osLocale>|"
//
// with the fields delimited by "||".
//
// The fifth clientInfo field is a 16-byte hex device id. The official
// client derives it from the install (persistent per account on a
// given machine); we derive it deterministically from the phone
// number so the same account on a re-run keeps the same value, while
// a different account gets a different one. The server uses it only
// for soft device-fingerprinting, so a stable per-account hash is
// good enough (live test: family uploads accept any plausible
// x-DeviceInfo value).
func (f *Fs) deviceInfoHeader() string {
	sum := md5.Sum([]byte(f.account))
	deviceID := strings.ToUpper(hex.EncodeToString(sum[:8])) // 16 hex chars
	return "||11|8.8.6.20260829|PC|REVTS1RPUC1CUklONDBD|" + deviceID + "|| Windows 11 (10.0.26200.8246)|1024X720|Q2hpbmVzZSAoU2ltcGxpZmllZCk=|||"
}

// familySeqNo returns a 32-char hex string the family-cloud create
// endpoint expects in seqNo. The server uses it as a deduplication
// key for batch operations; the value can be random.
func familySeqNo() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strings.Repeat("0", 32)
	}
	return hex.EncodeToString(b[:])
}

// personalCloudHost discovers and caches the per-account personal cloud host.
func (f *Fs) personalCloudHost(ctx context.Context) (string, error) {
	f.hostMu.Lock()
	defer f.hostMu.Unlock()
	if f.personalHost != "" {
		return f.personalHost, nil
	}
	req := api.QueryRoutePolicyReq{
		UserInfo: api.UserInfo{
			UserType:    1,
			AccountType: 1,
			AccountName: f.account,
		},
		ModAddrType: 1,
	}
	var resp api.QueryRoutePolicyResp
	if err := f.call(ctx, api.RoutePolicyURL, req, &resp); err != nil {
		return "", fmt.Errorf("yun139: query route policy: %w", err)
	}
	for _, p := range resp.Data.RoutePolicyList {
		if p.ModName == "personal" && p.HTTPSURL != "" {
			// The route policy host already ends with "/hcy"
			// (https://personal-kd-njs.yun.139.com/hcy); our
			// personalCall paths carry the /hcy prefix themselves,
			// so strip it here to avoid /hcy/hcy/*.
			f.personalHost = strings.TrimRight(strings.TrimSuffix(p.HTTPSURL, "/hcy"), "/")
			return f.personalHost, nil
		}
	}
	return "", errors.New("yun139: no personal cloud host in route policy response")
}

// queryFamilyCloud returns the user's family cloud list from
// POST /hcy/family/adapter/andAlbum/openApi/queryFamilyCloud.
//
// The official client (captured 2026-09-03) accepts an empty
// userDomainId input (the server falls back to the phone number from
// the auth header) and responds with the user's accountUserId plus
// every family cloud the account belongs to. We use this both to
// auto-discover userDomainId and to auto-pick a family_id when the
// user did not set one.
func (f *Fs) queryFamilyCloud(ctx context.Context) ([]api.FamilyCloud, string, error) {
	body := map[string]any{
		"commonAccountInfo": map[string]any{
			// Empty userDomainId is accepted; the server resolves
			// the account from the auth header. Sending the
			// accountType matches the official client.
			"accountType": 1,
		},
		"pageInfo": map[string]int{"pageNum": 1, "pageSize": 100},
	}
	var resp struct {
		FamilyCloudList []api.FamilyCloud `json:"familyCloudList"`
	}
	if err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/queryFamilyCloud", body, &resp); err != nil {
		return nil, "", fmt.Errorf("yun139: queryFamilyCloud: %w", err)
	}
	accountUserID := ""
	for _, fc := range resp.FamilyCloudList {
		if fc.CommonAccountInfo.AccountUserID != "" {
			accountUserID = fc.CommonAccountInfo.AccountUserID
			break
		}
	}
	return resp.FamilyCloudList, accountUserID, nil
}

// familyRoot discovers the server-side root catalog ID of the family cloud.
//
// The official client reads the root from the first entry of the
// queryContentListV3 response (`data.path` is "root:/<id>"). On
// first call, we also auto-discover the family_id from
// queryFamilyCloud when the user did not set it.
func (f *Fs) familyRoot(ctx context.Context) (string, error) {
	f.familyRootMu.Lock()
	defer f.familyRootMu.Unlock()
	if f.familyRootID != "" {
		return f.familyRootID, nil
	}
	if err := f.resolveFamilyID(ctx); err != nil {
		return "", err
	}
	body := map[string]any{
		"catalogSortType": 0,
		"catalogType":     3,
		"cloudID":         f.opt.FamilyID,
		"cloudType":       1,
		"contentSortType": 0,
		"sortDirection":   1,
		"path":            "",
		"commonAccountInfo": map[string]any{
			"userDomainId": f.userDomainID,
			"accountType":  1,
		},
		"pageInfo": map[string]int{"pageNum": 1, "pageSize": 1},
	}
	var resp struct {
		Path             string `json:"path"`
		CloudCatalogList []struct {
			CatalogID string `json:"catalogID"`
		} `json:"cloudCatalogList"`
	}
	if err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/queryContentListV3", body, &resp); err != nil {
		return "", err
	}
	// The root path arrives as "root:/<id>"; strip the prefix.
	p := strings.TrimSpace(resp.Path)
	p = strings.TrimPrefix(p, "root:/")
	p = strings.TrimPrefix(p, "root:")
	if p == "" {
		// Fall back to the first catalog's id.
		if len(resp.CloudCatalogList) > 0 && resp.CloudCatalogList[0].CatalogID != "" {
			f.familyRootID = resp.CloudCatalogList[0].CatalogID
			return f.familyRootID, nil
		}
		return "", errors.New("yun139: no path in family root response")
	}
	f.familyRootID = p
	return f.familyRootID, nil
}

// resolveFamilyID ensures f.opt.FamilyID is set, auto-discovering it from
// queryFamilyCloud when the user did not configure family_id.
//
// It is called once at NewFs time (freezing FamilyID before any operation
// can race on it) and again inside familyRoot, which keeps the lock held
// so the write can never race with a reader.
func (f *Fs) resolveFamilyID(ctx context.Context) error {
	if f.opt.FamilyID != "" {
		return nil
	}
	clouds, _, err := f.queryFamilyCloud(ctx)
	if err != nil {
		return err
	}
	switch len(clouds) {
	case 0:
		return errors.New("yun139: account has no family cloud; create one in the 139 client first")
	case 1:
		// Single family cloud: pick it for the user.
		f.opt.FamilyID = clouds[0].CloudID
	default:
		// Multiple family clouds: refuse to guess. List every
		// available cloudID and cloudName so the user can set
		// --yun139-family-id (or family_id in the conf) to the
		// one they want.
		ids := make([]string, 0, len(clouds))
		for _, c := range clouds {
			ids = append(ids, fmt.Sprintf("%s (%s)", c.CloudID, c.CloudName))
		}
		return fmt.Errorf("yun139: account belongs to %d family clouds: %s; set --yun139-family-id to the one you want", len(clouds), strings.Join(ids, ", "))
	}
	return nil
}

// ------------------------------------------------------------ NewFs -------

// parsePath parses a remote path
//
// path.Clean normalises ".", ".." and duplicate slashes so
// "a/../b" does not create a literal full-width dot directory on the
// server (audit BUG-5: the encoder would otherwise turn "." into "．").
func parsePath(p string) (root string) {
	p = strings.ReplaceAll(p, "\\", "/")
	clean := path.Clean(p)
	// path.Clean keeps a leading ".." (e.g. "../b"); strip any leading
	// ".." segments so a root of "../b" resolves to "b".
	for strings.HasPrefix(clean, "../") {
		clean = strings.TrimPrefix(clean, "../")
	}
	clean = strings.TrimPrefix(clean, "..")
	if clean == "." || clean == "/" || clean == "" {
		return ""
	}
	return strings.Trim(clean, "/")
}

// newFs constructs an Fs from the path
func newFs(ctx context.Context, name, root string, m configmap.Mapper) (*Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	if opt.Authorization == "" {
		return nil, errors.New("yun139: authorization is required")
	}
	switch opt.Space {
	case spacePersonal, spaceFamily:
	default:
		return nil, fmt.Errorf("yun139: space must be %q or %q, got %q", spacePersonal, spaceFamily, opt.Space)
	}
	// (family_id is auto-discovered in NewFs via queryFamilyCloud when
	// not set; personal space needs no extra config.)
	// Live-tested part sizes (audit 2026-09-04): the server accepts
	// 5 MiB and 10 MiB parts; 1/2/4 MiB all fail with
	// '04000002: 请求参数不合法(00010002)'. Reject anything smaller
	// than 5 MiB outright so misconfiguration fails fast instead of
	// at the first create call.
	if opt.PartSize < 5*1024*1024 {
		return nil, fmt.Errorf("yun139: part_size must be at least 5Mi (the server rejects smaller parts), got %s", opt.PartSize)
	}
	if opt.PartSize%64 != 0 {
		return nil, fmt.Errorf("yun139: part_size must be a multiple of 64 bytes, got %d", opt.PartSize)
	}
	if opt.UploadConcurrency < 1 {
		return nil, fmt.Errorf("yun139: upload_concurrency must be at least 1, got %d", opt.UploadConcurrency)
	}

	f := &Fs{
		name: name,
		root: parsePath(root),
		opt:  *opt,
		m:    m,
		httpClient: fshttp.NewClientCustom(ctx, func(t *http.Transport) {
			if opt.DisableHTTP2 {
				t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
			}
		}),
		space:   opt.Space,
		svcType: svcTypePersonal,
	}
	if opt.Space == spaceFamily {
		f.svcType = svcTypeFamily
	}
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))

	f.auth = opt.Authorization
	account, err := f.parseAuth()
	if err != nil {
		return nil, err
	}
	f.account = account
	f.userDomainID = opt.UserDomainID

	f.features = (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: true,
		SlowHash:                true,
		ServerSideAcrossConfigs: false,
	}).Fill(ctx, f)
	return f, nil
}

// NewFs constructs an Fs from the path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f, err := newFs(ctx, name, root, m)
	if err != nil {
		return nil, err
	}
	// Mutexes are pointers (so the dircache tempF copy stays lock-safe);
	// allocate them once per Fs.
	if f.tokMu == nil {
		f.tokMu = &sync.Mutex{}
	}
	if f.hostMu == nil {
		f.hostMu = &sync.Mutex{}
	}
	if f.familyRootMu == nil {
		f.familyRootMu = &sync.Mutex{}
	}
	// Validate the token and refresh if it is close to expiry.
	if err := f.refreshToken(ctx); err != nil {
		return nil, err
	}

	// Probe the account's member tier once (fail-open) to learn the single-file
	// upload limit, so oversized files are skipped up front instead of wasting a
	// full multi-part upload that the server rejects with 04010319.
	f.probeAndCacheMemberLevel(ctx)

	// Auto-discover the userDomainId (the <digits> id batchCopy and
	// quota need) from queryFamilyCloud. The official client caches
	// this from login; we learn it in one extra request. An explicit
	// --yun139-user-domain-id still wins.
	if f.userDomainID == "" {
		if _, accountUserID, err := f.queryFamilyCloud(ctx); err == nil && accountUserID != "" {
			f.userDomainID = accountUserID
			f.opt.UserDomainID = accountUserID
			f.m.Set("user_domain_id", accountUserID)
		}
		// A failure here is not fatal: personal-space operations
		// that do not need the domain id keep working (the quota
		// call falls back to the phone number).
	}

	rootID := f.opt.RootFolderID
	if f.space == spaceFamily {
		// Freeze family_id once, before any operation can race on it.
		// familyRoot also resolves it, but only when root_folder_id is
		// empty; resolving here covers the root_folder_id-set case too.
		if err := f.resolveFamilyID(ctx); err != nil {
			return nil, err
		}
	}
	if rootID == "" {
		if f.space == spaceFamily {
			rootID, err = f.familyRoot(ctx)
			if err != nil {
				return nil, err
			}
		} else {
			rootID = defaultRootID
		}
	}
	f.dirCache = dircache.New(f.root, rootID, f)
	f.srvPathOf = newSrvPathCache()

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			// Directory not found: assume it is a file
			newRoot, remote := dircache.SplitPath(f.root)
			tempF := *f // shares mutexes by pointer; dircache only reads
			// the root-relative fields, never the locks (see Fs struct).
			tempF.dirCache = dircache.New(newRoot, rootID, &tempF)
			tempF.root = newRoot
			// PathOfID shares f.pathMu via tempF copy (lock is by address
			// of the original Fs, which is what callers use afterwards).
			if err2 := tempF.dirCache.FindRoot(ctx, false); err2 != nil {
				// No root so return old f
				return f, nil
			}
			if _, err2 := tempF.NewObject(ctx, remote); err2 != nil {
				if err2 == fs.ErrorObjectNotFound {
					return f, nil
				}
				return nil, err2
			}
			f.features.Fill(ctx, &tempF)
			f.dirCache = tempF.dirCache
			f.root = tempF.root
			return f, fs.ErrorIsFile
		}
		return nil, err
	}
	return f, nil
}

// ------------------------------------------------------------ Fs basics ---

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string { return f.name }

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string { return f.root }

// String converts this Fs to a string
func (f *Fs) String() string {
	if f.space == spaceFamily {
		return fmt.Sprintf("yun139 root '%s' (family %s)", f.root, f.opt.FamilyID)
	}
	return fmt.Sprintf("yun139 root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features { return f.features }

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration { return time.Second }

// Hashes returns the supported hash sets.
// The personal-space listing carries a server-computed SHA-256
// (contentHash) for every file, so SHA-256 is the native hash. The
// family listing exposes no hash, so the family space advertises no
// hashes and rclone falls back to size+modtime comparison (check
// reports "no hashes supported" instead of aborting mid-way).
func (f *Fs) Hashes() hash.Set {
	if f.space == spaceFamily {
		return hash.Set(0)
	}
	return hash.Set(hash.SHA256)
}

// DirCacheFlush resets the directory cache
func (f *Fs) DirCacheFlush() { f.dirCache.ResetRoot() }

// ------------------------------------------------------------ listing -----

// listEntry is one entry of a directory listing, normalised across spaces.
type listEntry struct {
	id      string
	name    string
	size    int64
	isDir   bool
	modTime time.Time
	srvPath string // family/group server path of the parent dir
	sha256  string // personal space: contentHash from the listing (empty if absent)
}

// listPersonal lists the entries of a personal-cloud directory.
func (f *Fs) listPersonal(ctx context.Context, dirID string, fn func(listEntry) bool) error {
	cursor := ""
	for page := 0; page < maxListPages; page++ {
		body := map[string]any{
			"imageThumbnailStyleList": []string{"Small", "Large"},
			"orderBy":                 "updated_at",
			"orderDirection":          "DESC",
			"pageInfo": map[string]any{
				"pageCursor": cursor,
				"pageSize":   listPageSize,
			},
			"parentFileId": dirID,
		}
		var resp api.PersonalListResp
		err := f.pacer.Call(func() (bool, error) {
			err := f.personalCall(ctx, "/hcy/file/list", body, &resp)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return err
		}
		if len(resp.Data.Items) == 0 && cursor == "" {
			return nil
		}
		n := 0
		for i := range resp.Data.Items {
			item := &resp.Data.Items[i]
			if item.FileID == "" {
				continue
			}
			n++
			modTime, _ := api.ParseRFC3339(item.UpdatedAt)
			if modTime.IsZero() {
				modTime, _ = api.ParseRFC3339(item.CreatedAt)
			}
			entry := listEntry{
				id:      item.FileID,
				name:    f.opt.Enc.ToStandardName(item.Name),
				size:    item.Size,
				isDir:   item.Type == "folder",
				modTime: modTime,
				sha256:  item.ContentHash,
			}
			if fn(entry) {
				return nil
			}
		}
		cursor = resp.Data.NextPageCursor
		if cursor == "" || n < listPageSize {
			return nil
		}
	}
	return fmt.Errorf("yun139: personal listing of %s did not terminate after %d pages", dirID, maxListPages)
}

// listFamily lists the entries of a family-cloud directory.
func (f *Fs) listFamily(ctx context.Context, dirID string, fn func(listEntry) bool) error {
	rootID, err := f.familyRoot(ctx)
	if err != nil {
		return err
	}
	pageNum := 1
	for page := 0; page < maxListPages; page++ {
		body := map[string]any{
			"catalogSortType": 0,
			"catalogType":     3,
			"cloudID":         f.opt.FamilyID,
			"cloudType":       1,
			"contentSortType": 0,
			"sortDirection":   1,
			"path":            "",
			"commonAccountInfo": map[string]any{
				"userDomainId": f.userDomainID,
				"accountType":  1,
			},
			"pageInfo": map[string]int{"pageNum": pageNum, "pageSize": listPageSize},
		}
		if dirID != rootID {
			body["catalogID"] = dirID
			body["path"] = f.familySrvPath(dirID)
		}
		var resp struct {
			Path             string `json:"path"`
			TotalCount       int    `json:"totalCount"`
			CloudCatalogList []struct {
				CatalogID      string `json:"catalogID"`
				CatalogName    string `json:"catalogName"`
				LastUpdateTime string `json:"lastUpdateTime"`
			} `json:"cloudCatalogList"`
			CloudContentList []struct {
				ContentID      string `json:"contentID"`
				ContentName    string `json:"contentName"`
				ContentSize    int64  `json:"contentSize"`
				LastUpdateTime string `json:"lastUpdateTime"`
			} `json:"cloudContentList"`
		}
		err := f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/queryContentListV3", body, &resp)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return err
		}
		dirPath := resp.Path
		f.srvPathOf.put(dirID, dirPath)
		if dirPath == "" {
			dirPath = "root:/"
			f.srvPathOf.put(dirID, dirPath)
		}
		n := 0
		for i := range resp.CloudCatalogList {
			c := &resp.CloudCatalogList[i]
			if c.CatalogID == "" {
				continue
			}
			n++
			modTime, _ := api.ParseTime(c.LastUpdateTime)
			childPath := path.Join(dirPath, c.CatalogID)
			f.srvPathOf.put(c.CatalogID, childPath)
			entry := listEntry{
				id:      c.CatalogID,
				name:    f.opt.Enc.ToStandardName(c.CatalogName),
				isDir:   true,
				modTime: modTime,
				srvPath: childPath,
			}
			if fn(entry) {
				return nil
			}
		}
		for i := range resp.CloudContentList {
			c := &resp.CloudContentList[i]
			if c.ContentID == "" {
				continue
			}
			n++
			modTime, _ := api.ParseTime(c.LastUpdateTime)
			entry := listEntry{
				id:      c.ContentID,
				name:    f.opt.Enc.ToStandardName(c.ContentName),
				size:    c.ContentSize,
				isDir:   false,
				modTime: modTime,
				srvPath: dirPath,
			}
			if fn(entry) {
				return nil
			}
		}
		// totalCount counts the entries of this dir; stop when we have them all.
		if resp.TotalCount > 0 && n >= resp.TotalCount {
			return nil
		}
		if n < listPageSize {
			return nil
		}
		pageNum++
	}
	return fmt.Errorf("yun139: family listing of %s did not terminate after %d pages", dirID, maxListPages)
}

// listAll lists the entries of dirID in the current space.
func (f *Fs) listAll(ctx context.Context, dirID string, fn func(listEntry) bool) error {
	if f.space == spaceFamily {
		return f.listFamily(ctx, dirID, fn)
	}
	return f.listPersonal(ctx, dirID, fn)
}

// itemToDirEntry converts a listing entry into an fs.DirEntry.
func (f *Fs) itemToDirEntry(ctx context.Context, remote string, e listEntry) (fs.DirEntry, error) {
	if e.isDir {
		// cache the directory ID for later lookups
		f.dirCache.Put(remote, e.id)
		return fs.NewDir(remote, e.modTime).SetID(e.id), nil
	}
	return f.newObjectWithInfo(ctx, remote, e)
}

// List the objects and directories in dir into entries.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	var iErr error
	err = f.listAll(ctx, dirID, func(e listEntry) bool {
		entry, err := f.itemToDirEntry(ctx, path.Join(dir, e.name), e)
		if err != nil {
			iErr = err
			return true
		}
		entries = append(entries, entry)
		return false
	})
	if err != nil {
		return nil, err
	}
	if iErr != nil {
		return nil, iErr
	}
	return entries, nil
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	found = false
	err = f.listAll(ctx, pathID, func(e listEntry) bool {
		if e.isDir && strings.EqualFold(e.name, leaf) {
			pathIDOut = e.id
			found = true
			return true
		}
		return false
	})
	return
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	if err := validateName(leaf); err != nil {
		return "", err
	}
	leaf = f.opt.Enc.FromStandardName(leaf)
	if f.space == spaceFamily {
		srvPath := ""
		// The dircache remembers the root id; only fall back to
		// looking up a server-side path when the parent is NOT the
		// root. We use the dirCache root rather than the (possibly
		// empty) opt.RootFolderID.
		rootID, errRoot := f.familyRoot(ctx)
		if errRoot != nil {
			return "", errRoot
		}
		if pathID != "" && pathID != rootID {
			srvPath = f.familySrvPath(pathID)
		}
		// POST .../createCloudDocV2 (captured 2026-09-03) accepts
		// {catalogType, cloudID, docLibName, manualRename, path,
		//  commonAccountInfo} and returns {catalogInfo.catalogID}.
		body := map[string]any{
			"catalogType":  3,
			"cloudID":      f.opt.FamilyID,
			"docLibName":   leaf,
			"manualRename": 0,
			"path":         srvPath,
			"commonAccountInfo": map[string]any{
				"userDomainId": f.userDomainID,
				"accountType":  1,
			},
		}
		var resp struct {
			Result struct {
				ResultCode string `json:"resultCode"`
				ResultDesc string `json:"resultDesc"`
			} `json:"result"`
			CatalogInfo struct {
				CatalogID string `json:"catalogID"`
			} `json:"catalogInfo"`
		}
		if err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/createCloudDocV2", body, &resp); err != nil {
			return "", err
		}
		if resp.Result.ResultCode != "0" {
			return "", &apiError{Code: resp.Result.ResultCode, Message: resp.Result.ResultDesc}
		}
		if resp.CatalogInfo.CatalogID != "" {
			return resp.CatalogInfo.CatalogID, nil
		}
		// Fall back to listing the parent.
		return f.findDirID(ctx, pathID, leaf)
	}
	// The official client posts exactly {"name","type":"folder",
	// "parentFileId"} - no description, no fileRenameMode (captured
	// 2026-09-03).
	body := api.PersonalCreateFolderReq{
		ParentFileID: pathID,
		Name:         leaf,
		Type:         "folder",
	}
	var resp struct {
		BaseResp api.BaseResp
		Data     struct {
			FileID string `json:"fileId"`
		} `json:"data"`
	}
	if err := f.personalCall(ctx, "/hcy/file/create", body, &resp); err != nil {
		return "", err
	}
	if resp.Data.FileID == "" {
		// force_rename may have renamed; find by listing.
		return f.findDirID(ctx, pathID, leaf)
	}
	return resp.Data.FileID, nil
}

// findDirID returns the id of the directory named leaf inside pathID.
func (f *Fs) findDirID(ctx context.Context, pathID, leaf string) (string, error) {
	id, found, err := f.FindLeaf(ctx, pathID, leaf)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fs.ErrorDirNotFound
	}
	return id, nil
}

// familySrvPath resolves the server-side path ("root:/x/y") of a family dirID.
// The path is learned incrementally as listFamily walks the tree; the lookup
// will fail until every parent directory has been listed at least once, so
// the caller (Move / Rename) must list the parent first.
func (f *Fs) familySrvPath(dirID string) string {
	rootID, err := f.familyRoot(context.Background())
	if err != nil || rootID == "" {
		return ""
	}
	if dirID == rootID {
		return "root:/"
	}
	p, ok := f.srvPathOf.get(dirID) // cached from listFamily
	if ok {
		return p
	}
	// Fall back: ensure the parent is listed so we can learn the child's
	// path. We don't know the parent from id alone, so return empty and let
	// the caller retry after a List of the parent directory.
	return ""
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// Rmdir deletes the container
//
// Only empty directories can be deleted.
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	entries, err := f.List(ctx, dir)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	if f.space == spaceFamily {
		// Deleting the family root is not allowed.
		rootID, err := f.familyRoot(ctx)
		if err != nil {
			return err
		}
		if dirID == rootID {
			return errors.New("yun139: cannot remove the family root")
		}
		// Sub-folder deletion goes through the batch task pipeline
		// (createBatchOprTaskV2 with taskType=2 and the catalogList
		// field set to the dir id).
		return f.familyDeleteID(ctx, dirID, f.familySrvPath(dirID))
	}
	if err := f.deleteObject(ctx, dirID, "", false); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// ------------------------------------------------------------ objects ----

// newObjectWithInfo returns an Object from a listing entry.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, e listEntry) (fs.Object, error) {
	o := &Object{
		fs:         f,
		remote:     remote,
		id:         e.id,
		size:       e.size,
		modTime:    e.modTime,
		serverPath: e.srvPath,
		sha256:     e.sha256,
		hashMu:     &sync.Mutex{},
		urlMu:      &sync.Mutex{},
	}
	return o, nil
}

// NewObject finds the Object at remote. If it can't be found it returns
// fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	var found *listEntry
	err = f.listAll(ctx, dirID, func(e listEntry) bool {
		if strings.EqualFold(e.name, leaf) {
			if e.isDir {
				found = nil
			} else {
				found = &e
			}
			return true
		}
		return false
	})
	if err != nil {
		return nil, err
	}
	if found == nil {
		// Not found or a directory
		return nil, fs.ErrorObjectNotFound
	}
	return f.newObjectWithInfo(ctx, remote, *found)
}

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info { return o.fs }

// Remote returns the remote path
func (o *Object) Remote() string { return o.remote }

// String converts this Object to a string
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Size returns the size of the object
func (o *Object) Size() int64 { return o.size }

// Storable says whether this object can be stored
func (o *Object) Storable() bool { return true }

// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }

// SetModTime sets the modification time of the object.
//
// 139 has no API to change mtime after upload (and the personal space
// stamps the server clock on every upload, ignoring any client mtime -
// verified live: 2001-02-03 mtime on a file came back as upload time).
// wopan sidesteps this by remembering the upload-time mtime in
// `shootingTime` and returning it from ModTime; on 139 the server
// does not preserve that value, so we cannot mirror the trick.
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Hash returns the selected checksum of the file.
//
// SHA-256 (native): the server computes it for every uploaded file and
// returns it in the listing (contentHash). We cache it from the
// listing and from the upload pipeline (we compute the same digest to
// drive the upload), so no extra request is needed.
//
// Older note kept for history: the CDN pre-signed URLs reject HEAD
// requests with 403 (the signature only covers GET), so the previous
// ETag-based MD5 scheme never returned a hash in practice.
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.SHA256 {
		return "", hash.ErrUnsupported
	}
	o.hashMu.Lock()
	cached := o.sha256
	o.hashMu.Unlock()
	if cached != "" {
		return cached, nil
	}
	// Not cached (e.g. the object was built from a listing that predates
	// the field). Refetch the entry once.
	if o.fs.space == spacePersonal {
		leaf, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, false)
		if err == nil {
			var found string
			err = o.fs.listAll(ctx, dirID, func(e listEntry) bool {
				if !e.isDir && strings.EqualFold(e.name, path.Base(leaf)) {
					found = e.sha256
					return true
				}
				return false
			})
			if err == nil && found != "" {
				o.hashMu.Lock()
				o.sha256 = found
				o.hashMu.Unlock()
				return found, nil
			}
		}
	}
	return "", nil
}

// ------------------------------------------------------------ helpers ----

// getURL performs a GET for a download URL.
func (f *Fs) getURL(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	// The CDN download (ykj-eos-*.eos.yun.139.com) accepts the short UA
	// the official client sends (captured 2026-09-03). Avoid the long
	// Chrome UA - no reason to look like a browser.
	req.Header.Set("User-Agent", "Mozilla/5.0")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := f.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ------------------------------------------------------------ server-side ops ----

// Purge deletes the directory at dir and all of its contents.
//
// Personal space: a single batchDelete of the directory id is enough (the
// 139 server recurses). Family space: the orchestration API does not support
// deleting a non-empty directory, so we walk it and delete children by id
// before the directory itself.
func (f *Fs) Purge(ctx context.Context, dir string) error {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	if path.Join(f.root, dir) == "" {
		return errors.New("yun139: cannot purge root")
	}
	if f.space == spaceFamily {
		// Walk and delete every child first.
		err = f.listAll(ctx, dirID, func(e listEntry) bool {
			if e.isDir {
				_ = f.purgeFamilyDir(ctx, e.id)
			} else {
				_ = f.deleteObject(ctx, e.id, e.srvPath, true)
			}
			return false
		})
		if err != nil {
			return err
		}
		if err := f.deleteObject(ctx, dirID, "", true); err != nil {
			return err
		}
		f.dirCache.FlushDir(dir)
		return nil
	}
	if err := f.deleteObject(ctx, dirID, "", false); err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// purgeFamilyDir removes a non-empty family directory by walking its children
// first. There is no server-side recursive delete in the family API.
func (f *Fs) purgeFamilyDir(ctx context.Context, id string) error {
	err := f.listAll(ctx, id, func(e listEntry) bool {
		if e.isDir {
			_ = f.purgeFamilyDir(ctx, e.id)
		} else {
			_ = f.deleteObject(ctx, e.id, e.srvPath, true)
		}
		return false
	})
	if err != nil {
		return err
	}
	return f.deleteObject(ctx, id, "", true)
}

// About returns quota information for the personal cloud.
//
// POST user-njs.yun.139.com/user/disk/quota/detail with
// {"userDomainId": ...} returns diskSize/freeDiskSize in MiB units
// (captured 2026-09-03: diskSize 701440 = 685 MiB... actually the
// captured values 701440/700028 look like MiB for a 685 GB drive,
// so scale by 1024*1024 when reporting).
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	if f.space != spacePersonal {
		return nil, errors.New("yun139: about is only supported for the personal space")
	}
	userID := f.userDomainID
	if userID == "" {
		userID = f.account
	}
	var resp struct {
		api.BaseResp
		Data struct {
			FreeDiskSize int64 `json:"freeDiskSize"`
			DiskSize     int64 `json:"diskSize"`
		} `json:"data"`
	}
	err := f.pacer.Call(func() (bool, error) {
		err := f.call(ctx, "https://user-njs.yun.139.com/user/disk/quota/detail", map[string]any{"userDomainId": userID}, &resp)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return nil, err
	}
	if !resp.Success {
		return nil, &apiError{Code: resp.Code, Message: resp.Message}
	}
	// The server reports MiB units (701440 MiB ≈ 685 GiB); rclone's
	// Usage is in bytes.
	return quotaToUsage(resp.Data.DiskSize, resp.Data.FreeDiskSize), nil
}

// quotaToUsage converts the server's MiB disk/free values to byte-based
// fs.Usage. A captured response (2026-09-03) reported
// {"diskSize":701440,"freeDiskSize":367136} for a ~685 GiB account.
func quotaToUsage(diskMiB, freeMiB int64) *fs.Usage {
	total := diskMiB * 1024 * 1024
	free := freeMiB * 1024 * 1024
	return &fs.Usage{
		Total: &total,
		Free:  &free,
	}
}

// ============================================================================
// Upload pipeline (Put / Update / OpenChunkWriter / multipart)
// ============================================================================

// uploadResult is the outcome of one Put/Update upload.
type uploadResult struct {
	fileID   string
	fileName string // server-side name after auto_rename
	hashHex  string
}

// partPlan describes the byte range of one part of an upload.
type partPlan struct {
	index    int64 // 1-based part number
	offset   int64 // byte offset of the part start
	partSize int64 // bytes in this part
}

// planParts returns the part boundaries for a file of size bytes with the
// given part size. Mirrors the SDK's floor-division rule (last part absorbs
// the remainder), with a minimum of one part.
func planParts(size, partSize int64) []partPlan {
	if partSize <= 0 {
		partSize = personalPartSize
	}
	// total = ceil(size / partSize); minimum 1 part even for empty files.
	total := size / partSize
	if size%partSize != 0 {
		total++
	}
	if total <= 0 {
		total = 1
	}
	plans := make([]partPlan, 0, total)
	var sent int64
	for i := int64(1); i <= total; i++ {
		n := partSize
		if i == total {
			n = size - sent
			if n < 0 {
				n = 0
			}
		}
		plans = append(plans, partPlan{index: i, offset: sent, partSize: n})
		sent += n
	}
	return plans
}

// Put in to the remote path with the modTime given of the given size.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	size := src.Size()
	if size < 0 {
		return nil, errors.New("yun139: can't upload files of unknown size")
	}
	if size == 0 {
		return nil, fs.ErrorCantUploadEmptyFiles
	}
	if limit := f.maxFileSize(); limit > 0 && size > limit {
		fs.Logf(f, "SKIP %s: %d bytes exceeds the member single-file upload limit of %d bytes (tier %q); skipping to avoid a wasted multi-part upload",
			src.Remote(), size, limit, f.memberLevelName)
		return nil, errTooLarge(src.Remote(), size, limit, f.memberLevelName)
	}
	leaf, dirID, err := f.dirCache.FindPath(ctx, src.Remote(), true)
	if err != nil {
		return nil, err
	}
	if err := f.validateName(src.Remote()); err != nil {
		return nil, err
	}
	leaf = f.opt.Enc.FromStandardName(leaf)
	res, err := f.uploadFile(ctx, in, dirID, leaf, size)
	if err != nil {
		return nil, err
	}
	o := &Object{
		fs:      f,
		remote:  src.Remote(),
		id:      res.fileID,
		size:    size,
		modTime: src.ModTime(ctx),
		sha256:  res.hashHex,
		hashMu:  &sync.Mutex{},
		urlMu:   &sync.Mutex{},
	}
	return o, nil
}

// uploadFile is the single upload pipeline used by both Put and Update.
//
// The input is streamed once through a temp file while SHA-256 is
// computed, then /hcy/file/create + parallel part PUTs are driven from
// random-access SectionReader handles. This caps memory use to one
// part (chunkSize) regardless of the file size, and the single pass
// doubles as the midstate scan for parallelUpload.
func (f *Fs) uploadFile(ctx context.Context, in io.Reader, dirID, leaf string, size int64) (*uploadResult, error) {
	if size <= 0 {
		return nil, errors.New("yun139: size must be > 0")
	}
	// Small files: keep the reader in memory, hash it once.
	if size <= int64(5*1024*1024) {
		data := make([]byte, size)
		h := sha256.New()
		if _, err := io.ReadFull(io.TeeReader(in, h), data); err != nil {
			return nil, fmt.Errorf("yun139: read: %w", err)
		}
		return f.uploadFromRandom(ctx, bytes.NewReader(data), dirID, leaf, size, hex.EncodeToString(h.Sum(nil)))
	}
	// Streaming path for large files: stream the data through a temp file
	// while hashing it, then drive /hcy/file/create + parallel part PUTs
	// from random-access SectionReader handles. This caps memory use to
	// one part (chunkSize) regardless of the file size.
	tmp, err := os.CreateTemp("", "yun139-upload-")
	if err != nil {
		return nil, fmt.Errorf("yun139: temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), in)
	if err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("yun139: hash: %w", err)
	}
	if n != size {
		_ = tmp.Close()
		return nil, fmt.Errorf("yun139: short read: got %d bytes, want %d", n, size)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	res, err := f.uploadFromRandom(ctx, tmp, dirID, leaf, size, hex.EncodeToString(h.Sum(nil)))
	_ = tmp.Close()
	return res, err
}

// buildCreateBody assembles the /hcy/file/create payload exactly as the
// official PC client posts it (captured 2026-09-03, v8.8.6.20260829):
// contentHash + contentHashAlgorithm:SHA256 + contentType + parallelUpload:true
// + partInfos[:100] + fileRenameMode:auto_rename + localCreatedAt/localUpdatedAt
// in RFC3339 millisecond UTC.
func buildCreateBody(dirID, leaf string, size int64, hashHex string, partInfos []api.PartInfo, regionCode string) api.PersonalCreateReq {
	body := api.PersonalCreateReq{
		CommonUpload: api.CommonUpload{
			ParentID: dirID,
			Name:     leaf,
			Size:     size,
			Type:     "file",
		},
		FileRenameMode: "auto_rename",
	}
	body.ContentHash = hashHex
	body.ContentHashAlgorithm = "SHA256"
	body.ContentType = "application/octet-stream"
	body.ParallelUpload = true
	body.PartInfos = partInfos
	// userRegion:官方 PC 客户端随 create 发送(如 531:543),供服务端就近调度上传 CDN。
	// personal 默认不发送(历史行为);设置 region_code 后补上,可显著影响上传节点选择与吞吐。
	if regionCode != "" {
		if pc, ci, ok := strings.Cut(regionCode, ":"); ok && pc != "" && ci != "" {
			body.UserRegion = &api.Region{CityCode: ci, ProvinceCode: pc}
		}
	}
	// The official client sends localCreatedAt/localUpdatedAt as
	// RFC3339 UTC with milliseconds (captured 2026-09-03,
	// v8.8.6.20260829 - e.g. "2026-09-03T08:06:36.784Z"). The server
	// REJECTS empty strings with '04000002: 本地创建时间格式不符合标准',
	// but ignores the actual value in favour of its own clock. So we
	// send a valid-format stamp from time.Now() and let the server
	// overwrite the read-back value (mirrors official client + keeps
	// us within the format spec).
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	body.LocalCreatedAt = now
	body.LocalUpdatedAt = now
	if len(body.PartInfos) > maxPartsPerRequest {
		body.PartInfos = body.PartInfos[:maxPartsPerRequest]
	}
	return body
}

// uploadFromRandom drives a multi-part upload for data already on disk in
// the given *os.File (positioned at offset 0). The same file is used both
// for hashing (already done) and for reading each part on demand via
// io.SectionReader, so memory use is bounded by chunkSize.
func (f *Fs) uploadFromRandom(ctx context.Context, freader io.ReaderAt, dirID, leaf string, size int64, hashHex string) (*uploadResult, error) {
	if limit := f.maxFileSize(); limit > 0 && size > limit {
		fs.Logf(f, "SKIP %s: %d bytes exceeds the member single-file upload limit of %d bytes (tier %q); skipping to avoid a wasted multi-part upload",
			leaf, size, limit, f.memberLevelName)
		return nil, errTooLarge(leaf, size, limit, f.memberLevelName)
	}
	chunkSize := int64(f.opt.PartSize)
	if chunkSize <= 0 {
		chunkSize = api.DefaultChunkSize
	}
	parts := planParts(size, chunkSize)
	// The official PC client (captured 2026-09-03) sends
	// parallelUpload:true with a parallelHashCtx per part: the SHA-256
	// midstate (8 H registers) after all bytes before that part. The
	// server signs each part's context into its upload URL
	// (X-Amz-Iteration-Hash-Ctx) and accepts out-of-order concurrent
	// part PUTs. Part 1 carries no context (it starts from the IV).
	partInfos := make([]api.PartInfo, 0, len(parts))
	// Compute every part's SHA-256 midstate in a single forward pass.
	// Re-hashing 0..offset for each part is O(n^2) on large files (a
	// 500MiB/100-part upload re-hashes ~25GiB before the first PUT), so
	// feed one running hash once and capture the state at each part
	// boundary.
	h := sha256.New()
	buf := make([]byte, 1<<20)
	fed := int64(0)
	for _, p := range parts {
		pi := api.PartInfo{
			PartNumber: p.index,
			PartSize:   p.partSize,
		}
		// The official client always sets partOffset (we verified on
		// 2026-09-03 with mCloudDownload.zip) even on the first part;
		// only the h field is omitted for part 1.
		pi.ParallelHashCtx = &api.ParallelHashCtx{PartOffset: p.offset}
		if p.offset > 0 {
			if err := sha256Feed(h, freader, fed, p.offset, buf); err != nil {
				return nil, fmt.Errorf("yun139: midstate part %d: %w", p.index, err)
			}
			fed = p.offset
			regs, _, err := api.Sha256Midstate(h)
			if err != nil {
				return nil, fmt.Errorf("yun139: midstate part %d: %w", p.index, err)
			}
			pi.ParallelHashCtx.H = regs
		}
		partInfos = append(partInfos, pi)
	}
	// Limit the create payload to the first maxPartsPerRequest parts; the
	// rest are covered by /hcy/file/getUploadUrl later (alist does the
	// same 100-part batches). The full list is kept for the fetch step.
	allPartInfos := partInfos
	if len(partInfos) > maxPartsPerRequest {
		partInfos = partInfos[:maxPartsPerRequest]
	}
	body := buildCreateBody(dirID, leaf, size, hashHex, partInfos, f.opt.RegionCode)
	// Family space uses a different create endpoint
	// (/hcy/group/dynamic/file/create, captured 2026-09-03) on the
	// group.yun.139.com host, with extra fields {groupId, groupType,
	// seqNo, expireSec}. The response envelope is the same shape
	// (fileId / uploadId / partInfos / rapidUpload) but inside data
	// plus an extra "file" object. We treat the two flows as parallel
	// by mapping both into a unified PartInfos[] result.
	var resp struct {
		api.BaseResp
		Data struct {
			FileID      string               `json:"fileId"`
			FileName    string               `json:"fileName"`
			UploadID    string               `json:"uploadId"`
			RapidUpload bool                 `json:"rapidUpload"`
			Exists      *bool                `json:"exist"`
			PartInfos   []api.PartUploadInfo `json:"partInfos"`
		} `json:"data"`
	}
	var callPath string
	var callBody any = body
	if f.space == spaceFamily {
		callPath = "/hcy/group/dynamic/file/create"
		fb := map[string]any{
			"contentHash":          body.ContentHash,
			"contentHashAlgorithm": body.ContentHashAlgorithm,
			"expireSec":            86400,
			"fileRenameMode":       body.FileRenameMode,
			"groupId":              f.opt.FamilyID,
			"groupType":            1,
			"localCreatedAt":       body.LocalCreatedAt,
			"localUpdatedAt":       body.LocalUpdatedAt,
			"name":                 body.Name,
			"parallelUpload":       body.ParallelUpload,
			"parentFileId":         body.ParentID,
			"partInfos":            partInfos,
			"seqNo":                familySeqNo(),
			"size":                 body.Size,
			"type":                 body.Type,
		}
		if body.UserRegion != nil {
			fb["userRegion"] = map[string]any{
				"cityCode":     body.UserRegion.CityCode,
				"provinceCode": body.UserRegion.ProvinceCode,
			}
		}
		callBody = fb
	} else {
		callPath = "/hcy/file/create"
	}
	if f.space == spaceFamily {
		if err := f.familyCall(ctx, callPath, callBody, &resp); err != nil {
			return nil, noRetryOnMemberQuota(fmt.Errorf("create: %w", err))
		}
	} else {
		if err := f.personalCall(ctx, callPath, callBody, &resp); err != nil {
			return nil, noRetryOnMemberQuota(fmt.Errorf("create: %w", err))
		}
	}
	fs.Debugf(f, "yun139: create returned %d part URLs, rapid=%v exists=%v", len(resp.Data.PartInfos), resp.Data.RapidUpload, resp.Data.Exists)
	// rapidUpload success path: server already has the content (no part
	// URLs to fetch and no upload body to send), or the file already
	// exists under this name.
	if resp.Success && resp.Data.FileID != "" &&
		(resp.Data.RapidUpload || (resp.Data.Exists != nil && *resp.Data.Exists) || len(resp.Data.PartInfos) == 0) {
		return &uploadResult{fileID: resp.Data.FileID, fileName: leaf, hashHex: hashHex}, nil
	}
	if !resp.Success {
		return nil, noRetryOnMemberQuota(&apiError{Code: resp.Code, Message: resp.Message})
	}
	if len(resp.Data.PartInfos) == 0 {
		return nil, errors.New("create returned no upload URL")
	}
	// PUT every part in parallel, in batches of maxPartsPerRequest.
	// The first batch's URLs came from /file/create; the rest come from
	// /hcy/file/getUploadUrl (captured 2026-09-03: the client fetches
	// part 101+ exactly this way, with parallelUpload:true and the same
	// parallelHashCtx entries).
	allParts := make([]api.PartUploadInfo, 0, len(parts))
	allParts = append(allParts, resp.Data.PartInfos...)
	for i := maxPartsPerRequest; i < len(parts); i += maxPartsPerRequest {
		end := i + maxPartsPerRequest
		if end > len(parts) {
			end = len(parts)
		}
		// Reuse the precomputed partInfos (with parallelHashCtx) for
		// parts 101+ - the official client sends the same entries to
		// /hcy/file/getUploadUrl (personal) or
		// /hcy/group/dynamic/file/getUploadUrl (family).
		urlBody := map[string]any{
			"fileId":         resp.Data.FileID,
			"uploadId":       resp.Data.UploadID,
			"parallelUpload": true,
			"partInfos":      allPartInfos[i:end],
		}
		if f.space == spaceFamily {
			urlBody["groupId"] = f.opt.FamilyID
			urlBody["groupType"] = 1
		}
		var urlResp struct {
			api.BaseResp
			Data struct {
				PartInfos []api.PartUploadInfo `json:"partInfos"`
			} `json:"data"`
		}
		urlPath := "/hcy/file/getUploadUrl"
		if f.space == spaceFamily {
			urlPath = "/hcy/group/dynamic/file/getUploadUrl"
		}
		callURL := func() (string, error) {
			if f.space == spaceFamily {
				return api.FamilyBaseURL + urlPath, nil
			}
			host, err := f.personalCloudHost(ctx)
			if err != nil {
				return "", err
			}
			return host + urlPath, nil
		}
		if err := f.pacer.Call(func() (bool, error) {
			u, err := callURL()
			if err != nil {
				return false, err
			}
			err = f.call(ctx, u, urlBody, &urlResp)
			return shouldRetry(ctx, err)
		}); err != nil {
			return nil, fmt.Errorf("getUploadUrl: %w", err)
		}
		if !urlResp.Success {
			return nil, noRetryOnMemberQuota(&apiError{Code: urlResp.Code, Message: urlResp.Message})
		}
		allParts = append(allParts, urlResp.Data.PartInfos...)
	}
	// The server does not guarantee partInfos ordering in the
	// getUploadUrl response (captured 2026-09-03: [110, 111, 112, 101,
	// 113, ...]), so index-aligning allParts with parts would put
	// chunks on the wrong URLs. Match by partNumber instead.
	urlOfPart := make(map[int]string, len(parts))
	for _, pi := range allParts {
		if pi.PartNumber > 0 {
			urlOfPart[pi.PartNumber] = pi.UploadURL
		}
	}
	// Log the upload endpoint host (the CDN edge the part URLs point at),
	// mirroring wopan's per-run upload-zone attribution for diagnosis.
	if firstURL, ok := urlOfPart[1]; ok {
		if u, perr := neturl.Parse(firstURL); perr == nil {
			fs.Debugf(f, "yun139: upload endpoint %s", u.Host)
		}
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(f.opt.UploadConcurrency)
	for _, p := range parts {
		p := p
		g.Go(func() error {
			url, ok := urlOfPart[int(p.index)]
			if !ok {
				return fmt.Errorf("yun139: no upload URL for part %d", p.index)
			}
			rdr := io.NewSectionReader(freader, p.offset, p.partSize)
			return f.putPart(gctx, int(p.index), len(parts), rdr, url, p.partSize)
		})
	}
	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("put part: %w", err)
	}
	// Finally, mark the file complete. Mirrors the official client:
	// contentHash + contentHashAlgorithm + fileId + uploadId.
	cmpl := api.PersonalCompleteReq{
		FileID:               resp.Data.FileID,
		UploadID:             resp.Data.UploadID,
		ContentHash:          hashHex,
		ContentHashAlgorithm: "SHA256",
	}
	if f.space == spaceFamily {
		cmpl.GroupID = f.opt.FamilyID
		cmpl.AccountUserID = f.userDomainID
	}
	cmplPath := "/hcy/file/complete"
	if f.space == spaceFamily {
		cmplPath = "/hcy/group/dynamic/file/complete"
	}
	var cmplResp api.PersonalCompleteResp
	cmplHost := api.FamilyBaseURL
	if f.space != spaceFamily {
		var err error
		cmplHost, err = f.personalCloudHost(ctx)
		if err != nil {
			return nil, err
		}
	}
	if err := f.call(ctx, cmplHost+cmplPath, cmpl, &cmplResp); err != nil {
		return nil, noRetryOnMemberQuota(fmt.Errorf("complete: %w", err))
	}
	if !cmplResp.Success {
		return nil, noRetryOnMemberQuota(&apiError{Code: cmplResp.Code, Message: cmplResp.Message})
	}
	return &uploadResult{fileID: resp.Data.FileID, fileName: leaf, hashHex: hashHex}, nil
}

// putPart PUTs a single part to its pre-signed URL.
func (f *Fs) putPart(ctx context.Context, partIdx, total int, r io.Reader, url string, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Origin", yunBaseURL)
	req.Header.Set("Referer", yunBaseURL+"/")
	req.Header.Set("User-Agent", defaultUserAgent)
	// Content-Length is part of the CDN's S3 signature: leaving it out
	// makes the server reject the part with SignatureDoesNotMatch once
	// it tries to verify the body. The caller must know the part size.
	if size > 0 {
		req.ContentLength = size
		req.Header.Set("Content-Length", strconv.FormatInt(size, 10))
	}
	// Capture which endpoint + peer IP actually served this part: the
	// pre-signed URL's host is one thing, the connection may land on a
	// different CDN edge, so per-part attribution is the only way to
	// correlate speed with the node the request hit (mirrors wopan).
	host := ""
	if u, perr := neturl.Parse(url); perr == nil {
		host = u.Host
	}
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
	t0 := time.Now()
	res, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode >= 300 {
		body, _ := io.ReadAll(res.Body)
		// 5xx and 4xx are both treated as business errors; the part URL
		// is single-use so a retry on a transient 5xx would just hit the
		err := fmt.Errorf("yun139: upload part %d/%d via %s -> %s: %s: %s",
			partIdx, total, host, peerAddr, res.Status, truncate(string(body), 500))
		if res.StatusCode >= 400 && res.StatusCode < 500 {
			return fserrors.NoRetryError(err)
		}
		return err
	}
	elapsed := time.Since(t0)
	fs.Debugf(f, "yun139: uploaded part %d/%d (%v) via %s -> %s (reused=%v) in %v (%s)",
		partIdx, total, fs.SizeSuffix(size), host, peerAddr, reusedConn,
		elapsed.Round(time.Millisecond), fs.SizeSuffix(float64(size)/elapsed.Seconds()).ByteRateUnit())
	return nil
}

// Update in to the object with the modTime given of the given size.
//
// The 139 API has no in-place update, so the new content is uploaded under
// a temporary name, the old object is deleted, and the temp is renamed into
// place.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	size := src.Size()
	if size < 0 {
		return errors.New("yun139: can't upload files of unknown size")
	}
	if size == 0 {
		return fs.ErrorCantUploadEmptyFiles
	}
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}
	if err := o.fs.validateName(o.remote); err != nil {
		return err
	}
	leaf = o.fs.opt.Enc.FromStandardName(leaf)

	// Lossless Update: rename the old file away FIRST, upload the new
	// file under the target name, and only delete the renamed old file
	// once the new one is complete. If the upload fails, rename the old
	// file back - no data loss, no visible gap.
	//
	// Why not delete-first? (a) the server's rapidUpload reuses the old
	// storage when hashes match, so a size-changed Update could keep
	// the old content with a new size (audit 2026-09-04); (b) a plain
	// upload under the target name while the old file still exists
	// triggers auto_rename, leaving the new file under a (1) name.
	//
	// The backup name must not collide with the target's extension
	// semantics: personal /hcy/file/update replaces the whole name,
	// family modifyContentInfo keeps the extension, so use a backup
	// name that keeps the same extension for family.
	backupLeaf := leaf + ".rclone-old-" + random.String(8)
	oldPath := o.remote
	backupRemote := path.Join(path.Dir(o.remote), backupLeaf)
	if err := o.fs.renameObject(ctx, o.id, o.fs.opt.Enc.FromStandardName(backupLeaf), dirID, o.fs.space == spaceFamily); err != nil {
		// The rename is best-effort; if the server refuses (e.g. name
		// too long), fall back to delete-then-upload.
		fs.Debugf(o, "yun139: pre-rename failed (%v), falling back to delete-then-upload", err)
		if err := o.fs.deleteObject(ctx, o.id, o.serverPath, o.fs.space == spaceFamily); err != nil {
			return fserrors.NoLowLevelRetryError(fmt.Errorf("yun139: delete old object: %w", err))
		}
		backupRemote = ""
	}
	_ = oldPath
	_ = backupRemote
	res, err := o.fs.uploadFile(ctx, in, dirID, leaf, size)
	if err != nil {
		// Upload failed: try to restore the old file's name.
		if backupRemote != "" {
			if rerr := o.fs.renameObject(ctx, o.id, o.fs.opt.Enc.FromStandardName(leaf), dirID, o.fs.space == spaceFamily); rerr != nil {
				fs.Errorf(o, "yun139: restore old name after failed update: %v", rerr)
			}
		}
		return err
	}
	// Upload complete: the new file is in place. Refresh the
	// receiver, then delete the renamed old file. If the delete fails
	// the new file is correct; the old file lingers under the backup
	// name for the user to clean up.
	oldID := o.id
	o.id = res.fileID
	o.size = size
	o.modTime = src.ModTime(ctx)
	o.hashMu.Lock()
	o.sha256 = res.hashHex
	o.hashMu.Unlock()
	if backupRemote != "" {
		if err := o.fs.deleteObject(ctx, oldID, o.serverPath, o.fs.space == spaceFamily); err != nil {
			fs.Errorf(o, "yun139: delete backup after update: %v", err)
		}
	}
	// Clean up orphaned backups from a PREVIOUS killed run targeting the
	// same file (a crash between pre-rename and the new upload leaves
	// <name>.rclone-old-XXXX behind). Only touch entries whose base name
	// starts with this object's leaf - never delete anything else.
	o.fs.cleanupOldBackups(ctx, dirID, leaf)
	o.fs.dirCache.FlushDir(path.Dir(o.remote))
	return nil
}

// cleanupOldBackups deletes files in dirID whose names start with
// leaf+".rclone-old-" - the leftovers of an Update that crashed after
// renaming the old file away. The new file is already in place when
// this runs, so the orphan is safe to remove.
func (f *Fs) cleanupOldBackups(ctx context.Context, dirID, leaf string) {
	prefix := leaf + ".rclone-old-"
	var orphans []listEntry
	err := f.listAll(ctx, dirID, func(e listEntry) bool {
		if !e.isDir && strings.HasPrefix(e.name, prefix) {
			orphans = append(orphans, e)
		}
		return false
	})
	if err != nil {
		fs.Debugf(f, "yun139: orphan scan failed: %v", err)
		return
	}
	for _, e := range orphans {
		fs.Infof(f, "yun139: removing orphaned update backup %q", e.name)
		if err := f.deleteObject(ctx, e.id, e.srvPath, f.space == spaceFamily); err != nil {
			fs.Errorf(f, "yun139: remove orphaned backup %q: %v", e.name, err)
		}
	}
}

// deleteObject removes a single object by id.
//
// family=true uses the family batch-delete endpoint; false uses the personal
// /recyclebin/batchTrash (or /file/batchDelete if hard_delete is on).
func (f *Fs) deleteObject(ctx context.Context, id, srvPath string, family bool) error {
	if family {
		taskID, err := f.familyBatchOprTask(ctx, familyBatchReq{
			ContentList:       []string{id},
			DestCloudID:       "", // delete - no dest
			DestCatalogType:   1002,
			DestType:          "1",
			DestPath:          "",
			SourceCatalogType: 1002,
			SourceCloudID:     f.opt.FamilyID,
			SourceType:        "1",
			Path:              srvPath,
			TaskType:          2, // delete
			BusinessType:      2,
		})
		if err != nil {
			return err
		}
		return f.familyTaskPoll(ctx, taskID)
	}
	if f.opt.HardDelete {
		return f.deleteTask(ctx, "/hcy/file/batchDelete", id)
	}
	return f.deleteTask(ctx, "/hcy/recyclebin/batchTrash", id)
}

// deleteTask performs one batch-delete call and polls the returned task.
func (f *Fs) deleteTask(ctx context.Context, endpoint, id string) error {
	var out struct {
		api.BaseResp
		Data struct {
			TaskID string `json:"taskId"`
		} `json:"data"`
	}
	err := f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, endpoint, api.PersonalTrashReq{FileIDs: []string{id}, BusinessType: 0}, &out)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return err
	}
	if out.Data.TaskID == "" {
		// Some endpoints complete synchronously; nothing to poll.
		return nil
	}
	return f.taskGet(ctx, out.Data.TaskID, "delete")
}

// renameObject renames a file or folder.
//
// Family (captured 2026-09-03, PC client 8.8.6.20260829):
//   - file:   POST .../modifyContentInfo
//     {cloudID, contentID, contentName, path:<srvPath of the file>,
//     commonAccountInfo:{userDomainId, accountType:"1"}}
//   - folder: POST .../modifyCloudDocV2
//     {catalogType:3, cloudID, docLibName, docLibraryID, manualRename:0,
//     path:<srvPath of the folder>, commonAccountInfo:{...}}
//
// Personal: POST /hcy/file/update {fileId, name}.
func (f *Fs) renameObject(ctx context.Context, id, newName, dirID string, family bool) error {
	if family {
		srvPath := f.familySrvPath(id)
		body := map[string]any{
			"cloudID": f.opt.FamilyID,
			"commonAccountInfo": map[string]any{
				"userDomainId": f.userDomainID,
				"accountType":  "1",
			},
		}
		path := "/hcy/family/adapter/andAlbum/openApi/modifyContentInfo"
		if srvPath != "" {
			// The server-side path of a catalog entry is
			// "root:/<parentId...>/<own id>"; the cache stores the
			// entry's own path after a parent listing. The Update
			// flow renames a freshly-uploaded file whose path has
			// not been cached yet, so fall back to the parent path.
			body["path"] = srvPath
		} else {
			body["path"] = f.familySrvPath(dirID)
		}
		body["contentID"] = id
		body["contentName"] = newName
		return f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, path, body, nil)
			return shouldRetry(ctx, err)
		})
	}
	return f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, "/hcy/file/update", api.PersonalUpdateReq{
			FileID: id,
			Name:   newName,
		}, nil)
		return shouldRetry(ctx, err)
	})
}

// Remove deletes the object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteObject(ctx, o.id, o.serverPath, o.fs.space == spaceFamily)
}

// Move a file or directory
// Move a file using the server-side batch-move API.
//
// Signature follows fs.Mover: destination is a full remote path.
// Same-directory moves are a rename (batchMove with a changed name);
// cross-directory moves use the batch-move task. Returns the new
// object, or ErrorCantMove so the engine falls back to copy+delete.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}
	if srcObj.fs != f && !srcObj.fs.sameCloud(f) {
		return nil, fs.ErrorCantMove
	}
	dstLeaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	if err := f.validateName(remote); err != nil {
		return nil, err
	}
	srcLeaf := srcObj.leaf()
	if f.space == spaceFamily {
		// No native family move (new PC client has no move button).
		// Fall back to copy + delete (safe: the engine does the same
		// when we return ErrorCantMove).
		if err := f.familyCopy(ctx, srcObj, dstDirID); err != nil {
			return nil, err
		}
		if err := f.deleteObject(ctx, srcObj.id, srcObj.serverPath, true); err != nil {
			return nil, err
		}
		return f.NewObject(ctx, remote)
	}
	taskID, err := f.moveTaskID(ctx, srcObj.id, dstDirID)
	if err != nil {
		return nil, err
	}
	if err := f.taskGet(ctx, taskID, "move"); err != nil {
		return nil, err
	}
	// The batch-move keeps the id; if the leaf changed (rename-in-move),
	// the server-side move does not rename, so issue an update.
	if dstLeaf != "" && dstLeaf != srcLeaf {
		if err := f.renameObject(ctx, srcObj.id, f.opt.Enc.FromStandardName(dstLeaf), dstDirID, false); err != nil {
			return nil, err
		}
	}
	o, err := f.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return o, nil
}

// moveTaskID is a helper that performs one batchMove and returns the task id.
func (f *Fs) moveTaskID(ctx context.Context, id, dstDirID string) (string, error) {
	var out struct {
		api.BaseResp
		Data struct {
			TaskID string `json:"taskId"`
		} `json:"data"`
	}
	err := f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, "/hcy/file/batchMove", api.PersonalBatchMoveReq{
			FileIDs:        []string{id},
			ToParentFileID: dstDirID,
			UserID:         f.account,
			EventType:      "move",
			BusinessType:   0,
		}, &out)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return "", err
	}
	return out.Data.TaskID, nil
}

// DirMove moves a directory
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok || !srcFs.sameCloud(f) {
		// Different account/family - cannot move across clouds.
		return fs.ErrorCantDirMove
	}
	// Use lib/dircache's DirMove helper to resolve ids (it handles
	// the MoveDir root-relative semantics, src==dst and
	// dst-inside-src correctly).
	srcID, _, _, _, _, err :=
		f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}
	if f.space == spaceFamily {
		// No native family move API. The new server falls back to
		// file-level copy+delete; let the engine do that with the
		// verified per-file batch paths by returning ErrorCantDirMove
		// only when src is the family root itself (which is
		// undeletable anyway). Otherwise copy+delete recursively via
		// the family BatchOprTask API.
		//
		// Note: the familyBatchOprTask(...,taskType=1) for catalogs
		// is unreliable (02010501 with certain id/path combinations;
		// the audit round 2 saw it). A file-level walk is far more
		// reliable and uses the verified batchCopy for each file.
		return fs.ErrorCantDirMove
	}
	// Personal: the batch-move API takes fileIds; a directory move
	// via it fails with '04000002: 请求参数不合法' (audit 2026-09-04)
	// and the directory-level fields are not confirmed by captures.
	// Fall back to the engine's file-by-file path.
	_ = srcID
	return fs.ErrorCantDirMove
}

// Copy a file using the server-side batch-copy API.
//
// Signature follows fs.Copier (wopan/drive): the destination is a full
// remote path. Returns the new object; the caller falls back to a
// bandwidth copy when ErrorCantCopy is returned.
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	if srcObj.fs != f && !srcObj.fs.sameCloud(f) {
		// Different account/family - the API cannot server-side copy.
		return nil, fs.ErrorCantCopy
	}
	dstLeaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	if err := f.validateName(remote); err != nil {
		return nil, err
	}
	if f.space == spaceFamily {
		if err := f.familyCopy(ctx, srcObj, dstDirID); err != nil {
			return nil, err
		}
		// The family copy task has no direct id echo; re-resolve.
		o, err := f.NewObject(ctx, remote)
		if err == nil {
			return o, nil
		}
		// Fall through to the same findNewCopy+rename path as personal.
		if o, err2 := f.resolveCopyLeaf(ctx, srcObj, dstDirID, dstLeaf); err2 == nil {
			return o, nil
		}
		return nil, err
	}
	taskID, err := f.copyTaskID(ctx, srcObj.id, dstDirID)
	if err != nil {
		return nil, err
	}
	if err := f.taskGet(ctx, taskID, "copy"); err != nil {
		return nil, err
	}
	o, err := f.NewObject(ctx, remote)
	if err == nil {
		return o, nil
	}
	return f.resolveCopyLeaf(ctx, srcObj, dstDirID, dstLeaf)
}

// resolveCopyLeaf finds the server-created copy and, when the server
// auto-renamed it (batchCopy lands under <src>_<timestamp>.<ext> or
// <src>(1).<ext> instead of the requested leaf), renames it to the
// requested leaf and returns the object at remote. If nothing was
// found, returns ErrorObjectNotFound.
func (f *Fs) resolveCopyLeaf(ctx context.Context, srcObj *Object, dstDirID, dstLeaf string) (fs.Object, error) {
	o, err := f.findNewCopy(ctx, srcObj, dstDirID)
	if err != nil {
		return nil, err
	}
	newObj := o.(*Object)
	if newObj.leaf() == dstLeaf {
		return o, nil
	}
	// The server put the copy under a different name (auto_rename).
	// Rename it to the requested leaf; both spaces support this.
	if err := f.renameObject(ctx, newObj.id, f.opt.Enc.FromStandardName(dstLeaf), dstDirID, f.space == spaceFamily); err != nil {
		return nil, fmt.Errorf("yun139: rename copy to %q: %w", dstLeaf, err)
	}
	newObj.remote = path.Join(f.root, dstLeaf)
	return newObj, nil
}

// findNewCopy lists dstDirID and returns the first file entry that is
// not srcObj, matches its size, and appeared after the copy started.
// The family copy task does not echo the new id, and a same-directory
// copy is auto-renamed by the server, so this is the only reliable way
// to resolve the copy result.
func (f *Fs) findNewCopy(ctx context.Context, srcObj *Object, dstDirID string) (fs.Object, error) {
	var found listEntry
	start := time.Now().Add(-2 * time.Minute)
	err := f.listAll(ctx, dstDirID, func(e listEntry) bool {
		if e.isDir || e.id == srcObj.id || e.size != srcObj.size {
			return false
		}
		if e.modTime.Before(start) {
			return false
		}
		found = e
		return true
	})
	if err != nil {
		return nil, err
	}
	if found.id == "" {
		return nil, fs.ErrorObjectNotFound
	}
	return f.newObjectWithInfo(ctx, path.Join(f.root, found.name), found)
}

// sameCloud reports whether fs belongs to the same 139 account (and, for
// family, the same family cloud), so ids are interchangeable.
func (f *Fs) sameCloud(other *Fs) bool {
	if f.space != other.space {
		return false
	}
	if f.space == spaceFamily && f.opt.FamilyID != other.opt.FamilyID {
		return false
	}
	return true
}

// leaf returns the base name of the object's remote path.
func (o *Object) leaf() string { return path.Base(o.remote) }

// familyCopy copies srcObj (file or dir) into dstDirID inside the family
// cloud via createBatchOprTaskV2 taskType=1, then polls the task
// (captured 2026-09-03).
func (f *Fs) familyCopy(ctx context.Context, srcObj *Object, dstDirID string) error {
	return f.familyCopyID(ctx, srcObj.id, dstDirID, srcObj.isDir)
}

// primeSrvPathFor lists dirID via the family queryContentListV3 endpoint
// and records its server-side path in the srvPath cache.
func (f *Fs) primeSrvPathFor(ctx context.Context, dirID string) error {
	body := map[string]any{
		"catalogSortType": 0,
		"catalogType":     3,
		"cloudID":         f.opt.FamilyID,
		"cloudType":       1,
		"contentSortType": 0,
		"sortDirection":   1,
		"path":            "",
		"commonAccountInfo": map[string]any{
			"userDomainId": f.userDomainID,
			"accountType":  1,
		},
		"pageInfo": map[string]int{"pageNum": 1, "pageSize": 1},
	}
	body["catalogID"] = dirID
	var resp struct {
		Path string `json:"path"`
	}
	if err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/queryContentListV3", body, &resp); err != nil {
		return err
	}
	if resp.Path != "" {
		f.srvPathOf.put(dirID, resp.Path)
	}
	return nil
}

// familyCopyID copies the given content/catalog id into dstDirID.
func (f *Fs) familyCopyID(ctx context.Context, id, dstDirID string, isDir bool) error {
	// The server's batchOprTask requires the destination's server-side
	// path. The cache may be cold for a directory we never listed
	// (typical for a freshly-discovered family cloud); prime it on
	// demand. The batch task also needs a non-empty DestPath; an
	// empty one returns '02010501: 请求不合法'.
	if f.familySrvPath(dstDirID) == "" {
		if err := f.primeSrvPathFor(ctx, dstDirID); err != nil {
			fs.Debugf(f, "yun139: prime srvPath for dst %s: %v", dstDirID, err)
		}
	}
	req := familyBatchReq{
		DestCatalogType:   1002,
		DestCloudID:       f.opt.FamilyID,
		DestPath:          f.familySrvPath(dstDirID),
		DestType:          "1",
		Path:              "",
		SourceCatalogType: 1002,
		SourceCloudID:     f.opt.FamilyID,
		SourceType:        "1",
		TaskType:          1, // copy
		BusinessType:      2,
	}
	if isDir {
		req.CatalogList = []string{id}
	} else {
		req.ContentList = []string{id}
	}
	taskID, err := f.familyBatchOprTask(ctx, req)
	if err != nil {
		return err
	}
	return f.familyTaskPoll(ctx, taskID)
}

// familyDeleteID deletes the given family catalog id (taskType=2).
func (f *Fs) familyDeleteID(ctx context.Context, id, srvPath string) error {
	taskID, err := f.familyBatchOprTask(ctx, familyBatchReq{
		CatalogList:       []string{id},
		DestCatalogType:   1002,
		DestType:          "1",
		Path:              srvPath,
		SourceCatalogType: 1002,
		SourceCloudID:     f.opt.FamilyID,
		SourceType:        "1",
		TaskType:          2, // delete
		BusinessType:      2,
	})
	if err != nil {
		return err
	}
	return f.familyTaskPoll(ctx, taskID)
}

// copyTaskID performs one batchCopy and returns the task id.
func (f *Fs) copyTaskID(ctx context.Context, id, dstDirID string) (string, error) {
	var out struct {
		api.BaseResp
		Data struct {
			TaskID string `json:"taskId"`
		} `json:"data"`
	}
	err := f.pacer.Call(func() (bool, error) {
		// The official client sends userId = userDomainId (the
		// <user-domain-id>-style id) for copy; we only know the
		// phone number at this point, so fall back to it. If the server
		// rejects it, userDomainID discovery is needed.
		userID := f.userDomainID
		if userID == "" {
			userID = f.account
		}
		err := f.personalCall(ctx, "/hcy/file/batchCopy", api.PersonalBatchCopyReq{
			FileIDs:        []string{id},
			ToParentFileID: dstDirID,
			UserID:         userID,
			UserDomainID:   userID,
		}, &out)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return "", err
	}
	return out.Data.TaskID, nil
}

// OpenChunkWriter supports the multi-thread copy engine by staging
// chunks in a temp file and running the real upload at Close.
//
// 139's protocol needs the whole-file SHA-256 *before* /file/create can
// return the part upload URLs, so the chunked copy engine cannot drive
// the parts directly. Instead, WriteChunk stages each chunk into a temp
// file at its part offset (safe under concurrent out-of-order writes),
// and Close hashes the file and runs the same pipeline as Put
// (temp file + SHA-256 + /hcy/file/create + parallel part PUTs).
// The cost vs Put is one extra disk round-trip.
func (f *Fs) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (info fs.ChunkWriterInfo, writer fs.ChunkWriter, err error) {
	size := src.Size()
	if size < 0 {
		return info, nil, errors.New("yun139: can't upload files of unknown size")
	}
	// The multi-thread copy engine starts a download before the ChunkWriter's
	// Close() runs, so checking the member single-file limit here (size is known
	// up front) avoids downloading an oversized file to a local temp file only to
	// reject it at upload time. This is the earliest point for the chunk path.
	if limit := f.maxFileSize(); limit > 0 && size > limit {
		fs.Logf(f, "SKIP %s: %d bytes exceeds the member single-file upload limit of %d bytes (tier %q); skipping to avoid a wasted multi-part upload",
			remote, size, limit, f.memberLevelName)
		return info, nil, errTooLarge(remote, size, limit, f.memberLevelName)
	}
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return info, nil, err
	}
	if err := f.validateName(remote); err != nil {
		return info, nil, err
	}
	leaf = f.opt.Enc.FromStandardName(leaf)
	chunkSize := int64(f.opt.PartSize)
	if chunkSize <= 0 {
		chunkSize = api.DefaultChunkSize
	}
	if size < chunkSize {
		chunkSize = size
	}
	if size == 0 {
		chunkSize = 1 // zero-size files still need a temp file to hash
	}
	tmp, err := os.CreateTemp("", "yun139-chunkwriter-")
	if err != nil {
		return info, nil, fmt.Errorf("yun139: chunk writer temp: %w", err)
	}
	// Preallocate so WriteAt never hits EOF errors on sparse regions.
	if err := tmp.Truncate(size); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return info, nil, fmt.Errorf("yun139: chunk writer truncate: %w", err)
	}
	w := &yun139ChunkWriter{
		f:         f,
		tmp:       tmp,
		path:      tmp.Name(),
		dirID:     dirID,
		leaf:      leaf,
		size:      size,
		chunkSize: chunkSize,
		total:     (size + chunkSize - 1) / chunkSize,
	}
	info = fs.ChunkWriterInfo{
		ChunkSize:   chunkSize,
		Concurrency: f.opt.UploadConcurrency,
	}
	return info, w, nil
}

// yun139ChunkWriter stages chunks into a temp file for OpenChunkWriter.
type yun139ChunkWriter struct {
	f         *Fs
	tmp       *os.File
	path      string
	dirID     string
	leaf      string
	size      int64
	chunkSize int64
	total     int64
	closed    bool
	// written is the running total of bytes written by WriteChunk. It is
	// incremented atomically because the copy engine calls WriteChunk
	// concurrently; Close verifies it equals size to catch a silently
	// truncated chunk.
	written atomic.Int64
}

// WriteChunk writes chunkNumber at chunkNumber*chunkSize in the temp
// file. The copy engine seeks the reader to the chunk start before
// calling; concurrent out-of-order calls are safe because WriteAt is
// position-addressed.
func (w *yun139ChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (int64, error) {
	if w.closed {
		return 0, errors.New("yun139: chunk writer closed")
	}
	if int64(chunkNumber) >= w.total {
		return 0, fmt.Errorf("yun139: chunk %d out of range (total %d)", chunkNumber, w.total)
	}
	offset := int64(chunkNumber) * w.chunkSize
	// Read exactly this chunk's worth (the last chunk may be short).
	limit := w.chunkSize
	if remaining := w.size - offset; remaining < limit {
		limit = remaining
	}
	// The engine positions the reader at the chunk start; read from the
	// current position.
	buf := make([]byte, limit)
	m, err := io.ReadFull(reader, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, err
	}
	// The copy engine promises exactly size bytes across all chunks, so a
	// non-empty chunk must fill its full span. A short read here would
	// otherwise be written as a silent gap (zero-fill) and corrupt the
	// uploaded file.
	if int64(m) != limit {
		return 0, fmt.Errorf("yun139: chunk %d short read: got %d of %d bytes", chunkNumber, m, limit)
	}
	if _, err := w.tmp.WriteAt(buf[:m], offset); err != nil {
		return 0, err
	}
	w.written.Add(int64(m))
	return int64(m), nil
}

// Close hashes the staged file and runs the standard upload pipeline.
func (w *yun139ChunkWriter) Close(ctx context.Context) error {
	if w.closed {
		return errors.New("yun139: chunk writer already closed")
	}
	w.closed = true
	defer func() {
		_ = w.tmp.Close()
		_ = os.Remove(w.path)
	}()
	// Every chunk must have been written in full; otherwise a chunk was
	// silently dropped and the staged file is shorter than declared.
	if got := w.written.Load(); got != w.size {
		return fmt.Errorf("yun139: chunk writer staged %d of %d bytes", got, w.size)
	}
	if err := w.tmp.Sync(); err != nil {
		return err
	}
	h := sha256.New()
	if _, err := w.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.Copy(h, w.tmp); err != nil {
		return fmt.Errorf("yun139: chunk writer hash: %w", err)
	}
	if _, err := w.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := w.f.uploadFromRandom(ctx, w.tmp, w.dirID, w.leaf, w.size, hex.EncodeToString(h.Sum(nil)))
	if err != nil {
		return fmt.Errorf("yun139: chunk writer upload: %w", err)
	}
	return nil
}

// Abort removes the temp file without uploading.
func (w *yun139ChunkWriter) Abort(ctx context.Context) error {
	if w.closed {
		return nil
	}
	w.closed = true
	_ = w.tmp.Close()
	_ = os.Remove(w.path)
	return nil
}

// PutUnchecked aliases Put: 139's /file/create handles the
// "file already exists" case via its fileRenameMode field, so Put
// already does the right thing for the chunked copy engine's
// "I have already confirmed the target will be overwritten" path.
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// sha256Feed hashes src[off:end] into the running h in 1 MiB chunks,
// continuing any prior state. off and end are absolute file offsets; the
// caller advances fed past each part boundary so a whole file is hashed
// exactly once (single forward pass for all part midstates).
func sha256Feed(h interface {
	Write(p []byte) (int, error)
}, src io.ReaderAt, off, end int64, buf []byte) error {
	for off < end {
		want := end - off
		if want > int64(len(buf)) {
			want = int64(len(buf))
		}
		nr, err := src.ReadAt(buf[:want], off)
		if nr > 0 {
			if nw, werr := h.Write(buf[:nr]); werr != nil {
				return werr
			} else if nw != nr {
				return io.ErrShortWrite
			}
			off += int64(nr)
		}
		if err != nil {
			// ReadAt returns io.EOF when it reads fewer bytes than
			// requested at end of data; that is fine if we reached end.
			if err == io.EOF && off >= end {
				return nil
			}
			return err
		}
	}
	return nil
}

// ============================================================================
// Download (Open / fetchDownloadURL / getURL)
// ============================================================================

// downloadURL returns a cached download URL for the object, fetching a fresh
// one when empty or expired.
func (o *Object) downloadURL(ctx context.Context) (string, error) {
	o.urlMu.Lock()
	defer o.urlMu.Unlock()
	if o.url != "" && time.Now().Before(o.urlExpiry) {
		return o.url, nil
	}
	url, err := o.fs.fetchDownloadURL(ctx, o)
	if err != nil {
		return "", err
	}
	o.url = url
	o.urlExpiry = time.Now().Add(downloadURLTTL)
	return url, nil
}

// invalidateURL drops the cached download URL.
func (o *Object) invalidateURL() {
	o.urlMu.Lock()
	defer o.urlMu.Unlock()
	o.url = ""
	o.urlExpiry = time.Time{}
}

// fetchDownloadURL requests a fresh download URL for the object.
func (f *Fs) fetchDownloadURL(ctx context.Context, o *Object) (string, error) {
	if f.space == spaceFamily {
		// POST .../getFileDownLoadURLV2 (captured 2026-09-03). The
		// server returns a single "downloadURL" string.
		body := map[string]any{
			"catalogType": 3,
			"cloudID":     f.opt.FamilyID,
			"cloudType":   1,
			"commonAccountInfo": map[string]any{
				"account":     f.account,
				"accountType": "1",
			},
			"contentID": o.id,
			"extInfo": map[string]string{
				"isReturnCdnDownloadUrl": "1",
			},
			"path": o.serverPath,
		}
		var resp struct {
			Result struct {
				ResultCode string `json:"resultCode"`
				ResultDesc string `json:"resultDesc"`
			} `json:"result"`
			DownloadURL string `json:"downloadURL"`
		}
		err := f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/getFileDownLoadURLV2", body, &resp)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return "", err
		}
		if resp.Result.ResultCode != "0" {
			return "", &apiError{Code: resp.Result.ResultCode, Message: resp.Result.ResultDesc}
		}
		if resp.DownloadURL == "" {
			return "", fmt.Errorf("yun139: no download URL returned for %q", o.remote)
		}
		return resp.DownloadURL, nil
	}
	// The official client requests a 24h link (expireSec:86400).
	body := map[string]any{"fileId": o.id, "expireSec": 86400}
	var resp api.PersonalDownloadResp
	err := f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, "/hcy/file/getDownloadUrl", body, &resp)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return "", err
	}
	// Prefer cdnUrl when the server says the CDN switch is on, otherwise url.
	var dl string
	if resp.Data.CDNURL != "" && resp.Data.CDNSwitch {
		dl = resp.Data.CDNURL
	} else if resp.Data.URL != "" {
		dl = resp.Data.URL
	} else {
		return "", fmt.Errorf("yun139: no download URL returned for %q", o.remote)
	}
	if u, perr := neturl.Parse(dl); perr == nil {
		fs.Debugf(o, "yun139: download url-type=3 via %s", u.Host)
	} else {
		fs.Debugf(o, "yun139: download url-type=3 (unparsed host): %.120s", dl)
	}
	return dl, nil
}

// Open opens the file for read. Call Close() on the returned io.ReadCloser.
//
// A cached link that has been revoked (403) is refetched once before giving up.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.size)
	headers := fs.OpenOptionHeaders(options)
	url, err := o.downloadURL(ctx)
	if err != nil {
		return nil, err
	}
	res, err := o.fs.getURL(ctx, url, headers)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusForbidden {
		// The cached link is stale - drop it and fetch a fresh one once.
		_ = res.Body.Close()
		o.invalidateURL()
		url, err = o.downloadURL(ctx)
		if err != nil {
			return nil, err
		}
		res, err = o.fs.getURL(ctx, url, headers)
		if err != nil {
			return nil, err
		}
	}
	if res.StatusCode >= 300 {
		_ = res.Body.Close()
		return nil, fmt.Errorf("yun139: download: %s", res.Status)
	}
	return res.Body, nil
}

// ensure strings import is used
var _ = strings.TrimSpace

// ============================================================================
// Task polling (personal /hcy/task/get + family createBatchOprTask)
// ============================================================================

// taskPollResult mirrors the /hcy/task/get response envelope.
type taskPollResult struct {
	Success bool   `json:"success"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		TaskInfo struct {
			TaskID string `json:"taskId"`
			Status string `json:"status"` // Running | Succeed | Failed
		} `json:"taskInfo"`
	} `json:"data"`
}

// taskStatusDone reports whether a task/get status means the operation
// completed. The official client emits "Succeed"; "" appears when the
// response omits taskInfo.status (synchronous completion).
func taskStatusDone(status string) bool {
	switch status {
	case "Succeed", "SUCCESS", "success", "":
		return true
	}
	return false
}

// taskStatusFailed reports whether a task/get status means the operation
// failed.
func taskStatusFailed(status string) bool {
	switch status {
	case "Failed", "Failure", "failed":
		return true
	}
	return false
}

// taskGet polls /hcy/task/get until the task finishes.
//
// Delete / move / copy on 139 are task-based: the mutating call returns
// a taskId immediately and the change happens in the background. rclone
// callers expect the operation to be done when the call returns, so we
// poll. The official client polls ~1/s; we pace ourselves with the
// pacer and give up after 60s.
func (f *Fs) taskGet(ctx context.Context, taskID, what string) error {
	body := map[string]any{"taskId": taskID}
	deadline := time.Now().Add(60 * time.Second)
	for {
		var out taskPollResult
		err := f.pacer.Call(func() (bool, error) {
			err := f.personalCall(ctx, "/hcy/task/get", body, &out)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return fmt.Errorf("yun139: poll %s task: %w", what, err)
		}
		if !out.Success {
			return &apiError{Code: out.Code, Message: out.Message}
		}
		switch {
		case taskStatusDone(out.Data.TaskInfo.Status):
			return nil
		case taskStatusFailed(out.Data.TaskInfo.Status):
			return fmt.Errorf("yun139: %s task %s failed", what, taskID)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("yun139: %s task %s did not finish in 60s", what, taskID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// familyBatchReq is the request body for createBatchOprTaskV2, the
// family-cloud equivalent of batchMove / batchCopy / batchDelete
// (captured 2026-09-03).
//
//	taskType: 1 = copy, 2 = delete. The server uses the sourceType +
//	  destType pair to cross spaces: sourceType=3,destType=1 copies
//	  from personal into family; both 1 copies within family; both
//	  empty omits a side.
type familyBatchReq struct {
	ContentList       []string
	CatalogList       []string
	DestCloudID       string
	DestCatalogType   int
	DestType          string
	DestPath          string
	SourceCatalogType int
	SourceCloudID     string
	SourceType        string
	Path              string
	TaskType          int
	BusinessType      int
}

// familyBatchOprTask issues POST
// /hcy/family/adapter/andAlbum/openApi/createBatchOprTaskV2 and
// returns the task id.
func (f *Fs) familyBatchOprTask(ctx context.Context, req familyBatchReq) (string, error) {
	if req.ContentList == nil {
		req.ContentList = []string{}
	}
	if req.CatalogList == nil {
		req.CatalogList = []string{}
	}
	body := map[string]any{
		"catalogList":       req.CatalogList,
		"contentList":       req.ContentList,
		"destCatalogType":   req.DestCatalogType,
		"destCloudID":       req.DestCloudID,
		"destPath":          req.DestPath,
		"destType":          req.DestType,
		"path":              req.Path,
		"sourceCatalogType": req.SourceCatalogType,
		"sourceCloudID":     req.SourceCloudID,
		"sourceType":        req.SourceType,
		"taskType":          req.TaskType,
		"commonAccountInfo": map[string]any{
			"userDomainId": f.userDomainID,
			"accountType":  1,
		},
		"businessType": req.BusinessType,
		"userDomainId": f.userDomainID,
	}
	var resp struct {
		Result struct {
			ResultCode string `json:"resultCode"`
			ResultDesc string `json:"resultDesc"`
		} `json:"result"`
		TaskID string `json:"taskID"`
	}
	err := f.pacer.Call(func() (bool, error) {
		err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/createBatchOprTaskV2", body, &resp)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return "", err
	}
	if resp.Result.ResultCode != "0" {
		return "", &apiError{Code: resp.Result.ResultCode, Message: resp.Result.ResultDesc}
	}
	if resp.TaskID == "" {
		return "", errors.New("yun139: createBatchOprTaskV2 returned no taskID")
	}
	return resp.TaskID, nil
}

// familyTaskPoll polls queryBatchOprTaskDetailV3 until the task
// completes. State machine (captured 2026-09-03):
//
//	taskStatus 0 = running, 1 = running, 2 = success,
//	taskResultCode 1 means success, 0 means failed.
//	contentList[].reason == "0000" is success; any other code is the
//	per-item error.
func (f *Fs) familyTaskPoll(ctx context.Context, taskID string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		body := map[string]any{
			"taskID": taskID,
			"accountInfo": map[string]any{
				"userDomainId": f.userDomainID,
				"accountType":  1,
			},
			"taskId": taskID,
			"commonAccountInfo": map[string]any{
				"userDomainId": f.userDomainID,
				"accountType":  1,
			},
		}
		var out struct {
			Result struct {
				ResultCode string `json:"resultCode"`
				ResultDesc string `json:"resultDesc"`
			} `json:"result"`
			BatchOprTask struct {
				TaskStatus     int  `json:"taskStatus"`
				TaskResultCode *int `json:"taskResultCode"`
			} `json:"batchOprTask"`
			ContentList []struct {
				SrcID  string `json:"srcID"`
				RstID  string `json:"rstID"`
				Reason string `json:"reason"`
			} `json:"contentList"`
		}
		err := f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/queryBatchOprTaskDetailV3", body, &out)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return fmt.Errorf("yun139: poll family task: %w", err)
		}
		if out.Result.ResultCode != "0" {
			return &apiError{Code: out.Result.ResultCode, Message: out.Result.ResultDesc}
		}
		switch out.BatchOprTask.TaskStatus {
		case 0, 1:
			// 0 = created, 1 = running - keep polling
		case 2:
			// Success only when resultCode==1.
			if out.BatchOprTask.TaskResultCode != nil && *out.BatchOprTask.TaskResultCode == 1 {
				return nil
			}
			return fmt.Errorf("yun139: family task %s finished with resultCode %v", taskID, out.BatchOprTask.TaskResultCode)
		case 3:
			return fmt.Errorf("yun139: family task %s failed", taskID)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("yun139: family task %s did not finish in 60s", taskID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
