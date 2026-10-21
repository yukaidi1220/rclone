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
	mathrand "math/rand/v2"
	"net/http"
	"net/http/httptrace"
	neturl "net/url"
	"os"
	"path"
	"regexp"
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

	// Download URLs are minted per space and the server fixes their lifetime
	// whatever the client asks for, so these cache windows are deliberately well
	// under the granted life. A cached URL is therefore still valid when it is
	// handed out, and the expiry path in Open stays unreachable.
	//
	// The personal cloud grants 900s (15 minutes): measured 2026-10-01 (206 at
	// 5 minutes, 403 at 15 minutes), and the official client's own capture from
	// 2026-09-03 has a expireSec:86400 request answered with
	// expiration = issue time + 15 minutes and X-Amz-Expires=900.
	// The family cloud's API takes no expiry parameter and grants 24h.
	personalDownloadURLTTL = 5 * time.Minute
	familyDownloadURLTTL   = 20 * time.Hour

	// defaultLinkExpireSec is the expireSec the official client asks for (24h).
	// rclone link overrides it with --expire.
	defaultLinkExpireSec = 86400

	// maxLinkExpireSec is the longest lifetime the official client asks a
	// personal-space download link for, and therefore the ceiling rclone link
	// accepts for --expire.
	maxLinkExpireSec = 86400

	// minTokenLifetime is the remaining validity under which the token is
	// refreshed proactively (the official client uses 15 days).
	minTokenLifetime = 15 * 24 * time.Hour

	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

// Space names for the "space" option.
const (
	spacePersonal = "personal"
	spaceFamily   = "family"
	spaceGroup    = "group"
)

// svcType values for the x-SvcType header: 1 = personal, 2 = family.
//
// A shared group is addressed with the same family-space request shape on
// the same host, so group reuses svcTypeFamily. The value only reaches the
// wire through pcHeaders, which pins x-yun-svc-type itself; it is kept in
// sync so a future reader does not have to re-derive which space a token is
// pointed at.
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
	GroupID           string               `config:"group_id"`
	RootFolderID      string               `config:"root_folder_id"`
	UserDomainID      string               `config:"user_domain_id"` // <user-domain-id>-style id, optional
	HardDelete        bool                 `config:"hard_delete"`
	NoRefresh         bool                 `config:"no_refresh"` // never refresh the token
	PartSize          fs.SizeSuffix        `config:"part_size"`
	UploadConcurrency int                  `config:"upload_concurrency"`
	RegionCode        string               `config:"region_code"` // "province:city" 上传调度节点码,如 531:543(江苏无锡);留空不发送
	DisableHTTP2      bool                 `config:"disable_http2"`
	MaxFileSize       fs.SizeSuffix        `config:"max_file_size"`   // 单文件上传上限覆盖;0=按会员等级自动
	NoMemberCheck     bool                 `config:"no_member_check"` // 跳过会员等级探测与单文件上限检查,任意大小直接上传
	Enc               encoder.MultiEncoder `config:"encoding"`
}

// yun139DefaultEncoding is the default value of the encoding option: every
// name character the server refuses, plus the ones the encoder can round-trip.
//
// 139 rejects names with leading/trailing whitespace, control chars, leading
// tilde/period, invalid UTF-8, and the eight reserved ASCII characters
// `" * : < > ? \ |`, all with '04000002: 文件名称不符合标准'. The encoder's
// fullwidth substitutes for the reserved characters are stored verbatim by the
// server, so escaping them is what makes such names work at all.
const yun139DefaultEncoding = encoder.Standard | encoder.EncodeInvalidUtf8 |
	encoder.EncodeLeftSpace | encoder.EncodeLeftTilde |
	encoder.EncodeLeftCrLfHtVt | encoder.EncodeRightSpace |
	encoder.EncodeRightCrLfHtVt | encoder.EncodeLeftPeriod |
	encoder.EncodeRightPeriod |
	encoder.EncodeDoubleQuote | encoder.EncodeAsterisk |
	encoder.EncodeColon | encoder.EncodeLtGt |
	encoder.EncodeQuestion | encoder.EncodePipe |
	encoder.EncodeBackSlash

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
			}, {
				Value: spaceGroup,
				Help:  "Shared group (共享群)",
			}},
		}, {
			Name: "family_id",
			Help: "Family cloud ID (required when space is family).\n\n" +
				"A numeric id, e.g. 1303251918616070593. With exactly one family cloud it may be left " +
				"blank and is auto-discovered; with several it must be set, and the backend refuses to " +
				"guess. Family names can repeat, so only the id selects unambiguously.",
			Advanced: true,
		}, {
			Name: "group_id",
			Help: "Shared group ID (required when space is group).\n\n" +
				"A numeric id, e.g. 1234567890123456789. With exactly one group it may be left blank " +
				"and is auto-discovered; with several it must be set, and the backend refuses to " +
				"guess. A group id is not a family cloud id: pointing space=group at a family cloud id " +
				"fails in the server's own words as an internal error, not as \"not a group\".",
			Advanced: true,
		}, {
			Name:      "root_folder_id",
			Help:      "ID of the root folder. Leave blank for the top level.",
			Advanced:  true,
			Sensitive: true,
		}, {
			Name: "user_domain_id",
			Help: "The <user-domain-id>-style user domain id.\n\n" +
				"Found in the official client's request URLs ('u=' query param, " +
				"also returned by user/getUser and queryFamilyCloud). Optional: " +
				"when blank, the phone number is used where the server accepts it.",
			Advanced: true,
		}, {
			Name: "hard_delete",
			Help: "Delete permanently instead of moving files to the recycle bin.\n\n" +
				"Only applies to the personal space. 139 exposes no API to list or " +
				"restore the recycle bin, so a file deleted without this flag cannot " +
				"be recovered through rclone.",
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
				"Files larger than this are uploaded in parts of this size. Must be a multiple of 64 bytes; " +
				"the server rejects parts smaller than 5 MiB. The official client uses 100 MiB. Larger " +
				"parts mean fewer upload requests per file.",
			Default:  fs.SizeSuffix(personalPartSize),
			Advanced: true,
		}, {
			Name:     "upload_concurrency",
			Help:     "Concurrency for part uploads within a single file. A higher value speeds up large-file uploads at the cost of more concurrent connections.",
			Default:  4,
			Advanced: true,
		}, {
			Name: "region_code",
			Help: "Upload scheduling node code, in province:city form, e.g. 531:543 (Wuxi, Jiangsu).\n\n" +
				"The official PC client sends this with /file/create; when set, personal-space " +
				"uploads carry a userRegion so the server schedules a nearby upload CDN, which can " +
				"noticeably affect node choice and throughput. Leave blank to omit.",
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
				"that the server rejects with 04010319 (权益不足). Set a non-zero value to " +
				"force a specific cap regardless of the detected tier. Ignored when " +
				"no_member_check is set.",
			Default:  fs.SizeSuffix(0),
			Advanced: true,
		}, {
			Name: "no_member_check",
			Help: "Disable the automatic member-tier size check.\n\n" +
				"By default NewFs probes the account's member tier (vip userIdentity) " +
				"and skips files larger than the tier's single-file upload limit " +
				"(no-member 5G / silver 8G / gold 20G / diamond 500G). Set this to " +
				"skip that probe and upload files of any size, letting the server " +
				"reject an oversized file (04010319 权益不足) instead. Default false.",
			Default:  false,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default:  yun139DefaultEncoding,
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
	// Business errors (API rejections) are never retryable; transport errors
	// are - except the codes in transientAPICodes.
	var ae *apiError
	if errors.As(err, &ae) {
		if transientAPICodes[ae.Code] {
			return true, err
		}
		return false, err
	}
	return true, err
}

// transientAPICodes are business errors the 139 API answers for a request that
// succeeds when repeated, so they go through the pacer instead of ending the
// run.
//
// '02010002: 调用外部系统异常' was seen once while listing a deeply nested
// group directory; listing that same directory twice afterwards succeeded
// (2026-10-19), so treating it as terminal turns a server-side hiccup into a
// failed walk of a tree tens of thousands of objects deep.
//
// Deliberately absent: '1809112500: 系统内部错误', which is what the server
// answers when a shared-group id is sent to the family adapter (or the other
// way round). That is a misconfiguration, and retrying it only burns pacer
// attempts on a call that can never succeed.
var transientAPICodes = map[string]bool{
	"02010002": true,
}

// apiError is a business error returned by the 139 API.
type apiError struct {
	Code    string
	Message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("yun139: API error %s: %s", e.Code, e.Message)
}

// familyNotFoundCode is the family-space answer when modifyContentInfo is
// asked to rename content the server has not indexed yet. A rename issued
// immediately after the task that created the entry can see this, so the
// caller retries it for a bounded window.
const familyNotFoundCode = "1809111402"

// copyVisibilityTimeout bounds the wait for an entry a copy or move task
// just created to become visible, and the retry of a rename that races the
// server's indexing of it. It is a variable so tests can shorten it.
var copyVisibilityTimeout = 30 * time.Second

// copyVisibilityInterval is the pause between those visibility reads.
const copyVisibilityInterval = time.Second

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

// spaceQuotaErrorCode is the server rejection when the account has no storage
// left: /hcy/file/create and complete answer 04000012 资源配额不足 (measured
// 2026-10-02 as a full personal space filled mid-sync). It is a capacity
// rejection, not a request-rate one: the code appeared only after the last free
// bytes were consumed, and was absent from the preceding 70 minutes of far
// heavier API traffic.
const spaceQuotaErrorCode = "04000012"

// groupStalePathErrorCode is what the shared group's move task answers when the
// destination path it is handed does not exist: 1909011551 目录不存在
// (measured 2026-10-20). The task is the only group endpoint that reads the
// path field - the download link and the rename both resolve by content id and
// ignore it - so this arrives when a remote's cached path for the destination
// predates a move issued elsewhere. The move cannot be trusted to have
// happened, and a bandwidth move resolves the path from scratch.
const groupStalePathErrorCode = "1909011551"

func isGroupStalePathError(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Code == groupStalePathErrorCode
}

// groupSensitiveWordErrorCode is the shared group's answer when a name it is
// asked to create carries a word its filter rejects: 1909011520 敏感词校验失败
// (measured 2026-10-20, ~10 identical refusals per file in the refused
// directory). It is deterministic: the same name is refused every time, so
// retrying the files under it only repeats the answer.
const groupSensitiveWordErrorCode = "1909011520"

func isGroupSensitiveWordError(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Code == groupSensitiveWordErrorCode
}

// sensitiveFileNameErrorCode is the create endpoint's refusal of a file NAME
// that carries a word its filter rejects: 020101057 文件名包含敏感词 (measured
// 2026-10-21, three files refused, each retried the full ladder). The
// directory check uses a different code, so both are recognised.
const sensitiveFileNameErrorCode = "020101057"

func isSensitiveFileNameError(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Code == sensitiveFileNameErrorCode
}

// groupSensitiveSkipMessage is the line the migration executor's verify step
// turns into an --exclude pattern:
//
//	grep 'SKIP ' copy.log | sed -E 's/.*SKIP ([^:]+):.*/\1/'
//
// The trailing slash makes the pattern cover the whole directory, which is
// what the server refused to create. Group paths cannot contain a colon (the
// server rejects it), so the executor's first-colon split stays exact.
func groupSensitiveSkipMessage(dir string) string {
	return fmt.Sprintf("SKIP %s/: the server rejects a name in this path as a sensitive word (%s); skipping the subtree", dir, groupSensitiveWordErrorCode)
}

// groupDirSensitiveSkip turns the shared group's refusal of a blocked directory
// name into a skipped file.
//
// The refusal arrives while a file's parent directories are being created, and
// dirCache creates them without going through Mkdir, so it has to be recognised
// here too. Every file under the refused name hits it, so the pattern is the
// object's own parent rather than an ancestor: one line per failing directory,
// which excludes exactly the files the server will never accept.
//
// An empty or "." parent is the remote root, which the server does not refuse.
// The log line is withheld there: a caller turning it into an exclude pattern
// would then drop every object in the remote, so only the no-retry marking is
// applied.
func (f *Fs) groupDirSensitiveSkip(err error, dir string) error {
	if err == nil || f.space != spaceGroup || !isGroupSensitiveWordError(err) {
		return err
	}
	if dir != "" && dir != "." {
		fs.Logf(f, "%s", groupSensitiveSkipMessage(dir))
	}
	return fserrors.NoRetryError(err)
}

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
	// no_member_check disables the whole member-tier size guard: do not probe,
	// do not cache, and treat the limit as 0 (unlimited) so nothing is skipped
	// client-side. The server still rejects oversized files (04010319 权益不足).
	if f.opt.NoMemberCheck {
		f.memberMaxFileSize = 0
		return
	}
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
// no_member_check disables the guard entirely (always 0 / unlimited).
func (f *Fs) maxFileSize() int64 {
	if f.opt.NoMemberCheck {
		return 0
	}
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

// noSpaceError wraps the server 04000012 (资源配额不足) rejection - the account
// has no storage left - in a NoRetryError. Freeing space is a manual action
// taken outside the run, so neither the low-level retry ladder nor --retries
// can make the request succeed; retrying only turns a full account into a storm
// of identical failures. Unlike the member single-file limit, exhaustion can be
// reported by any call, so this applies to the control plane too.
// FIXME: a full shared group is unrecognised here. spaceQuotaErrorCode is the
// personal space's answer, and neither shared adapter has ever been observed
// returning it. A group that has run out of room therefore surfaces as an
// ordinary business error, which fails the transfer (business errors are not
// retried) but carries no "no space left" wording, so a caller cannot tell
// "stop retrying, this will never fit" from a transient rejection.
//
// How to collect the code without deliberately filling a group: every business
// failure already prints `yun139: API error <code>: <message>` (see apiError)
// and the migration CI tees its copy log, so after a run that ran out of room,
//
//	grep -o 'yun139: API error [0-9]*' copy.log | sort | uniq -c
//
// gives the histogram to add here.
func noSpaceError(err error) error {
	if err == nil || fserrors.IsNoRetryError(err) {
		return err
	}
	var ae *apiError
	if errors.As(err, &ae) && ae.Code == spaceQuotaErrorCode {
		fs.Errorf(nil, "yun139: the account has no space left (%s 资源配额不足): %v", ae.Code, err)
		return fserrors.NoRetryError(fmt.Errorf("yun139: the account has no space left (%s 资源配额不足): %w", ae.Code, err))
	}
	return err
}

// noRetryOnQuotaRejection wraps a server quota rejection in a NoRetryError so
// the low-level/upper retries do not resend the whole large file (each retry
// redoes every part PUT). Other errors pass through unchanged.
//
// 04010319 权益不足: the file exceeds the account's member single-file upload
// limit - only meaningful for an upload, so it is classified here and not on the
// control plane. 04000012 资源配额不足 is delegated to noSpaceError.
func noRetryOnQuotaRejection(err error) error {
	if err == nil || fserrors.IsNoRetryError(err) {
		return err
	}
	if e := noSpaceError(err); e != err {
		return e
	}
	var ae *apiError
	if errors.As(err, &ae) && ae.Code == memberQuotaErrorCode {
		fs.Errorf(nil, "yun139: upload rejected by member quota (%s 权益不足): %v", ae.Code, err)
		return fserrors.NoRetryError(fmt.Errorf("yun139: upload rejected by member quota (%s 权益不足): %w", ae.Code, err))
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
// emoji. The backend's default encoder encodes the control chars,
// leading/trailing space·dot·CR·LF·HT·VT, leading tilde/period, invalid UTF-8
// and all eight reserved characters, so none of them reach the server raw.
var yun139RejectedRunes = `"*:<>?\|`

// validateName checks a leaf name as it will be sent to the server.
//
// It returns an error wrapped with NoRetryError for names still containing one
// of yun139RejectedRunes. The default encoding escapes all eight to fullwidth
// substitutes, which the server stores verbatim, so this only fires when the
// encoding option is configured without the flags covering them. The
// NoRetryError wrapper stops --retries from re-running a whole sync round,
// which is pointless for a name that can never succeed.
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

// validateName checks the leaf name of remote against yun139's storage rules,
// encoding it first so the check sees the name the server would receive.
func (f *Fs) validateName(remote string) error {
	return validateName(f.opt.Enc.FromStandardName(path.Base(remote)))
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

	space   string // spacePersonal, spaceFamily or spaceGroup
	svcType string // svcTypePersonal or svcTypeFamily

	tokMu   *sync.Mutex // guards authorization + account (pointer so NewFs's tempF copy stays lock-safe)
	auth    string      // the raw base64 authorization token
	account string      // the phone number
	ts      *tokenState // account-wide shared token state (nil until NewFs sets it)

	hostMu       *sync.Mutex // guards personalHost
	personalHost string      // per-account personal cloud host, discovered once

	familyRootMu *sync.Mutex
	familyRootID string // server-side root catalog ID of the family cloud

	groupRootMu *sync.Mutex
	groupRootID string // server-side root catalog ID of the shared group

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
	uid := ""
	_, familyUID, err := f.queryFamilyCloud(ctx)
	if err != nil {
		return "", err
	}
	uid = familyUID
	if uid == "" {
		// No family cloud to learn from: an account that only shares in
		// groups answers queryFamilyCloud with an empty list (verified
		// 2026-10-19), and queryGroupV2 carries the same id.
		_, groupUID, gerr := f.queryGroup(ctx)
		if gerr != nil {
			return "", gerr
		}
		uid = groupUID
	}
	if uid == "" {
		return "", errors.New("yun139: neither queryFamilyCloud nor queryGroupV2 returned an accountUserId")
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

// groupHeaders returns the header set the 139 PC client uses for a shared
// group, split the way the client splits it (captured 2026-10-19):
//
//   - /hcy/mutual/adapter/isbo/openApi/* are issued by the Electron main
//     process, so they carry the full browser-like set (app_cp, cp_version,
//     mCloud UA, x-ExpRoute-Code, x-yun-svc-type: 1) plus x-yun-uni.
//   - /hcy/group/dynamic/* (upload, complete) and getGroupFileDownLoadURLV2 come
//     from the native module: short UA, x-DeviceInfo, x-huawei-channelSrc, and on
//     the download call x-MM-Source, x-deviceID and x-yun-svc-type: 2.
//
// x-yun-uni is the 19-digit user domain id and appears on every captured group
// request. It is empty until NewFs has learned it, and an empty value is sent
// as an empty header rather than guessed at: the server tolerates it (the
// family adapter never sends it), and inventing one would be worse than none.
func (f *Fs) groupHeaders(p, ts, randStr, sign string) map[string]string {
	if strings.HasSuffix(p, "/getGroupFileDownLoadURLV2") || strings.Contains(p, "/hcy/group/dynamic/") {
		// Native module: short UA, x-DeviceInfo, x-huawei-channelSrc, and on the
		// download-signing call x-MM-Source, x-deviceID, x-yun-svc-type: 2.
		//
		// x-yun-url-type: 3 belongs to this set and is the switch that makes the
		// signed link come back on an *.eos.yun.139.com host rather than a bare
		// cmecloud EOS host (measured both ways on 2026-10-19: without it the
		// link is ykj-eos-wx2-01.eos-wuxi-3.cmecloud.cn, with it
		// ykj-eos-*.eos.yun.139.com). Dropping it is not cosmetic - the direct
		// link gateway's rules key on the host.
		h := pcHeaders(f.accessToken(), f.account, ts, randStr, sign, f.svcType)
		h["x-DeviceInfo"] = f.deviceInfoHeader()
		h["x-huawei-channelSrc"] = pcAppChannel
		h["x-yun-url-type"] = "3"
		h["x-yun-uni"] = f.userDomainID
		if strings.HasSuffix(p, "/getGroupFileDownLoadURLV2") {
			h["x-MM-Source"] = "001"
			h["x-deviceID"] = strings.ToUpper(md5hex(f.account))
			h["x-yun-svc-type"] = "2"
			h["Accept"] = "text/html, image/gif, image/jpeg, *; q=.2, */*; q=.2"
		}
		return h
	}
	// Electron main process: the isbo/openApi metadata calls.
	//
	// batchTrash and task/get are the exception to this split - they live under
	// /hcy/group/dynamic/ so they take the native set above, while the capture
	// saw them issued from the browser with the set below. Both variants are
	// accepted by the server (verified 2026-10-19).
	h := pcHeadersFull(f.accessToken(), f.account, md5hex(f.account)+"-ENDIN")
	h["x-yun-uni"] = f.userDomainID
	return h
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

// delSubtree drops the cached path of a directory that moved and of everything
// under it: the paths are chains of ids, so every entry in the subtree has to be
// relearned from its new parent listing.
//
// oldPath is the directory's path before the move. When it is not known - the
// directory was never listed through this Fs - the moved id is matched as a path
// segment instead, so the subtree is still found and an untouched sibling is
// not: the match is on whole segments, never on a bare prefix, because catalog
// ids are prefixes of each other.
func (c *srvPathCache) delSubtree(oldPath, id string) {
	inSubtree := func(v string) bool {
		if oldPath != "" {
			return v == oldPath || strings.HasPrefix(v, oldPath+"/")
		}
		// Every cached value is a "root:/..." chain, so the moved id can only
		// appear as a trailing or interior segment - never as the whole value.
		return strings.HasSuffix(v, "/"+id) || strings.Contains(v, "/"+id+"/")
	}
	c.mu.Lock()
	for k, v := range c.m {
		if inSubtree(v) {
			delete(c.m, k)
		}
	}
	c.mu.Unlock()
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
	if host == "group.yun.139.com" && f.space == spaceGroup {
		// Shared group: the official client sends two different header sets on
		// this host (Electron main process for the isbo adapter, native module
		// for the dynamic/upload/download endpoints), so match them per call.
		p := strings.SplitN(url, "?", 2)[0]
		headers = f.groupHeaders(p, ts, randStr, sign)
	} else if host == "group.yun.139.com" {
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
		return noSpaceError(&apiError{Code: famEnv.Result.ResultCode, Message: famEnv.Result.ResultDesc})
	}
	if env.Code != "" && !env.Success {
		// Some endpoints reply success=false but carry a nested
		// resultCode=0 payload that actually succeeded (family move);
		// surface the message but keep it non-retryable.
		return noSpaceError(&apiError{Code: env.Code, Message: env.Message})
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

// ------------------------------------------------------- shared group -----

// groupCall issues one shared-group request. Groups live on the same host as
// family clouds and are served the same header set, so this differs from
// familyCall only in namespace: group metadata sits under
// /hcy/mutual/adapter/isbo/openApi, family metadata under
// /hcy/family/adapter/andAlbum/openApi.
func (f *Fs) groupCall(ctx context.Context, pathname string, body any, out any) error {
	return f.call(ctx, api.FamilyBaseURL+pathname, body, out)
}

// groupAccountInfo returns the account envelope the isbo adapter accepts.
//
// Unlike the family adapter, which wants commonAccountInfo and works with an
// empty userDomainId, the group adapter is keyed on accountName = the phone
// number, and the calls that need the 19-digit domain id (tasks, complete,
// trash) carry it as a top-level accountUserId instead.
func (f *Fs) groupAccountInfo() map[string]any {
	return map[string]any{"accountName": f.account, "accountType": 1}
}

// groupListBody builds a queryGroupContentListV2 request for one directory.
//
// Paging is a closed ordinal window (startNumber..endNumber), not the
// pageNum/pageSize pair the family adapter takes. dirType 7, catalogType 0
// and businessType 1 are the values the PC client sends for a normal group
// folder view; filterType must stay 0 or the server drops the files and only
// answers with folders.
func (f *Fs) groupListBody(catalogID, dirPath string, start, end int) map[string]any {
	return map[string]any{
		"dirType":         7,
		"catalogID":       catalogID,
		"groupID":         f.opt.GroupID,
		"path":            dirPath,
		"accountInfo":     f.groupAccountInfo(),
		"startNumber":     start,
		"endNumber":       end,
		"catalogSortType": 0,
		"contentSortType": 0,
		"sortDirection":   1,
		"catalogType":     0,
		"businessType":    1,
		"filterType":      0,
	}
}

// queryGroup returns the user's shared-group list from
// POST /hcy/mutual/adapter/isbo/openApi/queryGroupV2, plus the 19-digit user
// id the first entry carries.
//
// The client asks for the whole list in one page; groupList is empty when the
// account shares in no group, which is a real answer rather than a token
// problem (the same call with a family-only account returns an empty list
// here and a full familyCloudList from queryFamilyCloud).
func (f *Fs) queryGroup(ctx context.Context) ([]api.Group, string, error) {
	body := map[string]any{
		"accountInfo": f.groupAccountInfo(),
		"pageParameter": map[string]any{
			"isReturnTotal": "1",
			"pageNum":       1,
			"pageSize":      10000,
		},
	}
	var resp struct {
		GroupList []api.Group `json:"groupList"`
	}
	if err := f.groupCall(ctx, "/hcy/mutual/adapter/isbo/openApi/queryGroupV2", body, &resp); err != nil {
		return nil, "", fmt.Errorf("yun139: queryGroupV2: %w", err)
	}
	userID := ""
	for _, g := range resp.GroupList {
		if g.AccountInfo.UserID != "" {
			userID = g.AccountInfo.UserID
			break
		}
	}
	return resp.GroupList, userID, nil
}

// resolveGroupID ensures f.opt.GroupID is set, auto-discovering it from
// queryGroup when the user did not configure group_id.
//
// It is called once at NewFs time (freezing GroupID before any operation can
// race on it) and again inside groupRoot, which keeps the lock held so the
// write can never race with a reader.
func (f *Fs) resolveGroupID(ctx context.Context) error {
	if f.opt.GroupID != "" {
		return nil
	}
	groups, _, err := f.queryGroup(ctx)
	if err != nil {
		return err
	}
	switch len(groups) {
	case 0:
		return errors.New("yun139: account belongs to no shared group; create or join one in the 139 client first")
	case 1:
		f.opt.GroupID = groups[0].GroupID
	default:
		// Group names repeat freely (they are set by whoever created
		// the group), so only the id selects unambiguously.
		ids := make([]string, 0, len(groups))
		for _, g := range groups {
			ids = append(ids, fmt.Sprintf("%s (%s)", g.GroupID, g.GroupName))
		}
		return fmt.Errorf("yun139: account belongs to %d shared groups: %s; set --yun139-group-id to the one you want", len(groups), strings.Join(ids, ", "))
	}
	return nil
}

// groupRoot discovers the server-side root catalog ID of the shared group.
//
// A group root has no empty-path form: the client addresses it with
// catalogID = the numeric group id and path = the fixed mount prefix plus
// that id, and the response's parentCatalogID names the opaque root catalog
// every child hangs under. That catalog id, not the group id, is what the
// dircache stores as the root.
func (f *Fs) groupRoot(ctx context.Context) (string, error) {
	f.groupRootMu.Lock()
	defer f.groupRootMu.Unlock()
	if f.groupRootID != "" {
		return f.groupRootID, nil
	}
	if err := f.resolveGroupID(ctx); err != nil {
		return "", err
	}
	var resp struct {
		GetGroupContentResult struct {
			ParentCatalogID string `json:"parentCatalogID"`
		} `json:"getGroupContentResult"`
	}
	body := f.groupListBody(f.opt.GroupID, api.GroupRootPathPrefix+"/"+f.opt.GroupID, 1, 1)
	if err := f.groupCall(ctx, "/hcy/mutual/adapter/isbo/openApi/queryGroupContentListV2", body, &resp); err != nil {
		return "", err
	}
	id := strings.TrimSpace(resp.GetGroupContentResult.ParentCatalogID)
	if id == "" {
		return "", errors.New("yun139: group root response carried no parentCatalogID")
	}
	f.groupRootID = id
	// Children are addressed as "root:/<parent path>/<child id>", so the
	// root's own server path is what every first-level listing must build
	// from. Cache it here rather than re-deriving it per call.
	f.srvPathOf.put(id, "root:/"+id)
	return id, nil
}

// groupSrvPath returns the server path ("root:/...") of the directory dirID,
// or "" when it has not been learned from a listing yet.
func (f *Fs) groupSrvPath(dirID string) string {
	rootID, err := f.groupRoot(context.Background())
	if err != nil || rootID == "" {
		return ""
	}
	if dirID == rootID {
		return "root:/" + rootID
	}
	p, ok := f.srvPathOf.get(dirID) // cached from listGroup
	if ok {
		return p
	}
	// The parent is not derivable from an id alone; the caller has to list
	// the parent directory first, which is what fills srvPathOf.
	return ""
}

// listGroup lists the entries of a shared-group directory.
//
// One request returns folders and files together, so a walk costs one call
// per directory. The server labels each entry with its own "root:/..." path,
// which is why the backend can descend without guessing at the addressing.
func (f *Fs) listGroup(ctx context.Context, dirID string, fn func(listEntry) bool) error {
	rootID, err := f.groupRoot(ctx)
	if err != nil {
		return err
	}
	// The group root is addressed by the numeric group id under the fixed
	// mount path; any other directory by its catalog id and the server
	// path learned from the parent's listing.
	catalogID, reqPath := dirID, f.groupSrvPath(dirID)
	if dirID == rootID {
		catalogID = f.opt.GroupID
		reqPath = api.GroupRootPathPrefix + "/" + f.opt.GroupID
	} else if reqPath == "" {
		return fmt.Errorf("yun139: server path of group directory %s is not known; list its parent first", dirID)
	}
	dirPath := f.srvPathOfDirPath(dirID, rootID)
	start := 1
	seen := 0 // raw entries returned so far, across all pages of this listing
	for page := 0; page < maxListPages; page++ {
		end := start + listPageSize - 1
		var resp struct {
			GetGroupContentResult struct {
				NodeCount       int    `json:"nodeCount"`
				CtlgCnt         int    `json:"ctlgCnt"`
				ContCnt         int    `json:"contCnt"`
				IsCtlgCompleted int    `json:"isCtlgCompleted"`
				IsCntCompleted  int    `json:"isCntCompleted"`
				ParentCatalogID string `json:"parentCatalogID"`
				CatalogList     []struct {
					CatalogID   string `json:"catalogID"`
					CatalogName string `json:"catalogName"`
					UpdateTime  string `json:"updateTime"`
					Path        string `json:"path"`
					Tombstoned  int    `json:"tombstoned"`
					Moved       int    `json:"moved"`
				} `json:"catalogList"`
				ContentList []struct {
					ContentID   string `json:"contentID"`
					ContentName string `json:"contentName"`
					ContentSize int64  `json:"contentSize"`
					UpdateTime  string `json:"updateTime"`
					Tombstoned  int    `json:"tombstoned"`
					Moved       int    `json:"moved"`
				} `json:"contentList"`
			} `json:"getGroupContentResult"`
		}
		err := f.pacer.Call(func() (bool, error) {
			err := f.groupCall(ctx, "/hcy/mutual/adapter/isbo/openApi/queryGroupContentListV2", f.groupListBody(catalogID, reqPath, start, end), &resp)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return err
		}
		r := &resp.GetGroupContentResult
		// Structural check: a child listing must answer as the directory we
		// asked about. If it ever answers with a different parent, the
		// catalogID/path pairing is wrong and the entries belong to some other
		// directory - better to stop here than to walk (and later delete) a
		// tree we mis-identified. The group root is excluded because it is
		// addressed by the numeric group id while the reply names its catalog.
		if dirID != rootID && r.ParentCatalogID != "" && !strings.EqualFold(r.ParentCatalogID, dirID) {
			return fmt.Errorf("yun139: group listing of %s came back as the contents of %s", dirID, r.ParentCatalogID)
		}
		n := 0
		for i := range r.CatalogList {
			c := &r.CatalogList[i]
			if c.CatalogID == "" {
				continue
			}
			n++
			// A trashed entry can keep showing in a listing until the
			// delete task settles; only the flags say so.
			if c.Tombstoned != 0 || c.Moved != 0 {
				continue
			}
			modTime, _ := api.ParseTime(c.UpdateTime)
			childPath := c.Path
			if childPath == "" {
				childPath = path.Join(dirPath, c.CatalogID)
			}
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
		for i := range r.ContentList {
			c := &r.ContentList[i]
			if c.ContentID == "" {
				continue
			}
			n++
			if c.Tombstoned != 0 || c.Moved != 0 {
				continue
			}
			modTime, _ := api.ParseTime(c.UpdateTime)
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
		seen += n
		// A short window ends the listing. This is the only termination signal
		// that can be trusted: the isCtlgCompleted/isCntCompleted flags have
		// been seen 0 for a genuinely complete empty directory (2026-10-19),
		// and nodeCount is not a settled total either - a directory was
		// captured with nodeCount=3 while only 2 entries were listable, in the
		// window right after a delete. Paging on nodeCount would either skip
		// the ordinals this short page did not return or spin to the page cap,
		// so it is observed and reported instead of acted on.
		if n < end-start+1 {
			if r.NodeCount > seen {
				fs.Debugf(f, "yun139: group directory %s: stopped after %d entries but nodeCount said %d (delete-consistency window?)", dirID, seen, r.NodeCount)
			}
			return nil
		}
		start = end + 1
	}
	return fmt.Errorf("yun139: group listing of %s did not terminate after %d pages", dirID, maxListPages)
}

// srvPathOfDirPath returns the server path of dirID as it must appear as the
// parent of its children. groupRoot caches the root's own path, so the root
// needs no special case here beyond the fallback for an uncached id.
func (f *Fs) srvPathOfDirPath(dirID, rootID string) string {
	if p, ok := f.srvPathOf.get(dirID); ok {
		return p
	}
	if dirID == rootID {
		return "root:/" + rootID
	}
	return ""
}

// groupMkdir creates a directory inside a shared group.
//
// POST .../createGroupCatalog takes the parent as parentCatalogID plus the
// parent's server path, and answers with the new catalogID. The group root is
// addressed the same way a listing addresses it: parentCatalogID is the
// numeric group id and path is the fixed mount prefix, not "root:/...".
func (f *Fs) groupMkdir(ctx context.Context, parentID, leaf string) (string, error) {
	rootID, err := f.groupRoot(ctx)
	if err != nil {
		return "", err
	}
	parentCatalogID, parentPath := parentID, f.groupSrvPath(parentID)
	if parentID == rootID {
		parentCatalogID = f.opt.GroupID
		parentPath = api.GroupRootPathPrefix + "/" + f.opt.GroupID
	} else if parentPath == "" {
		return "", fmt.Errorf("yun139: server path of group directory %s is not known; list its parent first", parentID)
	}
	var resp struct {
		CatalogID string `json:"catalogID"`
	}
	body := map[string]any{
		"catalogName":     leaf,
		"parentCatalogID": parentCatalogID,
		"path":            parentPath,
		"groupID":         f.opt.GroupID,
		"manualRename":    0,
		"accountInfo":     f.groupAccountInfo(),
	}
	err = f.pacer.Call(func() (bool, error) {
		return shouldRetry(ctx, f.groupCall(ctx, "/hcy/mutual/adapter/isbo/openApi/createGroupCatalog", body, &resp))
	})
	if err != nil {
		return "", err
	}
	if resp.CatalogID == "" {
		return "", errors.New("yun139: createGroupCatalog returned no catalogID")
	}
	// Cache the child path so the new directory is listable without
	// re-listing the parent.
	f.srvPathOf.put(resp.CatalogID, path.Join(f.srvPathOfDirPath(parentID, rootID), resp.CatalogID))
	return resp.CatalogID, nil
}

// groupTaskPoll waits for a group-side async task (trash, and the move task
// once it is wired) to leave the running state.
//
// The group task endpoint reports the same triple the batch task does:
// taskStatus 2 = finished, taskResultCode 1 = success, progress 100.
func (f *Fs) groupTaskPoll(ctx context.Context, taskID string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		var out struct {
			TaskStatus     int  `json:"taskStatus"`
			TaskResultCode *int `json:"taskResultCode"`
		}
		body := map[string]any{
			"taskId":        taskID,
			"groupId":       f.opt.GroupID,
			"accountUserId": f.userDomainID,
		}
		err := f.pacer.Call(func() (bool, error) {
			err := f.groupCall(ctx, "/hcy/group/dynamic/task/get", body, &out)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return fmt.Errorf("yun139: poll group task: %w", err)
		}
		switch out.TaskStatus {
		case 0, 1:
			// created / running
		case 2:
			if out.TaskResultCode != nil && *out.TaskResultCode == 1 {
				return nil
			}
			return fmt.Errorf("yun139: group task %s finished with result code %v", taskID, out.TaskResultCode)
		case 3:
			return fmt.Errorf("yun139: group task %s failed", taskID)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("yun139: group task %s did not finish in 60s", taskID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// groupTrash moves one entry to the group recycle bin.
//
// Deleting is asynchronous. It goes to the group recycle bin, which really does
// exist - the PC client shows it and restores from it - but whose API is not
// known, so from rclone the bin can be neither listed nor emptied and a deleted
// entry is only recoverable by hand in the client or by re-uploading from the
// source. hard_delete does not apply here.
//
// Both entries and directories go through it: trashing a three-level tree
// (files plus the two parent catalogs) and re-listing the root left nothing
// behind (verified 2026-10-19 against a live group).
func (f *Fs) groupTrash(ctx context.Context, id string) error {
	var out struct {
		TaskID string `json:"taskId"`
	}
	body := map[string]any{
		"accountUserId": f.userDomainID,
		"groupId":       f.opt.GroupID,
		"fileIdList":    []string{id},
	}
	err := f.pacer.Call(func() (bool, error) {
		err := f.groupCall(ctx, "/hcy/group/dynamic/recyclebin/batchTrash", body, &out)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return err
	}
	if out.TaskID == "" {
		// batchTrash always answers with a taskId (captured 2026-10-19). A
		// success reply carrying none means the server accepted the call but
		// scheduled no delete, so treating it as success would let Rmdir,
		// Purge and Remove report a deletion that never happened.
		return errors.New("yun139: group batchTrash returned no taskId")
	}
	return f.groupTaskPoll(ctx, out.TaskID)
}

// groupRename renames a group entry.
//
// POST .../modifyGroupContent answers with the bare success envelope: it does
// not echo the stored name, so a rename the server auto-suffixed cannot be
// detected from the reply the way the family space detects one. manualRename 0
// asks the server to fail on a taken name instead of renaming around it.
//
// fileType is only ever observed as "file"; the value for a directory is
// unverified.
func (f *Fs) groupRename(ctx context.Context, id, newName, dirID string) error {
	return f.groupRenameEntry(ctx, id, newName, dirID, "file")
}

// groupRenameEntry renames one group entry. fileType is "file" for an entry and
// "catalog" for a directory: modifyGroupContent renames a directory when it is
// given the catalog type, which the capture of a file rename does not show
// (measured 2026-10-20 against a live group).
func (f *Fs) groupRenameEntry(ctx context.Context, id, newName, dirID, fileType string) error {
	rootID, err := f.groupRoot(ctx)
	if err != nil {
		return err
	}
	own := f.srvPathOfDirPath(id, rootID)
	if own == "" {
		// The path of a freshly uploaded entry is not cached, so rebuild it
		// from the parent, which modifyGroupContent wants with the entry's
		// own id appended.
		parent := f.groupSrvPath(dirID)
		if parent == "" {
			return fmt.Errorf("yun139: server path of group entry %s is not known; list its parent first", id)
		}
		own = path.Join(parent, id)
	}
	body := map[string]any{
		"contentName":     newName,
		"contentID":       id,
		"fileType":        fileType,
		"parentCatalogID": dirID,
		"path":            own,
		"groupID":         f.opt.GroupID,
		"manualRename":    0,
		"accountInfo":     f.groupAccountInfo(),
	}
	return f.pacer.Call(func() (bool, error) {
		return shouldRetry(ctx, f.groupCall(ctx, "/hcy/mutual/adapter/isbo/openApi/modifyGroupContent", body, nil))
	})
}

// groupRenameCatalog renames a directory and then reads the name back from the
// parent listing.
//
// modifyGroupContent answers a bare success envelope, and a rename onto a name
// the directory already holds is parked under a suffixed name, so a caller that
// trusts the reply would report a subtree at a path the server does not hold.
func (f *Fs) groupRenameCatalog(ctx context.Context, id, wantStd, dirID string) error {
	if err := f.groupRenameEntry(ctx, id, f.opt.Enc.FromStandardName(wantStd), dirID, "catalog"); err != nil {
		return err
	}
	return f.confirmRename(ctx, dirID, id, wantStd, "")
}

// groupTaskAccountInfo returns the account block the group's batch-task
// endpoints take. It is keyed on the 19-digit userDomainId, unlike the one
// createGroupCatalog and the listing take (accountName = the phone number), so
// the two are not interchangeable (captures 2026-10-19).
func (f *Fs) groupTaskAccountInfo() map[string]any {
	return map[string]any{"userDomainId": f.userDomainID, "accountType": "1"}
}

// groupMoveDest works out how createBatchOprTask addresses a destination
// directory: its own server path plus its id.
//
// The group root is not named by a "root:/..." path: the listing and
// createGroupCatalog both address it with the fixed mount prefix plus the
// numeric group id, so a move into the root does the same. Any other directory
// is addressed with the path a listing gave it, which is what the PC client
// puts in destPath (capture 2026-10-19: moving a file into 新建文件夹 sent
// destPath "root:/<root>/<that directory>", the string the listing reported).
func (f *Fs) groupMoveDest(ctx context.Context, dirID string) (destPath, destDirectoryID string, err error) {
	rootID, err := f.groupRoot(ctx)
	if err != nil {
		return "", "", err
	}
	if dirID == rootID {
		return api.GroupRootPathPrefix + "/" + f.opt.GroupID, f.opt.GroupID, nil
	}
	p := f.groupSrvPath(dirID)
	if p == "" {
		return "", "", fmt.Errorf("yun139: server path of group directory %s is not known; list its parent first", dirID)
	}
	return p, dirID, nil
}

// groupMoveTask moves entries inside a shared group using its native batch
// move task and waits for the task to finish.
//
// Directories and files are separate fields: catalogList takes directory ids
// and contentList takes file ids. Putting a directory id in contentList is not
// a move of the directory.
func (f *Fs) groupMoveTask(ctx context.Context, ids []string, dstDirID string, dirs bool) error {
	if len(ids) == 0 {
		return errors.New("yun139: group move called with no ids")
	}
	destPath, destDirectoryID, err := f.groupMoveDest(ctx, dstDirID)
	if err != nil {
		return err
	}
	catalogList, contentList := []string{}, []string{}
	if dirs {
		catalogList = ids
	} else {
		contentList = ids
	}
	body := map[string]any{
		"accountInfo":     f.groupTaskAccountInfo(),
		"catalogList":     catalogList,
		"contentList":     contentList,
		"destPath":        destPath,
		"destType":        "2",
		"srcGroupID":      f.opt.GroupID,
		"srcType":         2,
		"taskType":        3,
		"destDirectoryId": destDirectoryID,
		"businessType":    1,
		"eventType":       "move",
	}
	var resp struct {
		TaskID string `json:"taskID"`
	}
	err = f.pacer.Call(func() (bool, error) {
		err := f.groupCall(ctx, "/hcy/mutual/adapter/isbo/openApi/createBatchOprTask", body, &resp)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return err
	}
	if resp.TaskID == "" {
		return errors.New("yun139: group move task returned no taskID")
	}
	return f.groupBatchTaskPoll(ctx, resp.TaskID, ids)
}

// groupTaskDeadline is how long a group batch task is waited for. It is a var so
// the unit tests can sit through a timeout without waiting a minute.
var groupTaskDeadline = 60 * time.Second

// groupTaskReportGrace is how long a finished task may take to start listing the
// entries it was given. Also a var for the tests.
var groupTaskReportGrace = 10 * time.Second

// groupBatchTaskPoll waits for a group batch task to leave the running state
// and reports the per-entry outcome.
//
// The task record alone is not enough: it finishes with taskResultCode 1 while
// individual entries carry a reason, so an entry that was refused is only
// visible in contentList/catalogList (capture 2026-10-19 shows reason "0" for
// the entries that moved).
func (f *Fs) groupBatchTaskPoll(ctx context.Context, taskID string, want []string) error {
	type entry struct {
		SrcID  string `json:"srcId"`
		Reason string `json:"reason"`
	}
	deadline := time.Now().Add(groupTaskDeadline)
	var reportDeadline time.Time
	for {
		var out struct {
			BatchOprTask struct {
				TaskStatus     int  `json:"taskStatus"`
				TaskResultCode *int `json:"taskResultCode"`
			} `json:"batchOprTask"`
			ContentList []entry `json:"contentList"`
			CatalogList []entry `json:"catalogList"`
		}
		body := map[string]any{
			"taskID":            taskID,
			"taskId":            taskID,
			"accountInfo":       f.groupTaskAccountInfo(),
			"commonAccountInfo": f.groupTaskAccountInfo(),
		}
		err := f.pacer.Call(func() (bool, error) {
			err := f.groupCall(ctx, "/hcy/mutual/adapter/isbo/openApi/queryBatchOprTaskDetailV3", body, &out)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return fmt.Errorf("yun139: poll group batch task: %w", err)
		}
		if out.BatchOprTask.TaskStatus >= 2 {
			code := out.BatchOprTask.TaskResultCode
			if code == nil || *code != 1 {
				if code == nil {
					return fmt.Errorf("yun139: group move task %s finished without a result code", taskID)
				}
				return fmt.Errorf("yun139: group move task %s finished with result code %d", taskID, *code)
			}
			reported := make(map[string]bool, len(out.ContentList)+len(out.CatalogList))
			for _, list := range [][]entry{out.ContentList, out.CatalogList} {
				for _, e := range list {
					// A task that is still running can already list its entries,
					// and until it finishes they carry no reason at all, so the
					// reason is only read once the task has finished: an empty
					// reason while running means "not indexed yet", not "refused".
					if e.Reason != "0" {
						return fmt.Errorf("yun139: group move task %s refused %s with reason %q", taskID, e.SrcID, e.Reason)
					}
					reported[e.SrcID] = true
				}
			}
			// A task that says nothing about the entries it was given is not
			// evidence that they moved. The entry list is indexed asynchronously,
			// as the rest of 139 is, so it is waited for - but on its own clock,
			// so a task that finishes late still gets its grace.
			var missing []string
			for _, id := range want {
				if !reported[id] {
					missing = append(missing, id)
				}
			}
			if len(missing) == 0 {
				return nil
			}
			if reportDeadline.IsZero() {
				reportDeadline = time.Now().Add(groupTaskReportGrace)
			}
			if time.Now().After(reportDeadline) {
				return fmt.Errorf("yun139: group move task %s finished without reporting %s", taskID, strings.Join(missing, ", "))
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("yun139: group move task %s did not finish in %v", taskID, groupTaskDeadline)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
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
	case spacePersonal, spaceFamily, spaceGroup:
	default:
		return nil, fmt.Errorf("yun139: space must be one of %q, %q, %q, got %q", spacePersonal, spaceFamily, spaceGroup, opt.Space)
	}
	if opt.Space == spaceGroup && opt.RootFolderID != "" {
		// A group entry is addressed by catalogID *plus* the server path
		// learned from its parent listing, so a bare root_folder_id cannot be
		// resolved. Refuse here rather than failing later with "server path of
		// group directory … is not known".
		return nil, errors.New("yun139: root_folder_id is not supported for the shared group space")
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
		space:     opt.Space,
		svcType:   svcTypePersonal,
		srvPathOf: newSrvPathCache(),
	}
	if opt.Space == spaceFamily || opt.Space == spaceGroup {
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
	if f.groupRootMu == nil {
		f.groupRootMu = &sync.Mutex{}
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
		// queryFamilyCloud returns it for an account with a family
		// cloud; an account that only shares in groups has none, so
		// fall back to the group list, which carries the same id in
		// accountInfo.userId.
		uid := ""
		if _, accountUserID, err := f.queryFamilyCloud(ctx); err == nil {
			uid = accountUserID
		}
		if uid == "" {
			if _, accountUserID, err := f.queryGroup(ctx); err == nil {
				uid = accountUserID
			}
		}
		if uid != "" {
			f.userDomainID = uid
			f.opt.UserDomainID = uid
			f.m.Set("user_domain_id", uid)
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
	if f.space == spaceGroup {
		// Same freeze-before-any-operation rule as family_id.
		if err := f.resolveGroupID(ctx); err != nil {
			return nil, err
		}
	}
	if rootID == "" {
		switch f.space {
		case spaceFamily:
			rootID, err = f.familyRoot(ctx)
		case spaceGroup:
			rootID, err = f.groupRoot(ctx)
		default:
			rootID = defaultRootID
		}
		if err != nil {
			return nil, err
		}
	}
	f.dirCache = dircache.New(f.root, rootID, f)

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

// sharedSpace reports whether the remote points at a space served on
// group.yun.139.com - a family cloud or a shared group. Both speak the same
// /hcy/group/dynamic/* upload, trash and task pipeline; only the space id and
// the groupType field differ, so the upload code branches on this rather than
// on a specific space.
func (f *Fs) sharedSpace() bool { return f.space != spacePersonal }

// spaceID returns the id of the configured shared space: family_id for a
// family cloud, group_id for a shared group. It is empty for the personal
// space, which has no such id.
func (f *Fs) spaceID() string {
	if f.space == spaceGroup {
		return f.opt.GroupID
	}
	return f.opt.FamilyID
}

// dynamicGroupType is the groupType field the /hcy/group/dynamic/* upload
// endpoints expect: 1 for a family cloud (captured 2026-09-03), 2 for a shared
// group (captured 2026-10-19).
func (f *Fs) dynamicGroupType() int {
	if f.space == spaceGroup {
		return 2
	}
	return 1
}

// spaceCall issues one request against the shared-space host
// (group.yun.139.com), which serves both the family adapter and the group
// adapter. The transport, header set and envelope handling are identical, so
// callers that branch on sharedSpace() rather than on a specific space use
// this instead of familyCall or groupCall.
func (f *Fs) spaceCall(ctx context.Context, pathname string, body any, out any) error {
	return f.call(ctx, api.FamilyBaseURL+pathname, body, out)
}

// spaceSrvPath returns the server path ("root:/...") of a shared-space
// directory, learned from a listing. It is empty for the personal space, whose
// objects are addressed by id alone.
func (f *Fs) spaceSrvPath(dirID string) string {
	switch f.space {
	case spaceFamily:
		return f.familySrvPath(dirID)
	case spaceGroup:
		return f.groupSrvPath(dirID)
	}
	return ""
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string { return f.name }

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string { return f.root }

// String converts this Fs to a string
func (f *Fs) String() string {
	switch f.space {
	case spaceFamily:
		return fmt.Sprintf("yun139 root '%s' (family %s)", f.root, f.opt.FamilyID)
	case spaceGroup:
		return fmt.Sprintf("yun139 root '%s' (group %s)", f.root, f.opt.GroupID)
	}
	return fmt.Sprintf("yun139 root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features { return f.features }

// Precision returns the precision of this Fs.
//
// ModTimeNotSupported is deliberate: the server stamps its own clock on every
// upload and ignores any client-supplied mtime, so a returned ModTime never
// matches the one the caller asked for. With a real precision rclone sees the
// mismatch, cannot fix it without re-uploading (SetModTime returns
// ErrorCantSetModTime), and so rewrites every file on every sync. Reporting
// "not supported" makes rclone compare on size instead - unchanged files are
// left alone. Use --checksum to compare content where the size matches.
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// Hashes returns the supported hash sets.
// The personal-space listing carries a server-computed SHA-256
// (contentHash) for every file, so SHA-256 is free there. The shared group
// answers it too, but not in the listing: Object.Hash fetches it with a
// one-byte ranged GET against the download link (see groupContentHash), which
// is why the backend keeps SlowHash set. The family listing exposes no hash
// and no such header has been verified for it, so the family space advertises
// none and rclone falls back to size+modtime comparison (check reports
// "no hashes supported" instead of aborting mid-way).
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
	switch f.space {
	case spaceFamily:
		return f.listFamily(ctx, dirID, fn)
	case spaceGroup:
		return f.listGroup(ctx, dirID, fn)
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
	leaf = f.opt.Enc.FromStandardName(leaf)
	if err := validateName(leaf); err != nil {
		return "", err
	}
	if f.space == spaceGroup {
		return f.groupMkdir(ctx, pathID, leaf)
	}
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
	if err != nil && f.space == spaceGroup && isGroupSensitiveWordError(err) {
		// Every file below this directory would hit the same refused name and
		// be retried the full ladder for nothing. Decline once, name the
		// subtree in the log so the migration's verify step can exclude it,
		// and stop the retries on this one file.
		fs.Logf(f, "%s", groupSensitiveSkipMessage(dir))
		return fserrors.NoRetryError(fmt.Errorf("yun139: directory %q: %w", dir, err))
	}
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
	if f.space == spaceGroup {
		rootID, err := f.groupRoot(ctx)
		if err != nil {
			return err
		}
		if dirID == rootID {
			return errors.New("yun139: cannot remove the group root")
		}
		if err := f.groupTrash(ctx, dirID); err != nil {
			return err
		}
		f.dirCache.FlushDir(dir)
		return nil
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
	// A shared group's listing carries no hash (digest:null, fileEtag:0), but
	// the EOS answer for a 1-byte ranged GET does, so ask the object itself.
	//
	// A probe that fails is reported as "no hash" rather than an error:
	// operations.checkHashes turns an empty hash into a size comparison for
	// that one file, while an error here would be counted and would mark the
	// file unequal - one unreachable object would then look like corruption in
	// a long `rclone check` run.
	if o.fs.space == spaceGroup {
		h, herr := o.fs.groupContentHash(ctx, o)
		if herr != nil {
			fs.Debugf(o, "yun139: group sha256 probe failed: %v", herr)
			return "", nil
		}
		if h != "" {
			o.hashMu.Lock()
			o.sha256 = h
			o.hashMu.Unlock()
		}
		return h, nil
	}
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

// groupContentHash returns the server-computed SHA-256 of a shared-group
// object without reading its content.
//
// A one-byte ranged GET against the object's download link answers with
// x-eos-hash-sha256 (mirrored in x-amz-content-sha256) for the WHOLE object,
// single-part and multi-part alike - measured 2026-10-19 on a 1 MiB object and
// a 12 MiB object uploaded in three 5 MiB parts, both byte-identical to the
// local sha256.
//
// HEAD is not usable: the presignature covers the HTTP method, so a HEAD
// against a GET-signed link returns an XML error page carrying no hash. The
// ETag is not usable either - it is "<md5>-<part count>", so for a multipart
// object it is not the content MD5.
//
// Cost is one link mint plus one 1-byte request per object, which is why the
// backend keeps SlowHash set: rclone will not reach for this hash unless the
// user asked for checksum comparison. A missing header or a refused link
// returns an empty string rather than an error, so one odd object cannot abort
// a whole `rclone check`.
func (f *Fs) groupContentHash(ctx context.Context, o *Object) (string, error) {
	url, err := f.fetchDownloadURL(ctx, o)
	if err != nil {
		return "", err
	}
	return f.contentHashFromLink(ctx, url)
}

// contentHashFromLink issues the one-byte ranged GET and reads the hash header
// off the response. It is split out so the header handling can be tested
// without minting a real link.
func (f *Fs) contentHashFromLink(ctx context.Context, url string) (string, error) {
	res, err := f.getURL(ctx, url, map[string]string{"Range": "bytes=0-0"})
	if err != nil {
		return "", err
	}
	defer func() { _ = res.Body.Close() }()
	switch res.StatusCode {
	case http.StatusPartialContent, http.StatusOK:
		// 200 means the edge ignored Range and began sending the object; the
		// headers still carry the hash and closing the body (deferred above)
		// keeps us from pulling the file down.
	default:
		return "", fmt.Errorf("yun139: hash probe: HTTP %s", res.Status)
	}
	h := res.Header.Get("x-eos-hash-sha256")
	if h == "" {
		h = res.Header.Get("x-amz-content-sha256")
	}
	return strings.ToLower(strings.TrimSpace(h)), nil
}

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
	if f.space == spaceGroup {
		// No recursive delete on the group side either: walk the tree and
		// trash the leaves first. Unlike the family walk this one reports
		// the first failure instead of swallowing it, so a half-deleted
		// directory is visible to the caller.
		if err := f.purgeGroupDir(ctx, dirID); err != nil {
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

// purgeGroupDir removes a group directory and everything under it, deepest
// first.
//
// The group API has no recursive delete, and trashing a directory that still
// holds entries leaves the two sides inconsistent, so the listing is taken
// before anything is deleted and children are trashed one by one.
func (f *Fs) purgeGroupDir(ctx context.Context, id string) error {
	var entries []listEntry
	err := f.listAll(ctx, id, func(e listEntry) bool {
		entries = append(entries, e)
		return false
	})
	if err != nil {
		return err
	}
	for _, e := range entries {
		var lerr error
		if e.isDir {
			lerr = f.purgeGroupDir(ctx, e.id)
		} else {
			lerr = f.groupTrash(ctx, e.id)
		}
		if lerr != nil {
			return lerr
		}
	}
	return f.groupTrash(ctx, id)
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

// About returns the account-wide quota.
//
// POST user-njs.yun.139.com/user/disk/quota/detail with {"userDomainId": ...}
// returns diskSize/freeDiskSize in MiB units (captured 2026-09-03: diskSize
// 701440 = 685 MiB... actually the captured values 701440/700028 look like MiB
// for a 685 GB drive, so scale by 1024*1024 when reporting).
//
// The endpoint is keyed on the user, not on a space, and a family cloud or a
// shared group draws on the same pool as the personal cloud (confirmed by the
// account owner 2026-10-19), so all three spaces answer these same numbers.
// They are the account's numbers, not the tree's: `rclone size remote:` is the
// only way to measure what one space actually holds, and neither shared space
// has a quota API of its own.
//
// The different nodes behind this endpoint have been seen reporting different
// figures for the same account, so treat one reading as a sample rather than a
// fact; a full drive can also stop reporting freeDiskSize at all.
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	if f.space != spacePersonal {
		fs.Debugf(f, "yun139: reporting the account-wide quota - the %s space has no quota API of its own", f.space)
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
//
// fileName is the name the server reported for the entry it created, empty
// when the reply omitted it. It is not defaulted to the requested name: the
// caller has to distinguish "the server said it stored this name" from "the
// server said nothing", and only the first is evidence of where the upload
// landed.
type uploadResult struct {
	fileID   string
	fileName string
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
		// The sentinel is already a NoRetryError, so nothing is retried; the
		// SKIP line is what lets a migration's verify step exclude the file
		// instead of reporting it missing from the destination.
		fs.Logf(f, "SKIP %s: this remote cannot store empty files; skipping", src.Remote())
		return nil, fs.ErrorCantUploadEmptyFiles
	}
	if limit := f.maxFileSize(); limit > 0 && size > limit {
		fs.Logf(f, "SKIP %s: %d bytes exceeds the member single-file upload limit of %d bytes (tier %q); skipping to avoid a wasted multi-part upload",
			src.Remote(), size, limit, f.memberLevelName)
		return nil, errTooLarge(src.Remote(), size, limit, f.memberLevelName)
	}
	leafStd, dirID, err := f.dirCache.FindPath(ctx, src.Remote(), true)
	if err != nil {
		return nil, f.groupDirSensitiveSkip(err, path.Dir(src.Remote()))
	}
	if err := f.validateName(src.Remote()); err != nil {
		return nil, err
	}
	leaf := f.opt.Enc.FromStandardName(leafStd)
	res, err := f.uploadFile(ctx, in, dirID, leaf, size)
	if err != nil {
		// The create call may have registered an entry before the upload
		// failed; drop it so a failed Put leaves nothing at the target path.
		if res != nil && res.fileID != "" {
			f.discardUpload(ctx, res.fileID, dirID, f.sharedSpace())
		}
		if isSensitiveFileNameError(err) {
			// Same shape as the directory refusal: a name the filter rejects is
			// rejected for good, and verify needs the file named to exclude it.
			fs.Logf(f, "SKIP %s: the server rejects this file name as a sensitive word (%s); skipping", src.Remote(), sensitiveFileNameErrorCode)
			return nil, fserrors.NoRetryError(err)
		}
		return nil, err
	}
	// /hcy/file/create never overwrites: a name the directory already holds
	// makes the server store the content under a suffixed name instead. Put
	// must write at the requested path, so land the upload there.
	landed, err := f.uploadedLeaf(ctx, dirID, res, leafStd)
	if err != nil {
		return nil, err
	}
	if landed != leafStd {
		if err := f.settleAutoRename(ctx, dirID, leafStd, res.fileID); err != nil {
			return nil, err
		}
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
	if f.sharedSpace() {
		o.serverPath = f.spaceSrvPath(dirID)
	}
	return o, nil
}

// settleAutoRename lands an upload the server stored under a different name
// at the requested leaf. /hcy/file/create never overwrites: when the leaf is
// already held, the new content goes to a suffixed name and the held entry
// stays put. The held entry is parked under a backup name, the new content is
// renamed into place, and the parked entry is then deleted. A failure before
// the new content is in place restores the parked entry's name, so the
// previous content is never lost.
func (f *Fs) settleAutoRename(ctx context.Context, dirID, leafStd, newID string) error {
	family := f.space == spaceFamily
	var held *listEntry
	var dirHeld bool
	err := f.listAll(ctx, dirID, func(e listEntry) bool {
		if strings.EqualFold(e.name, leafStd) {
			if e.isDir {
				dirHeld = true
			} else {
				held = &e
			}
			return true
		}
		return false
	})
	if err != nil {
		return err
	}
	if dirHeld {
		// A directory holds the requested name, so a file cannot take it and
		// the rename would be stored under an appended name instead. Report
		// that rather than a landing at a path that is still a directory.
		f.discardUpload(ctx, newID, dirID, family)
		return fmt.Errorf("yun139: cannot land %q: a directory holds that name", leafStd)
	}
	if held == nil || held.id == newID {
		// Nothing holds the leaf any more; only the name is left to set.
		if err := f.renameObject(ctx, newID, f.opt.Enc.FromStandardName(leafStd), dirID, family); err != nil {
			f.discardUpload(ctx, newID, dirID, family)
			return err
		}
		return nil
	}
	// The backup name is built from the held entry's own name, not from the
	// requested leaf: the two can differ in case (the held entry is matched
	// case-insensitively), and a backup built from the requested spelling would
	// make the server append the entry's real extension to it.
	backupLeaf := backupLeafName(held.name)
	// The held entry is parked so the upload can take the leaf. A failed park
	// does not mean the entry is still there: the server answers success when
	// it stores the entry under a name other than the requested one, so the
	// entry is located by id. Treating a moved entry as unmoved would leave it
	// parked under a name nothing cleans up; treating an unmoved one as moved
	// would abandon the leaf.
	heldAt := backupLeaf
	if err := f.renameObject(ctx, held.id, f.opt.Enc.FromStandardName(backupLeaf), dirID, family); err != nil {
		name, found, lerr := f.entryName(ctx, dirID, held.id)
		if lerr != nil || !found || strings.EqualFold(name, leafStd) {
			f.discardUpload(ctx, newID, dirID, family)
			return fmt.Errorf("yun139: park existing %q: %w", leafStd, err)
		}
		// The rename took effect under a name the reply did not report; the
		// leaf is free, so the upload can still take it.
		heldAt = name
	}
	if err := f.renameObject(ctx, newID, f.opt.Enc.FromStandardName(leafStd), dirID, family); err != nil {
		// Put the held entry back under its name, then drop the upload that
		// could not take its place.
		if rerr := f.renameObject(ctx, held.id, f.opt.Enc.FromStandardName(leafStd), dirID, family); rerr != nil {
			fs.Errorf(f, "yun139: restore %q after failed upload: %v (previous content remains at %q)", leafStd, rerr, heldAt)
		}
		f.discardUpload(ctx, newID, dirID, family)
		return err
	}
	// The new content is in place; drop the parked entry. A failure here
	// leaves the old content under the backup name, which the next upload
	// of this leaf removes.
	if err := f.deleteObject(ctx, held.id, held.srvPath, family); err != nil {
		fs.Errorf(f, "yun139: delete parked %q after upload: %v", heldAt, err)
	}
	f.cleanupOldBackups(ctx, dirID, held.name, newID)
	return nil
}

// discardUpload removes an upload that could not be given its requested name,
// so a failed Put leaves no entry the caller never asked for. dirID supplies
// the parent whose server-side path a family delete task addresses.
func (f *Fs) discardUpload(ctx context.Context, id, dirID string, family bool) {
	srvPath := ""
	if f.space == spaceFamily {
		// A family delete task addresses its parent by server path, which is
		// only known once that parent has been listed; a fresh upload has not
		// listed it. A shared group delete task is addressed by id alone, so
		// the path stays empty there.
		srvPath = f.familySrvPath(dirID)
		if srvPath == "" {
			if err := f.primeSrvPathFor(ctx, dirID); err != nil {
				fs.Debugf(f, "yun139: prime srvPath for %s: %v", dirID, err)
			}
			srvPath = f.familySrvPath(dirID)
		}
	}
	if err := f.deleteObject(ctx, id, srvPath, family); err != nil {
		fs.Errorf(f, "yun139: remove upload %s that could not take its name: %v", id, err)
	}
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

// sharedCreateBody maps the personal create payload onto the one
// /hcy/group/dynamic/file/create takes, which is the same field set plus
// {groupId, groupType, seqNo, expireSec} and no contentType (captured
// 2026-09-03 and 2026-10-19).
func (f *Fs) sharedCreateBody(body api.PersonalCreateReq, partInfos []api.PartInfo) map[string]any {
	fb := map[string]any{
		"contentHash":          body.ContentHash,
		"contentHashAlgorithm": body.ContentHashAlgorithm,
		"expireSec":            86400,
		"fileRenameMode":       body.FileRenameMode,
		"groupId":              f.spaceID(),
		"groupType":            f.dynamicGroupType(),
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
	return fb
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
	if f.sharedSpace() {
		callPath = "/hcy/group/dynamic/file/create"
		callBody = f.sharedCreateBody(body, partInfos)
	} else {
		callPath = "/hcy/file/create"
	}
	if f.sharedSpace() {
		if err := f.spaceCall(ctx, callPath, callBody, &resp); err != nil {
			return nil, noRetryOnQuotaRejection(fmt.Errorf("create: %w", err))
		}
	} else {
		if err := f.personalCall(ctx, callPath, callBody, &resp); err != nil {
			return nil, noRetryOnQuotaRejection(fmt.Errorf("create: %w", err))
		}
	}
	fs.Debugf(f, "yun139: create returned %d part URLs, rapid=%v exists=%v", len(resp.Data.PartInfos), resp.Data.RapidUpload, resp.Data.Exists)
	// rapidUpload success path: server already has the content (no part
	// URLs to fetch and no upload body to send), or the file already
	// exists under this name.
	if resp.Success && resp.Data.FileID != "" &&
		(resp.Data.RapidUpload || (resp.Data.Exists != nil && *resp.Data.Exists) || len(resp.Data.PartInfos) == 0) {
		return &uploadResult{fileID: resp.Data.FileID, fileName: resp.Data.FileName, hashHex: hashHex}, nil
	}
	if !resp.Success {
		return nil, noRetryOnQuotaRejection(&apiError{Code: resp.Code, Message: resp.Message})
	}
	if len(resp.Data.PartInfos) == 0 {
		return &uploadResult{fileID: resp.Data.FileID, fileName: resp.Data.FileName},
			fserrors.NoLowLevelRetryError(errors.New("create returned no upload URL"))
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
		if f.sharedSpace() {
			urlBody["groupId"] = f.spaceID()
			urlBody["groupType"] = f.dynamicGroupType()
		}
		var urlResp struct {
			api.BaseResp
			Data struct {
				PartInfos []api.PartUploadInfo `json:"partInfos"`
			} `json:"data"`
		}
		urlPath := "/hcy/file/getUploadUrl"
		if f.sharedSpace() {
			urlPath = "/hcy/group/dynamic/file/getUploadUrl"
		}
		callURL := func() (string, error) {
			if f.sharedSpace() {
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
			return &uploadResult{fileID: resp.Data.FileID, fileName: resp.Data.FileName}, fmt.Errorf("getUploadUrl: %w", err)
		}
		if !urlResp.Success {
			return &uploadResult{fileID: resp.Data.FileID, fileName: resp.Data.FileName}, noRetryOnQuotaRejection(&apiError{Code: urlResp.Code, Message: urlResp.Message})
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
		// A part that exhausted its retries cancels gctx. Stop launching
		// the rest: a 200 GiB file is thousands of parts and each launch
		// would otherwise still issue a doomed request.
		if gctx.Err() != nil {
			break
		}
		p := p
		g.Go(func() error {
			url, ok := urlOfPart[int(p.index)]
			if !ok {
				return fserrors.NoLowLevelRetryError(fmt.Errorf("yun139: no upload URL for part %d", p.index))
			}
			return f.putPart(gctx, int(p.index), len(parts), leaf, freader, p.offset, p.partSize, url)
		})
	}
	if err := g.Wait(); err != nil {
		return &uploadResult{fileID: resp.Data.FileID, fileName: resp.Data.FileName}, fmt.Errorf("put part: %w", err)
	}
	// The launch loop may have stopped before starting any part when the
	// caller's context was cancelled. Do not go on to complete a file that
	// was never uploaded.
	if err := ctx.Err(); err != nil {
		return &uploadResult{fileID: resp.Data.FileID, fileName: resp.Data.FileName}, err
	}
	// Finally, mark the file complete. Mirrors the official client:
	// contentHash + contentHashAlgorithm + fileId + uploadId.
	cmpl := api.PersonalCompleteReq{
		FileID:               resp.Data.FileID,
		UploadID:             resp.Data.UploadID,
		ContentHash:          hashHex,
		ContentHashAlgorithm: "SHA256",
	}
	if f.sharedSpace() {
		cmpl.GroupID = f.spaceID()
		cmpl.AccountUserID = f.userDomainID
	}
	cmplPath := "/hcy/file/complete"
	if f.sharedSpace() {
		cmplPath = "/hcy/group/dynamic/file/complete"
	}
	var cmplResp api.PersonalCompleteResp
	cmplHost := api.FamilyBaseURL
	if !f.sharedSpace() {
		var err error
		cmplHost, err = f.personalCloudHost(ctx)
		if err != nil {
			return nil, err
		}
	}
	if err := f.call(ctx, cmplHost+cmplPath, cmpl, &cmplResp); err != nil {
		return &uploadResult{fileID: resp.Data.FileID, fileName: resp.Data.FileName}, noRetryOnQuotaRejection(fmt.Errorf("complete: %w", err))
	}
	if !cmplResp.Success {
		return &uploadResult{fileID: resp.Data.FileID, fileName: resp.Data.FileName}, noRetryOnQuotaRejection(&apiError{Code: cmplResp.Code, Message: cmplResp.Message})
	}
	return &uploadResult{fileID: resp.Data.FileID, fileName: resp.Data.FileName, hashHex: hashHex}, nil
}

// partRetryMaxServerDelay caps a server-supplied Retry-After. The point of
// the part retry loop is to recover a flapping edge quickly, not to park a
// transfer for minutes waiting for a rate limiter to clear.
const partRetryMaxServerDelay = 30 * time.Second

// partRetryDelay returns how long to wait before retrying a part PUT.
// It is a variable so tests can remove the wait.
var partRetryDelay = func(attempt, partIdx int) time.Duration {
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
	// Stagger the parts of one file so a flapping edge does not see all
	// of them come back at the same instant. partIdx is stable.
	d += time.Duration(partIdx%5) * 100 * time.Millisecond
	// Add jitter so parts of different files, which share part numbers,
	// do not retry in lockstep either.
	jitter := time.Duration(mathrand.Int64N(int64(d)/2+1)) - d/4
	d += jitter
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}

// sleepCtx waits for d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// partSleep is a seam for tests that need to inspect the chosen delay.
var partSleep = sleepCtx

// partErrorBodyTimeout bounds how long we wait for a CDN error page. It
// deliberately does not cover the upload body: a 32Mi part on a slow link
// can take more than ten minutes and still make progress.
var partErrorBodyTimeout = 5 * time.Second

// readPartErrorBody reads a bounded error body, then closes it if the
// server stalls. Closing from this goroutine unblocks the reader.
func readPartErrorBody(ctx context.Context, body io.ReadCloser, limit int64) ([]byte, bool) {
	type result struct{ b []byte }
	ch := make(chan result, 1)
	go func() {
		b, _ := io.ReadAll(io.LimitReader(body, limit))
		_, _ = io.Copy(io.Discard, io.LimitReader(body, 1<<20))
		ch <- result{b: b}
	}()
	timer := time.NewTimer(partErrorBodyTimeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		return r.b, false
	case <-timer.C:
		_ = body.Close()
		return nil, true
	case <-ctx.Done():
		_ = body.Close()
		return nil, true
	}
}

// partRetryAfterError carries the server's Retry-After (already capped at
// partRetryMaxServerDelay) alongside a retryable status error.
type partRetryAfterError struct {
	err   error
	after time.Duration
}

func (e *partRetryAfterError) Error() string { return e.err.Error() }
func (e *partRetryAfterError) Unwrap() error { return e.err }

// partRetryAfterFinalError carries the last Retry-After through the
// NoLowLevel wrapper so cmd's --retries sleep honors it without letting
// copy.go re-upload the file.
type partRetryAfterFinalError struct {
	err error
	at  time.Time
}

func (e *partRetryAfterFinalError) Error() string         { return e.err.Error() }
func (e *partRetryAfterFinalError) Unwrap() error         { return e.err }
func (e *partRetryAfterFinalError) RetryAfter() time.Time { return e.at }

// parseRetryAfter understands both forms in RFC 9110: delay-seconds and
// an HTTP-date. It returns 0 when the header is absent or unusable.
func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}
	if secs, err := strconv.Atoi(header); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(header); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// isTransientPartStatus reports whether the edge says "try again later".
// S3-compatible stores use 400 RequestTimeout when the body stalls, so the
// status alone is not enough.
func isTransientPartStatus(status int, body string) bool {
	switch {
	case status == http.StatusRequestTimeout,
		status == http.StatusTooManyRequests,
		status >= 500:
		return true
	case status == http.StatusBadRequest:
		return strings.Contains(body, "RequestTimeout") ||
			strings.Contains(body, "SlowDown") ||
			strings.Contains(body, "InternalError") ||
			strings.Contains(body, "IncompleteBody")
	}
	return false
}

// isExpiredPartURLStatus reports a 400 that says the signed URL's
// credentials expired. A fresh create can mint a new URL, so this must
// not be permanent.
func isExpiredPartURLStatus(status int, body string) bool {
	if status != http.StatusBadRequest {
		return false
	}
	return strings.Contains(body, "RequestExpired") ||
		strings.Contains(body, "ExpiredToken") ||
		strings.Contains(body, "AuthorizationQueryParametersError")
}

// signedPartQuery matches the credential-bearing query parameters that a
// pre-signed part URL carries. CDN error pages sometimes echo the request
// URI, and copy.log is uploaded as a build artifact. The second pattern
// covers a body where the URL itself was percent-encoded.
var (
	signedPartQuery     = regexp.MustCompile(`(?i)(X-Amz-(?:Signature|Credential|Security-Token)=)[^&\s"']+`)
	signedPartQueryJSON = regexp.MustCompile(`(?i)("?(?:X-Amz-(?:Signature|Credential|Security-Token))"?\s*:\s*"?)[^",}\s]+`)
)

func redactPartBodyOnce(body, url string) string {
	if url != "" {
		body = strings.ReplaceAll(body, url, "<part-url>")
	}
	body = signedPartQuery.ReplaceAllString(body, "${1}<redacted>")
	return signedPartQueryJSON.ReplaceAllString(body, "${1}<redacted>")
}

func redactPartBody(body, url string) string {
	body = redactPartBodyOnce(body, url)
	// A percent-encoded body hides the parameter separators; decode it and
	// redact the readable form so credentials containing %2F/%2B/%3D do
	// not survive past the first %.
	if unescaped, err := neturl.QueryUnescape(body); err == nil && unescaped != body {
		body = redactPartBodyOnce(unescaped, url)
	}
	return body
}

// partPermanentError stops both retry ladders: the copy engine must not
// re-upload the file and --retries must not re-run the command. The
// NoLowLevel marker is what stops ShouldRetry even when the response body
// happens to contain a phrase from fserrors' retriable list.
func partPermanentError(err error) error {
	return fserrors.NoLowLevelRetryError(fserrors.NoRetryError(err))
}

// putPart PUTs a single part to its pre-signed URL, retrying this part
// only. The URL is issued for one part and stays valid for the upload's
// lifetime; the same URL accepts repeated PUTs of that part (confirmed
// 2026-10-21), so a retry does not need a fresh getUploadUrl.
//
// Attempts are bounded by --low-level-retries. When they are exhausted
// the error is marked NoLowLevelRetry, which stops the copy engine from
// re-uploading the whole file while leaving --retries free to re-run the
// sync with a fresh upload URL.
func (f *Fs) putPart(ctx context.Context, partIdx, total int, leaf string, freader io.ReaderAt, offset, size int64, url string) error {
	if url == "" {
		// A fresh create may return a usable URL, so leave the command-level
		// retry available.
		return fserrors.NoLowLevelRetryError(fmt.Errorf(
			"yun139: %s: upload part %d/%d: server returned an empty upload URL", leaf, partIdx, total))
	}
	attempts := max(fs.GetConfig(ctx).LowLevelRetries, 1)
	var lastErr error
	var retryAt time.Time
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := f.putPartOnce(ctx, partIdx, total, freader, offset, size, url)
		if err == nil {
			return nil
		}
		if fserrors.IsNoRetryError(err) || fserrors.IsNoLowLevelRetryError(err) {
			return err
		}
		lastErr = err
		retryAt = time.Time{}
		var retryAfter *partRetryAfterError
		if errors.As(err, &retryAfter) {
			retryAt = time.Now().Add(retryAfter.after)
		}
		if attempt == attempts {
			break
		}
		delay := partRetryDelay(attempt, partIdx)
		if retryAfter != nil && retryAfter.after > delay {
			delay = retryAfter.after
		}
		fs.Debugf(f, "yun139: %s: upload part %d/%d attempt %d/%d failed, retrying in %v: %v",
			leaf, partIdx, total, attempt, attempts, delay, err)
		if err := partSleep(ctx, delay); err != nil {
			return err
		}
	}
	final := fserrors.NoLowLevelRetryError(fmt.Errorf(
		"yun139: %s: upload part %d/%d failed after %d attempts: %w", leaf, partIdx, total, attempts, lastErr))
	if !retryAt.IsZero() {
		// Keep --retries from hammering a rate limiter: Stats sees RetryAfter
		// and cmd sleeps, while the NoLowLevel marker still stops copy.go
		// from re-uploading the whole file.
		return &partRetryAfterFinalError{err: final, at: retryAt}
	}
	return final
}

// putPartOnce issues one PUT for a part. Retryable failures come back as
// plain errors so putPart can try again; permanent ones are wrapped here.
func (f *Fs) putPartOnce(ctx context.Context, partIdx, total int, freader io.ReaderAt, offset, size int64, url string) error {
	r := io.NewSectionReader(freader, offset, size)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, r)
	if err != nil {
		// Do not echo the URL: the parse error would carry the signed query.
		// A fresh create may mint a usable URL, so allow the command retry.
		return fserrors.NoLowLevelRetryError(fmt.Errorf(
			"yun139: upload part %d/%d: server returned an invalid upload URL", partIdx, total))
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Origin", yunBaseURL)
	req.Header.Set("Referer", yunBaseURL+"/")
	req.Header.Set("User-Agent", defaultUserAgent)
	// ContentLength makes the Transport write the Content-Length header the
	// CDN's signature covers. Every attempt needs a fresh reader because the
	// transport consumes the body of a failed attempt.
	if size > 0 {
		req.ContentLength = size
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
	client := *f.httpClient
	// The URL is pre-signed for this exact host and method. Following a
	// redirect would turn a 301/302 into a bodyless GET or replay the
	// signature against another host, so surface the 3xx instead.
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	res, err := client.Do(req)
	if err != nil {
		return f.partTransportError(partIdx, total, host, peerAddr, err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		// Never let a stalled error page hold the retry loop: this helper
		// closes the body after partErrorBodyTimeout. The upload body is
		// not subject to this deadline.
		body, timedOut := readPartErrorBody(ctx, res.Body, 64<<10)
		bodyText := redactPartBody(string(body), url)
		err := fmt.Errorf("yun139: upload part %d/%d via %s -> %s: %s: %s",
			partIdx, total, host, peerAddr, res.Status, truncate(bodyText, 500))
		if timedOut && res.StatusCode == http.StatusBadRequest {
			// The body never arrived, so the 400 code is unknown. Do not
			// turn an unreadable transient response into a permanent one.
			return fserrors.NoLowLevelRetryError(err)
		}
		if isTransientPartStatus(res.StatusCode, bodyText) {
			if d := parseRetryAfter(res.Header.Get("Retry-After")); d > 0 {
				if d > partRetryMaxServerDelay {
					d = partRetryMaxServerDelay
				}
				return &partRetryAfterError{err: err, after: d}
			}
			return err
		}
		switch {
		case res.StatusCode >= 300 && res.StatusCode < 400,
			res.StatusCode == http.StatusUnauthorized,
			res.StatusCode == http.StatusForbidden,
			res.StatusCode == http.StatusNotFound,
			res.StatusCode == http.StatusConflict,
			res.StatusCode == http.StatusGone,
			isExpiredPartURLStatus(res.StatusCode, bodyText):
			// Do not replay this URL, but let --retries re-run the command:
			// a fresh create may mint a URL that works.
			return fserrors.NoLowLevelRetryError(err)
		default:
			// A malformed request, bad credentials or an oversized part:
			// retrying it, even with a new URL, cannot succeed.
			return partPermanentError(err)
		}
	}
	elapsed := time.Since(t0)
	fs.Debugf(f, "yun139: uploaded part %d/%d (%v) via %s -> %s (reused=%v) in %v (%s)",
		partIdx, total, fs.SizeSuffix(size), host, peerAddr, reusedConn,
		elapsed.Round(time.Millisecond), fs.SizeSuffix(float64(size)/elapsed.Seconds()).ByteRateUnit())
	return nil
}

// partTransportError strips the *url.Error layers added by http.Client.
// Their Error() strings contain the full pre-signed URL, including
// X-Amz-Signature, and copy.log is uploaded as a build artifact. The
// underlying error keeps its Timeout/Temporary methods so ShouldRetry can
// still classify it.
func (f *Fs) partTransportError(partIdx, total int, host, peerAddr string, err error) error {
	for {
		var ue *neturl.Error
		if !errors.As(err, &ue) {
			break
		}
		if ue.Err == nil {
			return fserrors.NoLowLevelRetryError(fmt.Errorf(
				"yun139: upload part %d/%d via %s -> %s: transport error", partIdx, total, host, peerAddr))
		}
		err = ue.Err
	}
	wrapped := fmt.Errorf("yun139: upload part %d/%d via %s -> %s: %w",
		partIdx, total, host, peerAddr, err)
	if !fserrors.ShouldRetry(err) {
		// A certificate error, a DNS failure or another permanent transport
		// error will not improve by replaying the same URL. Leave the
		// command-level retry available for a fresh create.
		return fserrors.NoLowLevelRetryError(wrapped)
	}
	return wrapped
}

// restoreOldName puts the old object back under the requested leaf after a
// failed update. The object is addressed by id, so it returns to the requested
// name even when a rename had stored it under a name this code did not choose.
// A restore that fails names the place the content was last seen, so it is
// still findable.
func (o *Object) restoreOldName(ctx context.Context, leaf, dirID, lastSeen string) {
	if err := o.fs.renameObject(ctx, o.id, leaf, dirID, o.fs.space == spaceFamily); err != nil {
		fs.Errorf(o, "yun139: restore old name %q after failed update: %v (old content remains at %q)", leaf, err, lastSeen)
	}
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
		fs.Logf(o.fs, "SKIP %s: this remote cannot store empty files; skipping", src.Remote())
		return fs.ErrorCantUploadEmptyFiles
	}
	leafStd, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return o.fs.groupDirSensitiveSkip(err, path.Dir(o.remote))
	}
	if err := o.fs.validateName(o.remote); err != nil {
		return err
	}
	leaf := o.fs.opt.Enc.FromStandardName(leafStd)

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
	// The backup name is built in the STANDARD domain and encoded once: the
	// encoder is not a fixed point for every leaf (an invalid UTF-8 byte
	// encodes to a multi-rune escape group), so encoding an already-encoded
	// leaf would produce a name that decodes to something else.
	backupLeaf := backupLeafName(leafStd)
	family := o.fs.space == spaceFamily
	oldID := o.id
	oldAt := backupLeaf
	// The old entry is parked before the upload so a failed update never
	// leaves the requested path empty. A failed pre-rename does not mean the
	// rename did not happen: the server answers success when it stores the
	// entry under a name other than the requested one, so the entry is looked
	// up by id to learn where it actually is. Assuming either way loses the
	// old content - assuming it moved leaves it parked under a name the
	// restore arm cannot find, assuming it stayed skips the restore.
	oldMoved := false
	if err := o.fs.renameObject(ctx, o.id, o.fs.opt.Enc.FromStandardName(backupLeaf), dirID, family); err != nil {
		name, found, lerr := o.fs.entryName(ctx, dirID, o.id)
		switch {
		case lerr != nil:
			return fmt.Errorf("yun139: update %q: pre-rename failed (%v) and the old entry could not be located: %w", leaf, err, lerr)
		case !found:
			return fmt.Errorf("yun139: update %q: pre-rename failed (%v) and the old entry is not in its directory", leaf, err)
		case strings.EqualFold(name, leafStd):
			// The rename did not take effect: the old content still holds
			// the requested name, so the upload has to win it by itself.
			fs.Debugf(o, "yun139: pre-rename failed (%v), uploading under the target name", err)
		default:
			// The rename took effect under a name the reply did not report.
			fs.Debugf(o, "yun139: pre-rename stored the old entry as %q", name)
			oldMoved = true
			oldAt = name
		}
	} else {
		oldMoved = true
	}
	res, err := o.fs.uploadFile(ctx, in, dirID, leaf, size)
	if err != nil {
		// Upload failed: drop the entry it may have created, then put the old
		// content back under the name the caller asked for. A create reply
		// that names the entry the receiver came from is a dedup answer, not
		// a new entry: discarding that id would destroy the old content,
		// wherever the pre-rename left it.
		if res != nil && res.fileID != "" && res.fileID != oldID {
			o.fs.discardUpload(ctx, res.fileID, dirID, family)
		}
		if oldMoved {
			o.restoreOldName(ctx, leaf, dirID, oldAt)
		}
		if isSensitiveFileNameError(err) {
			fs.Logf(o.fs, "SKIP %s: the server rejects this file name as a sensitive word (%s); skipping", o.remote, sensitiveFileNameErrorCode)
			return fserrors.NoRetryError(err)
		}
		return err
	}
	// The upload must end up under the target name. A name the server still
	// holds (the pre-rename failed, or a concurrent upload) makes it store the
	// content under a suffixed name; landing it is the same operation Put
	// performs, and it is lossless because settleAutoRename only drops the
	// entry it parked.
	landed, err := o.fs.uploadedLeaf(ctx, dirID, res, leafStd)
	if err != nil {
		if oldMoved {
			o.restoreOldName(ctx, leaf, dirID, oldAt)
		}
		return err
	}
	if landed != leafStd {
		if serr := o.fs.settleAutoRename(ctx, dirID, leafStd, res.fileID); serr != nil {
			if oldMoved {
				// Put the old object back under the name the caller asked
				// for. If that name is taken the old content stays where it
				// was last seen, which the error names.
				o.restoreOldName(ctx, leaf, dirID, oldAt)
			}
			return fmt.Errorf("yun139: update %q: landing the upload failed: %w", leaf, serr)
		}
		// settleAutoRename dropped the entry that held the leaf. The parked
		// old entry is not that one, and it is left to cleanupOldBackups
		// below, which removes every backup of this leaf - so do not delete
		// it here as well.
		oldMoved = false
	}
	// Upload complete: the new file is in place. Refresh the receiver, then
	// delete the old file. If the delete fails the new file is correct; the
	// old file lingers under its backup name for the user to clean up.
	o.id = res.fileID
	o.size = size
	o.modTime = src.ModTime(ctx)
	o.hashMu.Lock()
	o.sha256 = res.hashHex
	o.hashMu.Unlock()
	// A rapidUpload that matched the old content's hash answers with the id of
	// the entry it deduplicated against, which can be the parked old entry
	// itself; deleting that id would remove the content the receiver now
	// points at.
	if oldMoved && oldID != res.fileID {
		if err := o.fs.deleteObject(ctx, oldID, o.serverPath, o.fs.space == spaceFamily); err != nil {
			fs.Errorf(o, "yun139: delete old content after update: %v", err)
		}
	}
	// Clean up orphaned backups from a PREVIOUS killed run targeting the
	// same file (a crash between pre-rename and the new upload leaves
	// <name>.rclone-old-XXXX behind). Only touch entries whose base name
	// starts with this object's leaf - never delete anything else. The
	// listing carries STANDARD names, so pass the standard leaf.
	o.fs.cleanupOldBackups(ctx, dirID, leafStd, o.id)
	o.fs.dirCache.FlushDir(path.Dir(o.remote))
	return nil
}

// backupParts splits a leaf into the part a backup name grows from and the
// extension it must keep. A name that is all extension (".gitignore") has no
// stem to build on: it keeps the empty stem and hands the whole name to ext,
// so the backup still ends with it.
func backupParts(leafStd string) (stem, ext string) {
	ext = path.Ext(leafStd)
	stem = strings.TrimSuffix(leafStd, ext)
	return stem, ext
}

// backupLeafName builds the name an entry is parked under while an update
// replaces it. The family space appends the renamed entry's own extension to a
// requested name that does not end with it, so the backup keeps the leaf's
// extension and a dotless leaf is parked under a dotless name: parking the
// entry and restoring it both then land under the name that was asked for.
// A leaf that is all extension (".gitignore") has no stem, so the marker goes
// in front of the whole name instead and the backup still ends with it.
func backupLeafName(leafStd string) string {
	stem, ext := backupParts(leafStd)
	if stem == "" {
		return "rclone-old-" + random.String(8) + ext
	}
	return stem + "-rclone-old-" + random.String(8) + ext
}

// backupPrefix is the leading part of every backup name for a leaf, so a
// listing can recognise the leftovers of an interrupted update.
func backupPrefix(leafStd string) string {
	stem, _ := backupParts(leafStd)
	if stem == "" {
		return "rclone-old-"
	}
	return stem + "-rclone-old-"
}

// backupRandomLen is the length of the distinguishing marker in a backup name
// (see random.String for its alphabet).
const backupRandomLen = 8

// isBackupMarker reports whether s is the marker backupLeafName generates:
// exactly backupRandomLen characters from random.String's consonant/vowel/digit
// pattern. Requiring the shape the generator produces keeps the cleanup of one
// leaf away from a user file that merely looks similar.
func isBackupMarker(s string) bool {
	const (
		vowel     = "aeiou"
		consonant = "bcdfghjklmnpqrstvwxyz"
		digit     = "0123456789"
	)
	pattern := [backupRandomLen]string{consonant, vowel, consonant, vowel, consonant, vowel, consonant, digit}
	if len(s) != backupRandomLen {
		return false
	}
	for i := 0; i < backupRandomLen; i++ {
		if !strings.ContainsRune(pattern[i], rune(s[i])) {
			return false
		}
	}
	return true
}

// isBackupOf reports whether name is a backup of leafStd: the leaf's prefix,
// then the marker, then the leaf's extension. Matching the whole shape and not
// just the prefix keeps the cleanup of one leaf away from the backups of a
// sibling whose name merely starts with the same text ("plain" vs "plain.txt").
//
// The comparison folds case because the server does: live-measured, it treats
// the spellings of a pair as one entry ("casecheck.txt"/"CaseCheck.txt",
// "ünïcode.txt"/"Ünïcode.txt", "Kelvin.txt"/"Kelvin.txt" each resolve to a
// single id), so a case-variant spelling of a backup name addresses the same
// entry. A parked entry is named from its own spelling, which can differ in
// case from the requested leaf, and a backup that only matched the requested
// spelling would be left behind for good.
func isBackupOf(name, leafStd string) bool {
	rest, ok := strings.CutPrefix(strings.ToLower(name), strings.ToLower(backupPrefix(leafStd)))
	if !ok {
		return false
	}
	_, ext := backupParts(leafStd)
	rest, ok = strings.CutSuffix(rest, strings.ToLower(ext))
	if !ok || !isBackupMarker(rest) {
		return false
	}
	return true
}

// cleanupOldBackups deletes files in dirID whose names are backups of the leaf
// - the leftovers of an Update that crashed after renaming the old file away.
// The new file is already in place when this runs, so the orphan is safe to
// remove.
//
// keepID is the id of the entry the caller now treats as the live content; it
// is never removed, because an upload the server deduplicated can answer with
// the id of the entry it parked, which would otherwise be deleted as a backup
// of itself.
func (f *Fs) cleanupOldBackups(ctx context.Context, dirID, leaf, keepID string) {
	var orphans []listEntry
	err := f.listAll(ctx, dirID, func(e listEntry) bool {
		if !e.isDir && e.id != keepID && isBackupOf(e.name, leaf) {
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
	if f.space == spaceGroup {
		// The group recycle bin is reached by id alone; srvPath and the
		// family flag carry no meaning here.
		return f.groupTrash(ctx, id)
	}
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
		_, err = f.familyTaskPoll(ctx, taskID)
		return err
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
	_, err = f.taskGet(ctx, out.Data.TaskID, "delete")
	return err
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
	if f.space == spaceGroup {
		if err := f.groupRename(ctx, id, newName, dirID); err != nil {
			return err
		}
		// modifyGroupContent answers with a bare success envelope, so the
		// stored name has to come from a listing of the parent.
		return f.confirmRename(ctx, dirID, id, f.opt.Enc.ToStandardName(newName), "")
	}
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
		// A rename issued right after the task that created the entry can
		// race the server's own indexing: the copy task reports success
		// before the new content is visible to modifyContentInfo, which
		// then answers '1809111402: 目录或文件不存在'. Retry that answer
		// for a bounded window; any other business error is returned as
		// is. This is not a transport retry, so it does not go through
		// shouldRetry.
		deadline := time.Now().Add(copyVisibilityTimeout)
		for {
			var out struct {
				UpdateContentInfoRes struct {
					ContentName string `json:"contentName"`
				} `json:"updateContentInfoRes"`
			}
			err := f.pacer.Call(func() (bool, error) {
				return shouldRetry(ctx, f.familyCall(ctx, path, body, &out))
			})
			var ae *apiError
			if err == nil || !errors.As(err, &ae) || ae.Code != familyNotFoundCode || time.Now().After(deadline) {
				if err == nil {
					if lerr := f.confirmRename(ctx, dirID, id, f.opt.Enc.ToStandardName(newName), out.UpdateContentInfoRes.ContentName); lerr != nil {
						return lerr
					}
				}
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(copyVisibilityInterval):
			}
		}
	}
	return f.pacer.Call(func() (bool, error) {
		var out struct {
			Data struct {
				Name string `json:"name"`
			} `json:"data"`
		}
		err := f.personalCall(ctx, "/hcy/file/update", api.PersonalUpdateReq{
			FileID: id,
			Name:   newName,
		}, &out)
		if err != nil {
			return shouldRetry(ctx, err)
		}
		if lerr := f.confirmRename(ctx, dirID, id, f.opt.Enc.ToStandardName(newName), out.Data.Name); lerr != nil {
			return false, lerr
		}
		return false, nil
	})
}

// checkRenameLanded returns an error when the server stored the renamed entry
// under a name other than the requested one.
//
// A rename onto a name the directory already holds does not overwrite: the
// server keeps the existing entry and stores the renamed one under a suffixed
// name, answering success either way. The name the reply reports is therefore
// the only signal that the requested name was not taken, and a caller that
// assumed success would leave the entry at a path nobody asked for.
//
// The family space additionally appends the source's extension whenever the
// requested name does not already end with it, so a rename to plain stores
// plain.mkv and a rename to other.mp4 stores other.mp4.mkv (verified live:
// the reply reports the appended name). That is a different name from the one
// requested, so it is refused like any other miss: rclone records the path it
// was asked for, and reporting success would leave every later lookup, move
// and delete addressing a path the server does not hold.
//
// An empty answer carries no information and is accepted: the caller's
// failure path deletes the upload, and deleting on an unknown outcome would
// destroy the content when the rename did land.
func checkRenameLanded(requested, stored string) error {
	if stored == "" || stored == requested {
		return nil
	}
	return fmt.Errorf("yun139: rename %q: server stored it as %q", requested, stored)
}

// Remove deletes the object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteObject(ctx, o.id, o.serverPath, o.fs.space == spaceFamily)
}

// Move moves a file using the server-side batch-move API.
//
// Signature follows fs.Mover: destination is a full remote path. A move
// that stays in one directory is issued as a plain rename, because the
// server rejects a batchMove to the directory the file already lives in.
// Returns the new object, or ErrorCantMove so the engine falls back to
// copy+delete.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	if f.space == spaceGroup {
		return f.groupMove(ctx, src, remote)
	}
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
		// No native family move: createBatchOprTaskV2 rejects taskType 3,
		// so the move is a copy followed by deleting the source. The copy
		// takes only a destination directory and keeps the source's name,
		// so a move that changes the leaf renames the copy afterwards.
		// The task detail echoes the id of the entry it created, which
		// identifies the copy exactly; a copy that does not end up under
		// the requested name is removed again and the move refused, so
		// the content is never left only under a name the caller was not
		// told about, and never silently duplicated either.
		//
		// The source's parent is resolved through the SOURCE's dircache:
		// a source from another Fs of the same account carries a remote
		// path relative to that Fs's root.
		_, srcDirID, srcErr := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
		if srcErr == nil && srcDirID == dstDirID && dstLeaf == srcLeaf {
			// Same directory and same name: nothing to do. Copying here
			// would duplicate the file.
			return srcObj, nil
		}
		newID, err := f.familyCopy(ctx, srcObj, dstDirID)
		if err != nil {
			return nil, err
		}
		o, err := f.resolveCopyLeaf(ctx, dstDirID, remote, srcObj.id, newID)
		if err != nil {
			f.removeCopy(ctx, newID, dstDirID, srcObj.id)
			return nil, fmt.Errorf("yun139: move to %q: %w", dstLeaf, err)
		}
		if err := f.deleteObject(ctx, srcObj.id, srcObj.serverPath, true); err != nil {
			return nil, err
		}
		return o, nil
	}
	// A move that stays in the same directory is a pure rename: the
	// server rejects batchMove to the directory the file already lives
	// in with '04010317: 移动失败，无法移动到自身、自身所在目录、自身子目录下',
	// so skip the task entirely and just rename in place.
	//
	// The source's parent is resolved through the SOURCE's dircache, not
	// this one: a source from another Fs of the same account carries a
	// remote path relative to that Fs's root, and resolving it here would
	// map it onto this root and mistake a cross-root move for a rename in
	// place. FindPath (not path.Dir) is what maps a bare leaf to that
	// Fs's root; path.Dir would turn it into ".".
	//
	// An unresolvable source directory leaves the same-directory case
	// undecidable, so fall through to the batch-move task rather than
	// fail the move.
	_, srcDirID, err := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
	if err == nil && srcDirID == dstDirID {
		if dstLeaf == srcLeaf {
			// Same directory and same name: nothing to do. The server
			// would reject the no-op batchMove as well.
			return srcObj, nil
		}
		if err := f.renameObject(ctx, srcObj.id, f.opt.Enc.FromStandardName(dstLeaf), dstDirID, false); err != nil {
			return nil, err
		}
		o, err := f.newObjectVisible(ctx, remote)
		if err != nil {
			return nil, err
		}
		// A rename onto a name the directory already holds does not
		// overwrite: the server keeps the existing entry and parks the
		// moved file under a suffixed name. The object at remote would
		// then be the old one, so refuse to report a move that did not
		// land rather than hand back the wrong object.
		if got := o.(*Object).id; got != srcObj.id {
			return nil, fmt.Errorf("yun139: rename %q: server kept the existing entry (moved file is under another name)", dstLeaf)
		}
		return o, nil
	}
	taskID, err := f.moveTaskID(ctx, srcObj.id, dstDirID)
	if err != nil {
		return nil, err
	}
	_, err = f.taskGet(ctx, taskID, "move")
	if err != nil {
		return nil, err
	}
	// The batch-move keeps the id; if the leaf changed (rename-in-move),
	// the server-side move does not rename, so issue an update.
	if dstLeaf != "" && dstLeaf != srcLeaf {
		if err := f.renameObject(ctx, srcObj.id, f.opt.Enc.FromStandardName(dstLeaf), dstDirID, false); err != nil {
			return nil, err
		}
	}
	o, err := f.newObjectVisible(ctx, remote)
	if err != nil {
		return nil, err
	}
	// The same non-overwrite rule applies across directories: if the
	// destination name was already taken the server keeps the existing
	// entry and parks the moved file under a suffixed name, so the
	// object at remote would be the old one.
	if got := o.(*Object).id; got != srcObj.id {
		return nil, fmt.Errorf("yun139: move to %q: server kept the existing entry (moved file is under another name)", dstLeaf)
	}
	return o, nil
}

// groupMove moves an entry inside a shared group with the native batch move
// task.
//
// A move that stays in the directory the entry already lives in is issued as a
// rename: a move of an entry into itself is something the task refuses.
// Renaming and moving are separate calls, so a move that also changes the leaf
// moves first and renames afterwards.
func (f *Fs) groupMove(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
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
	// The source's parent is resolved through the SOURCE's dircache: a source
	// from another Fs of the same group carries a remote path relative to that
	// Fs's root. An unresolvable source directory leaves the same-directory
	// case undecidable, so fall through to the task rather than fail the move.
	_, srcDirID, err := srcObj.fs.dirCache.FindPath(ctx, srcObj.remote, false)
	if err == nil && srcDirID == dstDirID {
		if dstLeaf == srcLeaf {
			// Same directory and same name: nothing to do.
			return srcObj, nil
		}
		if err := f.renameObject(ctx, srcObj.id, f.opt.Enc.FromStandardName(dstLeaf), dstDirID, false); err != nil {
			return nil, err
		}
		return f.groupMoveLanded(ctx, remote, srcObj.id, dstLeaf)
	}
	if err := f.groupMoveTask(ctx, []string{srcObj.id}, dstDirID, false); err != nil {
		if isGroupStalePathError(err) {
			// Every path under the destination chains through the string the
			// server just rejected, so the whole subtree goes stale together.
			f.srvPathOf.delSubtree("", dstDirID)
			return nil, fs.ErrorCantMove
		}
		return nil, err
	}
	// No path cache is touched here: only directories are cached, and an entry's
	// own path is rebuilt from its parent when it is renamed.
	if dstLeaf != "" && dstLeaf != srcLeaf {
		if err := f.renameObject(ctx, srcObj.id, f.opt.Enc.FromStandardName(dstLeaf), dstDirID, false); err != nil {
			// The content has moved even though the rename did not land, so
			// report where it actually is: a caller that retried the move would
			// be asking the server to move an entry that is no longer in the
			// directory it thinks it is in.
			if name, found, nerr := f.entryName(ctx, dstDirID, srcObj.id); nerr == nil && found {
				return nil, fmt.Errorf("yun139: move to %q: the entry moved but the server stores it as %q: %w", dstLeaf, name, err)
			}
			return nil, fmt.Errorf("yun139: rename after move to %q: %w", dstLeaf, err)
		}
	}
	return f.groupMoveLanded(ctx, remote, srcObj.id, dstLeaf)
}

// groupMoveLanded reads the moved entry back at remote and refuses to report
// success unless the object there carries the id that was moved. A destination
// name that was already taken is kept by the server, which parks the moved
// entry under a suffixed name, so the object at remote would be the other one.
func (f *Fs) groupMoveLanded(ctx context.Context, remote, wantID, dstLeaf string) (fs.Object, error) {
	o, err := f.newObjectVisible(ctx, remote)
	if err != nil {
		return nil, err
	}
	if got := o.(*Object).id; got != wantID {
		return nil, fmt.Errorf("yun139: move to %q: server kept the existing entry (moved file is under another name)", dstLeaf)
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
	// Use lib/dircache's DirMove helper to resolve ids (it refuses a move of or
	// onto a root and checks the destination does not exist; it knows nothing
	// about a backend's own subtree rules).
	srcID, srcDirectoryID, srcLeaf, dstDirID, dstLeaf, err :=
		f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}
	if f.space == spaceGroup {
		// The group is the one space with a native directory move: the batch
		// task takes the catalog id in catalogList and moves the subtree with
		// it (capture 2026-10-19: 新建文件夹 moved into 新建文件夹2 came back as
		// catalogList with reason "0").
		seg := "/" + srcID
		if p, ok := f.srvPathOf.get(dstDirID); ok && (strings.HasSuffix(p, seg) || strings.Contains(p, seg+"/")) {
			// The destination is the source itself or something inside it.
			// lib/dircache only refuses a move of or onto a root, and it has
			// already created the destination's parent chain by the time it
			// resolves the ids, so those empty directories stay behind.
			f.dirCache.FlushDir(dstRemote)
			return fmt.Errorf("yun139: cannot move %q into itself or a subdirectory", srcRemote)
		}
		if srcDirectoryID == dstDirID {
			// The new home is the parent the directory already lives in, so
			// this is a rename. The task refuses it - measured 2026-10-20: a
			// same-parent move came back with result code 3 - while
			// modifyGroupContent with the catalog type renames the directory
			// and keeps its subtree.
			if err := f.groupRenameCatalog(ctx, srcID, dstLeaf, dstDirID); err != nil {
				return err
			}
			srcFs.dirCache.FlushDir(srcRemote)
			f.dirCache.FlushDir(dstRemote)
			return nil
		}
		// The old server path has to be read before the move, because after it
		// the cached path of the moved subtree is exactly what must not be used.
		oldPath := srcFs.groupSrvPath(srcID)
		if err := f.groupMoveTask(ctx, []string{srcID}, dstDirID, true); err != nil {
			if isGroupStalePathError(err) {
				srcFs.srvPathOf.delSubtree("", dstDirID)
				f.srvPathOf.delSubtree("", dstDirID)
				return fs.ErrorCantDirMove
			}
			return err
		}
		srcFs.srvPathOf.delSubtree(oldPath, srcID)
		f.srvPathOf.delSubtree(oldPath, srcID)
		// Measured 2026-10-20: the batch task is the only group endpoint that
		// reads the path field. getGroupFileDownLoadURLV2 and modifyGroupContent
		// resolve the entry by contentID and answered identically when sent a
		// path that never existed, so an Object - or a third remote on the same
		// group - that still holds a pre-move path stays readable and renameable.
		// What cannot survive a stale path is a move out of it, and that arrives
		// as groupStalePathErrorCode, handled above.
		// The paths both Fs held for the moved subtree are gone now, so drop the
		// directory cache before the rename: a rename that fails must not leave
		// either side pointing at a location the subtree no longer lives at.
		srcFs.dirCache.FlushDir(srcRemote)
		f.dirCache.FlushDir(dstRemote)
		// The task keeps the directory's own name, so a move that also changes
		// the leaf has to rename it: the caller asked for the subtree to live at
		// dstRemote, not under whatever the directory was called before.
		if dstLeaf != srcLeaf {
			if err := f.groupRenameCatalog(ctx, srcID, dstLeaf, dstDirID); err != nil {
				return fmt.Errorf("yun139: rename after directory move to %q: %w", dstLeaf, err)
			}
		}
		return nil
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
	if f.space == spaceGroup {
		return f.groupRapidCopy(ctx, src, remote)
	}
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	if srcObj.fs != f && !srcObj.fs.sameCloud(f) {
		// Different account/family - the API cannot server-side copy.
		return nil, fs.ErrorCantCopy
	}
	_, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	if err := f.validateName(remote); err != nil {
		return nil, err
	}
	// The server never overwrites: a batch copy whose destination name is
	// already taken is auto-renamed to a suffixed name, so the copy cannot
	// be given the requested leaf and would have to be removed again. The
	// engine replaces an existing file with a bandwidth copy instead, whose
	// Update path renames the old entry away before uploading and is
	// lossless, so refuse here while nothing has been created yet. Refusing
	// afterwards would leave the auto-renamed copy behind as an orphan.
	taken, err := f.leafTaken(ctx, dstDirID, path.Base(remote))
	if err != nil {
		return nil, err
	}
	if taken {
		fs.Debugf(srcObj, "Destination name %q is taken; using a bandwidth copy to replace it", path.Base(remote))
		return nil, fs.ErrorCantCopy
	}
	if f.space == spaceFamily {
		newID, err := f.familyCopy(ctx, srcObj, dstDirID)
		if err != nil {
			return nil, err
		}
		o, err := f.resolveCopyLeaf(ctx, dstDirID, remote, srcObj.id, newID)
		if err != nil {
			// The copy exists but cannot be given the requested name;
			// remove it so a failed copy leaves nothing behind.
			f.removeCopy(ctx, newID, dstDirID, srcObj.id)
			return nil, err
		}
		return o, nil
	}
	taskID, err := f.copyTaskID(ctx, srcObj.id, dstDirID)
	if err != nil {
		return nil, err
	}
	newID, err := f.taskGet(ctx, taskID, "copy")
	if err != nil {
		return nil, err
	}
	o, err := f.resolveCopyLeaf(ctx, dstDirID, remote, srcObj.id, newID)
	if err != nil {
		f.removeCopy(ctx, newID, dstDirID, srcObj.id)
		return nil, err
	}
	return o, nil
}

// groupRapidCopy copies an entry inside a shared group without moving bytes.
//
// A group has no batch copy task, so the copy is offered as a rapid upload:
// the source's SHA-256 goes to /hcy/group/dynamic/file/create and the store
// mints a new entry for content it already holds. Measured 2026-10-19: a second
// create carrying the hash of an existing entry answered rapidUpload with no
// part URLs, and the entry it returned read back with that same SHA-256.
//
// When the hash matches nothing the answer carries part URLs instead. Nothing
// is then uploaded and the copy is refused, so the engine falls back to a
// bandwidth copy; a create whose parts are never PUT leaves no entry behind
// (measured: an upload killed one second after create returned was missing from
// the listing and stayed missing).
func (f *Fs) groupRapidCopy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}
	if srcObj.fs != f && !srcObj.fs.sameCloud(f) {
		return nil, fs.ErrorCantCopy
	}
	dstLeaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	if err := f.validateName(remote); err != nil {
		return nil, err
	}
	taken, err := f.leafTaken(ctx, dstDirID, path.Base(remote))
	if err != nil {
		return nil, err
	}
	if taken {
		fs.Debugf(srcObj, "Destination name %q is taken; using a bandwidth copy to replace it", path.Base(remote))
		return nil, fs.ErrorCantCopy
	}
	hashHex, err := srcObj.Hash(ctx, hash.SHA256)
	if err != nil || hashHex == "" {
		// A hash that cannot be read is a copy that cannot be offered, not a
		// transfer to abort: the bandwidth copy the engine falls back to reads
		// the content itself and will report a real access problem there.
		fs.Debugf(srcObj, "yun139: no content hash to offer for the rapid copy (%v); using a bandwidth copy", err)
		return nil, fs.ErrorCantCopy
	}
	size := srcObj.Size()
	// The entry is offered as a single part whatever size it is. The store
	// matches on the content hash and the size, not on the layout the content
	// arrived in: a 13 MiB file uploaded in three 5 MiB parts was copied this
	// way (measured 2026-10-20, "Copied (server-side copy)", no bytes moved).
	// Declaring the real three parts without their midstates is what the server
	// refuses, with 02010501 请求不合法 - the midstates are a function of the
	// bytes, and a copy does not read them.
	body := buildCreateBody(dstDirID, f.opt.Enc.FromStandardName(dstLeaf), size, hashHex, nil, f.opt.RegionCode)
	var resp struct {
		api.BaseResp
		Data struct {
			FileID      string               `json:"fileId"`
			FileName    string               `json:"fileName"`
			RapidUpload bool                 `json:"rapidUpload"`
			Exists      *bool                `json:"exist"`
			PartInfos   []api.PartUploadInfo `json:"partInfos"`
		} `json:"data"`
	}
	err = f.pacer.Call(func() (bool, error) {
		err := f.spaceCall(ctx, "/hcy/group/dynamic/file/create", f.sharedCreateBody(body, []api.PartInfo{{PartNumber: 1, PartSize: size}}), &resp)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		// A quota or entitlement refusal is a reason to stop, not a reason to
		// re-upload the file the slow way: the bandwidth copy would read the
		// whole object, land on the same server answer, and cost the transfer.
		if qerr := noRetryOnQuotaRejection(err); qerr != err || fserrors.IsNoRetryError(err) {
			return nil, qerr
		}
		// Anything else the server objects to is still a copy rclone can do by
		// moving bytes, so decline rather than fail: an error here would abort
		// the transfer outright.
		fs.Debugf(srcObj, "yun139: group refused the rapid copy (%v); using a bandwidth copy", err)
		return nil, fs.ErrorCantCopy
	}
	if resp.Data.Exists != nil && *resp.Data.Exists {
		// The name was taken between the check above and this call, so the id
		// in the answer is the other entry's. Acting on it - renaming it, or
		// removing it after a failed copy - would touch a file this copy did
		// not create.
		fs.Debugf(srcObj, "Destination name %q was taken during the copy; using a bandwidth copy", path.Base(remote))
		return nil, fs.ErrorCantCopy
	}
	if !resp.Data.RapidUpload || resp.Data.FileID == "" {
		fs.Debugf(srcObj, "yun139: the store has no content for this hash; using a bandwidth copy")
		return nil, fs.ErrorCantCopy
	}
	return f.resolveCopyLeaf(ctx, dstDirID, remote, srcObj.id, resp.Data.FileID)
}

// resolveCopyLeaf turns the id of a freshly created copy into an Object at
// remote. The batch copy keeps the source's name, so a copy to a different
// leaf is renamed to the requested one; both spaces support that. newID
// comes from the task detail and identifies the copy exactly, so the entry
// is never confused with one that already occupied the destination.
//
// srcID is the id of the object the copy was made from. A task that reports
// the source's id instead of the copy's must be refused: resolving it would
// hand back the source, and a rename or a cleanup driven by that id would
// act on the original file rather than on a copy.
func (f *Fs) resolveCopyLeaf(ctx context.Context, dstDirID, remote, srcID, newID string) (fs.Object, error) {
	if newID == "" || newID == srcID {
		return nil, errors.New("yun139: copy task reported no new id")
	}
	leaf := path.Base(remote)
	o, err := f.newObjectByID(ctx, dstDirID, remote, newID)
	if err != nil {
		return nil, err
	}
	newObj := o.(*Object)
	if newObj.leaf() == leaf {
		return o, nil
	}
	// The server put the copy under a different name (auto_rename).
	if err := f.renameObject(ctx, newObj.id, f.opt.Enc.FromStandardName(leaf), dstDirID, f.space == spaceFamily); err != nil {
		return nil, fmt.Errorf("yun139: rename copy to %q: %w", leaf, err)
	}
	// A rename onto a name the directory already holds does not
	// overwrite: the server keeps the existing entry and parks the
	// renamed file under a suffixed name. Re-read the entry by id and
	// require it to carry the requested name, so a copy that did not
	// land is reported instead of returning the wrong object.
	//
	// The comparison is exact on purpose, even on the family space, where
	// a rename also appends the source's extension when the requested name
	// does not end with it: the server then holds a name the caller never
	// asked for, and reporting the copy as landed would leave the content
	// at a path nobody can look up.
	got, err := f.newObjectByID(ctx, dstDirID, remote, newID)
	if err != nil {
		return nil, err
	}
	if got.(*Object).leaf() != leaf {
		return nil, fmt.Errorf("yun139: copy did not land under %q: the server stored it as %q", leaf, got.(*Object).leaf())
	}
	return got, nil
}

// newObjectVisible returns the object at remote, retrying while the entry is
// not yet visible. 139 is eventually consistent, so a rename that has landed
// can still be missing from a listing for a moment and a single read would
// report a completed operation as failed.
//
// An entry that is present but carries a different id is returned as is: that
// is a real collision, not a visibility lag, and the caller decides on it.
func (f *Fs) newObjectVisible(ctx context.Context, remote string) (fs.Object, error) {
	deadline := time.Now().Add(copyVisibilityTimeout)
	for {
		o, err := f.NewObject(ctx, remote)
		if err == nil {
			return o, nil
		}
		if !errors.Is(err, fs.ErrorObjectNotFound) || time.Now().After(deadline) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(copyVisibilityInterval):
		}
	}
}

// leafTaken reports whether dirID already holds an entry named leaf. A
// server-side copy cannot be given a name the directory holds, so the copy
// path consults this before creating anything.
//
// The comparison is on the standard (decoded) name: a listing reports the
// wire spelling of a name, which the encoder may have escaped.
func (f *Fs) leafTaken(ctx context.Context, dirID, leaf string) (bool, error) {
	taken := false
	err := f.listAll(ctx, dirID, func(e listEntry) bool {
		if strings.EqualFold(e.name, leaf) {
			taken = true
			return true
		}
		return false
	})
	if err != nil {
		return false, err
	}
	return taken, nil
}

// newObjectByID returns the entry with the given id in dstDirID, carrying
// the name the server actually gave it. remote is the requested destination
// path, which supplies the directory the entry lives in; Object.Remote is
// relative to the Fs root, so only the leaf is replaced.
//
// The listing is polled because 139 is eventually consistent: an entry a
// task just created can take a few seconds to appear, and a single read
// would report a completed operation as missing.
func (f *Fs) newObjectByID(ctx context.Context, dstDirID, remote, id string) (fs.Object, error) {
	var found listEntry
	deadline := time.Now().Add(copyVisibilityTimeout)
	for {
		err := f.listAll(ctx, dstDirID, func(e listEntry) bool {
			if e.id != id {
				return false
			}
			found = e
			return true
		})
		if err != nil {
			return nil, err
		}
		if found.id != "" {
			return f.newObjectWithInfo(ctx, path.Join(path.Dir(remote), found.name), found)
		}
		if time.Now().After(deadline) {
			return nil, fs.ErrorObjectNotFound
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(copyVisibilityInterval):
		}
	}
}

// entryName reports the standard name the entry with id carries in dirID.
//
// A reply that omits the name of a rename or an upload leaves the caller
// unable to tell whether the entry landed at the requested path or under a
// name the server chose. 139 is eventually consistent, so the listing is
// polled before the entry is declared absent. found is false when no entry
// with that id shows up in dirID.
func (f *Fs) entryName(ctx context.Context, dirID, id string) (name string, found bool, err error) {
	deadline := time.Now().Add(copyVisibilityTimeout)
	for {
		var e listEntry
		err := f.listAll(ctx, dirID, func(cand listEntry) bool {
			if cand.id != id {
				return false
			}
			e = cand
			return true
		})
		if err != nil {
			return "", false, err
		}
		if e.id != "" {
			return e.name, true, nil
		}
		if time.Now().After(deadline) {
			return "", false, nil
		}
		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case <-time.After(copyVisibilityInterval):
		}
	}
}

// uploadedLeaf reports the standard name an upload carries in dirID.
//
// The create reply names the entry it stored. A reply that omits the field
// would otherwise be read as the requested name, and an entry the server
// stored under a suffixed name would slip past the caller's landing check.
// An unnamed entry is therefore looked up by id.
func (f *Fs) uploadedLeaf(ctx context.Context, dirID string, res *uploadResult, leafStd string) (string, error) {
	if res.fileName != "" {
		return f.opt.Enc.ToStandardName(res.fileName), nil
	}
	name, found, err := f.entryName(ctx, dirID, res.fileID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("yun139: upload %q: the entry is not in its directory", leafStd)
	}
	return name, nil
}

// confirmRename checks that a rename left the entry under the requested name.
//
// The reply names the entry the server stored when it reports one. An empty
// answer carries no information, so the entry is located by id: the server
// answers success when it stores a rename onto a held name under a suffixed
// name, and a caller that read silence as success would address a path the
// server does not hold.
func (f *Fs) confirmRename(ctx context.Context, dirID, id, requestedStd, reportedWire string) error {
	if reportedWire != "" {
		return checkRenameLanded(requestedStd, f.opt.Enc.ToStandardName(reportedWire))
	}
	name, found, err := f.entryName(ctx, dirID, id)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("yun139: rename %q: the entry is not in its directory", requestedStd)
	}
	return checkRenameLanded(requestedStd, name)
}

// sameCloud reports whether fs belongs to the same 139 account (and, for
// family, the same family cloud), so ids are interchangeable.
func (f *Fs) sameCloud(other *Fs) bool {
	if f.accountKey() != other.accountKey() {
		return false
	}
	if f.space != other.space {
		return false
	}
	if f.space == spaceFamily && f.opt.FamilyID != other.opt.FamilyID {
		return false
	}
	if f.space == spaceGroup && f.opt.GroupID != other.opt.GroupID {
		return false
	}
	return true
}

// leaf returns the base name of the object's remote path.
func (o *Object) leaf() string { return path.Base(o.remote) }

// familyCopy copies srcObj (file or dir) into dstDirID inside the family
// cloud via createBatchOprTaskV2 taskType=1, then polls the task. It
// returns the id of the created entry, which the task detail echoes.
func (f *Fs) familyCopy(ctx context.Context, srcObj *Object, dstDirID string) (string, error) {
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

// familyCopyID copies the given content/catalog id into dstDirID and
// returns the id of the created entry.
func (f *Fs) familyCopyID(ctx context.Context, id, dstDirID string, isDir bool) (string, error) {
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
		return "", err
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
	_, err = f.familyTaskPoll(ctx, taskID)
	return err
}

// removeCopy deletes an entry a copy task created, so a copy that cannot be
// given the requested name does not linger under a name the caller never
// asked for. A failure to remove it is logged, not returned: the caller is
// already reporting why the operation did not complete.
//
// dstDirID is the directory the copy was made into; for family the delete
// task addresses the entry by its parent's server-side path, and the cache
// may be cold for a directory that was never listed, so it is primed on
// demand.
//
// srcID is the id of the object the copy was made from; an id equal to it
// names the original rather than a copy and is never removed.
func (f *Fs) removeCopy(ctx context.Context, id, dstDirID, srcID string) {
	if id == "" || id == srcID {
		return
	}
	srvPath := ""
	if f.space == spaceFamily {
		srvPath = f.familySrvPath(dstDirID)
		if srvPath == "" {
			if err := f.primeSrvPathFor(ctx, dstDirID); err != nil {
				fs.Debugf(f, "yun139: prime srvPath for %s: %v", dstDirID, err)
			}
			srvPath = f.familySrvPath(dstDirID)
		}
	}
	if err := f.deleteObject(ctx, id, srvPath, f.space == spaceFamily); err != nil {
		fs.Errorf(f, "yun139: remove unwanted copy %s: %v", id, err)
	}
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
	res, err := w.f.uploadFromRandom(ctx, w.tmp, w.dirID, w.leaf, w.size, hex.EncodeToString(h.Sum(nil)))
	if err != nil {
		if res != nil && res.fileID != "" {
			w.f.discardUpload(ctx, res.fileID, w.dirID, w.f.space == spaceFamily)
		}
		return fmt.Errorf("yun139: chunk writer upload: %w", err)
	}
	// /hcy/file/create never overwrites: a name the directory already holds
	// makes the server store the content under a suffixed name instead. The
	// caller looks the object up at the requested path once Close returns, so
	// the upload has to land there - otherwise the old entry is found, the
	// verification compares against it, and the new content is left under a
	// name nobody asked for.
	leafStd := w.f.opt.Enc.ToStandardName(w.leaf)
	landed, err := w.f.uploadedLeaf(ctx, w.dirID, res, leafStd)
	if err != nil {
		return err
	}
	if landed != leafStd {
		if err := w.f.settleAutoRename(ctx, w.dirID, leafStd, res.fileID); err != nil {
			return fmt.Errorf("yun139: chunk writer settle upload name: %w", err)
		}
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

// downloadURLTTL returns how long a fetched download URL may be reused. The
// server fixes each space's link lifetime, so the window is per space and well
// under the granted life (see the constants).
func (f *Fs) downloadURLTTL() time.Duration {
	switch f.space {
	case spaceFamily, spaceGroup:
		// Both shared spaces mint 24h links and take no expiry parameter:
		// family was captured 2026-09-03, the group link lifetime was
		// measured live on 2026-10-19 (X-Amz-Expires=86400).
		return familyDownloadURLTTL
	}
	return personalDownloadURLTTL
}

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
	o.urlExpiry = time.Now().Add(o.fs.downloadURLTTL())
	return o.url, nil
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
	if f.space == spaceGroup {
		// POST .../getGroupFileDownLoadURLV2 (captured 2026-10-19). Like
		// the family call it returns one "downloadURL" string and takes no
		// expiry parameter, and its path ends with the containing
		// directory rather than the file itself.
		body := map[string]any{
			"accountInfo": map[string]any{
				"accountName": f.account,
				"accountType": "1",
			},
			"contentID": o.id,
			"extInfo": map[string]string{
				"isReturnCdnDownloadUrl": "1",
			},
			"groupID": f.opt.GroupID,
			"path":    o.serverPath,
		}
		var resp struct {
			DownloadURL string `json:"downloadURL"`
		}
		err := f.pacer.Call(func() (bool, error) {
			err := f.groupCall(ctx, "/hcy/mutual/adapter/isbo/openApi/getGroupFileDownLoadURLV2", body, &resp)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return "", err
		}
		if resp.DownloadURL == "" {
			return "", fmt.Errorf("yun139: no download URL returned for %q", o.remote)
		}
		return resp.DownloadURL, nil
	}
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
	return f.fetchDownloadURLExpire(ctx, o, maxLinkExpireSec)
}

// fetchDownloadURLExpire asks for a personal-space download URL that the server
// should keep valid for expireSec. The granted lifetime is the server's to set, so
// callers that cache the result treat this as a request, not a promise.
func (f *Fs) fetchDownloadURLExpire(ctx context.Context, o *Object, expireSec int) (string, error) {
	body := map[string]any{"fileId": o.id, "expireSec": expireSec}
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

// PublicLink returns a direct download link for the given file.
//
// The link is always minted fresh - it does not come from the download-URL
// cache - so the holder gets the full lifetime the server grants.
//
// The personal cloud mints it with /file/getDownloadUrl, which takes an
// expireSec, so --expire is passed through (default defaultLinkExpireSec, the
// 86400s the official client asks for, capped at maxLinkExpireSec). The server
// fixes the granted lifetime regardless: measured 2026-10-01 the link dies after
// 15 minutes and is byte-identical for expireSec 3600/21600/86400, and the
// official client's capture of 2026-09-03 agrees (a expireSec:86400 request is
// answered with expiration = issue time + 15 minutes). The family cloud mints
// it with getFileDownLoadURLV2, which takes no expiry parameter and grants 24h,
// so a requested expire is ignored there with a warning.
//
// Either way the URL is a direct download link - anyone holding it can fetch
// the file until it expires - and unlink is meaningless because there is no
// stored share to revoke. Directories cannot be linked: they return
// fs.ErrorCantShareDirectories.
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (string, error) {
	if strings.HasSuffix(remote, "/") {
		return "", fs.ErrorCantShareDirectories
	}
	obj, err := f.NewObject(ctx, remote)
	if err != nil {
		return "", err
	}
	o := obj.(*Object)
	// Neither shared space takes an expiry parameter.
	if f.space != spacePersonal {
		if expire.IsSet() {
			fs.Logf(f, "Public Link: the 139 %s space has no expiry parameter, ignoring the requested expiry of %v", f.space, expire)
		}
		// fetchDownloadURL is the uncached path.
		return f.fetchDownloadURL(ctx, o)
	}
	expireSec := defaultLinkExpireSec
	if expire.IsSet() {
		expireSec = int(time.Duration(expire).Seconds())
		if expireSec > maxLinkExpireSec {
			fs.Logf(f, "Public Link: reducing expiry to %v, the longest the 139 client asks for", time.Duration(maxLinkExpireSec)*time.Second)
			expireSec = maxLinkExpireSec
		}
		if expireSec < 1 {
			expireSec = 1
		}
	}
	return f.fetchDownloadURLExpire(ctx, o, expireSec)
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
		// BatchFileResults carries one entry per file of a batch copy or
		// move. For a copy, rstFile is the created entry while fileId and
		// srcFile describe the source, so only rstFile identifies the
		// copy.
		BatchFileResults []struct {
			ErrCode string `json:"errCode"`
			FileID  string `json:"fileId"`
			RstFile struct {
				FileID string `json:"fileId"`
			} `json:"rstFile"`
		} `json:"batchFileResults"`
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

// taskGet polls /hcy/task/get until the task finishes and returns the id of
// the entry a copy created, which the response carries in
// data.batchFileResults[].rstFile.fileId. A move or delete reports no new
// entry, so the id is empty for those.
//
// Delete / move / copy on 139 are task-based: the mutating call returns
// a taskId immediately and the change happens in the background. rclone
// callers expect the operation to be done when the call returns, so we
// poll. The official client polls ~1/s; we pace ourselves with the
// pacer and give up after 60s.
func (f *Fs) taskGet(ctx context.Context, taskID, what string) (string, error) {
	body := map[string]any{"taskId": taskID}
	deadline := time.Now().Add(60 * time.Second)
	for {
		var out taskPollResult
		err := f.pacer.Call(func() (bool, error) {
			err := f.personalCall(ctx, "/hcy/task/get", body, &out)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return "", fmt.Errorf("yun139: poll %s task: %w", what, err)
		}
		if !out.Success {
			return "", &apiError{Code: out.Code, Message: out.Message}
		}
		switch {
		case taskStatusDone(out.Data.TaskInfo.Status):
			for _, r := range out.Data.BatchFileResults {
				if r.RstFile.FileID != "" {
					return r.RstFile.FileID, nil
				}
			}
			return "", nil
		case taskStatusFailed(out.Data.TaskInfo.Status):
			return "", fmt.Errorf("yun139: %s task %s failed", what, taskID)
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("yun139: %s task %s did not finish in 60s", what, taskID)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
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
// completes, and returns the id of the entry a copy created
// (contentList[].rstID). State machine (captured 2026-09-03):
//
//	taskStatus 0 = running, 1 = running, 2 = success,
//	taskResultCode 1 means success, 0 means failed.
//
// The per-item fields vary with the task type rather than the operation
// family: a copy carries the id of the entry it created in rstID, while a
// delete reports no rstID at all. Only the task-level taskResultCode is a
// success signal; the per-item reason is not compared against any code.
func (f *Fs) familyTaskPoll(ctx context.Context, taskID string) (string, error) {
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
				SrcID string `json:"srcID"`
				RstID string `json:"rstID"`
			} `json:"contentList"`
		}
		err := f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/queryBatchOprTaskDetailV3", body, &out)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return "", fmt.Errorf("yun139: poll family task: %w", err)
		}
		if out.Result.ResultCode != "0" {
			return "", &apiError{Code: out.Result.ResultCode, Message: out.Result.ResultDesc}
		}
		switch out.BatchOprTask.TaskStatus {
		case 0, 1:
			// 0 = created, 1 = running - keep polling
		case 2:
			// Success only when resultCode==1.
			if out.BatchOprTask.TaskResultCode != nil && *out.BatchOprTask.TaskResultCode == 1 {
				for _, c := range out.ContentList {
					if c.RstID != "" {
						return c.RstID, nil
					}
				}
				// No entry was created. That is the normal outcome of a
				// delete, which carries no rstID; a copy that produced
				// nothing is reported as an unidentified copy by the
				// caller. A copy that DID land without echoing its id
				// cannot be identified, so it is left in place and named
				// here rather than guessed at by name.
				fs.Debugf(f, "yun139: family task %s created no identifiable entry; a copy that landed without an echoed id is left in place", taskID)
				return "", nil
			}
			return "", fmt.Errorf("yun139: family task %s finished with resultCode %v", taskID, out.BatchOprTask.TaskResultCode)
		case 3:
			return "", fmt.Errorf("yun139: family task %s failed", taskID)
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("yun139: family task %s did not finish in 60s", taskID)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Second):
		}
	}
}
