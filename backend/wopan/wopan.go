// Package wopan provides an interface to the China Unicom cloud drive (联通云盘).
package wopan

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/rclone/rclone/backend/wopan/api"
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
	"github.com/rclone/rclone/lib/rest"
	"golang.org/x/sync/errgroup"
)

// ------------------------------------------------------------ constants ----

const (
	clientID         = "1001000021"
	clientSecret     = "XFmi9GS2hzk98jGX"
	appID            = "10000001"
	baseURL          = "https://panservice.mail.wo.cn"
	fixedIV          = "wNSOYIB1k1DjY5lA"
	partSize         = int64(8 * 1024 * 1024) // 8 MiB
	chanAPIUser      = "api-user"
	chanWoHome       = "wohome"
	chanWoCloud      = "wocloud"
	versionString    = "" // participates in the signature, always empty
	successCode      = "0000"
	defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.37"

	// aesKeyLen is the length of the wohome AES key, which is derived from the
	// first 16 bytes of the access token.
	aesKeyLen = 16

	// defaultRootID is the id of the account root in both the personal and the
	// family space.
	defaultRootID = "0"

	// spacePersonal and spaceFamily are the spaceType values.
	spacePersonal = "0"
	spaceFamily   = "1"

	// fileTypeDir and fileTypeFile are the numeric `type` values returned by
	// QueryAllFiles. QueryRecycleData uses strings instead.
	fileTypeDir  = 0
	fileTypeFile = 1

	listPageSize = 100
	// maxListPages bounds the paging loop in case the server keeps returning
	// the same page instead of running out of entries.
	maxListPages = 1000

	minSleep      = 10 * time.Millisecond
	maxSleep      = 2 * time.Second
	decayConstant = 2 // bigger for slower decay, exponential

	// downloadURLTTL is how long a fetched download URL is reused. The link lives
	// exactly 120 minutes from being issued (measured 2026-09-30; it does not slide
	// with access), so this is a quarter of the real lifetime. A link that ages out
	// early is refetched rather than reported as a read failure (see staleLinkStatus),
	// so this window is a minting-rate choice rather than a correctness ceiling.
	downloadURLTTL = 30 * time.Minute

	// statusLinkExpired is what the download CDN answers for a link that has aged
	// out on its own. Measured 2026-09-30: 519 = expiry, 518 = a forged/tampered
	// link, 403 = revoked or unauthorised. 518 is deliberately not in here: it is a
	// statement about the URL we built, so refetching the same way would retry the
	// bug rather than the expiry.
	statusLinkExpired = 519

	// hashGracePeriod is how long after an upload Hash() swallows transient
	// errors: within the ~15s listing-visibility window the download link may
	// not be ready, and an error would make verify delete the just-uploaded file.
	hashGracePeriod = 60 * time.Second

	// largeHashGracePeriod covers big uploads: every file of 8 MiB or
	// more is stored server-side in shards and the download link can
	// transiently fail for minutes after the upload. A single-part
	// upload (everything rclone sends) has a true content-MD5 ETag
	// within seconds of completion; multi-part uploads from other
	// clients keep a composite ETag forever, which ContentETag reports
	// as no hash rather than a wrong MD5.
	largeHashGracePeriod = 10 * time.Minute

	// dirFindAttempts/dirFindDelay bound the re-lookup when CreateDirectory
	// reports an existing directory: the entry can be missing from its parent
	// listing for a few seconds after creation (listing eventual
	// consistency), and a bare lookup would fail with ErrorDirNotFound.
	dirFindAttempts = 4
	dirFindDelay    = 2 * time.Second

	// renameDeadline bounds the rename retry loop in Update and the copy/move
	// paths: the name index is released asynchronously after a delete, and the
	// pacer's default budget (~4.5s) is too short.
	renameDeadline = 60 * time.Second

	// tempSuffix is the suffix appended to Update's temporary upload name.
	tempSuffix = ".rclone-tmp-"

	// backupSuffix is the suffix appended to the OLD object's name while an
	// Update swaps in new content, so the old bytes stay reachable (and
	// restorable) until the new object has taken over the real name.
	backupSuffix = ".rclone-old-"

	// recyclePageSize is the page size for QueryRecycleData. Whether the
	// server honours pageNum is unknown (early probing was inconclusive
	// beyond page 0), so listRecycleAll probes further pages and falls back
	// to a single large page when they repeat.
	recyclePageSize = 1000

	// maxRecyclePages bounds the recycle-bin scan: 10 full pages of 1000
	// entries cover any realistic personal bin while keeping a
	// page-num-ignoring server bounded to 10 identical fetches.
	maxRecyclePages = 10

	// recycleBudget bounds the wait for deleted entries to become visible in
	// the recycle bin; the measured visibility delay is ~2.1s.
	recycleBudget = 30 * time.Second

	// copyVisibilityBudget bounds the wait for a server-side copy to show up in
	// the destination listing.
	copyVisibilityBudget = 20 * time.Second

	// copyClockSlack tolerates second-truncated server timestamps and small
	// client/server clock skew when telling a fresh copy from a pre-existing
	// same-name file.
	copyClockSlack = 2 * time.Second
)

// RSP_CODE values we act on.
const (
	// codeAuthInvalid is the only code that means "the token is no longer
	// valid". Everything else is a business error.
	codeAuthInvalid = "1001"
	// codeIllegalName is returned when a file or directory name contains a
	// character wopan refuses to store (RSP_DESC "名称中含有非法字符").
	// The rejection is deterministic for the given name, so it must carry
	// NoRetryError: re-running the whole sync round can never succeed.
	codeIllegalName = "1009"
	// codeSensitiveWord is returned when a file or directory name trips the
	// server's sensitive-word filter (RSP_DESC "您输入的文件夹名称含有敏感词！").
	// Like codeIllegalName, it is deterministic for the given name.
	codeSensitiveWord = "4444"
	// codeSpaceFull is returned when the account's quota is exhausted
	// (RSP_DESC "空间已满"). Freeing space is a manual action taken outside the
	// run, so no retry inside it can ever succeed and it must carry
	// NoRetryError. Left retryable it turned one full account into a retry
	// storm - 7348 attempts in a single stage 2 run - that the logs made look
	// like a transport fault.
	codeSpaceFull = "2001"
	// codeDirExists is returned by CreateDirectory when the name is taken;
	// Mkdir must treat it as success.
	codeDirExists = "130007"
	// codeFileOccupied is returned by RenameFileOrDirectory while the source is
	// still being settled; it is transient and safe to retry.
	codeFileOccupied = "500010006"
	// codeNameOccupied is returned by RenameFileOrDirectory when the target name
	// is already taken. A genuine collision never resolves, but the name index
	// is released asynchronously after a delete, so a rename onto a just-freed
	// name can transiently answer it: renameWithBackoff retries it by id until
	// its deadline, which only delays the terminal error for a real conflict.
	// It must not be handed to rclone's --retries as a retryable error.
	codeNameOccupied = "130012"
)

// ------------------------------------------------------------ benefit ------

// Constants for the membership benefit probe.
//
// This is a second, unrelated request shape on the dispatcher's host: the body
// and the response are plain JSON, there is no AES param and no signature, and
// the envelope is meta/result. It is sent with the header set captured from the
// wocloud PC client because that is the request that was observed to work.
//
// Measured against a live non-member account (2026-10-19): the answer is the
// same for Client-Id 1001000209 and for the dispatcher's 1001000021, and it is
// unchanged by dropping User-Agent, Client-Id, pc-version/pc-os or by adding
// Origin and Referer. What the request cannot lose is the token: sending only
// the lowercase `accesstoken` (the name the dispatcher uses) is answered
// `10002404 用户未登录`, while `Access-Token` alone is accepted. All three names
// are therefore sent - dropping to one of them, or to the dispatcher's spelling,
// makes the probe fail open permanently and silently.
const (
	membershipBenefitPath = "/api/bff/membershipBenefit/displayMembershipBenefit"
	benefitClientID       = "1001000209"
	benefitPCVersion      = "2.8.13"
	benefitUserAgent      = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) wocloud/2.8.13 Chrome/108.0.5359.215 Electron/22.3.27 Safari/537.36"
	benefitQryType        = "1" // TODO: meaning unprobed; sent as captured
	benefitSuccessCode    = "200"
	// benefitTimeout bounds the probe that runs while the remote is opening. The
	// endpoint is not on the transfer path, so a stalled connection to it must never
	// become the reason a command does not start.
	benefitTimeout = 30 * time.Second

	// uploadFileSizeUnit converts the reported uploadFileSize to bytes: the value
	// is in MiB, measured against the same account's QueryCloudUsageInfo
	// byteTotalSize (see api.MembershipBenefit). That method also carries a
	// top-level vipLevel, and on the measured non-member account both endpoints
	// say "0" - so running `rclone about` on the same remote is a cross-check of
	// the tier code, at the cost of one extra quota request.
	uploadFileSizeUnit = int64(1) << 20
)

// benefitStaticHeaders are the captured request headers that do not carry the
// token. The token is sent under three different names, all with the same value.
//
// Host, Content-Length, Connection and Accept-Encoding are deliberately left to
// the Go transport: the capture advertises `gzip, deflate, br`, and rclone reads
// the response itself, so advertising brotli would ask the server for a body Go
// cannot decode.
//
// One of these is not what it looks like. rclone's transport sets User-Agent on
// every request it sends (fs/fshttp: req.Header.Set("User-Agent", ...)), so the
// wocloud/Electron string below is replaced by `rclone/<version>` on the wire
// unless the operator pins `--wopan-user_agent`, which is rclone's generic
// per-remote Networking option. Measured 2026-10-20 (_benefit-probe/ua-probe.txt):
// the captured UA, `rclone/v1.76.0` and no UA at all return byte-identical
// responses, so the endpoint does not read it. The entry stays because it is what
// the request was measured with, and because a transport that stops overwriting it
// would then send the right thing.
var benefitStaticHeaders = [][2]string{
	{"pc-version", benefitPCVersion},
	{"pc-os", "win"},
	{"Client-Id", benefitClientID},
	{"User-Agent", benefitUserAgent},
	{"Accept", "application/json, text/plain, */*"},
	{"Content-Type", "application/json"},
	{"sec-ch-ua", `"Not?A_Brand";v="8", "Chromium";v="108"`},
	{"sec-ch-ua-mobile", "?0"},
	{"sec-ch-ua-platform", `"Windows"`},
	{"Sec-Fetch-Site", "cross-site"},
	{"Sec-Fetch-Mode", "cors"},
	{"Sec-Fetch-Dest", "empty"},
	{"Accept-Language", "zh-CN"},
}

// benefitTokenHeaders are the three header names the PC client puts the access
// token in. Only `Access-Token` (and/or `X-YP-Access-Token`) is required - the
// dispatcher's lowercase `accesstoken` on its own is answered 用户未登录 - but all
// three are sent because that is the captured request.
//
// They are set through the header map rather than Header.Set so the captured
// spelling is what goes on the wire. That fidelity has not been shown to matter:
// the endpoint reads a name Header.Set would have spelled the same way. It is
// kept so the request rclone sends is the request that was measured.
var benefitTokenHeaders = []string{"accesstoken", "Access-Token", "X-YP-Access-Token"}

// ------------------------------------------------------------ 秒传 ----------

// Constants for the instant-upload ("秒传") probe.
//
// The host is not panservice: it is the openapi operator the official web bundle
// itself names for uploads (`du.smartont.net:/openapi/client/upload2C`), and
// b.smartont.net answers on the same address. The request is plain JSON with one
// credential header, `access-token`, holding the same access token the dispatcher
// uses; the envelope is meta/result with meta.code "0000".
//
// Verified against a live account on 2026-10-20: a SHA-256 the service had never
// seen answers `hasFile 0` with `fileId null`; the SHA-256 of content that had
// just been uploaded answers `hasFile 1` plus fid/wcFileId, and the entry appears
// in the destination directory at once with no bytes transferred.
const (
	quickTransferURL = "https://b.smartont.net/openapi/transfer/quickTransfer"
	// quickTransferMinSize is the floor the official client is reported to use,
	// below which the probe is not worth a request. Third-party claim, not
	// measured here.
	quickTransferMinSize = int64(3) << 20
	quickTransferTimeout = 30 * time.Second
	// quickTransferSuccessCode is meta.code when the probe was understood.
	quickTransferSuccessCode = "0000"
)

// ------------------------------------------------------------ crypto -------

// pkcs7Pad pads b with PKCS#7 padding to a multiple of the given block size.
func pkcs7Pad(b []byte, block int) []byte {
	n := block - len(b)%block
	return append(b, bytes.Repeat([]byte{byte(n)}, n)...)
}

// pkcs7Unpad removes PKCS#7 padding from b.
func pkcs7Unpad(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("empty ciphertext")
	}
	n := int(b[len(b)-1])
	if n == 0 || n > len(b) {
		return nil, fmt.Errorf("bad pkcs7 padding: %d", n)
	}
	return b[:len(b)-n], nil
}

// aesKeyFor selects the AES key by channel.
//
// wohome uses accessToken[:16]; api-user uses the fixed clientSecret. The key
// is unrelated to the signature, which uses the method name.
func aesKeyFor(channel, accessToken string) []byte {
	if channel == chanAPIUser {
		return []byte(clientSecret)
	}
	return []byte(accessToken[:16])
}

// aesEncrypt encrypts plain with AES-128-CBC + PKCS7 + base64 using the fixed IV.
func aesEncrypt(plain, key []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	padded := pkcs7Pad(plain, block.BlockSize())
	out := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, []byte(fixedIV)).CryptBlocks(out, padded)
	return base64.StdEncoding.EncodeToString(out), nil
}

// aesDecrypt decrypts a base64 ciphertext with AES-128-CBC + PKCS7 using the fixed IV.
func aesDecrypt(b64 string, key []byte) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if len(raw)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("ciphertext not a multiple of block size: %d", len(raw))
	}
	out := make([]byte, len(raw))
	cipher.NewCBCDecrypter(block, []byte(fixedIV)).CryptBlocks(out, raw)
	return pkcs7Unpad(out)
}

// md5hex returns the lower-case hex MD5 of s.
func md5hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

// sign computes the request signature.
//
// The string to sign is method + resTime + reqSeq + channel + version, where
// method is the method name (== header.key), NOT the AES encryption key.
func sign(method string, resTime int64, reqSeq int, channel string) string {
	return md5hex(fmt.Sprintf("%s%d%d%s%s", method, resTime, reqSeq, channel, versionString))
}

// ------------------------------------------------------------ envelope -----

// responseEnvelope mirrors the raw dispatcher response for decode purposes.
type responseEnvelope struct {
	Status string `json:"STATUS"`
	Msg    string `json:"MSG"`
	LogID  string `json:"LOGID"`
	Rsp    struct {
		RspCode string          `json:"RSP_CODE"`
		RspDesc string          `json:"RSP_DESC"`
		Data    json.RawMessage `json:"DATA"`
	} `json:"RSP"`
}

// decodeData applies the DATA three-branch rule to a raw DATA payload.
//
//   - a quoted non-empty string is ciphertext: strip quotes, decrypt
//   - the empty string "" (or the quoted empty string) means no data: return null
//   - anything else (a JSON object) is plaintext: return as-is
//
// The emptiness check must happen before decryption, otherwise decrypting an
// empty string errors out.
func decodeData(raw json.RawMessage, channel, accessToken string) (json.RawMessage, error) {
	s := string(raw)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		inner := s[1 : len(s)-1]
		if inner == "" {
			return json.RawMessage("null"), nil
		}
		dec, err := aesDecrypt(inner, aesKeyFor(channel, accessToken))
		if err != nil {
			return nil, fmt.Errorf("decrypt DATA: %w", err)
		}
		return json.RawMessage(dec), nil
	}
	// Empty string (no quotes) also means no data.
	if s == "" {
		return json.RawMessage("null"), nil
	}
	return raw, nil
}

// ------------------------------------------------------------ errors -------

// apiError is a business error signalled by a non-zero RSP_CODE.
type apiError struct {
	Code  string
	Desc  string
	LogID string
}

// Error implements the error interface.
func (e *apiError) Error() string {
	return fmt.Sprintf("RSP_CODE=%s RSP_DESC=%s LOGID=%s", e.Code, e.Desc, e.LogID)
}

// isAuthInvalid reports whether the error is the authentication-expired
// code (1001), which is the only code that triggers a token refresh.
//
// The check is a fail-closed whitelist: any other code, including unknown
// ones, is treated as a business error so that a refresh_token is never
// rotated for a request that failed for another reason.
func isAuthInvalid(err error) bool {
	var ae *apiError
	if !asAPIError(err, &ae) {
		return false
	}
	return ae.Code == codeAuthInvalid
}

func asAPIError(err error, target **apiError) bool {
	for err != nil {
		if ae, ok := err.(*apiError); ok {
			*target = ae
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// ------------------------------------------------------------ transport ----

// client abstracts the HTTP transport so call can be tested without a network.
type client interface {
	Do(*http.Request) (*http.Response, error)
}

// paramFunc 在发送时才给出请求体，而不是提前捕获一份。
// AppQueryUser 把 access_token 放在 param 里面，所以换票之后那次重试必须重建 param：
// 沿用换票前捕获的值，等于让服务端去校验一个刚被这次换票作废的令牌。
type paramFunc func() any

func resolveParam(p any) any {
	if pf, ok := p.(paramFunc); ok {
		return pf()
	}
	return p
}

// call issues one dispatcher request and returns the decoded DATA payload.
//
// rootURL is the dispatcher base (normally baseURL); method is the method
// name (== header.key); param, when non-nil, is encrypted into body.param, and
// may be a paramFunc to build it at send time; extra is merged into body as
// plaintext fields.
func call(ctx context.Context, c client, rootURL, accessToken, channel, method string, param any, extra map[string]any) (json.RawMessage, error) {
	if rootURL == "" {
		rootURL = baseURL
	}
	param = resolveParam(param)
	resTime := time.Now().UnixMilli()
	reqSeq := 100000 + rand.Intn(8999)
	signature := sign(method, resTime, reqSeq, channel)

	body := map[string]any{}
	for k, v := range extra {
		body[k] = v
	}
	if param != nil {
		plain, err := json.Marshal(param)
		if err != nil {
			return nil, err
		}
		enc, err := aesEncrypt(plain, aesKeyFor(channel, accessToken))
		if err != nil {
			return nil, err
		}
		body["param"] = enc
	}

	payload, err := json.Marshal(map[string]any{
		"header": api.Header{
			Key:     method,
			ResTime: resTime,
			ReqSeq:  reqSeq,
			Channel: channel,
			Sign:    signature,
			Version: versionString,
		},
		"body": body,
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rootURL+"/"+channel+"/dispatcher", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://pan.wo.cn")
	req.Header.Set("Referer", "https://pan.wo.cn/")
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accesstoken", accessToken)

	res, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("http %s: %s", res.Status, truncate(string(raw), 500))
	}

	var env responseEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return nil, fmt.Errorf("decode envelope: %w (body %s)", err, truncate(string(raw), 500))
	}
	if env.Status != "200" {
		return nil, fmt.Errorf("STATUS=%s MSG=%s", env.Status, env.Msg)
	}
	if env.Rsp.RspCode != successCode {
		err := &apiError{Code: env.Rsp.RspCode, Desc: env.Rsp.RspDesc, LogID: env.LogID}
		// 1009 and 4444 are deterministic for the given name: wopan refuses to
		// store the character (or trips the sensitive-word filter), no retry can
		// ever succeed, so carry NoRetryError with a message that says what
		// happened instead of a bare RSP_CODE dump.
		if env.Rsp.RspCode == codeIllegalName || env.Rsp.RspCode == codeSensitiveWord {
			return nil, fserrors.NoRetryError(fmt.Errorf("wopan: file or directory name is rejected by the server: %w", err))
		}
		if env.Rsp.RspCode == codeSpaceFull {
			return nil, noSpaceError(err)
		}
		return nil, err
	}
	return decodeData(env.Rsp.Data, channel, accessToken)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// noSpaceError marks the exhausted-quota code (2001) as a no-retry error.
// Freeing space is a manual cleanup outside the run, so neither the low level
// retry ladder nor --retries can make the request succeed; retrying only turns
// a full account into a storm of identical failures.
func noSpaceError(err error) error {
	return fserrors.NoRetryError(fmt.Errorf("wopan: the account has no space left: %w", err))
}

// ------------------------------------------------------------ helpers -----

// spaceParams builds the common wohome write-operation parameters.
//
// Personal space (spaceType "0") must omit the familyId key entirely; sending
// an empty-string familyId makes the server return RSP_CODE 9999.
func spaceParams(spaceType, familyID string) map[string]any {
	p := map[string]any{
		"spaceType": spaceType,
		"clientId":  clientID,
	}
	if spaceType == "1" {
		p["familyId"] = familyID
	}
	return p
}

// wopanBMPEmoji lists the BMP codepoints with the Unicode Emoji=Yes property
// (emoji pictographs), excluding the keycap bases (# * 0-9), the keycap joiner
// U+20E3, the ZWJ U+200D and the variation selector U+FE0F, which are
// Emoji=Yes only as parts of emoji sequences.
//
// Live probing against the wopan server (2026-09-19) showed it rejects exactly
// this class of name character: every sampled Emoji=Yes BMP codepoint was
// rejected with RSP_CODE 1009 "名称中含有非法字符" on CreateDirectory and a
// bare HTTP 500 at upload time, while every sampled non-emoji symbol
// (½ ¾ ¿ À é ✓ Ａ ° ± ² ³ ¼ …) was accepted. Generated from
// https://www.unicode.org/Public/UCD/latest/ucd/emoji/emoji-data.txt.
var wopanBMPEmoji = []struct{ lo, hi rune }{
	{0x00A9, 0x00A9},
	{0x00AE, 0x00AE},
	{0x203C, 0x203C},
	{0x2049, 0x2049},
	{0x2122, 0x2122},
	{0x2139, 0x2139},
	{0x2194, 0x2199},
	{0x21A9, 0x21AA},
	{0x231A, 0x231B},
	{0x2328, 0x2328},
	{0x23CF, 0x23CF},
	{0x23E9, 0x23F3},
	{0x23F8, 0x23FA},
	{0x24C2, 0x24C2},
	{0x25AA, 0x25AB},
	{0x25B6, 0x25B6},
	{0x25C0, 0x25C0},
	{0x25FB, 0x25FE},
	{0x2600, 0x2604},
	{0x260E, 0x260E},
	{0x2611, 0x2611},
	{0x2614, 0x2615},
	{0x2618, 0x2618},
	{0x261D, 0x261D},
	{0x2620, 0x2620},
	{0x2622, 0x2623},
	{0x2626, 0x2626},
	{0x262A, 0x262A},
	{0x262E, 0x262F},
	{0x2638, 0x263A},
	{0x2640, 0x2640},
	{0x2642, 0x2642},
	{0x2648, 0x2653},
	{0x265F, 0x2660},
	{0x2663, 0x2663},
	{0x2665, 0x2666},
	{0x2668, 0x2668},
	{0x267B, 0x267B},
	{0x267E, 0x267F},
	{0x2692, 0x2697},
	{0x2699, 0x2699},
	{0x269B, 0x269C},
	{0x26A0, 0x26A1},
	{0x26A7, 0x26A7},
	{0x26AA, 0x26AB},
	{0x26B0, 0x26B1},
	{0x26BD, 0x26BE},
	{0x26C4, 0x26C5},
	{0x26C8, 0x26C8},
	{0x26CE, 0x26CF},
	{0x26D1, 0x26D1},
	{0x26D3, 0x26D4},
	{0x26E9, 0x26EA},
	{0x26F0, 0x26F5},
	{0x26F7, 0x26FA},
	{0x26FD, 0x26FD},
	{0x2702, 0x2702},
	{0x2705, 0x2705},
	{0x2708, 0x270D},
	{0x270F, 0x270F},
	{0x2712, 0x2712},
	{0x2714, 0x2714},
	{0x2716, 0x2716},
	{0x271D, 0x271D},
	{0x2721, 0x2721},
	{0x2728, 0x2728},
	{0x2733, 0x2734},
	{0x2744, 0x2744},
	{0x2747, 0x2747},
	{0x274C, 0x274C},
	{0x274E, 0x274E},
	{0x2753, 0x2755},
	{0x2757, 0x2757},
	{0x2763, 0x2764},
	{0x2795, 0x2797},
	{0x27A1, 0x27A1},
	{0x27B0, 0x27B0},
	{0x27BF, 0x27BF},
	{0x2934, 0x2935},
	{0x2B05, 0x2B07},
	{0x2B1B, 0x2B1C},
	{0x2B50, 0x2B50},
	{0x2B55, 0x2B55},
	{0x3030, 0x3030},
	{0x303D, 0x303D},
	{0x3297, 0x3297},
	{0x3299, 0x3299},
}

// isWopanBMPEmoji reports whether r is a BMP emoji pictograph that wopan
// refuses to store in file or directory names. The ranges are non-overlapping
// and ascending, so both lo and hi are monotonic: a binary search over hi
// finds the first range that could contain r, then the lo bound is checked.
func isWopanBMPEmoji(r rune) bool {
	i := sort.Search(len(wopanBMPEmoji), func(i int) bool {
		return wopanBMPEmoji[i].hi >= r
	})
	return i < len(wopanBMPEmoji) && r >= wopanBMPEmoji[i].lo
}

// validateName checks a leaf file name against wopan's storage rules.
//
// It returns an error wrapped with NoRetryError for names longer than 100
// runes (which the server would silently truncate, risking silent data
// corruption), containing a non-BMP character (which the server rejects
// with HTTP 500), or containing a BMP emoji pictograph (which the server
// rejects with RSP_CODE 1009 "名称中含有非法字符", refusing the whole path).
// The NoRetryError wrapper stops --retries from re-running a whole sync
// round, which is pointless for a name that can never succeed.
//
// sanitize (自动替换非法字符为 _) is intentionally NOT offered: sync is
// bidirectional, and a name transformed on the way up cannot be untransformed
// on the way down.
func validateName(leaf string) error {
	n := utf8.RuneCountInString(leaf)
	if n > 100 {
		// Keep the sentinel wrapped so callers can still test for it, but say
		// which limit fired: the multithread path prefixes this error with
		// "failed to open chunk writer", which otherwise reads like a local
		// ENAMETOOLONG rather than a server-side name rejection.
		return fserrors.NoRetryError(fmt.Errorf("%w (wopan limit is 100 runes, got %d; the server would silently truncate it)", fs.ErrorFileNameTooLong, n))
	}
	for _, r := range leaf {
		if isWopanBMPEmoji(r) {
			return fserrors.NoRetryError(fmt.Errorf(
				"file name contains an emoji character %q (U+%04X) which wopan cannot store: %q", r, r, leaf))
		}
		if r > 0xFFFF {
			return fserrors.NoRetryError(fmt.Errorf(
				"file name contains a non-BMP character %q (U+%04X) which wopan cannot store: %q", r, r, leaf))
		}
	}
	return nil
}

// ------------------------------------------------------------ config -------

// Options defines the configuration for this backend
type Options struct {
	RefreshToken      string               `config:"refresh_token"`
	AccessToken       string               `config:"access_token"`
	FamilyID          string               `config:"family_id"`
	RootFolderID      string               `config:"root_folder_id"`
	NoRefresh         bool                 `config:"no_refresh"`
	HardDelete        bool                 `config:"hard_delete"`
	UploadZone        string               `config:"upload_zone"`
	DisableHTTP2      bool                 `config:"disable_http2"`
	UploadCutoff      fs.SizeSuffix        `config:"upload_cutoff"`
	ChunkSize         fs.SizeSuffix        `config:"chunk_size"`
	UploadConcurrency int                  `config:"upload_concurrency"`
	NoMemberCheck     bool                 `config:"no_member_check"`   // skip the membership probe and its over-limit notice entirely
	QuickTransfer     bool                 `config:"quick_transfer"`    // probe openapi/transfer/quickTransfer before uploading files >=3 MiB
	FatalIfNoSpace    bool                 `config:"fatal_if_no_space"` // turn the exhausted-quota answer into a fatal error that cancels the run
	Enc               encoder.MultiEncoder `config:"encoding"`
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "wopan",
		Description: "China Unicom Cloud Drive (联通云盘)",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "refresh_token",
			Help:      "Refresh token. Obtain from an existing OpenList wopan storage config, or by capturing the app login flow.",
			Sensitive: true,
		}, {
			Name: "access_token",
			Help: "Access token - refreshed automatically, do not set manually.\n\n" +
				"Only set this if you want a fully static token that rclone never refreshes.",
			Advanced:  true,
			Sensitive: true,
		}, {
			Name:     "family_id",
			Help:     "Family cloud ID. Leave blank for personal space.",
			Advanced: true,
		}, {
			Name:      "root_folder_id",
			Help:      "ID of the root folder. Leave blank for the top level.",
			Advanced:  true,
			Sensitive: true,
		}, {
			Name: "no_refresh",
			Help: "Never refresh the token. Use when the same account is shared with another program.\n\n" +
				"Set this when the same account is also in use by another program such as " +
				"OpenList - see [Sharing an account with OpenList](#sharing-an-account-with-openlist) below.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "hard_delete",
			Help: "Delete permanently instead of putting files into the recycle bin.\n\n" +
				"Deleting files from the recycle bin still works, so quota is only released " +
				"immediately with this flag. Note that permanent deletion works by purging the " +
				"file from the recycle bin: when the bin holds more entries than the server is " +
				"willing to page out, files beyond that page can only be moved into the bin " +
				"(rclone logs a warning) - empty the recycle bin from the app to restore full " +
				"hard delete.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "upload_zone",
			Help: "Upload endpoint override, e.g. https://tjupload.pan.wo.cn.\n\n" +
				"Leave blank to use the zone the server assigns per account (recommended). " +
				"When set, all upload traffic - file contents and the access token - goes " +
				"through the given host, so only point it at a server you trust, such as " +
				"your own reverse proxy. The URL must present a valid TLS certificate.",
			Advanced: true,
		}, {
			Name: "disable_http2",
			Help: "Disable HTTP/2 for all wopan traffic.\n\n" +
				"HTTP/2 multiplexes every concurrent part upload onto a single TCP " +
				"connection, which caps aggregate upload throughput at one connection's " +
				"worth of bandwidth. Set this to use HTTP/1.1 keep-alive instead, so the " +
				"concurrent part uploads spread across multiple parallel TCP connections.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "upload_cutoff",
			Help: "Cutoff for switching to chunked upload. Defaults to 64Mi.\n\n" +
				"Any files larger than this will be uploaded in chunks of chunk_size. " +
				"Smaller files are sent as a single request, where the server's ETag is " +
				"the content MD5 immediately at upload time, while chunked files never " +
				"carry a content MD5 - see [Chunked uploads](#chunked-uploads) below.",
			Default:  fs.SizeSuffix(64 * 1024 * 1024),
			Advanced: true,
		}, {
			Name: "chunk_size",
			Help: "Chunk size to use for uploading.\n\n" +
				"Files larger than upload_cutoff are uploaded in chunks of this size, " +
				"the last chunk absorbing the remainder and possibly reaching twice " +
				"this size, matching the official client. " +
				"The minimum is 5Mi: smaller chunks have triggered a server error " +
				"when the upload was completing.\n\n" +
				"Note that '--wopan-upload-concurrency' chunks of this size are " +
				"buffered in memory per transfer, and each buffer may reach " +
				"twice this size.",
			Default:  fs.SizeSuffix(8 * 1024 * 1024),
			Advanced: true,
		}, {
			Name: "upload_concurrency",
			Help: "Concurrency for chunked uploads.\n\n" +
				"This is the number of chunks of the same file that are uploaded " +
				"concurrently. The server accepts out-of-order parts within one " +
				"upload session and assembles them by part index. Increasing this " +
				"may speed up transfers of large files, at the cost of more memory - " +
				"see [Chunked uploads](#chunked-uploads) below.",
			Default:  4,
			Advanced: true,
		}, {
			Name: "no_member_check",
			Help: "Disable the membership probe and its over-limit notice.\n\n" +
				"By default NewFs asks the account once for its membership and logs " +
				"the single-file upload size it reports (1 GiB for a non-member " +
				"account at the time of writing), and names any file larger than that " +
				"in the log. Nothing is blocked because of it: measured 2026-10-20, " +
				"uploads past the reported size complete through the chunked path and " +
				"are stored at full size. Set this to skip that request and the notice " +
				"as well. Default false.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "quick_transfer",
			Help: "Try the instant-upload (" + "秒传" + ") service before uploading big files.\n\n" +
				"When set, a file of 3 MiB or more in the personal space is offered to\n" +
				"the service by SHA-256 first, and if the account already stores that\n" +
				"content the object is created without transferring any bytes. The cost\n" +
				"is the hash: getting SHA-256 means reading the source again, which for\n" +
				"a local source is a second full pass over the file and for a remote one\n" +
				"can mean downloading it. It is therefore off by default.\n" +
				"Measured 2026-10-20: a created entry carries the moment it was\n" +
				"registered, not the source modification time - the service ignores the\n" +
				"timestamp sent here and wopan cannot set a modification time later - so\n" +
				"a following copy or sync re-registers it. Use --size-only with this\n" +
				"option: --checksum compares sizes anyway for files above upload_cutoff\n" +
				"(they have no stored MD5) and needs a fragile per-file HEAD below it.\n" +
				"A miss, any probe failure and every family-space transfer fall through\n" +
				"to the normal upload; only a hit reported without an object id fails,\n" +
				"because uploading then would store a second copy of content the service\n" +
				"has already registered.",
			Default:  false,
			Advanced: true,
		}, {
			Name: "fatal_if_no_space",
			Help: "Make the exhausted-quota answer (RSP_CODE 2001) cancel the whole run.\n\n" +
				"A full account is only freed by hand, so the NoRetry marking on that\n" +
				"answer stops the low level retries but not the round itself: --retries\n" +
				"still re-runs the transfer list. Measured on a real run 2026-10-21,\n" +
				"an exhausted account spent 32 minutes failing 126 file paths about ten\n" +
				"times each before rclone gave up. With this set the first 2001 becomes\n" +
				"a fatal error, which cancels the sync at once (fs/sync/sync.go). It\n" +
				"defaults to false, so interactive use is unchanged. Pair it with a\n" +
				"check of what is missing afterwards, because cancelling early leaves\n" +
				"the rest of the list untouched rather than failed.",
			Default:  false,
			Advanced: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			// EncodeInvalidUtf8 is required: file names are marshalled to JSON
			// before being encrypted, and Go would silently replace invalid
			// UTF-8 bytes with U+FFFD, making the name irreversible.
			//
			// EncodeQuestion|EncodeAsterisk|EncodeLtGt is required: the
			// server refuses ? * < > in a name with
			// '1009 名称中含有非法字符', and the upload path reports that
			// rejection as a bare HTTP 500. The encoder's fullwidth
			// substitutes for exactly those characters are stored verbatim,
			// so escaping them is what makes such names work at all. The
			// other characters EncodeWin would escape (: " |) are stored
			// verbatim by the server and are left alone.
			//
			// No Left*/Right*/Percent flags: List decodes through
			// ToStandardName, so the wire-side spelling of a name is what
			// comes back; fstests FsEncoding round-trips all cases with this
			// setting.
			Default: encoder.Standard | encoder.EncodeQuestion | encoder.EncodeAsterisk | encoder.EncodeLtGt | encoder.EncodeInvalidUtf8,
		}},
	})
}

// ------------------------------------------------------------ types --------

// Fs represents a remote wopan
type Fs struct {
	name       string             // name of this remote
	root       string             // the path we are working on
	opt        Options            // parsed options
	m          configmap.Mapper   // config mapper, used to write tokens back
	features   *fs.Features       // optional features
	httpClient *http.Client       // the connection to the server
	pacer      *fs.Pacer          // pacer for API calls
	dirCache   *dircache.DirCache // Map of directory path to directory id

	tok       *tokenState // process wide token state shared by remotes of this account
	userID    string      // userId from AppQueryUser - the invariant registry key
	spaceType string      // spacePersonal or spaceFamily

	// Detected membership benefit. oversizeNotice() is the only consumer; 0 means
	// nothing is known, so nothing is ever reported.
	memberMaxFileSize int64  // reported single-file upload size in bytes; 0 = unknown
	memberLevelName   string // vipDesc, e.g. "非会员"; logging only
	memberLevelCode   string // vipLevel, e.g. "0"; logging only

	zoneMu     *sync.Mutex // protects zoneURL / zoneLoaded (a pointer so the NewFs tempF copy is safe)
	zoneURL    string      // upload endpoint from GetZoneInfo, lazily discovered
	zoneLoaded bool        // whether zoneURL has been fetched successfully

	// dirRejects remembers directory creations the server refused, so a
	// rejected subtree costs probes rather than one listing plus one create per
	// file per round. Pointer for the same reason as zoneMu.
	dirRejects *rejectCache

	// renameDeadlineOverride shortens renameWithBackoff's deadline; tests set
	// it so a persistently failing rename does not sleep for the full budget.
	renameDeadlineOverride time.Duration

	// listBudgetOverride shortens the eventual-consistency listing budget
	// (copyVisibilityBudget); tests set it so a missing entry does not stall
	// for the full budget.
	listBudgetOverride time.Duration
}

// Object describes a wopan object
type Object struct {
	fs           *Fs       // what this object is part of
	remote       string    // the remote path
	id           string    // ID of the object
	fid          string    // fid used to request a download URL
	size         int64     // size of the object
	createTime   time.Time // when the object was created
	shootingTime time.Time // the mtime set at upload time - used as ModTime
	thumbURL     string    // thumbnail URL

	uploadedAt time.Time // when this object was last uploaded; guards Hash() against the visibility window
	multipart  bool      // uploaded in several parts; the ETag never carries a content MD5

	urlMu     sync.Mutex // protects url / urlExpiry
	url       string     // cached download URL
	urlExpiry time.Time  // when the cached URL stops being valid
}

// ------------------------------------------------------------ tokens -------

// tokenState holds the token pair shared by every wopan remote that belongs to
// the same account.
//
// AppRefreshToken rotates the refresh_token, so two remotes of one account
// that kept independent state would invalidate each other.
type tokenState struct {
	mu           sync.Mutex
	accessToken  string
	refreshToken string
	mappers      map[string]configmap.Mapper // section name -> mapper for write back
}

// tokenRegistry indexes the shared token states.
//
// byKey is keyed by the userId, which never changes. bootstrap is keyed by the
// credentials a remote was created with and is never updated, so remotes of the
// same account can find each other before their userId is known; the entry is
// migrated to byKey as soon as the userId arrives.
var tokenRegistry = struct {
	sync.Mutex
	byKey     map[string]*tokenState
	bootstrap map[string]*tokenState
}{
	byKey:     map[string]*tokenState{},
	bootstrap: map[string]*tokenState{},
}

// accessTokenNow returns the current access token.
func (ts *tokenState) accessTokenNow() string {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.accessToken
}

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

// writeBackLocked saves the current tokens into every registered section.
//
// ts.mu must be held. The in-memory values are updated by the caller before
// this runs, so a concurrent request that already picked up the new token
// keeps working while the sections are being written.
//
// configmap.Set swallows save failures by design, so a token that cannot be
// persisted is lost on exit rather than interrupting the transfer.
func (ts *tokenState) writeBackLocked() {
	for _, m := range ts.mappers {
		if m == nil {
			continue
		}
		m.Set("access_token", ts.accessToken)
		m.Set("refresh_token", ts.refreshToken)
	}
}

// bootKey is the bootstrap registry key for this remote.
//
// It is built from the credentials as they were at NewFs time and is
// deliberately never updated, so it stays stable while the tokens rotate. The
// refresh_token is the natural account identifier; when only an access_token is
// configured that is used instead, so two unrelated access-token-only remotes
// cannot collide on the empty key.
func (f *Fs) bootKey() string {
	if f.opt.RefreshToken != "" {
		return "rt:" + f.opt.RefreshToken
	}
	return "at:" + f.opt.AccessToken
}

// newTokenState builds a token state seeded from this remote's config.
func (f *Fs) newTokenState() *tokenState {
	return &tokenState{
		accessToken:  f.opt.AccessToken,
		refreshToken: f.opt.RefreshToken,
		mappers:      map[string]configmap.Mapper{},
	}
}

// tokenState returns the shared token state for this remote.
//
// The userId is the preferred key because it is an invariant. Before the userId
// is known the bootstrap key is used instead, and the first remote of an
// account to learn its userId adopts the bootstrap entry so that its siblings
// share the same state.
func (f *Fs) tokenState() *tokenState {
	tokenRegistry.Lock()
	defer tokenRegistry.Unlock()
	if f.userID != "" {
		ts, ok := tokenRegistry.byKey[f.userID]
		if !ok {
			if adopted, ok := tokenRegistry.bootstrap[f.bootKey()]; ok {
				tokenRegistry.byKey[f.userID] = adopted
				// B1 (fifth round): the bootstrap alias is deliberately KEPT,
				// not deleted. A sibling remote started later with the same -
				// or a rotated - refresh_token finds the shared state through
				// this alias; deleting it forks the account into independent
				// states whose refreshes invalidate each other (D7's core
				// promise). refreshToken appends an alias for every rotated
				// credential so the aliases track the current tokens.
				ts = adopted
			} else {
				ts = f.newTokenState()
				tokenRegistry.byKey[f.userID] = ts
			}
		}
		ts.addMapper(f.name, f.m)
		return ts
	}
	ts, ok := tokenRegistry.bootstrap[f.bootKey()]
	if !ok {
		ts = f.newTokenState()
		tokenRegistry.bootstrap[f.bootKey()] = ts
	}
	ts.addMapper(f.name, f.m)
	return ts
}

// refreshToken exchanges the refresh token for a new access token.
//
// usedToken is the access token that the server rejected. If the shared state
// has already moved on then another goroutine did the refresh and there is
// nothing left to do.
func (f *Fs) refreshToken(ctx context.Context, ts *tokenState, usedToken string) error {
	newAT, newRT, err := f.refreshTokenLocked(ctx, ts, usedToken)
	if err != nil {
		return err
	}
	// B1 (fifth round): append bootstrap aliases for the rotated credentials so
	// that a remote started later holding the rotated refresh_token (or access
	// token) converges onto this shared state instead of forking into an
	// independent one whose refreshes invalidate ours. Lock order matters:
	// ts.mu is released above because tokenState() takes registry→ts.mu; the
	// reverse order here would be an ABBA deadlock.
	tokenRegistry.Lock()
	tokenRegistry.bootstrap["rt:"+newRT] = ts
	tokenRegistry.bootstrap["at:"+newAT] = ts
	tokenRegistry.Unlock()
	return nil
}

// refreshTokenLocked performs the refresh with ts.mu held and commits the new
// pair. It returns the committed credentials so the caller can register
// bootstrap aliases for them after releasing the lock.
func (f *Fs) refreshTokenLocked(ctx context.Context, ts *tokenState, usedToken string) (newAT, newRT string, err error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.accessToken != usedToken {
		// Somebody else refreshed the token while we were waiting for the lock.
		return ts.accessToken, ts.refreshToken, nil
	}
	if ts.refreshToken == "" {
		return "", "", errors.New("no refresh_token available to refresh the access token")
	}
	param := api.RefreshTokenRequest{
		RefreshToken: ts.refreshToken,
		ClientSecret: clientSecret,
	}
	// The api-user channel is used directly rather than through f.call so that
	// a rejected token cannot recurse back into the refresh path.
	data, err := call(ctx, f.httpClient, "", ts.accessToken, chanAPIUser, "AppRefreshToken", param,
		map[string]any{"clientId": clientID, "secret": true})
	if err != nil {
		return "", "", fmt.Errorf("wopan: refresh token: %w", err)
	}
	var tr api.RefreshTokenResponse
	if err := json.Unmarshal(data, &tr); err != nil {
		return "", "", fmt.Errorf("wopan: decode refresh token response: %w", err)
	}
	// The server has already invalidated the old pair, so a partial response
	// must never be committed or the account would be locked out.
	if tr.AccessToken == "" || tr.RefreshToken == "" {
		return "", "", errors.New("wopan: refresh token response has an empty access_token or refresh_token")
	}
	// A short token would panic on accessToken[:16] in aesKeyFor and, worse,
	// would be persisted into the config by writeBackLocked. Refuse it here.
	if len(tr.AccessToken) < aesKeyLen {
		return "", "", fmt.Errorf("wopan: refreshed access_token is shorter than %d characters, refusing to commit it", aesKeyLen)
	}
	ts.accessToken = tr.AccessToken
	ts.refreshToken = tr.RefreshToken
	ts.writeBackLocked()
	return ts.accessToken, ts.refreshToken, nil
}

// ------------------------------------------------------- membership benefit -

// benefitInfo is the account's detected membership tier and the single-file
// upload size it reports with it.
type benefitInfo struct {
	maxFileSize int64  // bytes; 0 = unknown, which is never reported as an over-limit
	levelCode   string // vipLevel, e.g. "0"
	levelName   string // vipDesc, e.g. "非会员"
	raw         string // the result block as received, for the Debug log
}

// benefitCache keys the detected benefit by userId.
//
// The limit is account level - the family space is covered by the same reported
// value - so one entry per account serves every remote and both space types. A
// failed probe is cached as the zero value, so an endpoint that is down or has
// changed shape costs one request per process rather than one per remote. The
// tier belongs to the account and not to the token, so a token refresh does not
// invalidate an entry.
var benefitCache = struct {
	mu       sync.Mutex
	byUserID map[string]benefitInfo
}{byUserID: map[string]benefitInfo{}}

// queryMembershipBenefit asks the account's benefit display for its single-file
// upload limit.
//
// It uses f.httpClient directly rather than call(): this endpoint is not the
// dispatcher, so there is no AES param to build, no signature to compute and no
// RSP envelope to decode. It never refreshes the token - a rejected token is
// simply a failed probe, and the caller fails open.
func (f *Fs) queryMembershipBenefit(ctx context.Context) (benefitInfo, error) {
	body, err := json.Marshal(map[string]string{"qryType": benefitQryType})
	if err != nil {
		return benefitInfo{}, err
	}
	// A deadline of its own: this runs while the remote is being opened, and the
	// only other bound would be --timeout, which is 0 (no limit) by default. A
	// stalled connection to this endpoint must not be able to hold up `rclone lsd`.
	reqCtx, cancel := context.WithTimeout(ctx, benefitTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, baseURL+membershipBenefitPath, bytes.NewReader(body))
	if err != nil {
		return benefitInfo{}, err
	}
	token := f.tok.accessTokenNow()
	for _, h := range benefitStaticHeaders {
		req.Header[h[0]] = []string{h[1]}
	}
	for _, name := range benefitTokenHeaders {
		req.Header[name] = []string{token}
	}

	res, err := f.httpClient.Do(req)
	if err != nil {
		return benefitInfo{}, fmt.Errorf("wopan: query membership benefit: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return benefitInfo{}, fmt.Errorf("wopan: query membership benefit: read: %w", err)
	}
	if res.StatusCode != http.StatusOK {
		return benefitInfo{}, fmt.Errorf("wopan: query membership benefit: http %s: %s", res.Status, truncate(string(raw), 200))
	}

	var env api.MembershipBenefitResponse
	if err := json.Unmarshal(raw, &env); err != nil {
		return benefitInfo{}, fmt.Errorf("wopan: query membership benefit: decode: %w (body %s)", err, truncate(string(raw), 200))
	}
	if env.Meta.Code != benefitSuccessCode {
		// A dead token is answered here rather than with an HTTP status: measured
		// 2026-10-19 against a live account, HTTP 200 with
		// `{"meta":{"code":"10002404","message":"用户未登录"}}` and no result block.
		// That is deliberately not wired into the refresh path - the benefit is
		// account level, so a rejected token here is worth nothing to a retry, and
		// rotating the pair on a shared account to learn a display value would cost
		// the other program using it.
		return benefitInfo{}, fmt.Errorf("wopan: query membership benefit: meta.code=%s meta.message=%s", env.Meta.Code, env.Meta.Message)
	}
	var b api.MembershipBenefit
	if err := json.Unmarshal(env.Result, &b); err != nil {
		return benefitInfo{}, fmt.Errorf("wopan: query membership benefit: decode result: %w", err)
	}
	// A missing or unparseable uploadFileSize is treated as "nothing reported" by
	// the caller: inventing a number here would put files in the log that the
	// account never named.
	mib, err := strconv.ParseInt(string(b.UploadFileSize), 10, 64)
	if err != nil || mib <= 0 {
		return benefitInfo{}, fmt.Errorf("wopan: query membership benefit: unusable uploadFileSize %q", string(b.UploadFileSize))
	}
	return benefitInfo{
		maxFileSize: mib * uploadFileSizeUnit,
		levelCode:   string(b.VipLevel),
		levelName:   b.VipDesc,
		raw:         string(env.Result),
	}, nil
}

// probeAndCacheMembershipBenefit learns this account's reported single-file
// upload limit once per process and logs it.
//
// no_member_check skips the probe, and with it the over-limit notice. Every
// failure path leaves the limit at 0, so an unreachable or unexpected endpoint
// can never block a mount or a sync.
func (f *Fs) probeAndCacheMembershipBenefit(ctx context.Context) {
	if f.opt.NoMemberCheck {
		return
	}

	key := f.userID
	benefitCache.mu.Lock()
	info, found := benefitCache.byUserID[key]
	benefitCache.mu.Unlock()

	if !found {
		var err error
		info, err = f.queryMembershipBenefit(ctx)
		if err != nil {
			fs.Debugf(f, "wopan: membership benefit probe failed, assuming no single-file upload limit: %v", err)
			info = benefitInfo{}
		}
		benefitCache.mu.Lock()
		// The first writer wins: two remotes of one account started at the same
		// moment can both probe, and a hung endpoint must not make the second one
		// wait on the first while holding the map lock.
		if cached, raced := benefitCache.byUserID[key]; raced {
			info = cached
		} else {
			benefitCache.byUserID[key] = info
		}
		benefitCache.mu.Unlock()
	}

	f.memberMaxFileSize = info.maxFileSize
	f.memberLevelCode = info.levelCode
	f.memberLevelName = info.levelName
	// Logged only for the remote that ran the probe, so a sync over several
	// remotes of one account says this once.
	if !found && info.maxFileSize > 0 {
		fs.Logf(f, "wopan: membership %q (vipLevel=%s): reported single-file upload size %s - files over it are warned about, not skipped",
			info.levelName, info.levelCode, fs.SizeSuffix(info.maxFileSize))
		fs.Debugf(f, "wopan: displayMembershipBenefit result: %s", info.raw)
	}
}

// singleFileLimit returns the account's reported single-file upload limit in
// bytes; 0 means nothing is known and nothing is reported. no_member_check turns
// the probe off, and with it the notice.
func (f *Fs) singleFileLimit() int64 {
	if f.opt.NoMemberCheck {
		return 0
	}
	return f.memberMaxFileSize
}

// oversizeNotice is the warning line for a file over the account's reported
// single-file limit, or "" when the file fits or no limit is known.
//
// It warns and never blocks. Measured 2026-10-20 on the non-member account that
// reports uploadFileSize 1024 MiB: uploads of limit+1 and of 1.2x that limit both
// completed through the chunked path and were read back at full size. So the
// reported number is a display of the benefit, not the ceiling rclone's chunked
// uploads are held to - blocking on it would skip files the server accepts, which
// is a silently smaller sync. The notice exists so that number is visible in the
// log, and so a transfer that the *app* would refuse is still distinguishable here.
func (f *Fs) oversizeNotice(leaf string, size int64) string {
	limit := f.singleFileLimit()
	if limit <= 0 || size <= limit {
		return ""
	}
	return fmt.Sprintf("wopan: %s is %d bytes, over the single-file upload size the account reports (%d bytes, membership %q); uploading anyway - the chunked upload path is not held to that number",
		leaf, size, limit, f.memberLevelName)
}

// warnOversize logs oversizeNotice when there is one. Every upload entry point
// calls it before the first request.
func (f *Fs) warnOversize(leaf string, size int64) {
	if msg := f.oversizeNotice(leaf, size); msg != "" {
		fs.Logf(f, "%s", msg)
	}
}

// ---------------------------------------------------------- instant upload --

// quickTransfer asks the openapi service to place `leaf` in dirID from content it
// may already store, and reports whether that produced the object.
//
// Everything that should fall through to an ordinary upload returns hit=false with
// no error: the feature disabled, a file below the probe floor, a source with no
// SHA-256, a source with no modification time, a family space (the request shape
// for spaceType 1 has not been measured), a miss, a refusal with a business code, a
// rejected request (4xx), or two failed attempts to get an answer. The one case
// that must not fall through is a hit whose ids are missing: uploading then would
// store a second copy of content the service has already registered, and the
// unreadable-answer path above exists to keep that case rare rather than to make
// the upload cheap.
//
// It asks at most twice, and only when the first answer never arrived. A miss is
// the expected answer for new content, so re-asking that would spend requests on an
// account other programs share for nothing; an unreadable answer is different,
// because the service may have registered the content already and re-registering
// the same (directoryId, fileName, sha256) was measured on 2026-10-20 to leave one
// entry, not two.
func (f *Fs) quickTransfer(ctx context.Context, dirID, leaf string, size int64, src fs.ObjectInfo) (api.UploadData, bool, error) {
	if !f.opt.QuickTransfer || size < quickTransferMinSize {
		return api.UploadData{}, false, nil
	}
	if f.spaceType != spacePersonal {
		// TODO: the family-space request shape is unmeasured. The family listing
		// itself is confirmed (spaceType "1" plus familyId, and its entries carry
		// no sha256 at all), but what quickTransfer wants for that space - whether
		// spaceType "1" plus familyId is enough, or a different directoryId - has
		// not been captured. Until it is, family transfers upload normally.
		fs.Debugf(src, "wopan: rapid upload not attempted: the family-space request shape is unmeasured")
		return api.UploadData{}, false, nil
	}
	// Hashing the source is the cost of this feature: for a remote source this
	// reads it once more. Nothing else here may fail the transfer, so an
	// unavailable hash simply means no probe.
	sha256Hex, err := src.Hash(ctx, hash.SHA256)
	if err != nil || sha256Hex == "" {
		fs.Debugf(src, "wopan: rapid upload not attempted: no SHA-256 available")
		return api.UploadData{}, false, nil
	}

	now := time.Now()
	// The service ignores this field: measured twice on 2026-10-20, an entry
	// created from a source dated 2019/2020 came back with ModTime equal to the
	// registration instant, whatever was sent here. It stays set to the source's
	// time because that is the field's meaning and the captured request fills it
	// in; wopan cannot correct the timestamp afterwards either (Object.SetModTime
	// is fs.ErrorCantSetModTime), which is why a hit has to say what it costs.
	modTime := src.ModTime(ctx)
	if modTime.IsZero() {
		// The ordinary upload path leaves the timestamp unset for such a source
		// (formatShootingTime returns ""), and no measured value stands in for
		// "unknown" here, so do not guess - just upload normally.
		fs.Debugf(src, "wopan: rapid upload not attempted: source has no modification time")
		return api.UploadData{}, false, nil
	}
	body, err := json.Marshal(map[string]any{
		"sha256":               sha256Hex,
		"fileSize":             size,
		"fileName":             leaf,
		"fileType":             "5",
		"batchNo":              now.In(api.Beijing).Format(api.TimeFormat),
		"spaceType":            f.spaceType,
		"directoryId":          dirID,
		"fileModificationTime": modTime.UnixMilli(),
	})
	if err != nil {
		return api.UploadData{}, false, nil
	}
	data, hit, inconclusive, err := f.quickTransferOnce(ctx, body, sha256Hex, leaf, size, src)
	if err != nil {
		return api.UploadData{}, false, err
	}
	if hit {
		return data, true, nil
	}
	if inconclusive {
		// The first answer could not be read, which is also the one shape in which
		// the service may have registered the content already. Re-ask once: three
		// registrations of the same (directoryId, fileName, sha256) were measured on
		// 2026-10-20 to leave exactly one entry, no renamed copy and no second one,
		// so asking again costs a request and cannot cost quota. Uploading without
		// asking would risk two copies of the content instead.
		fs.Debugf(src, "wopan: rapid upload answer was unreadable, asking once more")
		data, hit, _, err := f.quickTransferOnce(ctx, body, sha256Hex, leaf, size, src)
		if err != nil {
			return api.UploadData{}, false, err
		}
		if hit {
			return data, true, nil
		}
	}
	return api.UploadData{}, false, nil
}

// quickTransferOnce sends one instant-upload offer and classifies the answer.
//
// inconclusive means "the service may or may not have registered the content" -
// the request left and the answer never arrived, in any of its shapes. Every
// other outcome is definitive: a clean miss, a refusal with a business code, or a
// rejected request that never reached the business layer (4xx).
func (f *Fs) quickTransferOnce(ctx context.Context, body []byte, sha256Hex, leaf string, size int64, src fs.ObjectInfo) (data api.UploadData, hit, inconclusive bool, err error) {
	reqCtx, cancel := context.WithTimeout(ctx, quickTransferTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, quickTransferURL, bytes.NewReader(body))
	if err != nil {
		return api.UploadData{}, false, false, nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("access-token", f.tok.accessTokenNow())

	res, err := f.httpClient.Do(req)
	if err != nil {
		fs.Debugf(src, "wopan: rapid upload probe failed: %v", err)
		return api.UploadData{}, false, true, nil
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		fs.Debugf(src, "wopan: rapid upload probe unreadable: %v", err)
		return api.UploadData{}, false, true, nil
	}
	if res.StatusCode == http.StatusRequestTimeout || res.StatusCode >= http.StatusInternalServerError {
		fs.Debugf(src, "wopan: rapid upload probe http %s: %s", res.Status, truncate(string(raw), 200))
		return api.UploadData{}, false, true, nil
	}
	if res.StatusCode != http.StatusOK {
		fs.Debugf(src, "wopan: rapid upload probe http %s, uploading normally: %s", res.Status, truncate(string(raw), 200))
		return api.UploadData{}, false, false, nil
	}
	var env api.QuickTransferResponse
	if err := json.Unmarshal(raw, &env); err != nil {
		fs.Debugf(src, "wopan: rapid upload probe undecodable, uploading normally: %s", truncate(string(raw), 200))
		return api.UploadData{}, false, true, nil
	}
	// One answer line per probe on the Fs, the way yun139 reports its rapid
	// answer, so both backends can be grepped for "rapid" to see what each file did.
	fs.Debugf(f, "wopan: rapid upload answered code=%q hasFile=%q fid=%q", env.Meta.Code, string(env.Result.HasFile), string(env.Result.Fid))
	if env.Meta.Code != quickTransferSuccessCode {
		fs.Debugf(src, "wopan: rapid upload probe meta.code=%s message=%s, uploading normally", env.Meta.Code, env.Meta.Message)
		return api.UploadData{}, false, false, nil
	}
	if string(env.Result.HasFile) != "1" && string(env.Result.HasFile) != "true" {
		fs.Debugf(src, "wopan: rapid upload miss (sha256 %s not stored), uploading normally", sha256Hex)
		return api.UploadData{}, false, false, nil
	}
	data = api.UploadData{
		Fid:         string(env.Result.Fid),
		FileVersion: env.Result.FileVersion,
		WcFileID:    string(env.Result.WcFileID),
	}
	if data.Fid == "" {
		data.Fid = string(env.Result.FileID)
	}
	if data.Fid == "" || data.WcFileID == "" {
		// The service registered the object but we cannot name it. Uploading now
		// would duplicate it, so stop with what the response actually said.
		return api.UploadData{}, false, false, fmt.Errorf("wopan: rapid upload reported a hit for %q without a usable id (response %s)", leaf, truncate(string(raw), 200))
	}
	fs.Logf(src, "wopan: rapid upload hit - %v already stored under this SHA-256, no bytes uploaded; the service stamps the entry with the current time and wopan cannot set a modification time afterwards, so a later copy or sync re-registers it unless --size-only is used", fs.SizeSuffix(size))
	return data, true, false, nil
}

// uploadOrQuickTransfer places a whole file in dirID under leaf, asking the
// instant-upload service first and otherwise doing the normal upload. Put and
// Object.Update both go through here so the probe has one entry point.
func (f *Fs) uploadOrQuickTransfer(ctx context.Context, in io.Reader, dirID, leaf string, size int64, src fs.ObjectInfo) (api.UploadData, error) {
	data, hit, err := f.quickTransfer(ctx, dirID, leaf, size, src)
	if err != nil {
		return api.UploadData{}, err
	}
	if hit {
		return data, nil
	}
	return f.uploadSingle(ctx, in, dirID, leaf, size, src)
}

// transferredChunkWriter stands in for a chunked transfer the instant-upload
// probe has already completed: the chunks are acknowledged without being read and
// Close writes nothing. The multi-thread engine re-reads the destination with
// NewObject after Close(), which is exactly what a hit has just produced.
type transferredChunkWriter struct{}

// WriteChunk acknowledges a chunk without consuming it - the content is already
// stored. It reports 0 bytes because nothing was transferred by this call.
func (transferredChunkWriter) WriteChunk(_ context.Context, _ int, _ io.ReadSeeker) (int64, error) {
	return 0, nil
}

// Abort and Close have nothing to undo or finalise.
func (transferredChunkWriter) Abort(_ context.Context) error { return nil }

// Close completes the transfer.
func (transferredChunkWriter) Close(_ context.Context) error { return nil }

var _ fs.ChunkWriter = transferredChunkWriter{}

// ------------------------------------------------------------ Fs -----------

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	if f.spaceType == spaceFamily {
		return fmt.Sprintf("wopan root '%s' (family)", f.root)
	}
	return fmt.Sprintf("wopan root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.MD5)
}

// DirCacheFlush resets the directory cache - used in testing as an
// optional interface, and by rclone dedupe in a live run.
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
	// The refusal cache is the same class of knowledge: "what I think I know
	// about directory ids". Left behind, a reset would send every path back
	// through FindLeaf, where a remembered refusal - not a listing - decides the
	// answer, so a directory that really exists would be reported absent without
	// any request being made to discover otherwise.
	f.dirRejects.clear()
}

// parsePath parses a remote path
func parsePath(p string) (root string) {
	return strings.Trim(p, "/")
}

// spaceParams builds the common wohome parameters for this remote.
//
// The family space must send familyId; the personal space must omit the key
// entirely because an empty string makes the server return RSP_CODE 9999.
func (f *Fs) spaceParams() map[string]any {
	return spaceParams(f.spaceType, f.opt.FamilyID)
}

// validateName checks the leaf name of remote against wopan's storage rules.
func (f *Fs) validateName(remote string) error {
	return validateName(path.Base(remote))
}

// shouldRetryCall reports whether a failed dispatcher call may be retried.
//
// A nil error means success and must never be retried: the pacer wraps a
// retryable nil in fserrors.RetryError(nil), whose message is "needs retry",
// and the caller would then report failure for an operation that actually
// took effect (a deleted directory, say) - possibly many times over.
//
// A dead ctx also means no retry: the caller gave up (a concurrent hash-check
// abort cancels the probe mid-flight, --max-duration expired, Ctrl+C), and
// every further attempt would fail instantly, burning the whole
// low-level-retries budget on log spam without a chance of success.
//
// Business errors arrive as HTTP 200 with a non-zero RSP_CODE, meaning the
// request was understood and either took effect or never will, so retrying
// them would only rotate tokens and duplicate side effects. Everything else is
// treated as a transport level failure and retried.
func shouldRetryCall(ctx context.Context, err error) (bool, error) {
	if err == nil {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, err
	}
	// asAPIError 只走单路 Unwrap() error，穿不进带两个 %w 的错误，所以终态判定要在
	// 这里显式认 NoRetryError，否则 pacer 会把它当成可重试一直打到底。
	if fserrors.IsNoRetryError(err) {
		return false, err
	}
	var ae *apiError
	if asAPIError(err, &ae) {
		return false, err
	}
	return true, err
}

// call issues one dispatcher request through doCall and applies the
// fatal_if_no_space decision to whatever came back.
//
// This is the single choke point for the control plane: every wohome/wocloud
// and api-user request goes through here, so the option does not have to be
// honoured in the individual callers.
func (f *Fs) call(ctx context.Context, channel, method string, param any, extra map[string]any) (json.RawMessage, error) {
	data, err := f.doCall(ctx, channel, method, param, extra)
	if err != nil {
		err = f.wrapNoSpace(err)
	}
	return data, err
}

// wrapNoSpace promotes the exhausted-quota answer to a fatal error when
// fatal_if_no_space is set, and leaves every other error alone.
//
// fserrors.FatalError is what cancels the sync at the first one (sync.go
// checks IsFatalError and calls s.cancel()), which also stops --retries from
// re-running the round. The NoRetry marking stays underneath, so a caller that
// only recognises NoRetry sees the same decision it saw before, and asAPIError
// can still unwrap the *apiError from either shape.
func (f *Fs) wrapNoSpace(err error) error {
	if !f.opt.FatalIfNoSpace {
		return err
	}
	var ae *apiError
	if asAPIError(err, &ae) && ae.Code == codeSpaceFull {
		return fserrors.FatalError(err)
	}
	return err
}

// doCall issues one dispatcher request using the shared access token.
//
// param is encrypted into body.param; extra is merged into body as plaintext
// fields. When the server rejects the token with the auth-invalid code the
// token is refreshed once and the whole request is rebuilt - the old ciphertext
// was encrypted with the previous token's key and is useless otherwise.
func (f *Fs) doCall(ctx context.Context, channel, method string, param any, extra map[string]any) (json.RawMessage, error) {
	ts := f.tok
	token := ts.accessTokenNow()
	data, err := call(ctx, f.httpClient, "", token, channel, method, param, extra)
	if err == nil || !isAuthInvalid(err) {
		return data, err
	}
	if f.opt.NoRefresh {
		return nil, fmt.Errorf("%w - the token was rejected and no_refresh is set, so update the refresh_token or access_token by hand", err)
	}
	var ae *apiError
	code := "?"
	if asAPIError(err, &ae) {
		code = ae.Code
	}
	fs.Logf(f, "wopan: token rejected with RSP_CODE=%s, refreshing the access token", code)
	if rerr := f.refreshToken(ctx, ts, token); rerr != nil {
		// 换票失败是终态：被拒的 refresh_token 不会因为重试变成有效，而每次重试都是一次
		// 失败登录，都打在这个共用账号上。包成 NoRetryError 让 pacer 立刻收手。
		return nil, fserrors.NoRetryError(fmt.Errorf("wopan: token invalid and refresh failed: %w (original error: %w)", rerr, err))
	}
	return call(ctx, f.httpClient, "", ts.accessTokenNow(), channel, method, param, extra)
}

// queryUserID validates the access token and returns the account's userId,
// which is the invariant used to key the shared token registry.
//
// Wrapped in f.pacer.Call so that transient control-plane failures (504 Gateway
// Timeout, 502, 503, 429) are retried with the standard low-level retry budget
// instead of killing the entire Preflight / rclone lsd wopan: on the first
// connect attempt. was directly calling f.call before (TODO at line 888-894).
func (f *Fs) queryUserID(ctx context.Context) (string, error) {
	var u api.QueryUserResponse
	err := f.pacer.Call(func() (bool, error) {
		// access_token 走的是 param，所以递给 call 的是工厂：换票之后的重试必须带新令牌
		param := paramFunc(func() any {
			return api.QueryUserRequest{AccessToken: f.tok.accessTokenNow()}
		})
		data, err := f.call(ctx, chanAPIUser, "AppQueryUser", param,
			map[string]any{"clientId": clientID, "secret": true})
		if err != nil {
			return shouldRetryCall(ctx, err)
		}
		if err := json.Unmarshal(data, &u); err != nil {
			return false, fmt.Errorf("wopan: decode user info: %w", err)
		}
		if u.UserID == "" {
			return false, errors.New("wopan: AppQueryUser returned an empty userId")
		}
		return false, nil
	})
	if err != nil {
		return "", fmt.Errorf("wopan: couldn't validate the token: %w", err)
	}
	return u.UserID, nil
}

// newHTTPClient builds the http client for the wopan backend, optionally
// disabling HTTP/2 so concurrent part uploads spread across parallel TCP
// connections instead of multiplexing onto a single one.
func newHTTPClient(ctx context.Context, opt *Options) *http.Client {
	return fshttp.NewClientCustom(ctx, func(t *http.Transport) {
		if opt.DisableHTTP2 {
			t.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
		}
	})
}

// wopanFeatures is the feature set every wopan remote runs with.
//
// It is a function rather than a literal inside newFs so the unit tests build the
// same Fs the transfer engine sees, and so that the absence of one particular flag
// stays written down. ChunkWriterDoesntSeek would let multi-thread copies stream
// each part instead of buffering it - which is what the instant-upload hit wants,
// since its writer reads nothing - but fstests failed 19 times with "object not
// found" with it on (2026-10-20, runs 3 and 4, run alone on the account, green
// without it): buffering is also what protects a source whose Open does not honour
// the byte range. The hit path is made cheap without the flag, by never enlarging
// the chunk beyond the configured chunk size.
func wopanFeatures(ctx context.Context, f *Fs) *fs.Features {
	return (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: true,
		SlowHash:                true,
	}).Fill(ctx, f)
}

// newFs partially constructs Fs from the path
//
// It constructs a valid Fs but doesn't attempt to figure out whether
// it is a file or a directory.
func newFs(ctx context.Context, name, root string, m configmap.Mapper) (*Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	if err := configstruct.Set(m, opt); err != nil {
		return nil, err
	}
	if opt.ChunkSize < 5*1024*1024 {
		return nil, fmt.Errorf("wopan: chunk_size must be at least 5Mi, got %s", opt.ChunkSize)
	}
	if opt.UploadConcurrency < 1 {
		return nil, fmt.Errorf("wopan: upload_concurrency must be at least 1, got %d", opt.UploadConcurrency)
	}

	f := &Fs{
		name:       name,
		root:       parsePath(root),
		opt:        *opt,
		m:          m,
		httpClient: newHTTPClient(ctx, opt),
		spaceType:  spacePersonal,
		zoneMu:     new(sync.Mutex),
		dirRejects: newRejectCache(),
	}
	if opt.FamilyID != "" {
		f.spaceType = spaceFamily
	}
	f.pacer = fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant)))
	f.features = wopanFeatures(ctx, f)

	// The wohome AES key is accessToken[:aesKeyLen], so a shorter token would
	// panic on every encrypted request.
	if opt.AccessToken != "" && len(opt.AccessToken) < aesKeyLen {
		return nil, fmt.Errorf("wopan: access_token must be at least %d characters to derive the AES key, got %d", aesKeyLen, len(opt.AccessToken))
	}
	if opt.AccessToken == "" && opt.RefreshToken == "" {
		return nil, errors.New("wopan: need either a refresh_token or an access_token")
	}
	// Uploads put the access token in an unencrypted form field, so an
	// http:// override would silently leak it; reject non-https early.
	if opt.UploadZone != "" {
		u, err := url.Parse(opt.UploadZone)
		if err != nil || u.Scheme != "https" || u.Host == "" {
			return nil, fmt.Errorf("wopan: upload_zone must be a valid https:// URL, got %q", opt.UploadZone)
		}
		opt.UploadZone = strings.TrimSuffix(opt.UploadZone, "/")
	}

	// Bootstrap the shared state before anything needs a token. It is keyed by
	// the configured credentials until the userId is known.
	f.tok = f.tokenState()
	if opt.AccessToken == "" {
		if opt.NoRefresh {
			return nil, errors.New("wopan: no access_token and no_refresh is set")
		}
		if err := f.refreshToken(ctx, f.tok, ""); err != nil {
			return nil, err
		}
		f.opt.AccessToken = f.tok.accessTokenNow()
		if len(f.opt.AccessToken) < aesKeyLen {
			return nil, fmt.Errorf("wopan: refreshed access_token is shorter than %d characters", aesKeyLen)
		}
	}

	// Validate the token and pin the userId, then re-enter the registry under
	// the immutable key so remotes of the same account share one state.
	userID, err := f.queryUserID(ctx)
	if err != nil {
		return nil, err
	}
	f.userID = userID
	f.tok = f.tokenState()

	// Learn the account's reported single-file upload size once per process
	// (fail-open) so it is in the log before any upload starts.
	f.probeAndCacheMembershipBenefit(ctx)

	return f, nil
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f, err := newFs(ctx, name, root, m)
	if err != nil {
		return nil, err
	}

	rootID := f.opt.RootFolderID
	if rootID == "" {
		rootID = defaultRootID
	}
	f.dirCache = dircache.New(f.root, rootID, f)

	// Find the current root
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(f.root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, rootID, &tempF)
		tempF.root = newRoot
		// Make new Fs which is the parent
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
		// XXX: update the old f here instead of returning tempF, since
		// `features` were already filled with functions having *f as a receiver.
		// See https://github.com/rclone/rclone/issues/2182
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// ------------------------------------------------------------ listing ------

// listAllFn is called with each entry found by listAll. It returns true to
// stop the listing early.
type listAllFn func(*api.File) bool

// listAll lists the entries of dirID, calling fn for each one.
//
// Paging stops only once a page comes back empty. Stopping on "fewer than
// pageSize" would truncate the listing whenever the server caps pageSize below
// what we asked for, and a truncated listing makes sync delete the files it
// believes are extra. Ids are de-duplicated for the same reason, and
// maxListPages bounds the loop against a server that keeps repeating a page.
func (f *Fs) listAll(ctx context.Context, dirID string, fn listAllFn) (found bool, err error) {
	seen := map[string]struct{}{}
	terminated := false
	for pageNum := 0; pageNum < maxListPages; pageNum++ {
		p := f.spaceParams()
		p["parentDirectoryId"] = dirID
		p["pageNum"] = pageNum
		p["pageSize"] = listPageSize
		p["sortRule"] = 0

		var page struct {
			Files []*api.File `json:"files"`
		}
		err = f.pacer.Call(func() (bool, error) {
			data, err := f.call(ctx, chanWoHome, "QueryAllFiles", p, map[string]any{"secret": true})
			if err != nil {
				return shouldRetryCall(ctx, err)
			}
			// systemDirs is deliberately left out of this struct: those entries
			// are not part of the user's tree, and returning them would make
			// sync treat them as stray files to delete.
			if err := json.Unmarshal(data, &page); err != nil {
				return false, fmt.Errorf("wopan: decode listing: %w", err)
			}
			return false, nil
		})
		if err != nil {
			return found, err
		}
		if len(page.Files) == 0 {
			terminated = true
			break
		}
		for _, item := range page.Files {
			if item.ID != "" {
				if _, dup := seen[item.ID]; dup {
					continue
				}
				seen[item.ID] = struct{}{}
			}
			item.Name = f.opt.Enc.ToStandardName(item.Name)
			if fn(item) {
				return true, nil
			}
		}
	}
	if !terminated {
		// The loop ran out before the server returned an empty page. Returning
		// a truncated listing silently would make sync delete the entries it
		// believes are extra, so fail loudly instead.
		return false, fmt.Errorf("wopan: listing of directory %s did not terminate after %d pages, the server keeps returning data", dirID, maxListPages)
	}
	return false, nil
}

// listDirEntries returns every entry in dirID, paging through the whole listing.
func (f *Fs) listDirEntries(ctx context.Context, dirID string) ([]*api.File, error) {
	var entries []*api.File
	_, err := f.listAll(ctx, dirID, func(item *api.File) bool {
		entries = append(entries, item)
		return false
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// itemModTime returns the modification time of a listing entry, preferring
// shootingTime (the mtime written at upload) over createTime.
func itemModTime(item *api.File) time.Time {
	if t, err := api.ParseTime(item.ShootingTime); err == nil && !t.IsZero() {
		return t
	}
	if t, err := api.ParseTime(item.CreateTime); err == nil {
		return t
	}
	return time.Time{}
}

// itemToDirEntry converts a listing entry into an fs.DirEntry.
func (f *Fs) itemToDirEntry(ctx context.Context, remote string, item *api.File) (fs.DirEntry, error) {
	if item.Type == fileTypeDir {
		// cache the directory ID for later lookups
		f.dirCache.Put(remote, item.ID)
		return fs.NewDir(remote, itemModTime(item)).SetID(item.ID), nil
	}
	return f.newObjectWithInfo(ctx, remote, item)
}

// List the objects and directories in dir into entries. The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dirID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	var iErr error
	_, err = f.listAll(ctx, dirID, func(item *api.File) bool {
		entry, err := f.itemToDirEntry(ctx, path.Join(dir, item.Name), item)
		if err != nil {
			iErr = err
			return true
		}
		if entry != nil {
			entries = append(entries, entry)
		}
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

// ------------------------------------------------------------ dir cache ----

const (
	// dirRejectTTL bounds how long a replayed directory-creation failure is
	// trusted before the directory is offered to the server again.
	dirRejectTTL = 60 * time.Second
	// dirRejectProbes caps how many consecutive refusals one directory is
	// allowed to pay for. Without it a 60 second window turns a half hour round
	// into 30 probes per bad directory.
	dirRejectProbes = 3
	// dirRejectReopen is how long an exhausted probe budget holds before the
	// directory is offered again.
	//
	// Nothing is ever remembered for the life of the process, not even the
	// explicit name rejections (1009, 4444). The refusal cache is consulted on
	// the read side too - List, Rmdir, Purge and dircache's "destination does
	// not exist" check in DirMove all reach FindLeaf - so a wrong verdict would
	// hide a directory that really exists and, because a held key sends no
	// request, would never learn otherwise. rcd and mount live for days, so
	// "never" is unbounded there. Measurement says permanence buys nothing: the
	// savings come from the cap, and one reopen costs 2 requests per bad
	// directory per half hour.
	dirRejectReopen = 30 * time.Minute
	// dirRejectMaxEntries bounds the table. Everything else here is a per-run
	// quantity, but --backup-dir style use adds a fresh timestamped directory
	// every round, so the map itself needs a ceiling. At the ceiling new keys
	// stop being remembered and the table is never wiped: wiping would hand every
	// known-bad directory a fresh probe budget, and right at the ceiling a new key
	// on every call would clear it again and again. A directory that never got a
	// slot is still answered honestly - one listing plus one CreateDirectory per
	// file, which is the pre-cache cost - so the ceiling trades the saving away,
	// it does not trade correctness.
	dirRejectMaxEntries = 4096
)

// dirKey identifies one directory creation attempt: the leaf name inside a
// specific parent id. The parent id, not the parent path, is what the server
// judges, so a recreated parent gets its own entry.
type dirKey struct {
	pathID string
	leaf   string
}

// rejectedDir is one remembered CreateDirectory refusal.
type rejectedDir struct {
	err    error     // replayed unchanged, so the caller sees the same error it saw first
	probes int       // refusals recorded for this key so far
	expiry time.Time // when the last answer stops being trusted
}

// rejectCache remembers directory creations the server refused.
//
// dircache only caches successes (lib/dircache/dircache.go _findDir Puts the id
// after CreateDir returns), so without this a rejected subtree costs one
// listing plus one CreateDirectory per file per round. Run 801 measured 733 of
// those lines for 3 directories.
//
// It hangs off the Fs as a pointer because NewFs copies the struct (tempF := *f)
// while walking a root that turns out to be a file; a value-typed map would give
// the copy its own cache and the mutex would be copied while held.
//
// A nil *rejectCache means "no caching". NewFs always installs one; the unit
// tests that build an Fs literal without it get the uncached behaviour.
//
// Two goroutines can never race into one probe for the same key only because
// every FindLeaf/CreateDir pair runs under dircache's single cache-wide lock
// (lib/dircache/dircache.go FindDir). held() and record() are not atomic
// together; anything that calls them off that lock would start double-sending
// refused directories.
type rejectCache struct {
	mu      sync.Mutex
	entries map[dirKey]rejectedDir
	now     func() time.Time // tests substitute a clock
}

func newRejectCache() *rejectCache {
	return &rejectCache{
		entries: make(map[dirKey]rejectedDir),
		now:     time.Now,
	}
}

// held reports whether key must not be sent to the server, and the error to
// replay when it must not.
func (rc *rejectCache) held(key dirKey) (bool, error) {
	if rc == nil {
		return false, nil
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	e, ok := rc.entries[key]
	if !ok {
		return false, nil
	}
	switch {
	case rc.now().Before(e.expiry):
	case e.probes >= dirRejectProbes && rc.now().Before(e.expiry.Add(dirRejectReopen)):
	default:
		return false, nil
	}
	return true, e.err
}

// record remembers the failure. Only server business errors belong here: a
// transport error says nothing about the name, and caching one would turn a
// network blip into a refusal that outlives it.
func (rc *rejectCache) record(key dirKey, err error) {
	if rc == nil {
		return
	}
	var ae *apiError
	if !asAPIError(err, &ae) {
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if _, known := rc.entries[key]; !known && len(rc.entries) >= dirRejectMaxEntries {
		// Full. Stop recording rather than dropping the table: wiping it would
		// hand every known-bad directory a fresh probe budget, and at exactly the
		// ceiling a new key on every call would clear it again and again, which
		// is the pre-cache cost profile wearing a cache's clothes.
		return
	}
	e := rejectedDir{
		err:    err,
		probes: rc.entries[key].probes + 1,
		expiry: rc.now().Add(dirRejectTTL),
	}
	rc.entries[key] = e
}

// clear drops every remembered refusal. A directory move changes the full path
// of everything beneath it, and a purge can free a name, so neither answer is
// safe to keep. Both are rare and the cache refills itself.
func (rc *rejectCache) clear() {
	if rc == nil {
		return
	}
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.entries = make(map[dirKey]rejectedDir)
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	// A directory the server refused to create is not in the parent listing, so
	// page through the parent only to conclude that. Report "not found" without
	// the query; dircache then calls CreateDir, which replays the refusal.
	if held, _ := f.dirRejects.held(dirKey{pathID, leaf}); held {
		return "", false, nil
	}
	// Names are matched case-insensitively because the server is.
	found, err = f.listAll(ctx, pathID, func(item *api.File) bool {
		if item.Type == fileTypeDir && strings.EqualFold(item.Name, leaf) {
			pathIDOut = item.ID
			return true
		}
		return false
	})
	return pathIDOut, found, err
}

// retryFindDir runs lookup with a bounded retry across the listing
// visibility window: a directory that was just created (or just renamed
// into place) can be missing from its parent listing for seconds before
// the server makes it visible, and a bare lookup would fail the caller
// with ErrorDirNotFound. Only ErrorDirNotFound retries; every other error
// surfaces immediately.
func retryFindDir(ctx context.Context, attempts int, delay time.Duration, lookup func() (string, error)) (string, error) {
	for attempt := 1; ; attempt++ {
		id, err := lookup()
		if err != fs.ErrorDirNotFound || attempt >= attempts {
			return id, err
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(delay):
		}
	}
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

// dirPresentUngated reports whether a directory called leaf sits directly
// inside parentID, by paging the parent itself instead of consulting the
// refusal cache.
//
// dircache.DirMove asks "does the destination exist" through FindDir, which
// reaches FindLeaf - and a held refusal answers "absent" without sending
// anything. That would cost rclone its fs.ErrorDirExists answer for a move onto
// a directory that really is there, along with the copy+purge fallback that
// depends on it. This is the one question that needs the truth rather than the
// cheap answer, so it is asked on every move: two requests per directory move,
// never per file. Gating it on "is the refusal still held" would make one call
// ask a clock twice with real requests in between, and the second answer is the
// one that would decide whether to look.
func (f *Fs) dirPresentUngated(ctx context.Context, parentID, leaf string) (bool, error) {
	return f.listAll(ctx, parentID, func(item *api.File) bool {
		return item.Type == fileTypeDir && strings.EqualFold(item.Name, leaf)
	})
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	// Long names are silently truncated by the server and non-BMP characters
	// are rejected outright, so refuse them before they can cause damage.
	if err := f.validateName(leaf); err != nil {
		return "", err
	}
	key := dirKey{pathID, leaf}
	if held, cached := f.dirRejects.held(key); held {
		return "", cached
	}
	p := f.spaceParams()
	p["parentDirectoryId"] = pathID
	p["directoryName"] = f.opt.Enc.FromStandardName(leaf)
	// 重要1 (fifth round): the server truncates the ENCODED name, and the
	// encoder can expand characters, so re-check the rune count on what is
	// actually sent.
	if err := validateName(p["directoryName"].(string)); err != nil {
		return "", err
	}

	var resp api.CreateDirectoryResponse
	// recoveryFailed marks a failure of the lookup that follows a "name taken"
	// answer. That error came from QueryAllFiles, so it says nothing about
	// whether the name is acceptable and must not be remembered as a refusal.
	var recoveryFailed bool
	err = f.pacer.Call(func() (bool, error) {
		data, err := f.call(ctx, chanWoHome, "CreateDirectory", p, map[string]any{"secret": true})
		if err != nil {
			var ae *apiError
			if asAPIError(err, &ae) && ae.Code == codeDirExists {
				// The directory already exists - Mkdir must not report an error.
				// The lookup that follows is deliberately left alone by the
				// refusal cache: this branch was only reached from a call that
				// held() allowed, and the stored probe count rises only on
				// failures, so the entry cannot have turned back into a hold
				// during it. Forgetting it here would have no observable effect
				// either - the recovery failure below is what has to be kept out
				// of the budget, not re-labelled.
				//
				// A just-created directory can be missing from its parent
				// listing for a few seconds, so look it up with a bounded
				// retry across the visibility window.
				id, ferr := retryFindDir(ctx, dirFindAttempts, dirFindDelay, func() (string, error) {
					return f.findDirID(ctx, pathID, leaf)
				})
				if ferr != nil {
					recoveryFailed = true
					return false, ferr
				}
				resp.ID = id
				return false, nil
			}
			return shouldRetryCall(ctx, err)
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			return false, fmt.Errorf("wopan: decode create directory: %w", err)
		}
		return false, nil
	})
	// A remembered refusal is never dropped on the way out, here or in the
	// name-taken branch above: every call that got this far was one held() said
	// "probe" for, and the stored probe count rises only on failures, so an entry
	// cannot have turned back into a hold during it. Mutation testing confirmed
	// the forget() that used to sit in both places had no observable effect.
	if err != nil {
		if !recoveryFailed {
			f.dirRejects.record(key, err)
		}
		return "", err
	}
	// 重要2 (fifth round): a 0000 response with empty DATA yields an empty id.
	// Returning ("", nil) would poison the dircache - every later operation on
	// this directory would target the root. Fail loudly instead.
	if resp.ID == "" {
		return "", errors.New("wopan: CreateDirectory returned an empty id")
	}
	return resp.ID, nil
}

// Mkdir creates the container if it doesn't exist
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	_, err := f.dirCache.FindDir(ctx, dir, true)
	return err
}

// ------------------------------------------------------------ objects ------

// findEntry looks up leaf in dirID, matching case-insensitively.
//
// A directory match yields fs.ErrorIsDir and a miss yields
// fs.ErrorObjectNotFound.
func (f *Fs) findEntry(ctx context.Context, dirID, leaf string) (*api.File, error) {
	var (
		result    *api.File
		resultErr = fs.ErrorObjectNotFound
	)
	_, err := f.listAll(ctx, dirID, func(item *api.File) bool {
		if !strings.EqualFold(item.Name, leaf) {
			return false
		}
		if item.Type == fileTypeDir {
			result = nil
			resultErr = fs.ErrorIsDir
			return true
		}
		result = item
		resultErr = nil
		return true
	})
	if err != nil {
		return nil, err
	}
	if resultErr != nil {
		return nil, resultErr
	}
	return result, nil
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.File) (fs.Object, error) {
	o := &Object{
		fs:     f,
		remote: remote,
	}
	if info != nil {
		o.setMetaData(info)
		return o, nil
	}
	if err := o.readMetaData(ctx); err != nil {
		return nil, err
	}
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return f.newObjectWithInfo(ctx, remote, nil)
}

// readMetaData reads the metadata of the object from the parent directory.
func (o *Object) readMetaData(ctx context.Context) error {
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			// The parent doesn't exist, so neither does the object.
			return fs.ErrorObjectNotFound
		}
		return err
	}
	entry, err := o.fs.findEntry(ctx, dirID, leaf)
	if err != nil {
		return err
	}
	o.setMetaData(entry)
	return nil
}

// setMetaData copies a listing entry into the object.
//
// Timestamp parsing errors are ignored: a malformed value leaves the field
// zero and ModTime falls back to the other timestamp.
func (o *Object) setMetaData(item *api.File) {
	o.id = item.ID
	o.fid = item.Fid
	o.size = item.Size
	// Chunked uploads never carry a content MD5 in the ETag (same rule as
	// refreshFromUpload), so Hash() skips the ETag probe for them.
	o.multipart = item.Size > int64(o.fs.opt.UploadCutoff)
	o.thumbURL = item.ThumbURL
	o.shootingTime, _ = api.ParseTime(item.ShootingTime)
	o.createTime, _ = api.ParseTime(item.CreateTime)
}

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// String returns a description of the Object.
//
// fstests asserts that a nil object stringifies to "<nil>", so the receiver
// must be checked.
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Size returns the size of the object
func (o *Object) Size() int64 {
	return o.size
}

// Storable says whether this object can be stored
func (o *Object) Storable() bool {
	return true
}

// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time {
	if !o.shootingTime.IsZero() {
		return o.shootingTime
	}
	return o.createTime
}

// SetModTime sets the modification time of the object
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	// The server only accepts an mtime at upload time and has no API to change
	// it afterwards.
	return fs.ErrorCantSetModTime
}

// hashGrace returns how long after this object's upload Hash() should
// swallow transient errors. Small files settle within seconds, but files
// of 8 MiB or more are stored in server-side shards whose ETag takes
// minutes to settle to the content MD5 (manual test B3).
func (o *Object) hashGrace() time.Duration {
	if o.size >= partSize {
		return largeHashGracePeriod
	}
	return hashGracePeriod
}

// Hash returns the selected checksum of the file.
//
// The MD5 is the ETag of the download URL. A multipart ETag (with a "-N"
// suffix) is not a content hash, so it is reported as no-hash via an empty
// string rather than an error, which would make verify treat the file as
// corrupted. A transient error right after upload is also swallowed for the
// same reason. Objects uploaded in several parts skip the probe entirely,
// since their ETag never carries a content MD5.
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	// Multi-part uploads never carry a content MD5 in the ETag (confirmed for
	// rclone-initiated sessions; external clients behave the same), so the
	// probe is skipped and callers fall back to size-only comparisons.
	if o.multipart {
		return "", nil
	}
	url, err := o.downloadURL(ctx)
	if err != nil {
		if time.Since(o.uploadedAt) < o.hashGrace() {
			// GetDownloadUrlV2 itself can fail transiently within the
			// settling window (rate limit, pacer budget exhausted); fall
			// back to no-hash instead of making verify delete the freshly
			// uploaded file.
			return "", nil
		}
		return "", err
	}
	etag, err := o.fs.headETag(ctx, url)
	if errors.Is(err, errStaleLink) {
		// The link aged out between being fetched and being used. One refetch,
		// the same bounded self-heal Open does; a still-broken answer after
		// that falls through to the error handling below.
		o.invalidateURL()
		if url, err = o.downloadURL(ctx); err == nil {
			etag, err = o.fs.headETag(ctx, url)
		}
	}
	if err != nil {
		if time.Since(o.uploadedAt) < o.hashGrace() {
			// The download link may not be settled yet; fall back to no-hash
			// instead of making verify delete the freshly uploaded file.
			return "", nil
		}
		return "", err
	}
	return api.ContentETag(strings.Trim(etag, `"`)), nil
}

// Open opens the file for read. Call Close() on the returned io.ReadCloser.
//
// A cached link that the download CDN rejects as no longer usable - 403
// (revoked/unauthorised) or the non-standard 519 (aged out) - is refetched once
// before giving up.
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
	if staleLinkStatus(res.StatusCode) {
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
		return nil, fmt.Errorf("wopan: download: %s", res.Status)
	}
	return res.Body, nil
}

// Update in to the object with the modTime given of the given size.
//
// Direct overwrite cannot change the mtime, so the new content is uploaded
// under a temporary name, the old object is parked under a backup name, and the
// temp is renamed into place; the backup is dropped once the new content owns
// the name. The old object is never deleted before its replacement is complete,
// so a failed or interrupted update leaves the file readable. The receiver is
// then refreshed in place so that verify (which reuses it) compares against the
// new object.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	size := src.Size()
	if size < 0 {
		return errors.New("wopan: can't upload files of unknown size")
	}
	if size == 0 {
		return fs.ErrorCantUploadEmptyFiles
	}
	o.fs.warnOversize(o.remote, size)
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}
	// The install rename (step 3) puts the new object under this exact name. A
	// name the server would silently truncate (>100 runes, e.g. created from
	// the app) must fail before anything is uploaded or renamed - otherwise the
	// update cannot ever complete under the name the user asked for.
	if err := validateName(leaf); err != nil {
		return err
	}
	leaf = o.fs.opt.Enc.FromStandardName(leaf)
	// 重要1 (fifth round): the encoder can expand characters, so re-check the
	// rune count on the encoded name that is actually sent.
	if err := validateName(leaf); err != nil {
		return err
	}

	// Lossless Update, in four steps:
	//
	//	1. upload the new content under a temporary name
	//	2. park the old object under a backup name
	//	3. rename the temporary object onto the real name
	//	4. drop the backup
	//
	// The old content is untouched until the new content is completely on the
	// server, so a failed or interrupted upload leaves the file readable the
	// whole time, and the window in which the real name is absent spans two
	// renames rather than the whole upload.
	tempLeaf := tempName(o.fs.opt.Enc, leaf)

	// Step 1. A failure here is reported as-is and may be retried whole: the
	// old object is untouched. A multipart failure leaves only orphaned parts,
	// which are not visible as a file; a single-part upload whose response was
	// lost can leave a temp file behind, which costs quota and is removable
	// with --include "*rclone-tmp-*".
	tmpData, err := o.fs.uploadOrQuickTransfer(ctx, in, dirID, tempLeaf, size, src)
	if err != nil {
		return err
	}

	// From here on the temp object exists, so a whole-file retry could never
	// repair the state it left: it would upload yet another temp every round.
	// Every error below is marked NoLowLevelRetry to suppress that retry, and
	// renameWithBackoff strips the pacer's RetryError marker at the source so
	// the marking actually holds for transport-level failures too. Nothing is
	// deleted on a path whose outcome is unknown: once a rename result is
	// unknown, the id it was issued for IS the file's id.
	oldID := o.id
	backupLeaf := backupName(o.fs.opt.Enc, leaf)

	// Step 2: park the old object. Only its name moves - the id and the bytes
	// are unchanged - so this is reversible until step 3 succeeds.
	if err := o.fs.renameWithBackoff(ctx, oldID, backupLeaf); err != nil {
		// Nothing is deleted here: the park's outcome is uncertain (the request
		// may have landed), so the old content is either still under the real
		// name or under backupLeaf - both recoverable. Deleting the temp would
		// not help either: if the park landed, the temp is the only copy of
		// the new content, and a stray temp only costs quota anyway
		// (removable with --include "*rclone-tmp-*").
		return noLowLevelRetry(fmt.Errorf("wopan: rename old object to the backup name %q: %w", backupLeaf, err))
	}

	// Step 3: move the new content onto the real name.
	if err := o.fs.renameWithBackoff(ctx, tmpData.WcFileID, leaf); err != nil {
		// A failed rename does not mean the move did not happen: if the
		// response was lost, the install may well have landed. The server
		// never overwrites on rename, so a listing settles it - and the two
		// answers need opposite handling.
		//
		//   - the name holds the NEW id: the install landed. The update has
		//     succeeded; report that, and let step 4 drop the backup.
		//   - the name does not hold it: the install did not land. Put the old
		//     object back under its own name.
		//
		// The listing is best-effort: if it fails, the safe reading is "did not
		// land" (the restore is harmless when it did land - it simply fails
		// with the name taken - whereas treating a non-landed install as
		// success would leave the real name empty).
		landed, lerr := o.fs.leafHolds(ctx, dirID, leaf, tmpData.WcFileID)
		if lerr != nil {
			fs.Debugf(o, "wopan: install rename failed (%v) and the confirming listing failed too (%v), assuming it did not land", err, lerr)
		}
		if landed {
			fs.Debugf(o, "wopan: install rename reported %v but %q holds the new object, so the update succeeded", err, leaf)
		} else {
			// Put the old object back under its own name. If even that fails,
			// the old content is still intact under backupLeaf, which the
			// error names so the file can be recovered by hand.
			//
			// The temp object is deliberately NOT deleted here: the listing
			// may have been wrong (or raced a concurrent writer), in which
			// case the temp id holds the only copy of the new content.
			if rerr := o.fs.renameWithBackoff(ctx, oldID, leaf); rerr != nil {
				return noLowLevelRetry(fmt.Errorf(
					"wopan: rename new object to %q: %w (the previous content is still intact under the backup name %q, but renaming it back to %q also failed: %v)",
					leaf, err, backupLeaf, leaf, rerr))
			}
			return noLowLevelRetry(fmt.Errorf("wopan: rename new object to %q: %w", leaf, err))
		}
	}

	// The new content owns the name now, so the update has succeeded. Refresh
	// the receiver before step 4 so that a failure to drop the backup cannot
	// turn a completed update into a reported error.
	o.refreshFromUpload(ctx, src, tmpData, size)

	// Step 4: drop the backup. The file is already correct, so a failure here
	// only leaks quota - never fail the update over it.
	if err := o.fs.deleteFile(ctx, oldID); err != nil {
		fs.Errorf(o, "wopan: update succeeded but removing the backup %q failed, it still consumes quota: %v", backupLeaf, err)
	}
	return nil
}

// Remove deletes the object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteFile(ctx, o.id)
}

// deleteFile deletes a single file id, and with hard_delete also purges the
// recycle-bin entries the delete created.
//
// This is the single collection point for file deletion: Remove and Update's
// step 4 (dropping the parked backup) both go through here, so the
// snapshot-before-delete ordering lives in one place. The snapshot is taken
// BEFORE the delete (red line) - entries already in it belong to the user and
// must never be destroyed.
func (f *Fs) deleteFile(ctx context.Context, id string) error {
	var snap map[string][]string
	if f.opt.HardDelete {
		s, err := f.snapshotRecycle(ctx)
		if err != nil {
			return err
		}
		snap = s
	}
	p := f.spaceParams()
	p["vipLevel"] = "0"
	p["dirList"] = []string{}
	p["fileList"] = []string{id}
	if err := f.pacer.Call(func() (bool, error) {
		_, err := f.call(ctx, chanWoHome, "DeleteFile", p, map[string]any{"secret": true})
		return shouldRetryCall(ctx, err)
	}); err != nil {
		return err
	}
	if f.opt.HardDelete {
		return f.purgeRecycle(ctx, snap, []string{id})
	}
	return nil
}

// Put in to the remote path with the modTime given of the given size
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	size := src.Size()
	if size < 0 {
		return nil, errors.New("wopan: can't upload files of unknown size")
	}
	if size == 0 {
		// Return the bare sentinel: fstests compares it with ==.
		return nil, fs.ErrorCantUploadEmptyFiles
	}
	f.warnOversize(src.Remote(), size)
	if err := f.validateName(src.Remote()); err != nil {
		return nil, err
	}
	leaf, dirID, err := f.dirCache.FindPath(ctx, src.Remote(), true)
	if err != nil {
		return nil, err
	}
	leaf = f.opt.Enc.FromStandardName(leaf)
	// 重要1 (fifth round): the server truncates the ENCODED name, and the
	// encoder can expand characters, so re-check the rune count on what is
	// actually sent.
	if err := validateName(leaf); err != nil {
		return nil, err
	}

	data, err := f.uploadOrQuickTransfer(ctx, in, dirID, leaf, size, src)
	if err != nil {
		return nil, err
	}
	// wcFileId equals the object's listing id, so the result is built from the
	// upload response rather than a re-list (which has a ~15s visibility delay).
	o := &Object{
		fs:     f,
		remote: src.Remote(),
	}
	o.refreshFromUpload(ctx, src, data, size)
	return o, nil
}

// ------------------------------------------------------------ upload --------

// tempName builds Update's temporary upload name, keeping the result within the
// 100-rune name limit.
func tempName(enc encoder.MultiEncoder, leaf string) string {
	return truncateName(enc, leaf, tempSuffix+random.String(8))
}

// backupName builds the name the OLD object is parked under while Update swaps
// in new content. Like tempName it stays within the 100-rune limit.
func backupName(enc encoder.MultiEncoder, leaf string) string {
	return truncateName(enc, leaf, backupSuffix+random.String(8))
}

// truncateName appends suffix to leaf, truncating leaf first so the total stays
// within the 100-rune name limit.
//
// Truncating an ENCODED name is not just a matter of cutting at a rune
// boundary: the encoder represents a reserved character as an escape group
// (QuoteRune plus one rune, or QuoteRune plus two hex digits for an invalid
// byte), so a cut inside a group leaves a name that decodes to something else
// entirely - a stray QuoteRune, or hex digits read back as ordinary characters.
// The encoder is therefore used as its own oracle: a well-formed encoded name
// survives a decode/re-encode round trip unchanged, so trailing runes are
// dropped until that holds. Only a handful of runes are ever affected, because
// malformedness is confined to the tail.
func truncateName(enc encoder.MultiEncoder, leaf, suffix string) string {
	max := 100 - utf8.RuneCountInString(suffix)
	runes := []rune(leaf)
	if len(runes) > max {
		runes = runes[:max]
		for len(runes) > 0 {
			s := string(runes)
			if enc.FromStandardName(enc.ToStandardName(s)) == s {
				break
			}
			runes = runes[:len(runes)-1]
		}
	}
	return string(runes) + suffix
}

// formatShootingTime formats t as the 14-digit UTC+8 string carried in the
// upload fileInfo. A zero time yields an empty string so that the field is
// omitted from the request.
func formatShootingTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.In(api.Beijing).Format(api.TimeFormat)
}

// uploadZone returns the upload endpoint: the manually configured upload_zone
// if set, otherwise the server-assigned zone fetched via GetZoneInfo on first
// use. The endpoint varies by account, so it must not be hardcoded.
func (f *Fs) uploadZone(ctx context.Context) (string, error) {
	if f.opt.UploadZone != "" {
		return f.opt.UploadZone, nil
	}
	f.zoneMu.Lock()
	defer f.zoneMu.Unlock()
	if f.zoneLoaded {
		return f.zoneURL, nil
	}
	data, err := f.call(ctx, chanWoHome, "GetZoneInfo", api.GetZoneInfoRequest{AppID: appID}, map[string]any{"key": true})
	if err != nil {
		return "", fmt.Errorf("wopan: get zone info: %w", err)
	}
	var z api.GetZoneInfoResponse
	if err := json.Unmarshal(data, &z); err != nil {
		return "", fmt.Errorf("wopan: decode zone info: %w", err)
	}
	if z.URL == "" {
		return "", errors.New("wopan: GetZoneInfo returned an empty url")
	}
	f.zoneURL = z.URL
	f.zoneLoaded = true
	return f.zoneURL, nil
}

// uploadSession holds the per-attempt state shared by every part of one
// upload2C session: the zone endpoint, the encrypted fileInfo together with
// the token it was encrypted with, and the uniqueId that binds the parts.
// Every call generates a fresh uniqueId, so a retried upload cannot mix with
// the orphaned parts of the aborted session.
func (f *Fs) newUploadSession(ctx context.Context, dirID, name string, size int64, src fs.ObjectInfo) (zoneURL, fiEnc string, fiJSON []byte, uniqueID, token string, err error) {
	zoneURL, err = f.uploadZone(ctx)
	if err != nil {
		return "", "", nil, "", "", err
	}
	fi := api.UploadFileInfo{
		SpaceType:   f.spaceType,
		DirectoryID: dirID,
		BatchNo:     time.Now().In(api.Beijing).Format(api.TimeFormat),
		FileName:    name,
		FileSize:    size,
		FileType:    "5",
	}
	if f.spaceType == spaceFamily {
		fi.FamilyID = f.opt.FamilyID
	}
	// shootingTime is the only mtime carrier and only takes effect at creation,
	// so a zero source mtime is omitted rather than formatted to a bogus value.
	fi.ShootingTime = formatShootingTime(src.ModTime(ctx))
	fiJSON, err = json.Marshal(fi)
	if err != nil {
		return "", "", nil, "", "", err
	}
	// Read the token once: the fileInfo is encrypted with the key derived from
	// it and the form carries the same value, so a concurrent refresh between
	// the two reads cannot produce a ciphertext the server cannot decrypt.
	token = f.tok.accessTokenNow()
	fiEnc, err = aesEncrypt(fiJSON, aesKeyFor(chanWoHome, token))
	if err != nil {
		return "", "", nil, "", "", err
	}
	// A random uniqueId: the server aggregates a shard session by it, so a
	// collision between concurrent uploads (same millisecond under
	// --transfers > 1) could merge or clobber sessions. A timestamp alone is
	// not enough (fifth round B4).
	uniqueID = random.String(16)
	return zoneURL, fiEnc, fiJSON, uniqueID, token, nil
}

// uploadSingle uploads a file of known size to the upload2C endpoint.
//
// Files up to upload_cutoff are sent as one part, where the server's ETag is
// the content MD5 immediately at upload time. Larger files are read
// sequentially and sent as chunk_size chunks by upload_concurrency workers,
// so the request body stays friendly to reverse proxies with body limits;
// each part is one POST and the last part's response holds the fid.
// uniqueId is generated once per upload attempt because it must stay stable
// across parts, and changed between attempts so a retried upload cannot mix
// with the orphaned parts of the aborted session.
func (f *Fs) uploadSingle(ctx context.Context, in io.Reader, dirID, name string, size int64, src fs.ObjectInfo) (api.UploadData, error) {
	zoneURL, fiEnc, fiJSON, uniqueID, token, err := f.newUploadSession(ctx, dirID, name, size, src)
	if err != nil {
		return api.UploadData{}, err
	}
	plans := []api.PartPlan{{Index: 1, PartSize: size}}
	if size > int64(f.opt.UploadCutoff) {
		plans = api.PlanParts(size, int64(f.opt.ChunkSize))
	}
	total := int64(len(plans))
	fs.Debugf(src, "wopan: upload session %s: %d part(s) of up to %v",
		uniqueID, total, f.opt.ChunkSize)

	st := &partState{}
	// The upload channel never goes through the dispatcher, so an expired
	// token cannot refresh itself mid-upload: on 1001 refresh once, re-encrypt
	// the fileInfo with the new key, and retry a single time (fifth round
	// 重要3). The retry is only possible when the reader can be rewound;
	// otherwise the token is still refreshed so that subsequent files are not
	// hit by the same expiry, and this file's error is reported as-is.
	uploadStart := time.Now()
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			seeker, ok := in.(io.Seeker)
			if !ok {
				// ⚠️ X1 (sixth round): err is nil here (the re-encryption above
				// succeeded), so returning it would report SUCCESS for an upload
				// that never happened - and Update would then delete the old
				// object. Fail loudly instead.
				return api.UploadData{}, errors.New("wopan: cannot re-upload after token refresh: reader is not seekable")
			}
			if _, serr := seeker.Seek(0, io.SeekStart); serr != nil {
				return api.UploadData{}, serr
			}
			zoneURL, fiEnc, fiJSON, uniqueID, token, err = f.newUploadSession(ctx, dirID, name, size, src)
			if err != nil {
				return api.UploadData{}, err
			}
			st = &partState{} // the new session has produced no fid yet
			// The fresh session gets a new uniqueId: without logging it, the
			// retried POSTs cannot be correlated in server-side logs.
			fs.Debugf(src, "wopan: upload session %s: %d part(s) of up to %v",
				uniqueID, total, f.opt.ChunkSize)
		}
		if total == 1 {
			err = f.pacer.CallNoRetry(func() (bool, error) {
				d, err := f.uploadPart(ctx, io.LimitReader(in, size), zoneURL, dirID, name, size, fiEnc, uniqueID, token, plans[0], total)
				if err != nil {
					var ae *apiError
					if asAPIError(err, &ae) {
						// A business rejection never succeeds on retry.
						return false, err
					}
					// A transport failure: signal the pacer to back off, but do not
					// retry here - that is left to the outer copy retry loop.
					return true, err
				}
				st.note(d)
				return false, nil
			})
		} else {
			// The reader is consumed sequentially while the parts are sent
			// concurrently, mirroring the s3 backend's multipart pipeline.
			err = f.uploadPartsConcurrent(ctx, in, zoneURL, dirID, name, size, fiEnc, uniqueID, token, plans, total, st)
		}
		fs.Debugf(f, "wopan: upload attempt #%d took %s (err=%v)", attempt+1, time.Since(uploadStart).Round(time.Millisecond), err)
		if err == nil || !isAuthInvalid(err) || attempt > 0 || f.opt.NoRefresh {
			break
		}
		// This retry re-runs the whole upload (possibly minutes), a
		// user-visible retry/degradation event.
		fs.Logf(f, "wopan: upload token rejected, refreshing the access token and retrying the upload")
		if rerr := f.refreshToken(ctx, f.tok, token); rerr != nil {
			return api.UploadData{}, rerr
		}
		token = f.tok.accessTokenNow()
		fiEnc, err = aesEncrypt(fiJSON, aesKeyFor(chanWoHome, token))
		if err != nil {
			return api.UploadData{}, err
		}
	}
	if err != nil {
		return api.UploadData{}, err
	}
	data := st.snapshot()
	if data.WcFileID == "" || data.Fid == "" {
		// A ghost object: rclone would build an Object that cannot be
		// downloaded or updated. Fail loudly instead.
		return api.UploadData{}, fmt.Errorf("wopan: upload session %s response has an empty fid or wcFileId", uniqueID)
	}
	return data, nil
}

// partJob pairs a part plan with the buffer holding its bytes, read
// sequentially from the source.
type partJob struct {
	plan api.PartPlan
	buf  []byte
}

// uploadPartsConcurrent sends the parts of one file through
// upload_concurrency workers sharing the uniqueId session. The source is read
// sequentially - the server assembles out-of-order parts by part index, so
// only the sending needs concurrency. A part whose transport fails is re-posted
// before anything else is decided; any failure that survives that cancels the
// remaining parts.
func (f *Fs) uploadPartsConcurrent(ctx context.Context, in io.Reader, zoneURL, dirID, name string, size int64, fiEnc, uniqueID, token string, plans []api.PartPlan, total int64, st *partState) error {
	concurrency := f.opt.UploadConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	if int64(concurrency) > total {
		concurrency = int(total)
	}

	g, gctx := errgroup.WithContext(ctx)
	// Fixed buffer pool sized for the largest part: the SDK split lets the
	// last part absorb the remainder, so it can reach chunk_size plus one
	// chunk's worth of remainder.
	maxPart := plans[total-1].PartSize
	free := make(chan []byte, concurrency)
	for i := 0; i < concurrency; i++ {
		free <- make([]byte, maxPart)
	}
	jobs := make(chan partJob)

	// Producer: fill buffers from the reader one part at a time.
	g.Go(func() error {
		defer close(jobs)
		for _, plan := range plans {
			var buf []byte
			select {
			case buf = <-free:
			case <-gctx.Done():
				return gctx.Err()
			}
			n, err := io.ReadFull(in, buf[:plan.PartSize])
			if err != nil {
				return fmt.Errorf("wopan: reading part %d: %w", plan.Index, err)
			}
			job := partJob{plan: plan, buf: buf[:n]}
			select {
			case jobs <- job:
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		return nil
	})
	for w := 0; w < concurrency; w++ {
		g.Go(func() error {
			for job := range jobs {
				// The buffer only goes back to the pool once this part is
				// settled: a re-post reads the same bytes, and returning the
				// array earlier would let the producer fill it with the next
				// part while this one is still being sent.
				_, err := f.postPartRetry(gctx, st, func(int) (io.Reader, error) {
					return bytes.NewReader(job.buf), nil
				}, zoneURL, dirID, name, size, fiEnc, uniqueID, token, job.plan, total)
				free <- job.buf
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	return g.Wait()
}

// uploadPart sends one part to the upload2C endpoint and decodes the response.
// in must yield exactly plan.PartSize bytes.
func (f *Fs) uploadPart(ctx context.Context, in io.Reader, zoneURL, dirID, name string, size int64, fileInfoEnc, uniqueID, token string, plan api.PartPlan, total int64) (api.UploadData, error) {
	params := url.Values{}
	params.Set("uniqueId", uniqueID)
	params.Set("accessToken", token)
	params.Set("fileName", name)
	params.Set("psToken", "undefined")
	params.Set("fileSize", strconv.FormatInt(size, 10))
	params.Set("totalPart", strconv.FormatInt(total, 10))
	params.Set("channel", chanWoCloud)
	params.Set("directoryId", dirID)
	params.Set("fileInfo", fileInfoEnc)
	params.Set("partSize", strconv.FormatInt(plan.PartSize, 10))
	params.Set("partIndex", strconv.FormatInt(plan.Index, 10))

	// ⚠️ MultipartUpload's third return value is the OVERHEAD only (form
	// fields + part headers + closing boundary) - the godoc says "in addition
	// to the file contents". The file itself contributes exactly `size` bytes,
	// so ContentLength must be overhead + size. http2 rejects a body larger
	// than the declared length ("request body larger than specified content
	// length"), and http1 would silently truncate the upload.
	body, contentType, overhead, err := rest.MultipartUpload(ctx, in, params, "file", name, "application/octet-stream")
	if err != nil {
		return api.UploadData{}, err
	}
	defer func() { _ = body.Close() }()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, zoneURL+"/openapi/client/upload2C", body)
	if err != nil {
		return api.UploadData{}, err
	}
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = overhead + plan.PartSize
	req.Header.Set("Origin", "https://pan.wo.cn")
	req.Header.Set("Referer", "https://pan.wo.cn/")
	req.Header.Set("User-Agent", defaultUserAgent)

	// Capture which endpoint (zone host + peer IP) actually served this part:
	// with a CDN/reverse proxy in front of the upload zone the DNS answer
	// rotates, so per-part attribution is the only way to correlate speed
	// with the edge the request landed on.
	zoneHost := zoneURL
	if u, perr := url.Parse(zoneURL); perr == nil {
		zoneHost = u.Host
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
		return api.UploadData{}, err
	}
	defer func() { _ = res.Body.Close() }()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return api.UploadData{}, err
	}
	if res.StatusCode >= 300 {
		// part/session context is the only way to attribute a failed POST
		// from the logs: the Put/Update path has no per-chunk logging above
		// this layer (multi-thread's "chunk %d/%d failed" covers only that
		// engine's path).
		err := fmt.Errorf("wopan: upload: part %d/%d session %s via %s -> %s: http %s: %s", plan.Index, total, uniqueID, zoneHost, peerAddr, res.Status, truncate(string(raw), 500))
		// For single-part requests an HTTP-level rejection is deterministic
		// for the given request - the server answers a bare 500 for file
		// names it cannot store (manual test G7: certain special-symbol and
		// 4-byte names) - and NoRetryError stops the retry ladder and
		// --retries from re-running a request that will never succeed.
		//
		// A chunked upload posts one request per part, and probe E4 observed
		// bare 500s there unrelated to the file name, so a 5xx in the
		// chunked path stays retryable: a minutes-long upload should get its
		// whole-file retry instead of dying without one. 4xx rejections
		// remain deterministic.
		if total <= 1 || res.StatusCode < 500 {
			return api.UploadData{}, fserrors.NoRetryError(err)
		}
		return api.UploadData{}, err
	}
	var ur api.UploadResponse
	if err := json.Unmarshal(raw, &ur); err != nil {
		return api.UploadData{}, fmt.Errorf("wopan: decode upload response (part %d/%d session %s): %w", plan.Index, total, uniqueID, err)
	}
	if ur.Code != successCode {
		ae := &apiError{Code: ur.Code, Desc: ur.Msg}
		var err error = ae
		// An exhausted account is freed by hand outside the run, so the
		// whole-file retry --retries offers cannot succeed either.
		if ur.Code == codeSpaceFull {
			err = f.wrapNoSpace(noSpaceError(ae))
		}
		// Wrapped with %w so asAPIError can still unwrap the *apiError for
		// isAuthInvalid while the message carries the part/session context.
		return api.UploadData{}, fmt.Errorf("wopan: upload part %d/%d session %s: %w", plan.Index, total, uniqueID, err)
	}
	elapsed := time.Since(t0)
	fs.Debugf(f, "wopan: uploaded part %d/%d (%v) in session %s via %s -> %s (reused=%v) in %v (%s)",
		plan.Index, total, fs.SizeSuffix(plan.PartSize), uniqueID, zoneHost, peerAddr, reusedConn,
		elapsed.Round(time.Millisecond), fs.SizeSuffix(float64(plan.PartSize)/elapsed.Seconds()).ByteRateUnit())
	return ur.Data, nil
}

// partState holds the one response that completes a chunked upload session.
//
// The fid belongs to whichever part's POST completes the server's set, not to
// the highest part index. Measured 2026-10-21 against a live account: a session
// missing a part answers 0000 with empty data even for the highest index, and
// the part that later fills the gap returns the fid and wcFileId itself. Every
// worker of one session shares a single partState, so "already completed" is a
// session-wide fact rather than each worker's local knowledge.
type partState struct {
	mu   sync.Mutex
	data api.UploadData
}

// finalised reports whether some part's response has already completed the session.
func (s *partState) finalised() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.Fid != ""
}

// note records a response that carried the fid. Responses without one are
// intermediate parts or a duplicate post after completion, so they are ignored.
func (s *partState) note(d api.UploadData) {
	if d.Fid == "" {
		return
	}
	s.mu.Lock()
	s.data = d
	s.mu.Unlock()
}

// snapshot returns the recorded answer, or the zero value if no part ever carried a fid.
func (s *partState) snapshot() api.UploadData {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data
}

// partRetryDelay is how long to wait before re-posting a part. It is a variable
// so tests can remove the wait.
//
// The wait is short and capped because the point is to outlast a flapping edge,
// not to park a transfer: a part that is still failing after this ladder has
// had its attempts is left to the caller's retry decision.
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
	// Stagger the parts of one file so a flapping edge does not see all of
	// them come back at the same instant.
	d += time.Duration(partIdx%5) * 100 * time.Millisecond
	if jitter := int64(d) / 2; jitter > 0 {
		d += time.Duration(rand.Int63n(jitter+1)) - d/4
	}
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return d
}

// partSleep waits for d, giving up if ctx is done first.
func partSleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// postPartRetry posts one part of a chunked session, re-posting that part while
// the transport keeps failing it.
//
// next(attempt) must hand back exactly plan.PartSize bytes again, positioned at
// the start of the part: a re-post overwrites the same part index, and a partial
// or mis-positioned body would be stored as that part's content. It is told which
// attempt it is so a reader that only has to be rewound for a re-post is not
// touched on the first one. A next that cannot rewind returns an error, which is
// not retried.
//
// Only transport-level failures are re-posted. A business code is deterministic
// for the request, and 4xx rejections and the single-part path are already
// marked no-retry by uploadPart, so they come straight back.
//
// A part is never re-posted once the session has produced a fid. Completing a
// session requires every part to be present, so a fid means this part's bytes are
// already stored and the failure was only in the answer; re-posting then costs a
// part's worth of bytes and, measured 2026-10-21, cannot report the fid twice.
//
// When the attempts run out the error carries no fserrors.RetryError marker, so
// the copy engine does not re-upload the whole file this round and --retries
// decides whether to run the command again.
func (f *Fs) postPartRetry(ctx context.Context, st *partState, next func(attempt int) (io.Reader, error), zoneURL, dirID, name string, size int64, fileInfoEnc, uniqueID, token string, plan api.PartPlan, total int64) (api.UploadData, error) {
	attempts := max(fs.GetConfig(ctx).LowLevelRetries, 1)
	if total <= 1 {
		// One part holds the whole file. uploadPart marks an HTTP rejection
		// there as no-retry because the server answers a bare 500 for names it
		// cannot store, and a second POST of that cannot succeed; leave the
		// whole-file ladder in the caller as the only retry, as before.
		attempts = 1
	}
	var lastErr error
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return api.UploadData{}, err
		}
		// Only a re-post may be skipped this way. A part that has never been
		// posted has to be posted: the measured invariant is that a completed
		// session implies every part arrived, so a fid that appeared while this
		// part was retrying means its bytes are already stored - it says nothing
		// about a part that was never sent at all.
		if attempt > 1 && st.finalised() {
			return st.snapshot(), nil
		}
		body, err := next(attempt)
		if err != nil {
			if lastErr != nil {
				return api.UploadData{}, noLowLevelRetry(lastErr)
			}
			return api.UploadData{}, err
		}
		var data api.UploadData
		err = f.pacer.CallNoRetry(func() (bool, error) {
			d, uerr := f.uploadPart(ctx, body, zoneURL, dirID, name, size, fileInfoEnc, uniqueID, token, plan, total)
			if uerr != nil {
				var ae *apiError
				if asAPIError(uerr, &ae) {
					// A business rejection never succeeds on retry.
					return false, uerr
				}
				return true, uerr
			}
			data = d
			return false, nil
		})
		if err == nil {
			st.note(data)
			return data, nil
		}
		var ae *apiError
		if asAPIError(err, &ae) || fserrors.IsNoRetryError(err) {
			return api.UploadData{}, err
		}
		lastErr = err
		if attempt >= attempts {
			if total <= 1 {
				return api.UploadData{}, err
			}
			fs.Debugf(f, "wopan: part %d/%d of session %s still failing after %d attempt(s)", plan.Index, total, uniqueID, attempt)
			return api.UploadData{}, noLowLevelRetry(err)
		}
		delay := partRetryDelay(attempt, int(plan.Index))
		fs.Debugf(f, "wopan: part %d/%d of session %s attempt %d/%d failed, re-posting in %v: %v",
			plan.Index, total, uniqueID, attempt, attempts, delay, err)
		if err := partSleep(ctx, delay); err != nil {
			return api.UploadData{}, noLowLevelRetry(lastErr)
		}
	}
}

// OpenChunkWriter returns a ChunkWriter for the multi-thread copy engine.
//
// The engine splits src into ceil(size/ChunkSize) chunks of ChunkSize bytes
// (the last absorbing the remainder) and calls WriteChunk once per chunk, in
// completion order, from up to Concurrency goroutines; the server assembles
// the parts by part index, so completion order does not matter. One upload
// session (a single uniqueId) spans the whole file, and the caller's retry of
// a failed copy opens a fresh session via a new OpenChunkWriter call.
func (f *Fs) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (info fs.ChunkWriterInfo, writer fs.ChunkWriter, err error) {
	size := src.Size()
	if size < 0 {
		return info, nil, errors.New("wopan: can't upload files of unknown size")
	}
	if size == 0 {
		return info, nil, fs.ErrorCantUploadEmptyFiles
	}
	// The multi-thread copy engine starts reading the source before Close() runs,
	// so the over-limit notice belongs here, where the size is first known.
	f.warnOversize(remote, size)
	if err := f.validateName(remote); err != nil {
		return info, nil, err
	}
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return info, nil, err
	}
	leaf = f.opt.Enc.FromStandardName(leaf)
	// 重要1 (fifth round): the server truncates the ENCODED name, and the
	// encoder can expand characters, so re-check the rune count on what is
	// actually sent.
	if err := validateName(leaf); err != nil {
		return info, nil, err
	}
	// A big file is what the instant-upload probe is worth most, and this is the
	// last point where the transfer can still be avoided: the engine opens the
	// source only after OpenChunkWriter returns.
	if _, hit, qerr := f.quickTransfer(ctx, dirID, leaf, size, src); qerr != nil {
		return info, nil, qerr
	} else if hit {
		// Keep the configured chunk size and one worker. The writer reads nothing,
		// so the engine's only cost is walking the file; advertising the whole file
		// as one chunk instead makes the engine reserve that many bytes in a single
		// pooled buffer, and lib/pool waits for memory without ever giving up, so a
		// --max-buffer-memory below the file size would hang a transfer of no bytes.
		hitChunk := int64(f.opt.ChunkSize)
		if size < hitChunk {
			hitChunk = size
		}
		info = fs.ChunkWriterInfo{ChunkSize: hitChunk, Concurrency: 1}
		return info, transferredChunkWriter{}, nil
	}
	zoneURL, fiEnc, _, uniqueID, token, err := f.newUploadSession(ctx, dirID, leaf, size, src)
	if err != nil {
		return info, nil, err
	}
	chunkSize := int64(f.opt.ChunkSize)
	if size < chunkSize {
		chunkSize = size
	}
	w := &wopanChunkWriter{
		f:        f,
		zoneURL:  zoneURL,
		dirID:    dirID,
		name:     leaf,
		size:     size,
		chunk:    chunkSize,
		total:    (size + chunkSize - 1) / chunkSize,
		fiEnc:    fiEnc,
		uniqueID: uniqueID,
		token:    token,
	}
	info = fs.ChunkWriterInfo{
		ChunkSize:   chunkSize,
		Concurrency: f.opt.UploadConcurrency,
	}
	fs.Debugf(src, "wopan: open chunk writer: %d parts of %v in session %s", w.total, fs.SizeSuffix(chunkSize), uniqueID)
	return info, w, nil
}

// wopanChunkWriter uploads the parts of one file through a single upload2C
// session, one POST per WriteChunk call.
type wopanChunkWriter struct {
	f        *Fs
	zoneURL  string
	dirID    string
	name     string
	size     int64
	chunk    int64
	total    int64
	fiEnc    string
	uniqueID string
	token    string

	state partState // holds whichever part's response completed the session
}

// planFor computes the part parameters of a 0-based chunk number.
func (w *wopanChunkWriter) planFor(chunkNumber int) api.PartPlan {
	plan := api.PartPlan{Index: int64(chunkNumber + 1), PartSize: w.chunk}
	if int64(chunkNumber) == w.total-1 {
		plan.PartSize = w.size - int64(chunkNumber)*w.chunk
	}
	return plan
}

// WriteChunk posts one part of the session. The engine may call it
// concurrently and out of order; the server reassembles by part index and the
// part whose response completes the set carries the fid.
//
// The reader is rewound only for a re-post. Its offset is relative to this part
// either way: the buffered path hands over a pool.RW holding just this chunk
// (lib/pool SeekStart is the buffer's own coordinate) and a local source hands
// over an operations.ReOpen whose seek base is the range start, not the file
// start (fs/operations/reopen.go builds the next RangeOption from start+offset).
func (w *wopanChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (bytesWritten int64, err error) {
	plan := w.planFor(chunkNumber)
	_, err = w.f.postPartRetry(ctx, &w.state, func(attempt int) (io.Reader, error) {
		if attempt > 1 {
			if _, serr := reader.Seek(0, io.SeekStart); serr != nil {
				return nil, serr
			}
		}
		return io.LimitReader(reader, plan.PartSize), nil
	}, w.zoneURL, w.dirID, w.name, w.size, w.fiEnc, w.uniqueID, w.token, plan, w.total)
	if err != nil {
		return 0, err
	}
	return plan.PartSize, nil
}

// Close finishes the session. The part that completed the set already assembled
// the file server-side, so this only checks that a fid came back.
func (w *wopanChunkWriter) Close(ctx context.Context) error {
	data := w.state.snapshot()
	if data.WcFileID == "" || data.Fid == "" {
		return fmt.Errorf("wopan: chunked upload session %s (%s) response has an empty fid or wcFileId", w.uniqueID, w.name)
	}
	return nil
}

// Abort abandons the session. There is no server-side abort API: the orphaned
// parts stay invisible, consume no quota and expire on their own, so nothing
// needs to be done here.
func (w *wopanChunkWriter) Abort(ctx context.Context) error {
	return nil
}

// ------------------------------------------------------------ rename --------

// renameObject renames the object with id to name via RenameFileOrDirectory.
func (f *Fs) renameObject(ctx context.Context, id, name string) error {
	p := f.spaceParams()
	p["type"] = fileTypeFile
	p["fileType"] = "5"
	p["id"] = id
	p["name"] = name
	return f.pacer.Call(func() (bool, error) {
		_, err := f.call(ctx, chanWoHome, "RenameFileOrDirectory", p, map[string]any{"secret": true})
		return shouldRetryCall(ctx, err)
	})
}

// noLowLevelRetry marks err as not worthy of copy.go's whole-file retry.
//
// fserrors.NoLowLevelRetryError alone is not enough for an error that came back
// from f.pacer.Call: the pacer wraps an exhausted-retry transport failure in
// fserrors.RetryError (fs/pacer.go pacerInvoker), and copy.go tests
// IsRetryError BEFORE ShouldRetry - and ShouldRetry is the only place the
// NoLowLevelRetry marker is honoured. Without stripping the pacer's marker
// first, the whole-file retry fires anyway and re-uploads the file, which is
// precisely what the marker exists to prevent. Business errors are unaffected:
// the pacer does not wrap those, so this is a no-op for them.
func noLowLevelRetry(err error) error {
	return fserrors.NoLowLevelRetryError(stripPacerRetry(err))
}

// stripPacerRetry removes the fserrors.RetryError marker that f.pacer.Call adds
// to an exhausted-retry transport failure.
//
// fserrors.IsRetryError walks the WHOLE error chain, so a marker anywhere in it
// counts. That is why callers must strip at the source, before any fmt.Errorf
// adds context on top: once a marker is buried, removing it would mean throwing
// away the surrounding message. Here the marker is the outermost wrapper, which
// is the shape pacerInvoker produces.
func stripPacerRetry(err error) error {
	for {
		if _, ok := err.(fserrors.Retrier); !ok {
			return err
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		inner := u.Unwrap()
		if inner == nil {
			return err
		}
		err = inner
	}
}

// renameTimeout returns the rename retry deadline. It is a method so tests can
// shorten the 60s production budget; without that a test driving a persistently
// failing rename would sleep through the whole backoff.
func (f *Fs) renameTimeout() time.Duration {
	if f.renameDeadlineOverride > 0 {
		return f.renameDeadlineOverride
	}
	return renameDeadline
}

// listBudget returns the budget for waiting out the listing's visibility delay.
// It is a method so tests can shorten it.
func (f *Fs) listBudget() time.Duration {
	if f.listBudgetOverride > 0 {
		return f.listBudgetOverride
	}
	return copyVisibilityBudget
}

// leafHolds reports whether the file named leaf currently has the given id,
// waiting out the listing's visibility delay.
//
// leaf must be in the SAME encoded form the rename was issued with; the
// conversion to the standard form that listings use happens here, so a caller
// cannot accidentally compare an encoded name against standard ones.
//
// It is the oracle for a rename whose outcome is unknown. The server never
// overwrites on rename, so if the name is held by the very object that was being
// moved there, the move landed; a different id means somebody else owns the name
// now. An entry that is absent at the end of the budget reports false.
func (f *Fs) leafHolds(ctx context.Context, dirID, leaf, id string) (bool, error) {
	// listDirEntries returns standard names.
	standard := f.opt.Enc.ToStandardName(leaf)
	deadline := time.Now().Add(f.listBudget())
	backoff := time.Second
	for {
		entries, err := f.listDirEntries(ctx, dirID)
		if err != nil {
			return false, err
		}
		for _, item := range entries {
			if item.Type == fileTypeFile && strings.EqualFold(item.Name, standard) {
				return item.ID == id, nil
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false, nil
		}
		// Never oversleep past the deadline: the backoff starts at a second,
		// which would otherwise dwarf a short budget.
		wait := backoff
		if wait > remaining {
			wait = remaining
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(wait):
		}
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
}

// renameWithBackoff renames a file, retrying transiently until a time deadline.
//
// The name index is released asynchronously after a delete, so a rename to a
// just-freed name can transiently fail - with either 500010006 (file busy) or
// 130012 (name taken, index not yet released). Retrying by id is safe in both
// cases: a rename that would overwrite a different object never succeeds
// (130012), so the backoff can only heal the transient window and merely
// delays the terminal error for a genuine concurrent-update conflict. The
// pacer's default budget (~4.5s) is too short, so the deadline is tracked here.
func (f *Fs) renameWithBackoff(ctx context.Context, id, name string) error {
	deadline := time.Now().Add(f.renameTimeout())
	start := time.Now()
	backoff := time.Second
	for {
		attemptStart := time.Now()
		err := f.renameObject(ctx, id, name)
		fs.Debugf(f, "wopan: rename attempt took %s (err=%v)", time.Since(attemptStart).Round(time.Millisecond), err)
		if err == nil {
			return nil
		}
		// Drop the pacer's RetryError marker here, at the source: this function
		// has already spent its own retry budget, so telling a caller to retry
		// again is misleading, and every caller wraps the error with context -
		// once the marker is buried under a fmt.Errorf it can no longer be
		// removed, and copy.go's IsRetryError (which walks the whole chain)
		// would trigger a whole-file retry that this marker exists to prevent.
		err = stripPacerRetry(err)
		code := ""
		var ae *apiError
		if asAPIError(err, &ae) {
			code = ae.Code
			switch code {
			case codeNameOccupied, codeFileOccupied:
				// Transient (index release / file busy) or a genuine
				// concurrent-update conflict; both are safe to retry by
				// id until the deadline.
			default:
				// Any other business error is terminal.
				return err
			}
		}
		// codeNameOccupied/codeFileOccupied or a transport error: retry until deadline.
		if time.Now().After(deadline) {
			fs.Debugf(f, "wopan: rename deadline exceeded after %s", time.Since(start).Round(time.Millisecond))
			return err
		}
		fs.Debugf(f, "wopan: rename transient failure RSP_CODE=%q, retrying in %v", code, backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
}

// ------------------------------------------------------------ download -----

// downloadRequest builds the GetDownloadUrlV2 parameter for one fid.
//
// The family space is addressed the same way as in every other wohome call, with
// spaceType and familyId; personal space sends neither. That asymmetry is not
// cosmetic: an empty familyId is answered RSP_CODE 9999, so "always include the key"
// would break the path that works today.
//
// The family branch follows that convention rather than a capture: the recorded
// personal-space request in the protocol notes carries no space fields, and no
// family-space download has been sampled yet.
func (f *Fs) downloadRequest(fid string) api.DownloadURLRequest {
	p := api.DownloadURLRequest{Type: "1", FidList: []string{fid}, ClientID: clientID}
	if f.spaceType == spaceFamily {
		p.SpaceType = f.spaceType
		p.FamilyID = f.opt.FamilyID
	}
	return p
}

// fetchDownloadURL requests a fresh download URL for a fid.
func (f *Fs) fetchDownloadURL(ctx context.Context, fid string) (string, error) {
	p := f.downloadRequest(fid)
	var resp api.DownloadURLResponse
	err := f.pacer.Call(func() (bool, error) {
		data, err := f.call(ctx, chanWoHome, "GetDownloadUrlV2", p, map[string]any{"secret": true})
		if err != nil {
			return shouldRetryCall(ctx, err)
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			return false, fmt.Errorf("wopan: decode download url: %w", err)
		}
		return false, nil
	})
	if err != nil {
		return "", err
	}
	if len(resp.List) == 0 || resp.List[0].DownloadURL == "" {
		return "", fmt.Errorf("wopan: no download URL returned for fid %q", fid)
	}
	return resp.List[0].DownloadURL, nil
}

// PublicLink returns a direct download link for the given file.
//
// The link comes from GetDownloadUrlV2 and points straight at the file's
// download endpoint, so anyone holding it can fetch the file until it expires.
// wopan fixes that lifetime on the server (120 minutes, measured and not extended
// by access) and the API
// takes no expiry parameter, so a requested expire is ignored with a warning.
// unlink is meaningless too - there is no stored share to revoke. Directories
// cannot be linked and return fs.ErrorCantShareDirectories.
//
// FIXME: 120 minutes is measured in the personal space only. A family-space request
// now carries the same spaceType/familyId pair as every other wohome call, but neither
// that parameter shape nor the granted lifetime has been sampled for a family file, so
// downloadURLTTL's 30 minutes is applied to both spaces on personal-space evidence.
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (string, error) {
	if strings.HasSuffix(remote, "/") {
		return "", fs.ErrorCantShareDirectories
	}
	if expire.IsSet() {
		fs.Logf(f, "Public Link: wopan links expire 2 hours after being issued, ignoring the requested expiry of %v", expire)
	}
	obj, err := f.NewObject(ctx, remote)
	if err != nil {
		if errors.Is(err, fs.ErrorIsDir) {
			// The trailing-slash test above only catches `dir/`. fstests hands
			// over `dir`, and found this in the run of 2026-10-20: the lookup came
			// back "is a directory not a file", which reads as a broken path rather
			// than as "this drive cannot share a directory".
			return "", fs.ErrorCantShareDirectories
		}
		return "", err
	}
	return obj.(*Object).downloadURL(ctx)
}

// errStaleLink marks "this download URL is no longer usable, a new one may
// work". It is returned by headETag so a caller can tell "refetch" apart from
// "the object is broken", and Hash uses it to retry exactly once.
var errStaleLink = errors.New("download link no longer valid")

// staleLinkStatus reports a download-CDN answer meaning the link, not the
// request, is what failed: the server's own 403 (revoked/unauthorised) and the
// non-standard 519 (aged out). Both are worth one refetch; neither is worth a
// retry loop, so callers do this once per operation.
func staleLinkStatus(code int) bool {
	return code == http.StatusForbidden || code == statusLinkExpired
}

// headETag returns the ETag header of a download URL.
//
// A response with no ETag header yields ("", nil) rather than an error, so a
// Hash() caller treats it as "no hash available" (size-only fallback) instead
// of a corrupted transfer.
func (f *Fs) headETag(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	res, err := f.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = res.Body.Close() }()
	if staleLinkStatus(res.StatusCode) {
		return "", fmt.Errorf("wopan: HEAD %s: %w: %s", url, errStaleLink, res.Status)
	}
	if res.StatusCode >= 300 {
		return "", fmt.Errorf("wopan: HEAD %s: %s", url, res.Status)
	}
	return res.Header.Get("ETag"), nil
}

// getURL performs a GET for a download URL, applying the given headers (which
// carry the Range option).
func (f *Fs) getURL(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	res, err := f.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ------------------------------------------------------------ object url ---

// downloadURL returns a cached download URL, fetching a fresh one when empty or
// expired.
func (o *Object) downloadURL(ctx context.Context) (string, error) {
	o.urlMu.Lock()
	defer o.urlMu.Unlock()
	if o.url != "" && time.Now().Before(o.urlExpiry) {
		return o.url, nil
	}
	url, err := o.fs.fetchDownloadURL(ctx, o.fid)
	if err != nil {
		return "", err
	}
	o.url = url
	o.urlExpiry = time.Now().Add(downloadURLTTL)
	return o.url, nil
}

// invalidateURL drops the cached download URL.
func (o *Object) invalidateURL() {
	o.urlMu.Lock()
	o.url = ""
	o.urlExpiry = time.Time{}
	o.urlMu.Unlock()
}

// refreshFromUpload updates the receiver in place after an upload.
//
// It is pure memory: id and fid come from the upload response, and the cached
// URL is cleared because the old fid's link is no longer valid. The receiver
// must be updated so that verify (which reuses it) compares against the new
// object instead of the deleted one.
func (o *Object) refreshFromUpload(ctx context.Context, src fs.ObjectInfo, data api.UploadData, size int64) {
	o.urlMu.Lock()
	defer o.urlMu.Unlock()
	o.id = data.WcFileID
	o.fid = data.Fid
	o.size = size
	o.createTime = time.Now() // the upload API does not return a creation time
	// The server stores mtimes at second precision; truncating here keeps the
	// in-memory fingerprint identical to a fresh read (fstests CheckObjects).
	o.shootingTime = src.ModTime(ctx).Truncate(time.Second)
	o.uploadedAt = time.Now()
	// Multi-part uploads never carry a content MD5 in the ETag, so remember
	// the split and skip the probe in Hash().
	o.multipart = size > int64(o.fs.opt.UploadCutoff)
	o.url = ""
	o.urlExpiry = time.Time{}
}

// ------------------------------------------------------------ delete -------

// purgeCheck deletes dir, if check is set it refuses to do so if it's not empty
func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	// Only the true account root is protected. path.Join is used rather than
	// testing dir == "" because Rmdir(ctx, "") is a legal way to remove a
	// subdirectory when the remote root is that directory.
	if path.Join(f.root, dir) == "" {
		return errors.New("can't purge root directory")
	}
	id, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}
	if check {
		entries, err := f.listDirEntries(ctx, id)
		if err != nil {
			return err
		}
		if len(entries) > 0 {
			return fs.ErrorDirectoryNotEmpty
		}
	}
	// Snapshot BEFORE the delete (red line, same as deleteFile): a directory
	// lands in the recycle bin as a single entry, so purging by the directory id
	// below is enough - no walk over children is needed.
	var snap map[string][]string
	if f.opt.HardDelete {
		s, err := f.snapshotRecycle(ctx)
		if err != nil {
			return err
		}
		snap = s
	}
	// The delete parameters travel inside the encrypted param; the body only
	// carries the plaintext extras.
	p := f.spaceParams()
	p["vipLevel"] = "0"
	p["dirList"] = []string{id}
	p["fileList"] = []string{}
	err = f.pacer.Call(func() (bool, error) {
		_, err := f.call(ctx, chanWoHome, "DeleteFile", p, map[string]any{"secret": true})
		return shouldRetryCall(ctx, err)
	})
	if err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	// The names under here are gone, so a refusal remembered for any of them is
	// no longer evidence about the present tree.
	f.dirRejects.clear()
	if f.opt.HardDelete {
		return f.purgeRecycle(ctx, snap, []string{id})
	}
	return nil
}

// Rmdir deletes the root folder
//
// Returns an error if it isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return f.purgeCheck(ctx, dir, true)
}

// Purge deletes all the files and the container
//
// Optional interface: Only implement this if you have a way of
// deleting all the files quicker than just running Remove() on the
// result of List()
func (f *Fs) Purge(ctx context.Context, dir string) error {
	return f.purgeCheck(ctx, dir, false)
}

// ------------------------------------------------------------ recycle ------

// listRecyclePage lists one page of the recycle bin.
func (f *Fs) listRecyclePage(ctx context.Context, pageNum int) ([]api.RecycleItem, error) {
	p := f.spaceParams()
	p["vipLevel"] = "0"
	p["pageNum"] = pageNum
	p["pageSize"] = recyclePageSize
	var items api.RecycleListResponse
	err := f.pacer.Call(func() (bool, error) {
		data, err := f.call(ctx, chanWoHome, "QueryRecycleData", p, map[string]any{"secret": true})
		if err != nil {
			return shouldRetryCall(ctx, err)
		}
		if err := json.Unmarshal(data, &items); err != nil {
			return false, fmt.Errorf("wopan: decode recycle listing: %w", err)
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// listRecycleAll fetches the recycle listing as completely as the server
// allows.
//
// ⚠️ Fifth-round B6 follow-up (verified against the live server): the API
// IGNORES the page number entirely - pageNum 0/1/2/3 all return the same
// first pageSize items, and every candidate param name (pageIdx/pageIndex/
// page/curPage) behaves the same. "Paging until an empty page" therefore
// never terminates; the plan's "翻完所有分页" assumption was wrong. What the
// server does honor is pageSize, so one large page is fetched instead.
// Newly deleted entries sort first, so hard_delete's pending ids are found
// even when the bin holds more entries than one page.
func (f *Fs) listRecycleAll(ctx context.Context) ([]api.RecycleItem, error) {
	items, truncated, err := collectRecyclePages(ctx, func(page int) ([]api.RecycleItem, error) {
		return f.listRecyclePage(ctx, page)
	}, recyclePageSize, maxRecyclePages)
	if truncated {
		// A repeated page means the server ignores pageNum, so entries beyond
		// the first page are unreachable: hard_delete stays fail-loud (a
		// pending id it cannot find is left in the bin and reported), but the
		// condition must be visible to the user.
		fs.Logf(f, "recycle bin holds more than %d entries and the server ignored recycling paging: entries beyond the first page are invisible to hard_delete; empty the recycle bin from the app to restore full hard_delete", recyclePageSize)
	}
	return items, err
}

// collectRecyclePages fetches recycle pages until the bin is exhausted. A
// full page triggers a probe of the next page: the server ignored pageNum in
// early probing (35-entry bin, inconclusive beyond page 0), so a page that
// repeats the previous one stops the scan and reports truncated - when the
// server does honour paging, further pages are appended and nothing is
// truncated. fetch must be safe for page numbers 0..maxPages-1.
func collectRecyclePages(ctx context.Context, fetch func(page int) ([]api.RecycleItem, error), pageSize, maxPages int) (items []api.RecycleItem, truncated bool, err error) {
	var prev []api.RecycleItem
	for page := 0; page < maxPages; page++ {
		pageItems, err := fetch(page)
		if err != nil {
			return items, false, err
		}
		if page > 0 && recycleSamePage(prev, pageItems) {
			return items, true, nil
		}
		items = append(items, pageItems...)
		if len(pageItems) < pageSize {
			return items, false, nil
		}
		prev = pageItems
	}
	return items, true, nil
}

// recycleSamePage reports whether two recycle pages hold the same entries in
// the same order - the server's signature of an ignored pageNum.
func recycleSamePage(a, b []api.RecycleItem) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].DeleteNo != b[i].DeleteNo || a[i].ID != b[i].ID {
			return false
		}
	}
	return true
}

// snapshotRecycle records the deleteNos already in the recycle bin, indexed by
// original object id.
//
// Without this snapshot a purge could not tell our own freshly deleted entries
// from ones the user had parked there earlier, and would permanently destroy
// the user's entries.
func (f *Fs) snapshotRecycle(ctx context.Context) (map[string][]string, error) {
	snap := map[string][]string{}
	items, err := f.listRecycleAll(ctx)
	if err != nil {
		return nil, err
	}
	for _, it := range items {
		snap[it.ID] = append(snap[it.ID], it.DeleteNo)
	}
	return snap, nil
}

// deleteRecycleNos permanently deletes recycle-bin entries by deleteNo.
//
// The server is idempotent here: an already-gone deleteNo still returns 0000.
func (f *Fs) deleteRecycleNos(ctx context.Context, nos []string) error {
	// The deleteNos parameter travels inside the encrypted param; the body only
	// carries the plaintext extras.
	p := map[string]any{
		"deleteNos": nos,
		"clientId":  clientID,
	}
	return f.pacer.Call(func() (bool, error) {
		_, err := f.call(ctx, chanWoHome, "DeleteRecycleData", p, map[string]any{"secret": true})
		return shouldRetryCall(ctx, err)
	})
}

// purgeRecycleOpts carries the dependencies of purgeRecycle so that tests can
// inject a canned recycle listing, a stepped clock and a recording delete.
type purgeRecycleOpts struct {
	listAll func(ctx context.Context) ([]api.RecycleItem, error)
	now     func() time.Time
	sleep   func(time.Duration)
	delete  func(ctx context.Context, nos []string) error
	budget  time.Duration
}

// purgeRecycle permanently deletes the recycle-bin entries created by deleting
// deletedIDs.
//
// snap holds the deleteNos that were already in the bin before the delete; ids
// covered by it are considered resolved (an idempotent delete or an entry that
// never reached the bin) and are never waited on. Only the remaining pending
// ids are expected to appear, and the scan retries with backoff until every
// pending id has at least one entry or the budget runs out - a shortfall is an
// error, never a silent success.
func purgeRecycle(ctx context.Context, opt purgeRecycleOpts, snap map[string][]string, deletedIDs []string) error {
	// no-op semantics: ids already in the snapshot were resolved before this
	// call, so return immediately without scanning, sleeping or deleting.
	// deletedIDs is de-duplicated first (S-2): a repeated id would inflate
	// len(pending) while found is keyed by id, so the completion check could
	// never be satisfied.
	seen := make(map[string]bool, len(deletedIDs))
	var pending []string
	for _, id := range deletedIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		if len(snap[id]) == 0 {
			pending = append(pending, id)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	pendingSet := make(map[string]bool, len(pending))
	for _, id := range pending {
		pendingSet[id] = true
	}

	// scan collects the deleteNos of pending ids from the listing, keyed by id:
	// several entries for one id count as that one id being found, so the
	// completion check below compares id sets rather than entry counts.
	// (Fifth-round B6 follow-up: the server ignores the page number entirely -
	// verified live - so the listing is fetched as a single large page via
	// listRecycleAll instead of a page loop that could never terminate.)
	scan := func() (map[string][]string, error) {
		found := map[string][]string{}
		items, err := opt.listAll(ctx)
		if err != nil {
			return nil, err
		}
		for _, it := range items {
			if !pendingSet[it.ID] {
				continue
			}
			if !containsString(found[it.ID], it.DeleteNo) {
				found[it.ID] = append(found[it.ID], it.DeleteNo)
			}
		}
		return found, nil
	}

	found, err := scan()
	if err != nil {
		return err
	}
	deadline := opt.now().Add(opt.budget)
	for backoff := time.Second; len(found) < len(pending) && opt.now().Before(deadline); backoff *= 2 {
		if backoff > 8*time.Second {
			backoff = 8 * time.Second
		}
		opt.sleep(backoff)
		found, err = scan()
		if err != nil {
			return err
		}
	}
	if len(found) < len(pending) {
		return fmt.Errorf("wopan: hard_delete found only %d/%d deleted ids in the recycle bin (visibility delay?)", len(found), len(pending))
	}

	var nos []string
	for _, list := range found {
		nos = append(nos, list...)
	}
	if len(nos) == 0 {
		return nil
	}
	return opt.delete(ctx, nos)
}

// purgeRecycle purges the recycle-bin entries created by deleting deletedIDs,
// using the live recycle API and the wall clock.
func (f *Fs) purgeRecycle(ctx context.Context, snap map[string][]string, deletedIDs []string) error {
	return purgeRecycle(ctx, purgeRecycleOpts{
		listAll: f.listRecycleAll,
		now:     time.Now,
		sleep: func(d time.Duration) {
			// ctx-aware: a cancelled context must not wait out the full backoff.
			select {
			case <-time.After(d):
			case <-ctx.Done():
			}
		},
		delete: f.deleteRecycleNos,
		budget: recycleBudget,
	}, snap, deletedIDs)
}

// containsString reports whether list contains s.
func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// ------------------------------------------------------------ server ops ---

// moveCopyParams builds the shared MoveFile/CopyFile parameters.
//
// The family space needs both family keys: fromFamilyId for the source side
// and familyId for the target side, so an intra-family move sends both.
func (f *Fs) moveCopyParams(targetDirID string, dirIDs, fileIDs []string) map[string]any {
	p := f.spaceParams()
	p["targetDirId"] = targetDirID
	p["sourceType"] = f.spaceType
	p["targetType"] = f.spaceType
	if f.spaceType == spaceFamily {
		p["fromFamilyId"] = f.opt.FamilyID
		p["familyId"] = f.opt.FamilyID
	}
	p["dirList"] = dirIDs
	p["fileList"] = fileIDs
	// secret:false lives inside the encrypted param; the body carries the
	// plaintext secret:true - both are required.
	p["secret"] = false
	return p
}

// sameDir reports whether the source object's directory equals the
// destination directory. The two remotes are relative to their OWN Fs roots,
// so when source and destination are different Fs instances of the same
// backend (a --backup-dir move, a moveto across directories) their relative
// directories can both be "." even though the directories differ server-side
// - comparing the remotes directly would misread a cross-directory move as a
// same-directory one: with equal leaves Move would skip the server-side move
// and report success while the object never moved (the caller then overwrites
// it - silent data loss), and with different leaves it would rename the
// object in place instead of moving it.
func sameDir(srcObj *Object, dstRoot, dstRemote string) bool {
	return path.Join(srcObj.fs.root, path.Dir(srcObj.remote)) == path.Join(dstRoot, path.Dir(dstRemote))
}

// Copy src to this remote using server-side copy operations.
//
// CopyFile creates a NEW id and lands the copy under the source's name, so the
// destination listing is re-scanned to find it (with a name/age/id filter that
// can never mistake a pre-existing same-name file for the copy), and a rename
// follows when the requested leaf differs.
//
// The membership single-file upload size is deliberately not reported here: a
// server-side copy transfers no bytes, and a copy rclone cannot do this way falls
// back to Put, which does report it.
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (dst fs.Object, err error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	if err := f.validateName(remote); err != nil {
		return nil, err
	}
	dstLeaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	srcLeaf := path.Base(src.Remote())
	// Same-directory copies are beyond CopyFile's reach: the API takes no
	// target name, so the copy lands under the source's name in the target
	// directory - colliding with the source itself, the server auto-renames
	// it (z(1).txt) and findNewEntry can never match the requested name.
	// Fall back to a bandwidth copy (always safe: nothing has been moved and
	// no copy exists yet).
	if sameDir(srcObj, f.root, remote) {
		return nil, fs.ErrorCantCopy
	}
	// Age discriminator for the re-list below, relaxed by a small slack for the
	// second-truncated server timestamps and clock skew.
	copyStart := time.Now().Add(-copyClockSlack)

	copyTotalStart := time.Now()
	phaseStart := time.Now()
	p := f.moveCopyParams(dstDirID, []string{}, []string{srcObj.id})
	// CallNoRetry: CopyFile is the only non-idempotent write in this backend.
	// A lost response followed by a transport-level retry would create a
	// second copy on the server; failing here instead is always safe because
	// copy.go falls back to a bandwidth copy when it sees the bare sentinel.
	err = f.pacer.CallNoRetry(func() (bool, error) {
		_, err := f.call(ctx, chanWoHome, "CopyFile", p, map[string]any{"secret": true})
		return shouldRetryCall(ctx, err)
	})
	fs.Debugf(f, "wopan: CopyFile took %s", time.Since(phaseStart).Round(time.Millisecond))
	if err != nil {
		// Bare sentinel: copy.go compares with == to fall back to a bandwidth
		// copy, which is always safe here because nothing has been moved.
		fs.Debugf(srcObj, "Server side copy failed: %v", err)
		return nil, fs.ErrorCantCopy
	}

	item, err := f.findNewEntry(ctx, dstDirID, srcLeaf, srcObj.id, copyStart)
	if err != nil {
		return nil, err
	}
	fs.Debugf(f, "wopan: findNewEntry took %s (Copy total %s)", time.Since(phaseStart).Round(time.Millisecond), time.Since(copyTotalStart).Round(time.Millisecond))
	if dstLeaf != srcLeaf {
		if err := f.validateName(dstLeaf); err != nil {
			return nil, err
		}
		// 重要1 (fifth round): the encoder can expand characters, so re-check
		// the rune count on the encoded name that is actually sent.
		dstLeafEnc := f.opt.Enc.FromStandardName(dstLeaf)
		if err := validateName(dstLeafEnc); err != nil {
			return nil, err
		}
		if err := f.renameWithBackoff(ctx, item.ID, dstLeafEnc); err != nil {
			return nil, fmt.Errorf("wopan: rename copied object: %w", err)
		}
	}
	o := &Object{fs: f, remote: remote}
	o.setMetaData(item)
	// S-3 (fifth round): the copy is brand new on the server, so give it the
	// same Hash grace period a fresh upload gets.
	o.uploadedAt = time.Now()
	return o, nil
}

// findNewEntry locates the entry a server-side copy created in dirID.
//
// Only file entries are matched, the source id is excluded, and the entry must
// be at least as new as copyStart so a pre-existing same-name file is never
// mistaken for the copy (verify would then delete the user's file on a hash
// mismatch). Zero hits are retried with backoff against the listing's
// visibility delay; zero or several hits at the end of the budget, or several
// hits at any point, are errors - a guess here is a data-loss risk.
func (f *Fs) findNewEntry(ctx context.Context, dirID, leaf, srcID string, copyStart time.Time) (*api.File, error) {
	findStart := time.Now()
	deadline := findStart.Add(copyVisibilityBudget)
	backoff := time.Second
	scans := 0
	for {
		scans++
		scanStart := time.Now()
		entries, err := f.listDirEntries(ctx, dirID)
		fs.Debugf(f, "wopan: findNewEntry scan #%d took %s, got %d entries", scans, time.Since(scanStart).Round(time.Millisecond), len(entries))
		if err != nil {
			return nil, err
		}
		var hits []*api.File
		for _, item := range entries {
			if item.Type != fileTypeFile {
				continue
			}
			if item.ID == srcID {
				continue
			}
			if when, err := api.ParseTime(item.CreateTime); err != nil || when.Before(copyStart) {
				continue
			}
			if strings.EqualFold(item.Name, leaf) {
				hits = append(hits, item)
			}
		}
		switch {
		case len(hits) == 1:
			fs.Debugf(f, "wopan: findNewEntry found the copy after %d scans, total %s", scans, time.Since(findStart).Round(time.Millisecond))
			return hits[0], nil
		case len(hits) > 1:
			return nil, fmt.Errorf("wopan: found %d candidate copies of %q in the target directory", len(hits), leaf)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("wopan: copied object %q did not appear in the target directory after %d scans / %s (visibility delay?)", leaf, scans, time.Since(findStart).Round(time.Millisecond))
		}
		fs.Debugf(f, "wopan: copy not visible yet (scan #%d, elapsed %s), retrying in %v", scans, time.Since(findStart).Round(time.Millisecond), backoff)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
}

// Move src to this remote using server-side move operations.
//
// MoveFile and rename both keep the object's id, so the destination is the
// source object cloned in memory with only the remote changed - no re-list.
// Re-listing here would be actively wrong: an "exclude id == srcID" filter
// never matches a preserved id, and mapping any post-move failure to the bare
// ErrorCantMove sentinel would send rclone into its copy+delete fallback, where
// the delete (of the preserved id) removes the object at the destination.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (dst fs.Object, err error) {
	moveStart := time.Now()
	fs.Debugf(f, "wopan: Move f.root=%q remote=%q src=%q", f.root, remote, src.Remote())
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}
	if err := f.validateName(remote); err != nil {
		return nil, err
	}
	dstLeaf, dstDirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	srcLeaf := path.Base(src.Remote())

	if !sameDir(srcObj, f.root, remote) {
		// Different parent: move first. A failure here leaves the object at the
		// source, but its outcome is uncertain (the request may have landed),
		// so it is reported wrapped - the bare sentinel would trigger rclone's
		// copy+delete fallback against an id that may already live at the
		// destination.
		phaseStart := time.Now()
		p := f.moveCopyParams(dstDirID, []string{}, []string{srcObj.id})
		if err := f.pacer.Call(func() (bool, error) {
			_, err := f.call(ctx, chanWoHome, "MoveFile", p, map[string]any{"secret": true})
			return shouldRetryCall(ctx, err)
		}); err != nil {
			return nil, fmt.Errorf("wopan: move object: %w", err)
		}
		fs.Debugf(f, "wopan: MoveFile took %s", time.Since(phaseStart).Round(time.Millisecond))
		// The server acknowledges a move with 0000 even while the moved entry
		// is still missing from the target listing, and a move that silently
		// failed is indistinguishable from one that succeeded - the caller
		// would then overwrite the object it believes was relocated (manual
		// test BUG-D: a --backup-dir move lost the old version this way).
		// Confirm the entry by id with a bounded retry and fail loudly
		// instead of reporting success for data that may not have moved.
		verify := func() (string, error) {
			entries, err := f.listDirEntries(ctx, dstDirID)
			if err != nil {
				return "", err
			}
			for _, item := range entries {
				if item.ID == srcObj.id {
					return item.ID, nil
				}
			}
			return "", fs.ErrorDirNotFound
		}
		if _, err := retryFindDir(ctx, dirFindAttempts, dirFindDelay, verify); err != nil {
			return nil, fmt.Errorf("wopan: moved file %q did not appear in the target directory: %w", srcLeaf, err)
		}
	}
	if srcLeaf != dstLeaf {
		// MoveFile does not rename, so a leaf change needs a rename; the id is
		// preserved by renames. The comparison above is in STANDARD names and
		// the encoding happens only here (fifth-round B-1: comparing the
		// encoded dstLeaf against the standard srcLeaf misjudges pairs like
		// full-width ？ vs ASCII ? and skips a rename the server still needs -
		// the object then lives under the old name while rclone reports
		// success at the new one). The encoded form is re-validated because
		// the encoder can expand characters past the 100-rune limit.
		dstLeafEnc := f.opt.Enc.FromStandardName(dstLeaf)
		if err := validateName(dstLeafEnc); err != nil {
			return nil, err
		}
		phaseStart := time.Now()
		if err := f.renameWithBackoff(ctx, srcObj.id, dstLeafEnc); err != nil {
			return nil, fmt.Errorf("wopan: rename moved object: %w", err)
		}
		fs.Debugf(f, "wopan: rename took %s", time.Since(phaseStart).Round(time.Millisecond))
	}
	fs.Debugf(f, "wopan: Move total %s", time.Since(moveStart).Round(time.Millisecond))

	// The id, fid, size and timestamps all survive a move and a rename, so the
	// destination object is the source cloned field by field (a struct copy
	// would duplicate the URL mutex). The clone starts with an empty link cache
	// and re-fetches on first use.
	return &Object{
		fs:           srcObj.fs,
		remote:       remote,
		id:           srcObj.id,
		fid:          srcObj.fid,
		size:         srcObj.size,
		createTime:   srcObj.createTime,
		shootingTime: srcObj.shootingTime,
		thumbURL:     srcObj.thumbURL,
	}, nil
}

// DirMove moves src, srcRemote to this remote at dstRemote using server-side
// move operations.
//
// MoveFile only relocates a directory without renaming it, so a leaf change
// needs a follow-up rename. The dircache helper only prepares the move; the
// source path's cache entry must be flushed by the caller once the directory
// has actually moved - and that flush must happen even when the rename fails,
// because a stale entry would make `rmdir old/path` delete the directory at its
// new location.
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(src, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}
	// A destination inside the source directory (e.g. the degenerate
	// "directory into its own subdirectory" a moveto can degrade to when
	// the source file is not yet listing-visible) would ask the server to
	// move a directory into itself; refuse before touching the server.
	srcPath := path.Join(srcFs.root, srcRemote)
	dstPath := path.Join(f.root, dstRemote)
	if dstPath == srcPath {
		// Moving onto itself means the destination already exists.
		return fs.ErrorDirExists
	}
	if strings.HasPrefix(dstPath, srcPath+"/") {
		return fs.ErrorCantDirMove
	}
	srcID, srcDirectoryID, srcLeaf, dstDirectoryID, dstLeaf, err :=
		f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}

	// Ask the server, not the cache, whether the destination name is taken. The
	// helper above answered that question through FindLeaf, which can answer it
	// from the refusal cache without sending anything, so a move onto a real
	// directory would otherwise report success. This runs on every move rather
	// than only when a refusal is "still held": holding is a clock predicate, the
	// source resolution in between really does go to the server, and re-asking
	// the cache later in the same call can read a different answer - the one
	// direction that is never allowed is skipping this check. The cost is two
	// requests per directory move, not per file.
	exists, err := f.dirPresentUngated(ctx, dstDirectoryID, dstLeaf)
	if err != nil {
		return err
	}
	if exists {
		return fs.ErrorDirExists
	}

	moved := false
	defer func() {
		if moved {
			// The helper does not do this (its godoc defers it to the caller).
			srcFs.dirCache.FlushDir(srcRemote)
			// Everything below the moved directory now has a different full
			// path, and the destination parent may have freed a name. Neither
			// side's remembered refusals describe the tree any more.
			srcFs.dirRejects.clear()
			f.dirRejects.clear()
		}
	}()

	if srcDirectoryID != dstDirectoryID {
		// Different parent: move first.
		p := f.moveCopyParams(dstDirectoryID, []string{srcID}, []string{})
		if err := f.pacer.Call(func() (bool, error) {
			_, err := f.call(ctx, chanWoHome, "MoveFile", p, map[string]any{"secret": true})
			return shouldRetryCall(ctx, err)
		}); err != nil {
			return fmt.Errorf("wopan: move directory: %w", err)
		}
		moved = true
	}
	if srcLeaf != dstLeaf {
		// The directory kept its name through the move; rename it into place.
		if err := f.validateName(dstLeaf); err != nil {
			return err
		}
		// 重要1 (fifth round): re-check the rune count on the encoded name.
		dstLeafEnc := f.opt.Enc.FromStandardName(dstLeaf)
		if err := validateName(dstLeafEnc); err != nil {
			return err
		}
		rp := f.spaceParams()
		rp["type"] = fileTypeDir
		rp["fileType"] = "0"
		rp["id"] = srcID
		rp["name"] = dstLeafEnc
		if err := f.pacer.Call(func() (bool, error) {
			_, err := f.call(ctx, chanWoHome, "RenameFileOrDirectory", rp, map[string]any{"secret": true})
			return shouldRetryCall(ctx, err)
		}); err != nil {
			return fmt.Errorf("wopan: rename directory: %w", err)
		}
		moved = true
	}
	return nil
}

// ------------------------------------------------------------ about --------

// About gets quota information.
//
// The numbers are nested inside usageInfo, and byteTotalSize arrives as a
// string while byteUsedSize is numeric. The sibling totalSize/usedSize pair is
// ignored because its unit is unspecified.
func (f *Fs) About(ctx context.Context) (usage *fs.Usage, err error) {
	p := map[string]any{
		"phoneNum": "",
		"clientId": clientID,
	}
	var resp api.UsageResponse
	err = f.pacer.Call(func() (bool, error) {
		data, err := f.call(ctx, chanWoHome, "QueryCloudUsageInfo", p, map[string]any{"secret": true})
		if err != nil {
			return shouldRetryCall(ctx, err)
		}
		if err := json.Unmarshal(data, &resp); err != nil {
			return false, fmt.Errorf("wopan: decode usage: %w", err)
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	total, err := strconv.ParseInt(resp.UsageInfo.ByteTotalSize, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("wopan: parse quota total %q: %w", resp.UsageInfo.ByteTotalSize, err)
	}
	used := resp.UsageInfo.ByteUsedSize
	return &fs.Usage{
		Total: fs.NewUsageValue(total),
		Used:  fs.NewUsageValue(used),
		Free:  fs.NewUsageValue(total - used),
	}, nil
}

// ------------------------------------------------------------ checks -------

// Interface checks: these fail at compile time if Fs or Object stop satisfying
// what rclone expects.
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.OpenChunkWriter = (*Fs)(nil)
	_ dircache.DirCacher = (*Fs)(nil)
)
