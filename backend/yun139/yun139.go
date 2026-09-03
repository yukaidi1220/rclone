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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
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
	yunBaseURL       = "https://yun.139.com"
	userNJSBaseURL   = "https://user-njs.yun.139.com"
	minSleep         = 10 * time.Millisecond
	maxSleep         = 2 * time.Second
	decayConstant    = 2
	listPageSize     = 100
	maxListPages     = 1000
	defaultRootID    = "/"
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

	deviceInfo = "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||"
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
	Authorization string `config:"authorization"` // base64("pc:account:token")
	Space         string `config:"space"`
	FamilyID      string `config:"family_id"`
	RootFolderID  string `config:"root_folder_id"`
	UserDomainID  string `config:"user_domain_id"` // <user-domain-id>-style id, optional
	HardDelete    bool   `config:"hard_delete"`
	PartSize      fs.SizeSuffix `config:"part_size"`
	UploadConcurrency int       `config:"upload_concurrency"`
	Enc           encoder.MultiEncoder `config:"encoding"`
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
			Name: "part_size",
			Help: "Part size for uploads.\n\n" +
				"Files larger than this are uploaded in parts of this size. Must be a multiple of 64 bytes. " +
				"The official client uses 100 MiB.",
			Default:  fs.SizeSuffix(personalPartSize),
			Advanced: true,
		}, {
			Name: "upload_concurrency",
			Help: "Concurrency for part uploads within a single file.",
			Default:  4,
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

// errTokenExpired is returned when the authorization token is expired.
var errTokenExpired = fserrors.NoRetryError(errors.New("yun139: authorization token has expired"))

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

// ------------------------------------------------------------ Fs ----------

// Fs represents a remote yun139
type Fs struct {
	name       string             // name of this remote
	root       string             // the path we are working on
	opt        Options            // parsed options
	m          configmap.Mapper   // config mapper, used to write tokens back
	features   *fs.Features      // optional features
	httpClient *http.Client       // the connection to the server
	pacer      *fs.Pacer          // pacer for API calls
	dirCache   *dircache.DirCache // Map of directory path to directory id
	// srvPathOf caches family dirID -> server "root:/..." path. Held by
	// pointer so copying the Fs struct (dircache's tempFs trick) stays
	// lock-copy-free.
	srvPathOf *srvPathCache

	space      string // spacePersonal or spaceFamily
	svcType    string // svcTypePersonal or svcTypeFamily

	tokMu   *sync.Mutex // guards authorization + account (pointer so NewFs's tempF copy stays lock-safe)
	auth    string      // the raw base64 authorization token
	account string      // the phone number

	hostMu       *sync.Mutex // guards personalHost
	personalHost string      // per-account personal cloud host, discovered once

	familyRootMu *sync.Mutex
	familyRootID string // server-side root catalog ID of the family cloud

	userDomainID string // <user-domain-id>-style domain id, learned from queryFamilyCloud
}

// Object describes a yun139 object
type Object struct {
	fs         *Fs     // what this object is part of
	remote     string  // the remote path
	id         string  // ID of the object
	size       int64   // size of the object
	modTime    time.Time // modification time
	isDir      bool    // whether this is a directory
	serverPath string  // family/group: server-side path (root:/...)
	sha256     string  // personal space: contentHash from listing/upload

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

// tokenExpiry returns the expiry time encoded in the token, if present.
func (f *Fs) tokenExpiry() (time.Time, bool) {
	decoded, err := base64.StdEncoding.DecodeString(f.auth)
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
func (f *Fs) accessToken() string {
	f.tokMu.Lock()
	defer f.tokMu.Unlock()
	return f.auth
}

// refreshToken refreshes the token via the SSO endpoint when it has less than
// minTokenLifetime left. The new token is written back to the config.
func (f *Fs) refreshToken(ctx context.Context) error {
	f.tokMu.Lock()
	auth := f.auth
	account := f.account
	f.tokMu.Unlock()

	expiry, ok := f.tokenExpiry()
	if !ok {
		// No expiry in the token: assume it is valid.
		return nil
	}
	if time.Until(expiry) > minTokenLifetime {
		return nil
	}
	// Within minTokenLifetime of expiry, or already past it: refresh.
	// 139 rejects API calls with a fully-expired token, so refreshing
	// after the deadline is the only path to a working session.

	decoded, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return err
	}
	parts := strings.SplitN(string(decoded), ":", 3)
	if len(parts) < 3 {
		return errors.New("yun139: invalid authorization format")
	}
	token := parts[2]

	// The PC client refreshes with a JSON heartbeat
	// {"authToken":..., "userId":...} to note-njs.yun.139.com
	// (captured 2026-09-03). The response is a user profile; it does
	// NOT contain a new token, so a 200 means the token is still valid
	// (its window was extended server-side).
	userID := f.userDomainID
	if userID == "" {
		userID = account
	}
	reqBody, err := json.Marshal(map[string]string{"authToken": token, "userId": userID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.AuthTokenRefreshURL, bytes.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	req.Header.Set("User-Agent", pcUserAgentShort)
	req.Header.Set("APP_CP", "pc")
	req.Header.Set("CP_VERSION", pcAppVersion)

	res, err := f.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("yun139: refresh token: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("yun139: refresh token: http %s: %s", res.Status, truncate(string(body), 200))
	}
	// A 200 body that parses as JSON means the token is still good.
	// The response is a user profile (userphone/username/...), not an
	// envelope, so any 200 with JSON is success.
	var env struct {
		Success bool   `json:"success"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("yun139: refresh token: decode %w", err)
	}
	if !env.Success && env.Message != "" {
		// An explicit error envelope (e.g. {"success":false,...})
		// means the token is dead.
		return fserrors.NoRetryError(fmt.Errorf("yun139: refresh token: %s %s", env.Code, env.Message))
	}
	return nil
}

// ------------------------------------------------------------ headers -----

// commonHeaders returns the base header set shared by both API families.
func commonHeaders() map[string]string {
	return map[string]string{
		"Accept":         "application/json, text/plain, */*",
		"mcloud-channel": "1000101",
		"mcloud-client":  "10701",
		"mcloud-version": "7.14.0",
		"Origin":         yunBaseURL,
		"Referer":        yunBaseURL + "/w/",
		"x-DeviceInfo":   deviceInfo,
		"x-huawei-channelSrc": "10000034",
		"x-inner-ntwk":   "2",
		"x-m4c-caller":   "PC",
		"x-m4c-src":      "10002",
		"Inner-Hcy-Router-Https": "1",
	}
}

// newHeaders returns the header set for the PersonalNew API (web client).
func newHeaders(auth, ts, randStr, sign, svcType string) map[string]string {
	h := map[string]string{
		"Accept":               "application/json, text/plain, */*",
		"Caller":               "web",
		"CMS-DEVICE":           "default",
		"Mcloud-Channel":       "1000101",
		"Mcloud-Client":        "10701",
		"Mcloud-Route":         "001",
		"Mcloud-Sign":          fmt.Sprintf("%s,%s,%s", ts, randStr, sign),
		"Mcloud-Version":       "7.14.0",
		"x-DeviceInfo":         deviceInfo,
		"x-huawei-channelSrc":  "10000034",
		"x-inner-ntwk":         "2",
		"x-m4c-caller":         "PC",
		"x-m4c-src":            "10002",
		"x-SvcType":            svcType,
		"X-Yun-Api-Version":    "v1",
		"X-Yun-App-Channel":    "10000034",
		"X-Yun-Channel-Source": "10000034",
		"X-Yun-Client-Info":    deviceInfo + "dW5kZWZpbmVk||",
		"X-Yun-Module-Type":    "100",
		"X-Yun-Svc-Type":       svcType,
		"User-Agent":           "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Inner-Hcy-Router-Https": "1",
		"Content-Type":         "application/json",
	}
	h["Authorization"] = "Basic " + auth
	return h
}

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
		"Accept":             "*/*",
		"Authorization":      "Basic " + auth,
		"Content-Type":       "application/json",
		"x-yun-api-version":  "v1",
		"x-yun-app-channel":  pcAppChannel,
		"x-yun-client-info":  pcDeviceInfo,
		"x-yun-device-id":    deviceID,
		"x-yun-market-source": "001",
		"x-yun-module-type":  "100",
		"x-yun-op-type":      "1",
		"x-yun-svc-type":     "1",
		"Accept-Language":    "zh-CN,en,*",
		"User-Agent":         "Mozilla/5.0",
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

// legacyHeaders returns the header set for the orchestration API.
func legacyHeaders(auth, ts, randStr, sign, svcType string) map[string]string {
	h := commonHeaders()
	h["CMS-DEVICE"] = "default"
	h["Authorization"] = "Basic " + auth
	h["mcloud-sign"] = fmt.Sprintf("%s,%s,%s", ts, randStr, sign)
	h["x-SvcType"] = svcType
	h["Content-Type"] = "application/json"
	return h
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

	f.tokMu.Lock()
	auth := f.auth
	f.tokMu.Unlock()

	var headers map[string]string
	host := strings.SplitN(url, "/", 4)[2]
	p := strings.SplitN(url, "?", 2)[0]
	if strings.HasPrefix(url, yunBaseURL+"/orchestration") {
		headers = legacyHeaders(auth, ts, randStr, sign, f.svcType)
	} else if host == "group.yun.139.com" || strings.HasSuffix(p, "/hcy/group/dynamic/file/create") {
		// Family space: lean header set, short UA, x-yun-url-type:3 on
		// every call (captured 2026-09-03, family cloud).
		headers = pcHeaders(auth, f.account, ts, randStr, sign, f.svcType)
		headers["x-yun-url-type"] = "3"
		headers["Accept"] = "*/*"
		headers["x-DeviceInfo"] = f.deviceInfoHeader()
	} else if f.space == spacePersonal {
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
	} else {
		headers = newHeaders(auth, ts, randStr, sign, f.svcType)
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
//   "||<clientInfo>|<osInfo>|<screen>|<localeBase64>|||<osLocale>|"
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
		if p.ModName == "personal" && p.HttpsURL != "" {
			// The route policy host already ends with "/hcy"
			// (https://personal-kd-njs.yun.139.com/hcy); our
			// personalCall paths carry the /hcy prefix themselves,
			// so strip it here to avoid /hcy/hcy/*.
			f.personalHost = strings.TrimRight(strings.TrimSuffix(p.HttpsURL, "/hcy"), "/")
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
	if f.opt.FamilyID == "" {
		clouds, _, err := f.queryFamilyCloud(ctx)
		if err != nil {
			return "", err
		}
		switch len(clouds) {
		case 0:
			return "", errors.New("yun139: account has no family cloud; create one in the 139 client first")
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
			return "", fmt.Errorf("yun139: account belongs to %d family clouds: %s; set --yun139-family-id to the one you want", len(clouds), strings.Join(ids, ", "))
		}
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
		Path               string `json:"path"`
		CloudCatalogList   []struct {
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

// ------------------------------------------------------------ NewFs -------

// parsePath parses a remote path
func parsePath(p string) (root string) {
	return strings.Trim(p, "/")
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
		httpClient: fshttp.NewClient(ctx),
		space: opt.Space,
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

	// Auto-discover the userDomainId (the <digits> id batchCopy and
	// quota need) from queryFamilyCloud. The official client caches
	// this from login; we learn it in one extra request. An explicit
	// --yun139-user-domain-id still wins.
	if f.userDomainID == "" {
		if _, accountUserID, err := f.queryFamilyCloud(ctx); err == nil && accountUserID != "" {
			f.userDomainID = accountUserID
		}
		// A failure here is not fatal: personal-space operations
		// that do not need the domain id keep working (the quota
		// call falls back to the phone number).
	}

	rootID := f.opt.RootFolderID
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
// family listing exposes no hash; Hash returns "" there and rclone
// falls back to size+modtime comparison.
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.SHA256) }

// DirCacheFlush resets the directory cache
func (f *Fs) DirCacheFlush() { f.dirCache.ResetRoot() }

// ------------------------------------------------------------ listing -----

// listEntry is one entry of a directory listing, normalised across spaces.
type listEntry struct {
	id       string
	name     string
	size     int64
	isDir    bool
	modTime  time.Time
	srvPath  string // family/group server path of the parent dir
	sha256   string // personal space: contentHash from the listing (empty if absent)
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
			if item.FileId == "" {
				continue
			}
			n++
			modTime, _ := api.ParseRFC3339(item.UpdatedAt)
			if modTime.IsZero() {
				modTime, _ = api.ParseRFC3339(item.CreatedAt)
			}
			entry := listEntry{
				id:      item.FileId,
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
			"catalogType": 3,
			"cloudID":     f.opt.FamilyID,
			"docLibName":  leaf,
			"manualRename": 0,
			"path":        srvPath,
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
			FileId string `json:"fileId"`
		} `json:"data"`
	}
	if err := f.personalCall(ctx, "/hcy/file/create", body, &resp); err != nil {
		return "", err
	}
	if resp.Data.FileId == "" {
		// force_rename may have renamed; find by listing.
		return f.findDirID(ctx, pathID, leaf)
	}
	return resp.Data.FileId, nil
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
	if o.sha256 != "" {
		return o.sha256, nil
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
				o.sha256 = found
				return found, nil
			}
		}
	}
	return "", nil
}

// readMetaData reads the object metadata from its parent directory listing.
func (o *Object) readMetaData(ctx context.Context) error {
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			return fs.ErrorObjectNotFound
		}
		return err
	}
	var found *listEntry
	err = o.fs.listAll(ctx, dirID, func(e listEntry) bool {
		if strings.EqualFold(e.name, leaf) {
			found = &e
			return true
		}
		return false
	})
	if err != nil {
		return err
	}
	if found == nil || found.isDir {
		return fs.ErrorObjectNotFound
	}
	o.id = found.id
	o.size = found.size
	o.modTime = found.modTime
	o.serverPath = found.srvPath
	return nil
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
	fileID  string
	fileName string // server-side name after auto_rename
	hashHex string
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
	leaf, dirID, err := f.dirCache.FindPath(ctx, src.Remote(), true)
	if err != nil {
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
func buildCreateBody(dirID, leaf string, size int64, hashHex string, partInfos []api.PartInfo) api.PersonalCreateReq {
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
	for i, p := range parts {
		pi := api.PartInfo{
			PartNumber: p.index,
			PartSize:   p.partSize,
		}
		// The official client always sets partOffset (we verified on
		// 2026-09-03 with mCloudDownload.zip) even on the first part;
		// only the h field is omitted for part 1.
		pi.ParallelHashCtx = &api.ParallelHashCtx{PartOffset: p.offset}
		if i > 0 {
			regs, _, err := api.Sha256Midstate(sha256MidstateHash(freader, p.offset))
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
	body := buildCreateBody(dirID, leaf, size, hashHex, partInfos)
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
			return nil, fmt.Errorf("create: %w", err)
		}
	} else {
		if err := f.personalCall(ctx, callPath, callBody, &resp); err != nil {
			return nil, fmt.Errorf("create: %w", err)
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
		return nil, &apiError{Code: resp.Code, Message: resp.Message}
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
			return nil, &apiError{Code: urlResp.Code, Message: urlResp.Message}
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
	var eg errgroup.Group
	eg.SetLimit(f.opt.UploadConcurrency)
	for _, p := range parts {
		p := p
		eg.Go(func() error {
			url, ok := urlOfPart[int(p.index)]
			if !ok {
				return fmt.Errorf("yun139: no upload URL for part %d", p.index)
			}
			rdr := io.NewSectionReader(freader, p.offset, p.partSize)
			return f.putPart(ctx, rdr, url, p.partSize)
		})
	}
	if err := eg.Wait(); err != nil {
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
		return nil, fmt.Errorf("complete: %w", err)
	}
	if !cmplResp.Success {
		return nil, &apiError{Code: cmplResp.Code, Message: cmplResp.Message}
	}
	return &uploadResult{fileID: resp.Data.FileID, fileName: leaf, hashHex: hashHex}, nil
}

// putPart PUTs a single part to its pre-signed URL.
func (f *Fs) putPart(ctx context.Context, r io.Reader, url string, size int64) error {
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
	res, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode >= 300 {
		body, _ := io.ReadAll(res.Body)
		// 5xx and 4xx are both treated as business errors; the part URL
		// is single-use so a retry on a transient 5xx would just hit the
		// same dead URL.
		err := fmt.Errorf("yun139: upload part: %s: %s", res.Status, truncate(string(body), 500))
		if res.StatusCode >= 400 && res.StatusCode < 500 {
			return fserrors.NoRetryError(err)
		}
		return err
	}
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
	leaf = o.fs.opt.Enc.FromStandardName(leaf)
	if o.fs.space == spaceFamily {
		// The family modifyContentInfo endpoint treats contentName as
		// the base name and re-appends the ORIGINAL extension, so a
		// temp-name + rename flow leaves files stuck on
		// "name.ext.rclone-tmp-XXXX" (verified live by the audit).
		// Instead: delete the old file first, then upload straight to
		// the target name (fileRenameMode=auto_rename guards the
		// tiny race window).
		if err := o.fs.deleteObject(ctx, o.id, o.serverPath, true); err != nil {
			return fserrors.NoLowLevelRetryError(fmt.Errorf("yun139: delete old object: %w", err))
		}
		res, err := o.fs.uploadFile(ctx, in, dirID, leaf, size)
		if err != nil {
			return err
		}
		o.id = res.fileID
		o.size = size
		o.modTime = src.ModTime(ctx)
		o.sha256 = res.hashHex
		o.fs.dirCache.FlushDir(path.Dir(o.remote))
		return nil
	}
	tempLeaf := leaf + ".rclone-tmp-" + random.String(8)
	res, err := o.fs.uploadFile(ctx, in, dirID, tempLeaf, size)
	if err != nil {
		return err
	}
	// The new file may have been auto-renamed on the server; for the
	// post-rename step we want to address the just-created file by id and
	// rename it to the target name. If the new id is empty (the upload did
	// not return a usable id), treat as failure.
	if res.fileID == "" {
		// The complete response did not echo the id; refresh the parent to
		// look up the new file id by name.
		found, err := o.fs.findByNameInDir(ctx, dirID, res.fileName)
		if err != nil {
			return err
		}
		if found == "" {
			return errors.New("yun139: cannot resolve the new file id after upload")
		}
		res.fileID = found
	}
	// Delete the old object.
	if err := o.fs.deleteObject(ctx, o.id, o.serverPath, o.fs.space == spaceFamily); err != nil {
		return fserrors.NoLowLevelRetryError(fmt.Errorf("yun139: delete old object: %w", err))
	}
	// Rename the new file to the target name.
	if err := o.fs.renameObject(ctx, res.fileID, leaf, dirID, o.fs.space == spaceFamily); err != nil {
		return fserrors.NoLowLevelRetryError(fmt.Errorf("yun139: rename temp object: %w", err))
	}
	// Refresh the receiver in place.
	o.id = res.fileID
	o.size = size
	o.modTime = src.ModTime(ctx)
	return nil
}

// findByNameInDir lists dirID and returns the first file id matching name.
func (f *Fs) findByNameInDir(ctx context.Context, dirID, name string) (string, error) {
	var id string
	err := f.listAll(ctx, dirID, func(e listEntry) bool {
		if !e.isDir && strings.EqualFold(e.name, name) {
			id = e.id
			return true
		}
		return false
	})
	return id, err
}

// deleteObject removes a single object by id.
//
// family=true uses the family batch-delete endpoint; false uses the personal
// /recyclebin/batchTrash (or /file/batchDelete if hard_delete is on).
func (f *Fs) deleteObject(ctx context.Context, id, srvPath string, family bool) error {
	if family {
		taskID, err := f.familyBatchOprTask(ctx, familyBatchReq{
			ContentList:       []string{id},
			DestCloudID:       "",    // delete - no dest
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
		err := f.personalCall(ctx, endpoint, api.PersonalTrashReq{FileIds: []string{id}, BusinessType: 0}, &out)
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
			FileId: id,
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
func (f *Fs) Move(ctx context.Context, src fs.Object, dst fs.Fs, dstDir string) error {
	if dst != f {
		return fs.ErrorCantMove
	}
	srcObj, ok := src.(*Object)
	if !ok {
		return fs.ErrorCantMove
	}
	dstDirID, _, err := f.dirCache.FindPath(ctx, path.Join(f.root, dstDir), true)
	if err != nil {
		return err
	}
	if f.space == spaceFamily {
		// The PC client no longer has a "move" button inside the family
		// cloud (transfers are copy + manual delete). rclone's Move
		// falls back to "copy then delete" which the server surfaces
		// as taskType 1 (copy) followed by taskType 2 (delete).
		if err := f.familyCopy(ctx, srcObj, dstDirID); err != nil {
			return err
		}
		return f.deleteObject(ctx, srcObj.id, srcObj.serverPath, true)
	}
	taskID, err := f.moveTaskID(ctx, srcObj.id, dstDirID)
	if err != nil {
		return err
	}
	return f.taskGet(ctx, taskID, "move")
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
			FileIds:        []string{id},
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
	if !ok || srcFs != f {
		return fs.ErrorCantDirMove
	}
	srcID, _, err := f.dirCache.FindPath(ctx, path.Join(f.root, srcRemote), false)
	if err != nil {
		return err
	}
	dstID, _, err := f.dirCache.FindPath(ctx, path.Join(f.root, dstRemote), true)
	if err != nil {
		return err
	}
	if f.space == spaceFamily {
		// No native family move - fall back to copy+delete.
		if err := f.familyCopyID(ctx, srcID, dstID, true /*isDir*/); err != nil {
			return err
		}
		return f.familyDeleteID(ctx, srcID, f.familySrvPath(srcID))
	}
	taskID, err := f.moveTaskID(ctx, srcID, dstID)
	if err != nil {
		return err
	}
	return f.taskGet(ctx, taskID, "dirmove")
}

// Copy a file
func (f *Fs) Copy(ctx context.Context, src fs.Object, dst fs.Fs, dstDir string) error {
	if dst != f {
		return fs.ErrorCantCopy
	}
	srcObj, ok := src.(*Object)
	if !ok {
		return fs.ErrorCantCopy
	}
	dstDirID, _, err := f.dirCache.FindPath(ctx, path.Join(f.root, dstDir), true)
	if err != nil {
		return err
	}
	if f.space == spaceFamily {
		return f.familyCopy(ctx, srcObj, dstDirID)
	}
	taskID, err := f.copyTaskID(ctx, srcObj.id, dstDirID)
	if err != nil {
		return err
	}
	return f.taskGet(ctx, taskID, "copy")
}

// familyCopy copies srcObj (file or dir) into dstDirID inside the family
// cloud via createBatchOprTaskV2 taskType=1, then polls the task
// (captured 2026-09-03).
func (f *Fs) familyCopy(ctx context.Context, srcObj *Object, dstDirID string) error {
	return f.familyCopyID(ctx, srcObj.id, dstDirID, srcObj.isDir)
}

// familyCopyID copies the given content/catalog id into dstDirID.
func (f *Fs) familyCopyID(ctx context.Context, id, dstDirID string, isDir bool) error {
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
			FileIds:        []string{id},
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
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
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
	if _, err := w.tmp.WriteAt(buf[:m], offset); err != nil {
		return 0, err
	}
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
// sha256MidstateHash hashes the first n bytes of src through a fresh
// SHA-256 and returns the digest. Caller exports the midstate via
// api.Sha256Midstate. The hash is never re-used: each part of the file
// needs its own digest up to its starting offset.
func sha256MidstateHash(src io.ReaderAt, n int64) interface {
	Write(p []byte) (int, error)
	Sum(b []byte) []byte
} {
	h := sha256.New()
	// Copy in 1 MiB chunks. With 5 MiB parts and typical 64 KiB Go
	// buffer defaults this stays comfortably off the GC.
	buf := make([]byte, 1<<20)
	off := int64(0)
	for off < n {
		want := n - off
		if want > int64(len(buf)) {
			want = int64(len(buf))
		}
		nr, err := src.ReadAt(buf[:want], off)
		if nr > 0 {
			h.Write(buf[:nr])
		}
		if err != nil {
			break
		}
		off += int64(nr)
	}
	return h
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
	if resp.Data.CDNURL != "" && resp.Data.CDNSwitch {
		return resp.Data.CDNURL, nil
	}
	if resp.Data.URL == "" {
		return "", fmt.Errorf("yun139: no download URL returned for %q", o.remote)
	}
	return resp.Data.URL, nil
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
			"taskID":    taskID,
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
				TaskStatus   int `json:"taskStatus"`
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
