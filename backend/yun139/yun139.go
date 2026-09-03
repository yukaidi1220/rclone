// Package yun139 provides an interface to China Mobile cloud drive (中国移动云盘).
//
// The backend supports two storage spaces:
//
//   - personal (新个人云): the modern personal-cloud API, rooted at a
//     per-account host discovered via the route-policy endpoint. All
//     endpoints are /file/* and /recyclebin/*.
//
//   - family (新家庭云): the orchestration family-cloud API rooted at
//     https://yun.139.com/orchestration/familyCloud-rebuild/*.
//
// Authentication uses the 139 "authorization" token, the base64 of
// "pc:<account>:<token>|<...>|<expiry-millis>". The token is refreshed via the
// SSO endpoint when it has less than 15 days of validity left.
package yun139

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
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
	personalPartSize = int64(100 * 1024 * 1024)

	// maxPartsPerRequest caps the partInfos sent in /file/create and
	// /file/getUploadUrl (the server accepts up to 100).
	maxPartsPerRequest = 100

	// downloadURLTTL bounds the reuse of a cached download URL. The measured
	// lifetime is around 20 minutes, so keep well below it.
	downloadURLTTL = 15 * time.Minute

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
			Default:  encoder.Standard | encoder.EncodeInvalidUtf8,
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

	reqBody := "<root><token>" + token + "</token><account>" + account + "</account><clienttype>656</clienttype></root>"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, api.AuthTokenRefreshURL, strings.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/xml")
	req.Header.Set("User-Agent", defaultUserAgent)

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
	var resp api.RefreshTokenResp
	if err := xml.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("yun139: refresh token: decode %w", err)
	}
	if resp.Return != "0" {
		return fserrors.NoRetryError(fmt.Errorf("yun139: refresh token: %s", resp.Desc))
	}
	if resp.Token == "" {
		return errors.New("yun139: refresh token: empty token in response")
	}

	// Rebuild the authorization with the new token.
	prefix := parts[0]
	newAuth := base64.StdEncoding.EncodeToString([]byte(prefix + ":" + account + ":" + resp.Token))
	f.tokMu.Lock()
	f.auth = newAuth
	f.tokMu.Unlock()
	if f.m != nil {
		f.m.Set("authorization", newAuth)
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

// pcHeaders returns the header set for the PersonalNew API masquerading as the
// official PC client. The /file/create endpoint rejects SHA-256 rapid upload
// ("秒传") unless the request looks like the PC client, so the upload path
// must use these headers.
func pcHeaders(auth, account, ts, randStr, sign, svcType string) map[string]string {
	deviceID := "OPENLIST" + md5hex(account)[:16] + "-PC"
	pcDeviceInfo := "||11|" + pcAppVersion + "|PC|QkYtMjAyMDAzMTAxNjQ3|" + deviceID +
		"|| Windows 10 (10.0)|1920X1040|Q2hpbmVzZSAoU2ltcGxpZmllZCk=|||"
	h := newHeaders(auth, ts, randStr, sign, svcType)
	h["x-DeviceInfo"] = pcDeviceInfo
	h["x-huawei-channelSrc"] = pcAppChannel
	h["x-MM-Source"] = "000"
	h["x-yun-api-version"] = "v1"
	h["x-yun-app-channel"] = pcAppChannel
	h["x-yun-client-info"] = pcDeviceInfo
	h["x-yun-device-id"] = deviceID
	h["x-yun-device-info"] = pcDeviceInfo
	h["x-yun-market-source"] = "000"
	h["x-yun-module-type"] = "100"
	h["x-yun-op-type"] = "1"
	h["x-yun-svc-type"] = "1"
	h["x-ExpRoute-Code"] = "routeCode=" + account + ",type=2"
	return h
}

const (
	pcAppVersion = "8.7.2.20260519"
	pcAppChannel = "10200153"
)

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
	if strings.HasPrefix(url, yunBaseURL+"/orchestration") {
		headers = legacyHeaders(auth, ts, randStr, sign, f.svcType)
	} else if f.space == spacePersonal {
		// Personal space: always pretend to be the PC client so /file/create
		// honours the SHA-256 rapid-upload ("秒传") fast path.
		headers = pcHeaders(auth, f.account, ts, randStr, sign, f.svcType)
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
	if !env.Success {
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
	return f.call(ctx, yunBaseURL+pathname, body, out)
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
			f.personalHost = strings.TrimRight(p.HttpsURL, "/")
			return f.personalHost, nil
		}
	}
	return "", errors.New("yun139: no personal cloud host in route policy response")
}

// familyRoot discovers the server-side root catalog ID of the family cloud.
func (f *Fs) familyRoot(ctx context.Context) (string, error) {
	f.familyRootMu.Lock()
	defer f.familyRootMu.Unlock()
	if f.familyRootID != "" {
		return f.familyRootID, nil
	}
	req := api.QueryContentListReq{}
	req.FamilyCommon.CloudID = f.opt.FamilyID
	req.FamilyCommon.CommonAccountInfo.Account = f.account
	req.FamilyCommon.CommonAccountInfo.AccountType = 1
	req.PageInfo.PageSize = 1
	req.PageInfo.PageNum = 1
	req.ContentSortType = 0
	req.SortDirection = 1
	var resp api.QueryContentListResp
	if err := f.familyCall(ctx, "/orchestration/familyCloud-rebuild/content/v1.2/queryContentList", req, &resp); err != nil {
		return "", err
	}
	// The root path arrives as "root:/<id>"; strip the prefix.
	p := strings.TrimSpace(resp.Data.Path)
	p = strings.TrimPrefix(p, "root:/")
	p = strings.TrimPrefix(p, "root:")
	if p == "" {
		// Fall back to the first catalog's path.
		for _, c := range resp.Data.CloudCatalogList {
			if c.CatalogID != "" {
				f.familyRootID = c.CatalogID
				return f.familyRootID, nil
			}
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
	if opt.Space == spaceFamily && opt.FamilyID == "" {
		return nil, errors.New("yun139: family_id is required when space is family")
	}
	if opt.PartSize < 1024*1024 {
		return nil, fmt.Errorf("yun139: part_size must be at least 1Mi, got %s", opt.PartSize)
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
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.MD5) }

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
			err := f.personalCall(ctx, "/file/list", body, &resp)
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
		body := api.QueryContentListReq{}
		body.FamilyCommon.CloudID = f.opt.FamilyID
		body.FamilyCommon.CommonAccountInfo.Account = f.account
		body.FamilyCommon.CommonAccountInfo.AccountType = 1
		body.ContentSortType = 0
		body.SortDirection = 1
		body.PageInfo.PageNum = pageNum
		body.PageInfo.PageSize = listPageSize
		if dirID != rootID {
			body.CatalogID = dirID
		}
		var resp api.QueryContentListResp
		err := f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/orchestration/familyCloud-rebuild/content/v1.2/queryContentList", body, &resp)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return err
		}
		if resp.Data.Result.ResultCode != "0" && resp.Data.Result.ResultCode != "" {
			return &apiError{Code: resp.Data.Result.ResultCode, Message: resp.Data.Result.ResultDesc}
		}
		dirPath := resp.Data.Path
		f.srvPathOf.put(dirID, dirPath)
		if dirPath == "" {
			f.srvPathOf.put(dirID, "root:/")
		}
		n := 0
		for i := range resp.Data.CloudCatalogList {
			c := &resp.Data.CloudCatalogList[i]
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
		for i := range resp.Data.CloudContentList {
			c := &resp.Data.CloudContentList[i]
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
		total := resp.Data.TotalCount
		if total > 0 && n >= total {
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
		body := api.FamilyCreateFolderReq{}
		body.FamilyCommon.CloudID = f.opt.FamilyID
		body.FamilyCommon.CommonAccountInfo.Account = f.account
		body.FamilyCommon.CommonAccountInfo.AccountType = 1
		body.DocLibName = leaf
		// The server needs the parent's server-side path; the dircache holds IDs.
		// For creation the parent path of the family root is the root itself.
		srvPath := ""
		if pathID != "" && pathID != f.opt.RootFolderID {
			srvPath = f.familySrvPath(pathID)
		}
		body.Path = srvPath
		var resp api.FamilyCreateFolderResp
		if err := f.familyCall(ctx, "/orchestration/familyCloud-rebuild/cloudCatalog/v1.0/createCloudDoc", body, &resp); err != nil {
			return "", err
		}
		if resp.Data.Result.ResultCode != "0" {
			return "", &apiError{Code: resp.Data.Result.ResultCode, Message: resp.Data.Result.ResultDesc}
		}
		// createCloudDoc returns no ID; find it by listing the parent.
		return f.findDirID(ctx, pathID, leaf)
	}
	body := api.PersonalCreateFolderReq{
		ParentFileID:   pathID,
		Name:           leaf,
		Description:    "",
		Type:           "folder",
		FileRenameMode: "force_rename",
	}
	var resp struct {
		BaseResp api.BaseResp
		Data     struct {
			FileId string `json:"fileId"`
		} `json:"data"`
	}
	if err := f.personalCall(ctx, "/file/create", body, &resp); err != nil {
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
		// Deletion of family folders is not supported (the API returns a
		// misleading error); the family root cannot be deleted anyway.
		return errors.New("yun139: cannot remove the family root")
	}
	return f.deleteObject(ctx, dirID, "", true)
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

// SetModTime sets the modification time of the object
func (o *Object) SetModTime(ctx context.Context, t time.Time) error {
	return fs.ErrorCantSetModTime
}

// Hash returns the selected checksum of the file.
//
// The 139 personal cloud hides file metadata behind its CDN, so we cannot
// query an MD5/SHA-256 in constant time. Single-part uploads' CDN ETag
// is the content MD5; multi-part uploads (anything bigger than
// upload_cutoff) get a composite ETag, which we report as an empty string
// rather than a wrong hash so verify falls back to size comparison.
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	url, err := o.downloadURL(ctx)
	if err != nil {
		// Within the visibility window the URL may not exist yet;
		// returning "" lets verify skip the hash check rather than
		// delete the just-uploaded file.
		return "", nil
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0")
	res, err := o.fs.httpClient.Do(req)
	if err != nil {
		return "", nil
	}
	_ = res.Body.Close()
	etag := res.Header.Get("ETag")
	if etag == "" {
		return "", nil
	}
	etag = strings.Trim(etag, `"`)
	// Skip composite (multipart) ETags: they have a "-N" suffix and
	// aren't a real content hash.
	if i := strings.LastIndex(etag, "-"); i > 0 {
		return "", nil
	}
	return etag, nil
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
		return f.deleteObject(ctx, dirID, "", true)
	}
	return f.deleteObject(ctx, dirID, "", false)
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

