// probe is a standalone tool that talks to 139 cloud directly, so we can
// experiment with headers / client-info fields and see what the server
// reports back, without going through rclone.
//
// Usage:
//
//	go run ./cmd/probe -token "<Basic xxx...>" -action create-large
//	go run ./cmd/probe -token "<Basic xxx...>" -action probe-size
//
// It always uses our real backend code (probeHeaders) so the headers
// match the backend's newHeaders/pcHeaders output exactly.
package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rclone/rclone/backend/yun139/api"
)

var (
	flagToken   = flag.String("token", "", "Basic <base64> authorization")
	flagAction  = flag.String("action", "probe-size", "probe-size|create-large|pc-test|web-test")
	flagSize    = flag.Int64("size", 6*1024*1024*1024, "file size in bytes for create-large")
	flagName    = flag.String("name", "rclone-probe.bin", "filename for create-large")
	flagParent  = flag.String("parent", "", "parent dir id (default: cloud root)")
	flagHeaders = flag.Bool("headers", true, "dump outgoing headers to stderr")
)

func main() {
	flag.Parse()
	if *flagToken == "" {
		fmt.Fprintln(os.Stderr, "need -token")
		os.Exit(2)
	}

	auth := strings.TrimPrefix(*flagToken, "Basic ")
	auth = strings.TrimSpace(auth)
	raw, err := base64.StdEncoding.DecodeString(auth)
	must(err)
	parts := strings.SplitN(string(raw), ":", 3)
	if len(parts) != 3 {
		fatalf("token must decode to pc:account:token|..., got %q", string(raw))
	}
	account := parts[1]
	fmt.Fprintf(os.Stderr, "account = %s\n", account)

	ctx := context.Background()
	switch *flagAction {
	case "probe-size":
		probeSizeLimit(ctx, account, auth)
	case "pc-test":
		probePersonalClientInfo(ctx, account, auth, true)
	case "web-test":
		probePersonalClientInfo(ctx, account, auth, false)
	case "create-large":
		createLarge(ctx, account, auth)
	default:
		fatalf("unknown action %q", *flagAction)
	}
}

// probeSizeLimit lists the root of the personal cloud and prints the
// account-level quota. 139 returns a single number that we compare to
// 5 GB to figure out what the per-file cap actually is.
func probeSizeLimit(ctx context.Context, account, auth string) {
	_, err := ensurePersonalHost(ctx, account, auth)
	must(err)

	// The quota endpoint differs across versions; try the common ones.
	quotaPaths := []string{
		"/disk/v1/user/diskCapacity",
		"/disk/v1/user/getDiskInfo",
		"/orchestration/personalCloud-rebuild/user/v1.0/getUserInfo",
	}
	for _, p := range quotaPaths {
		var body map[string]any
		err := callPersonal(ctx, account, auth, "POST", p, map[string]any{}, &body, false)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s -> err: %v\n", p, err)
			continue
		}
		fmt.Fprintf(os.Stderr, "%s -> OK: %s\n", p, shortJSON(body))
	}
}

// probePersonalClientInfo asks /file/list and inspects the response to
// see whether the server includes any hint about the per-file cap.
func probePersonalClientInfo(ctx context.Context, account, auth string, usePC bool) {
	_, err := ensurePersonalHost(ctx, account, auth)
	must(err)
	var list map[string]any
	err = callPersonal(ctx, account, auth, "POST", "/file/list", map[string]any{
		"parentFileId": *flagParent,
		"pageNum":      1,
		"pageSize":     10,
	}, &list, usePC)
	must(err)
	fmt.Printf("=== %s mode, /file/list result ===\n", ifPC(usePC, "PC", "web"))
	pretty(list)
}

// createLarge fakes a /file/create with a chosen size and SHA-256 to
// see whether the server accepts the request, what fileId/uploadId it
// returns, and whether partInfos has 1 or 100 entries.
func createLarge(ctx context.Context, account, auth string) {
	_, err := ensurePersonalHost(ctx, account, auth)
	must(err)

	parent := *flagParent
	if parent == "" {
		parent = "root"
	}

	// Fake SHA-256: all-zeros is fine; the server is happy to allocate
	// part URLs even when the file does not yet exist.
	sum := md5.Sum([]byte("probe:" + strconv.FormatInt(time.Now().UnixNano(), 10)))
	fakeHash := hex.EncodeToString(sum[:])

	body := map[string]any{
		"parentFileId":   parent,
		"name":           *flagName,
		"sha256":         fakeHash,
		"size":           *flagSize,
		"md5":            "",
		"fileRenameMode": "auto_rename",
		"rapidUpload":    true,
	}
	fmt.Fprintf(os.Stderr, "create-large: size=%d, sha256=%s\n", *flagSize, fakeHash)

	// Try once with PC headers, once with web headers.
	for _, usePC := range []bool{true, false} {
		fmt.Printf("\n=== create-large / %s mode ===\n", ifPC(usePC, "PC", "web"))
		var resp map[string]any
		err := callPersonal(ctx, account, auth, "POST", "/file/create", body, &resp, usePC)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			continue
		}
		pretty(resp)
		// If we got an uploadId + partInfos, we can stop here.
		if data, ok := resp["data"].(map[string]any); ok {
			if _, ok := data["uploadId"]; ok {
				fmt.Printf("=> ACCEPTED in %s mode\n", ifPC(usePC, "PC", "web"))
				return
			}
		}
	}
}

// ---- HTTP plumbing -------------------------------------------------------

var httpClient = &http.Client{Timeout: 30 * time.Second}

func callPersonal(ctx context.Context, account, auth, method, path string, body any, out any, usePC bool) error {
	host, err := ensurePersonalHost(ctx, account, auth)
	if err != nil {
		return err
	}
	ts := time.Now().Format("2006-01-02 15:04:05")
	randStr := randHex(16)
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	sign := api.Sign(string(payload), ts, randStr)
	var hdr map[string]string
	if usePC {
		hdr = pcHeaders(auth, account, ts, randStr, sign, "1")
	} else {
		hdr = webHeaders(auth, ts, randStr, sign, "1")
	}
	return doRequest(ctx, method, host+path, payload, hdr, out)
}

func ensurePersonalHost(ctx context.Context, account, auth string) (string, error) {
	if *flagParent == "host=" || *flagParent != "" && strings.HasPrefix(*flagParent, "host=") {
		// "host=foo:bar" forces a host override for tests
		return strings.TrimPrefix(*flagParent, "host="), nil
	}
	body := map[string]any{
		"commonAccountInfo": map[string]any{
			"account":     account,
			"accountType": 1,
		},
	}
	payload, _ := json.Marshal(body)
	ts := time.Now().Format("2006-01-02 15:04:05")
	randStr := randHex(16)
	sign := api.Sign(string(payload), ts, randStr)
	hdr := webHeaders(auth, ts, randStr, sign, "1")

	// Decode raw body to find the host field name.
	var raw struct {
		Data struct {
			RoutePolicyList []struct {
				HTTPURL  string `json:"httpUrl"`
				HTTPSURL string `json:"httpsUrl"`
			} `json:"routePolicyList"`
		} `json:"data"`
	}
	if err := doRequest(ctx, "POST", "https://user-njs.yun.139.com/user/route/qryRoutePolicy", payload, hdr, &raw); err != nil {
		return "", err
	}
	for _, p := range raw.Data.RoutePolicyList {
		if p.HTTPSURL != "" {
			return p.HTTPSURL, nil
		}
		if p.HTTPURL != "" {
			return p.HTTPURL, nil
		}
	}
	b, _ := json.Marshal(raw)
	return "", fmt.Errorf("no host in route response: %s", truncate(string(b), 300))
}

func doRequest(ctx context.Context, method, fullURL string, body []byte, hdr map[string]string, out any) error {
	u, err := url.Parse(fullURL)
	if err != nil {
		return err
	}
	if *flagHeaders {
		fmt.Fprintf(os.Stderr, "\n>>> %s %s\n", method, u.Host+u.Path)
		for k, v := range hdr {
			if k == "Authorization" {
				fmt.Fprintf(os.Stderr, "    %s: Basic %s...\n", k, truncate(v, 30))
				continue
			}
			fmt.Fprintf(os.Stderr, "    %s: %s\n", k, v)
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, fullURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	resBody, _ := io.ReadAll(res.Body)
	if *flagHeaders {
		fmt.Fprintf(os.Stderr, "<<< %s %d\n", method, res.StatusCode)
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("HTTP %d: %s", res.StatusCode, truncate(string(resBody), 200))
	}
	if out == nil {
		fmt.Printf("body: %s\n", truncate(string(resBody), 500))
		return nil
	}
	return json.Unmarshal(resBody, out)
}

// ---- headers (must match backend/yun139/yun139.go) -----------------------

const (
	deviceInfoWeb = "||9|7.14.0|chrome|120.0.0.0|||windows 10||zh-CN|||"
)

func webHeaders(auth, ts, randStr, sign, svcType string) map[string]string {
	h := map[string]string{
		"Accept":                 "application/json, text/plain, */*",
		"Caller":                 "web",
		"CMS-DEVICE":             "default",
		"Mcloud-Channel":         "1000101",
		"Mcloud-Client":          "10701",
		"Mcloud-Route":           "001",
		"Mcloud-Sign":            ts + "," + randStr + "," + sign,
		"Mcloud-Version":         "7.14.0",
		"x-DeviceInfo":           deviceInfoWeb,
		"x-huawei-channelSrc":    "10000034",
		"x-inner-ntwk":           "2",
		"x-m4c-caller":           "PC",
		"x-m4c-src":              "10002",
		"x-SvcType":              svcType,
		"X-Yun-Api-Version":      "v1",
		"X-Yun-App-Channel":      "10000034",
		"X-Yun-Channel-Source":   "10000034",
		"X-Yun-Client-Info":      deviceInfoWeb + "dW5kZWZpbmVk||",
		"X-Yun-Module-Type":      "100",
		"X-Yun-Svc-Type":         svcType,
		"User-Agent":             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"Inner-Hcy-Router-Https": "1",
		"Content-Type":           "application/json",
	}
	h["Authorization"] = "Basic " + auth
	return h
}

func pcHeaders(auth, account, ts, randStr, sign, svcType string) map[string]string {
	deviceID := "OPENLIST" + strings.ToUpper(md5Hex(account)[:16]) + "-PC"
	pcDeviceInfo := "||11|8.7.2.20260519|PC|QkYtMjAyMDAzMTAxNjQ3|" + deviceID +
		"|| Windows 10 (10.0)|1920X1040|Q2hpbmVzZSAoU2ltcGxpZmllZCk=|||"
	h := webHeaders(auth, ts, randStr, sign, svcType)
	h["x-DeviceInfo"] = pcDeviceInfo
	h["x-huawei-channelSrc"] = "10200153"
	h["x-MM-Source"] = "000"
	h["x-yun-api-version"] = "v1"
	h["x-yun-app-channel"] = "10200153"
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

func md5Hex(s string) string {
	sum := md5.Sum([]byte(s))
	return hex.EncodeToString(sum[:])
}

func randHex(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + (time.Now().UnixNano()+int64(i))%16)
	}
	return string(b)
}

// ---- helpers ------------------------------------------------------------

func must(err error) {
	if err != nil {
		fatalf("%v", err)
	}
}

func fatalf(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", a...)
	os.Exit(1)
}

func ifPC(b bool, a, c string) string {
	if b {
		return a
	}
	return c
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func shortJSON(v any) string {
	b, _ := json.Marshal(v)
	return truncate(string(b), 200)
}

func pretty(v any) {
	b, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(b))
}
