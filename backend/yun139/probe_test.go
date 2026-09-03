package yun139

import (
	"strings"
	"testing"

	"github.com/rclone/rclone/backend/yun139/api"
)

// TestPersonalCall_HeaderShape pins the PC-client header set to the
// request captured from the official client (2026-09-03, v8.8.6.20260829):
// the native upload module sends a lean set on /hcy/file/create with
// NO mcloud-* headers and NO mcloud-sign.
func TestPersonalCall_HeaderShape(t *testing.T) {
	ts := "1700000000000"
	randStr := "abcd1234abcd1234abcd1234abcd1234"
	sign := api.Sign(`{"k":1}`, ts, randStr)
	auth := "Basic " + encodeB64("pc:13800138000:TestToken|fake")

	h := pcHeaders(auth, "13800138000", ts, randStr, sign, "1")

	mustHeaderContains := []string{
		"Accept",
		"Authorization",
		"Content-Type",
		"x-yun-api-version",
		"x-yun-app-channel",
		"x-yun-client-info",
		"x-yun-device-id",
		"x-yun-market-source",
		"x-yun-module-type",
		"x-yun-op-type",
		"x-yun-svc-type",
		"User-Agent",
	}
	for _, k := range mustHeaderContains {
		if h[k] == "" {
			t.Errorf("pcHeaders missing %q", k)
		}
	}
	// Lean set: the upload module does NOT send these.
	mustNotContain := []string{
		"Mcloud-Sign",
		"mcloud-sign",
		"x-SvcType",
		"x-m4c-caller",
		"x-DeviceInfo",
		"x-huawei-channelSrc",
		"Inner-Hcy-Router-Https",
		"Caller",
		"CMS-DEVICE",
	}
	for _, k := range mustNotContain {
		if h[k] != "" {
			t.Errorf("pcHeaders must NOT contain %q (official client omits it)", k)
		}
	}
	if h["x-yun-app-channel"] != "10200153" {
		t.Errorf("x-yun-app-channel = %q, want 10200153", h["x-yun-app-channel"])
	}
	if h["x-yun-market-source"] != "001" {
		t.Errorf("x-yun-market-source = %q, want 001 (captured value)", h["x-yun-market-source"])
	}
	if h["x-yun-svc-type"] != "1" {
		t.Errorf("x-yun-svc-type = %q, want 1", h["x-yun-svc-type"])
	}
	if !strings.HasPrefix(h["Authorization"], "Basic ") {
		t.Errorf("Authorization = %q, want Basic prefix", h["Authorization"])
	}
	if !strings.Contains(h["x-yun-client-info"], pcAppVersion) {
		t.Errorf("x-yun-client-info = %q, want version %s inside", h["x-yun-client-info"], pcAppVersion)
	}
	// Device id is md5(account)+"-ENDIN", matching the captured
	// "<32HEX>-ENDIN" shape.
	if !strings.HasSuffix(h["x-yun-device-id"], "-ENDIN") {
		t.Errorf("x-yun-device-id = %q, want *-ENDIN suffix", h["x-yun-device-id"])
	}
	// UA is the short native-module one on the upload path.
	if h["User-Agent"] != pcUserAgentShort {
		t.Errorf("User-Agent = %q, want %q", h["User-Agent"], pcUserAgentShort)
	}
}

// TestPCHeadersFull_Shape pins the Electron-main-process header set for
// general API calls (folder create, trash, list, family) - the one with
// APP_AUTH/APP_CP/CP_VERSION and the long UA.
func TestPCHeadersFull_Shape(t *testing.T) {
	auth := "Basic " + encodeB64("pc:13800138000:TestToken|fake")
	h := pcHeadersFull(auth, "13800138000", md5hex("13800138000")+"-ENDIN")

	for _, k := range []string{
		"APP_AUTH", "APP_CP", "CP_VERSION", "Accept", "Authorization",
		"Content-Type", "Sec-Fetch-Dest", "Sec-Fetch-Mode", "Sec-Fetch-Site",
		"User-Agent", "x-DeviceInfo", "x-ExpRoute-Code", "x-yun-api-version",
		"x-yun-app-channel", "x-yun-client-info", "x-yun-device-id",
		"x-yun-market-source", "x-yun-module-type", "x-yun-op-type", "x-yun-svc-type",
	} {
		if h[k] == "" {
			t.Errorf("pcHeadersFull missing %q", k)
		}
	}
	if h["APP_CP"] != "pc" {
		t.Errorf("APP_CP = %q, want pc", h["APP_CP"])
	}
	if h["User-Agent"] != pcUserAgentFull {
		t.Errorf("User-Agent = %q, want the Electron UA", h["User-Agent"])
	}
	if h["x-yun-app-channel"] != pcAppChannel {
		t.Errorf("x-yun-app-channel = %q, want %q", h["x-yun-app-channel"], pcAppChannel)
	}
}

// TestNewHeaders_Family covers the family-side header set. The orchestration
// API still expects svcType=2.
func TestNewHeaders_Family(t *testing.T) {
	ts := "1700000000000"
	randStr := "abcd1234abcd1234abcd1234abcd1234"
	sign := api.Sign(`{}`, ts, randStr)
	h := newHeaders("Basic xyz", ts, randStr, sign, "2")
	if h["x-SvcType"] != "2" {
		t.Errorf("x-SvcType = %q, want 2", h["x-SvcType"])
	}
	if h["X-Yun-Svc-Type"] != "2" {
		t.Errorf("X-Yun-Svc-Type = %q, want 2", h["X-Yun-Svc-Type"])
	}
}

// TestLegacyHeaders verifies the orchestration endpoint variant is separate
// from the personal new API and carries the x-SvcType contract.
func TestLegacyHeaders(t *testing.T) {
	ts := "1700000000000"
	randStr := "abcd1234abcd1234abcd1234abcd1234"
	sign := api.Sign(`{}`, ts, randStr)
	h := legacyHeaders("Basic xyz", ts, randStr, sign, "2")
	if h["x-SvcType"] != "2" {
		t.Errorf("legacy x-SvcType = %q, want 2", h["x-SvcType"])
	}
	if h["mcloud-sign"] == "" {
		t.Errorf("legacy mcloud-sign empty")
	}
}

// encodeB64 is a tiny helper to avoid pulling encoding/base64 into the
// probe test file's import list.
func encodeB64(s string) string {
	const tab = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	bs := []byte(s)
	var out strings.Builder
	for i := 0; i < len(bs); i += 3 {
		var b [3]byte
		n := copy(b[:], bs[i:])
		out.WriteByte(tab[b[0]>>2])
		out.WriteByte(tab[((b[0]&0x3)<<4)|(b[1]>>4)])
		if n > 1 {
			out.WriteByte(tab[((b[1]&0xf)<<2)|(b[2]>>6)])
		} else {
			out.WriteByte('=')
		}
		if n > 2 {
			out.WriteByte(tab[b[2]&0x3f])
		} else {
			out.WriteByte('=')
		}
	}
	return out.String()
}