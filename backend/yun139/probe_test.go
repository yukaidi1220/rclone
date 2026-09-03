package yun139

import (
	"strings"
	"testing"

	"github.com/rclone/rclone/backend/yun139/api"
)

// TestPersonalCall_HeaderShape drives the header constructors directly
// (the same path the request pipeline takes) and asserts every header
// 139's PersonalNew API requires. PC-client masquerade is critical for
// /file/create to honour SHA-256 秒传, so we pin those headers too.
func TestPersonalCall_HeaderShape(t *testing.T) {
	ts := "1700000000000"
	randStr := "abcd1234abcd1234abcd1234abcd1234"
	sign := api.Sign(`{"k":1}`, ts, randStr)
	auth := "Basic " + encodeB64("pc:13800138000:TestToken|fake")

	h := pcHeaders(auth, "13800138000", ts, randStr, sign, "1")

	mustHeaderContains := []string{
		"Accept",
		"Authorization",
		"Mcloud-Sign",
		"x-SvcType",
		"x-yun-device-id",
		"x-yun-app-channel",
		"x-m4c-caller",
		"User-Agent",
		"Content-Type",
	}
	for _, k := range mustHeaderContains {
		if h[k] == "" {
			t.Errorf("pcHeaders missing %q", k)
		}
	}
	if !strings.Contains(h["Mcloud-Sign"], ts) {
		t.Errorf("Mcloud-Sign %q does not contain ts %q", h["Mcloud-Sign"], ts)
	}
	if !strings.Contains(h["Mcloud-Sign"], sign) {
		t.Errorf("Mcloud-Sign %q does not contain sign %q", h["Mcloud-Sign"], sign)
	}
	if h["x-SvcType"] != "1" {
		t.Errorf("x-SvcType = %q, want 1", h["x-SvcType"])
	}
	if !strings.HasPrefix(h["Authorization"], "Basic ") {
		t.Errorf("Authorization = %q, want Basic prefix", h["Authorization"])
	}
	if !strings.HasPrefix(h["x-yun-device-id"], "OPENLIST") {
		t.Errorf("x-yun-device-id = %q, want OPENLIST prefix", h["x-yun-device-id"])
	}
	// PC masquerade: the official PC client uses x-yun-app-channel=10200153
	// and Caller=PC.
	if h["x-yun-app-channel"] != pcAppChannel {
		t.Errorf("x-yun-app-channel = %q, want %q", h["x-yun-app-channel"], pcAppChannel)
	}
	if h["x-m4c-caller"] != "PC" {
		t.Errorf("x-m4c-caller = %q, want PC", h["x-m4c-caller"])
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
