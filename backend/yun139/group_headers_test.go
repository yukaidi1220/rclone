package yun139

import (
	"sync"
	"testing"
)

// newHeaderTestFs builds the minimal Fs the header builders read: account,
// token state and a learned userDomainId. No network.
func newHeaderTestFs() *Fs {
	return &Fs{
		space:        spaceGroup,
		auth:         "Zm9v",
		tokMu:        &sync.Mutex{},
		account:      "13900000000",
		userDomainID: "1000000000000000001",
		svcType:      svcTypeFamily,
	}
}

// TestGroupHeaders_URLTypeRidesTheNativeSet pins the split that decides which
// host the signed download link comes back on. x-yun-url-type: 3 must go with
// the native-module set (getGroupFileDownLoadURLV2 and /hcy/group/dynamic/*) and
// must NOT go on the isbo/openApi metadata calls. Keying the split on the
// adapter prefix instead of the endpoint puts the download call in the metadata
// branch, the header is dropped, and the link comes back on a bare
// *.cmecloud.cn EOS host rather than *.eos.yun.139.com (measured both ways
// 2026-10-19) - which silently breaks any rule that keys on the link host.
func TestGroupHeaders_URLTypeRidesTheNativeSet(t *testing.T) {
	f := newHeaderTestFs()
	cases := []struct {
		path     string
		wantType string
		native   bool
	}{
		{"/hcy/mutual/adapter/isbo/openApi/getGroupFileDownLoadURLV2", "3", true},
		{"/hcy/group/dynamic/file/create", "3", true},
		{"/hcy/group/dynamic/recyclebin/batchTrash", "3", true},
		{"/hcy/mutual/adapter/isbo/openApi/queryGroupContentListV2", "", false},
		{"/hcy/mutual/adapter/isbo/openApi/createGroupCatalog", "", false},
	}
	for _, c := range cases {
		h := f.groupHeaders(c.path, "ts", "rand", "sign")
		got, has := h["x-yun-url-type"]
		if c.wantType == "" {
			if has {
				t.Errorf("%s: metadata call carries x-yun-url-type=%q, want it absent", c.path, got)
			}
			continue
		}
		if got != c.wantType {
			t.Errorf("%s: x-yun-url-type = %q, want %q", c.path, got, c.wantType)
		}
	}
}

// TestGroupHeaders_SetsMatchTheCapture asserts the two header sets differ the
// way the PC client's two requesters differ: Electron main process (browser-like
// set, svc-type 1, no x-MM-Source) versus the native module (short UA, no
// app_cp, and on the download call svc-type 2 + x-MM-Source + x-deviceID).
func TestGroupHeaders_SetsMatchTheCapture(t *testing.T) {
	f := newHeaderTestFs()

	meta := f.groupHeaders("/hcy/mutual/adapter/isbo/openApi/queryGroupV2", "ts", "rand", "sign")
	if meta["APP_CP"] != "pc" || meta["CP_VERSION"] == "" {
		t.Errorf("metadata call lost the Electron set: app_cp=%q cp_version=%q", meta["APP_CP"], meta["CP_VERSION"])
	}
	if meta["User-Agent"] != pcUserAgentFull {
		t.Errorf("metadata call UA = %.40q, want the full mCloud UA", meta["User-Agent"])
	}
	if meta["x-yun-svc-type"] != "1" {
		t.Errorf("metadata call x-yun-svc-type = %q, want 1", meta["x-yun-svc-type"])
	}

	upload := f.groupHeaders("/hcy/group/dynamic/file/create", "ts", "rand", "sign")
	if upload["User-Agent"] != pcUserAgentShort {
		t.Errorf("upload UA = %.40q, want the native short UA", upload["User-Agent"])
	}
	if _, ok := upload["APP_CP"]; ok {
		t.Errorf("upload call wrongly carries the Electron set (app_cp present)")
	}
	if upload["x-MM-Source"] != "" {
		t.Errorf("upload call must not carry x-MM-Source, got %q", upload["x-MM-Source"])
	}

	link := f.groupHeaders("/hcy/mutual/adapter/isbo/openApi/getGroupFileDownLoadURLV2", "ts", "rand", "sign")
	if link["x-MM-Source"] != "001" || link["x-deviceID"] == "" || link["x-yun-svc-type"] != "2" {
		t.Errorf("download call lost the native extras: mm-source=%q deviceID=%q svc-type=%q",
			link["x-MM-Source"], link["x-deviceID"], link["x-yun-svc-type"])
	}
	if _, ok := link["APP_CP"]; ok {
		t.Errorf("download call wrongly carries the Electron set (app_cp present)")
	}

	// x-yun-uni is on every captured group request, on both sets.
	for name, h := range map[string]map[string]string{"meta": meta, "upload": upload, "link": link} {
		if h["x-yun-uni"] != f.userDomainID {
			t.Errorf("%s call: x-yun-uni = %q, want the userDomainId %q", name, h["x-yun-uni"], f.userDomainID)
		}
	}
}

// TestFamilyHeadersUnchanged guards the space split: a family remote must keep
// sending the lean set with x-yun-url-type on every call, because that is what
// every family measurement so far was made with.
func TestFamilyHeadersUnchanged(t *testing.T) {
	f := newHeaderTestFs()
	f.space = spaceFamily
	headers := pcHeaders(f.accessToken(), f.account, "ts", "rand", "sign", f.svcType)
	if _, ok := headers["APP_CP"]; ok {
		t.Fatal("pcHeaders must not carry the Electron set")
	}
	if headers["x-yun-svc-type"] != "1" {
		t.Errorf("family lean set svc-type = %q, want 1", headers["x-yun-svc-type"])
	}
}
