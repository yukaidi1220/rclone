package yun139

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/rclone/rclone/backend/yun139/api"
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
// host the signed download link comes back on. With url_type_header enabled,
// x-yun-url-type: 3 must go with the native-module set (getGroupFileDownLoadURLV2
// and /hcy/group/dynamic/*) and must NOT go on the isbo/openApi metadata calls.
// Keying the split on the adapter prefix instead of the endpoint puts the
// download call in the metadata branch, the header is dropped, and the link
// comes back on a bare *.cmecloud.cn EOS host rather than *.eos.yun.139.com
// (measured both ways 2026-10-19) - which silently breaks any rule that keys
// on the link host.
func TestGroupHeaders_URLTypeRidesTheNativeSet(t *testing.T) {
	f := newHeaderTestFs()
	f.opt.URLTypeHeader = true
	cases := []struct {
		path     string
		wantType string
	}{
		{"/hcy/mutual/adapter/isbo/openApi/getGroupFileDownLoadURLV2", "3"},
		{"/hcy/group/dynamic/file/create", "3"},
		{"/hcy/group/dynamic/recyclebin/batchTrash", "3"},
		{"/hcy/mutual/adapter/isbo/openApi/queryGroupContentListV2", ""},
		{"/hcy/mutual/adapter/isbo/openApi/createGroupCatalog", ""},
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

// TestGroupHeaders_URLTypeOffByDefault pins the default: without
// url_type_header the native-module set - the only set that carries
// x-yun-url-type at all - sends no such header.
func TestGroupHeaders_URLTypeOffByDefault(t *testing.T) {
	f := newHeaderTestFs()
	if f.opt.URLTypeHeader {
		t.Fatal("fixture must default url_type_header to false")
	}
	for _, p := range []string{
		"/hcy/mutual/adapter/isbo/openApi/getGroupFileDownLoadURLV2",
		"/hcy/group/dynamic/file/create",
	} {
		if got, has := f.groupHeaders(p, "ts", "rand", "sign")["x-yun-url-type"]; has {
			t.Errorf("%s: x-yun-url-type = %q, want it absent by default", p, got)
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
// using the lean set (no Electron app_cp, svc-type 1). x-yun-url-type rides on
// it only when url_type_header is enabled, gated in call() rather than here.
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

// headerRecorder is an http.RoundTripper that captures the request call()
// issues and answers it with a canned success envelope, so which headers a
// branch puts on the wire can be asserted without a server.
type headerRecorder struct {
	got http.Header
}

func (r *headerRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.got = req.Header.Clone()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"success":true,"code":"0"}`)),
	}, nil
}

// newCallTestFs returns an Fs whose requests go to rec. The URLs handed to
// call() keep the production hosts, because the branch selection keys on them.
func newCallTestFs(space string, rec *headerRecorder) *Fs {
	f := newHeaderTestFs()
	f.space = space
	f.svcType = svcTypePersonal
	if space != spacePersonal {
		f.svcType = svcTypeFamily
	}
	f.httpClient = &http.Client{Transport: rec}
	return f
}

// TestCall_URLTypeGatedOnFamily pins the family-space branch of call(): every
// family call carries x-yun-url-type: 3 only while url_type_header is on. The
// branch keys on the host alone (group.yun.139.com without space=group), so
// any family path lands in it.
func TestCall_URLTypeGatedOnFamily(t *testing.T) {
	for _, on := range []bool{false, true} {
		rec := &headerRecorder{}
		f := newCallTestFs(spaceFamily, rec)
		f.opt.URLTypeHeader = on
		err := f.call(context.Background(), api.FamilyBaseURL+"/hcy/family/adapter/andAlbum/openApi/queryFamilyCloud", map[string]any{}, nil)
		if err != nil {
			t.Fatalf("call: %v", err)
		}
		got := rec.got.Get("x-yun-url-type")
		if on && got != "3" {
			t.Errorf("url_type_header=true: x-yun-url-type = %q, want 3", got)
		}
		if !on && got != "" {
			t.Errorf("url_type_header=false: x-yun-url-type = %q, want it absent", got)
		}
	}
}

// TestCall_URLTypeGatedOnPersonalDownload pins the personal-space branch: the
// header rides the download-signing call only, and only while url_type_header
// is on - the create call that shares the lean header set never carries it.
// The host is a stand-in for whatever personalCloudHost resolved to; the
// branch keys on the path, not the host. The UA check pins both cases to the
// lean set, so a create whose body ever stops carrying partInfos fails here
// instead of silently falling through to the full set.
func TestCall_URLTypeGatedOnPersonalDownload(t *testing.T) {
	personalHost := "https://personal-njs.yun.139.com"
	for _, on := range []bool{false, true} {
		cases := []struct {
			path string
			body any
			want string
		}{
			{"/hcy/file/getDownloadUrl", map[string]any{"fileId": "1"}, "3"},
			{"/hcy/file/create", map[string]any{"partInfos": []any{}}, ""},
		}
		for _, c := range cases {
			want := c.want
			if !on {
				want = ""
			}
			rec := &headerRecorder{}
			f := newCallTestFs(spacePersonal, rec)
			f.opt.URLTypeHeader = on
			err := f.call(context.Background(), personalHost+c.path, c.body, nil)
			if err != nil {
				t.Fatalf("call %s: %v", c.path, err)
			}
			if got := rec.got.Get("User-Agent"); got != pcUserAgentShort {
				t.Errorf("url_type_header=%v %s: UA = %.40q, want the lean set's short UA", on, c.path, got)
			}
			if got := rec.got.Get("x-yun-url-type"); got != want {
				t.Errorf("url_type_header=%v %s: x-yun-url-type = %q, want %q", on, c.path, got, want)
			}
		}
	}
}
