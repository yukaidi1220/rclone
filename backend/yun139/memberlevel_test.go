package yun139

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rclone/rclone/backend/yun139/api"
	"github.com/rclone/rclone/fs/fserrors"
)

// TestMaxFileSizeForLevel checks the tier -> single-file limit mapping,
// including fail-open on unrecognised tiers.
func TestMaxFileSizeForLevel(t *testing.T) {
	cases := []struct {
		code, name string
		want       int64
	}{
		{"1001", "白银会员", memberLimitSilver},
		{"1002", "黄金会员", memberLimitGold},
		{"1003", "钻石会员", memberLimitDiamond},
		// typeName takes priority; typeCode alone is a fallback
		{"1001", "", memberLimitSilver},
		{"1000", "", memberLimitNoMember},
		// unrecognised => 0 (no limit, fail-open)
		{"", "", 0},
		{"9999", "", 0},
		{"", "未知等级", 0},
	}
	for _, c := range cases {
		if got := maxFileSizeForLevel(c.code, c.name); got != c.want {
			t.Errorf("maxFileSizeForLevel(%q,%q) = %d, want %d", c.code, c.name, got, c.want)
		}
	}
}

// TestQueryMemberLevel verifies the vip userIdentity request shape and that a
// silver-tier response is decoded into type/typeName.
func TestQueryMemberLevel(t *testing.T) {
	var gotAuth, gotCT, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"resultCode":"0","resultDesc":"请求成功接收并处理","data":[{"type":"1001","typeName":"白银会员"}]}`))
	}))
	defer srv.Close()

	// point the package constant at the test server
	old := api.MemberLevelURL
	api.MemberLevelURL = srv.URL
	defer func() { api.MemberLevelURL = old }()

	f := &Fs{httpClient: srv.Client(), auth: "dG9rZW4=", account: "13800138000", ts: &tokenState{auth: "dG9rZW4="}}
	typ, name, err := f.queryMemberLevel(context.Background())
	if err != nil {
		t.Fatalf("queryMemberLevel: %v", err)
	}
	if gotAuth != "Basic dG9rZW4=" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Basic dG9rZW4=")
	}
	if !strings.Contains(gotCT, "application/json") {
		t.Errorf("Content-Type = %q, want json", gotCT)
	}
	var b map[string]any
	if err := json.Unmarshal([]byte(gotBody), &b); err != nil {
		t.Fatalf("body not json: %v (%q)", err, gotBody)
	}
	if _, ok := b["memberTypeList"]; !ok {
		t.Errorf("body missing memberTypeList: %q", gotBody)
	}
	if typ != "1001" || name != "白银会员" {
		t.Errorf("queryMemberLevel = (%q,%q), want (1001,白银会员)", typ, name)
	}
}

// TestQueryMemberLevel_NoMember verifies an empty data array maps to the 5G
// no-member limit (typeCode "1000"), so >5G files are skipped up front instead
// of wasting a full multi-part upload.
func TestQueryMemberLevel_NoMember(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"resultCode":"0","resultDesc":"请求成功接收并处理","data":[]}`))
	}))
	defer srv.Close()
	old := api.MemberLevelURL
	api.MemberLevelURL = srv.URL
	defer func() { api.MemberLevelURL = old }()

	f := &Fs{httpClient: srv.Client(), auth: "dG9rZW4=", account: "13800138000", ts: &tokenState{auth: "dG9rZW4="}}
	typ, name, err := f.queryMemberLevel(context.Background())
	if err != nil {
		t.Fatalf("queryMemberLevel: %v", err)
	}
	if typ != "1000" || name != "无会员" {
		t.Errorf("queryMemberLevel = (%q,%q), want (1000,无会员)", typ, name)
	}
	// end-to-end: empty data must yield the 5G no-member limit, not "no limit"
	if v := maxFileSizeForLevel(typ, name); v != memberLimitNoMember {
		t.Errorf("maxFileSizeForLevel(%q,%q) = %d, want %d (no member 5G)", typ, name, v, memberLimitNoMember)
	}
}

// TestQueryMemberLevel_FailOpen: a 500 keeps the probe non-fatal and the tier
// mapping stays 0 (no limit), so mounting is never blocked.
func TestQueryMemberLevel_FailOpen(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	old := api.MemberLevelURL
	api.MemberLevelURL = srv.URL
	defer func() { api.MemberLevelURL = old }()

	f := &Fs{httpClient: srv.Client(), auth: "dG9rZW4=", account: "13800138000", ts: &tokenState{auth: "dG9rZW4="}}
	_, _, err := f.queryMemberLevel(context.Background())
	if err == nil {
		t.Fatal("queryMemberLevel: want error on 500")
	}
	if v := maxFileSizeForLevel("", ""); v != 0 {
		t.Errorf("maxFileSizeForLevel(,) = %d, want 0 (fail-open)", v)
	}
}

// TestMaxFileSizeOverride: an explicit max_file_size always wins over the
// detected value.
func TestMaxFileSizeOverride(t *testing.T) {
	f := &Fs{opt: Options{MaxFileSize: 1 << 30}, memberMaxFileSize: memberLimitSilver}
	if got := f.maxFileSize(); got != 1<<30 {
		t.Errorf("maxFileSize() = %d, want %d", got, 1<<30)
	}
	f2 := &Fs{memberMaxFileSize: memberLimitSilver}
	if f2.maxFileSize() != memberLimitSilver {
		t.Errorf("maxFileSize() (detected) = %d, want %d", f2.maxFileSize(), memberLimitSilver)
	}
}

// TestUploadTooLarge_NoRetry: an oversized file is rejected before any
// create/putPart request is made, with a NoRetryError identifying the file.
func TestUploadTooLarge_NoRetry(t *testing.T) {
	var hits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := &Fs{httpClient: srv.Client(), opt: Options{MaxFileSize: 8 << 30}}
	// The size-over-limit check runs before any part is read, so a minimal
	// ReaderAt is enough (the oversized entry never reaches it).
	_, err := f.uploadFromRandom(context.Background(), strings.NewReader("unused"), "", "big.bin", int64(9<<30), "hash")
	if err == nil {
		t.Fatal("uploadFromRandom: want error for oversized file")
	}
	if !fserrors.IsNoRetryError(err) {
		t.Errorf("err not NoRetryError: %v", err)
	}
	if !strings.Contains(err.Error(), "big.bin") || !strings.Contains(err.Error(), "8") {
		t.Errorf("err missing path/limit: %v", err)
	}
	if hits != 0 {
		t.Errorf("create/putPart called %d times on oversized file, want 0", hits)
	}
}

// TestNoRetryOnMemberQuota_04010319: the server quota error is wrapped in a
// NoRetryError (even when the create path wraps it once more with %w); other
// codes pass through unchanged.
func TestNoRetryOnMemberQuota_04010319(t *testing.T) {
	quota := fmt.Errorf("create: %w", &apiError{Code: "04010319", Message: "权益不足"})
	if got := noRetryOnMemberQuota(quota); !fserrors.IsNoRetryError(got) {
		t.Errorf("04010319 wrapped => want NoRetryError, got %v", got)
	}
	// bare apiError with matching code
	if got := noRetryOnMemberQuota(&apiError{Code: "04010319", Message: "权益不足"}); !fserrors.IsNoRetryError(got) {
		t.Errorf("04010319 bare => want NoRetryError, got %v", got)
	}
	// other code passes through as non-NoRetry
	other := fmt.Errorf("create: %w", &apiError{Code: "04000002", Message: "文件类型不允许为空"})
	if got := noRetryOnMemberQuota(other); fserrors.IsNoRetryError(got) {
		t.Errorf("non-quota error => should pass through non-NoRetry, got %v", got)
	}
}

// TestErrTooLarge_NoRetry checks errTooLarge is a NoRetryError with a clear reason.
func TestErrTooLarge_NoRetry(t *testing.T) {
	err := errTooLarge("big.bin", 9<<30, memberLimitSilver, "白银会员")
	if !fserrors.IsNoRetryError(err) {
		t.Errorf("errTooLarge not NoRetryError: %v", err)
	}
	for _, want := range []string{"big.bin", "8", "白银会员", "exceeding"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("errTooLarge missing %q: %v", want, err)
		}
	}
}
