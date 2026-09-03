package yun139

import (
	"errors"
	"testing"
)

// TestAPIError_Message exercises the apiError type returned by upload
// /file/create and /file/complete on server-side rejection. The live
// 04010319 (权益不足) was being swallowed by fmt.Errorf and shown as
// 'create failed: 04010319 权益不足' - same info, but a typed error
// lets future callers (verify, retry) inspect the code without
// parsing strings.
func TestAPIError_Message(t *testing.T) {
	e := &apiError{Code: "04010319", Message: "权益不足"}
	if got := e.Error(); got != "yun139: API error 04010319: 权益不足" {
		t.Errorf("Error() = %q, want %q", got, "yun139: API error 04010319: 权益不足")
	}
	// errors.As / errors.Is round-trip
	var ae *apiError
	if !errors.As(e, &ae) {
		t.Fatal("errors.As failed to extract *apiError")
	}
	if ae.Code != "04010319" || ae.Message != "权益不足" {
		t.Errorf("round-tripped apiError = %+v, want Code=04010319 Message=权益不足", ae)
	}
}

// TestAPIError_OtherCodes covers the other business codes seen live:
// 04000002 (空内容) and the generic 1xxxx flow-error.
func TestAPIError_OtherCodes(t *testing.T) {
	cases := []struct {
		code, msg string
	}{
		{"04000002", "文件类型不允许为空"},
		{"04000002", "上传id不允许为空"},
		{"1001", "token expired"},
	}
	for _, c := range cases {
		e := &apiError{Code: c.code, Message: c.msg}
		got := e.Error()
		if got == "" {
			t.Errorf("Error() empty for %+v", c)
		}
	}
}