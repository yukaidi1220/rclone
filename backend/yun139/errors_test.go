package yun139

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestShouldRetry_TransientAPICodes pins which business codes go back through
// the pacer. The two halves both matter: 02010002 must retry (server-side
// "调用外部系统异常", observed once and then succeeding twice on the same
// directory), while 1809112500 must NOT, because that is the answer to a
// shared-group id sent to the family adapter - retrying a misconfiguration
// would only burn attempts. A table that only asserted the retry half would
// also pass if everything retried, so the non-retry rows are the point.
func TestShouldRetry_TransientAPICodes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"transient external-system error", &apiError{Code: "02010002", Message: "调用外部系统异常"}, true},
		{"transient code wrapped in context", fmt.Errorf("listing: %w", &apiError{Code: "02010002", Message: "调用外部系统异常"}), true},
		{"wrong space answers internal error", &apiError{Code: "1809112500", Message: "系统内部错误"}, false},
		{"family cloud missing", &apiError{Code: "1809012303", Message: "家庭云不存在"}, false},
		{"member tier too low", &apiError{Code: "04010319", Message: "权益不足"}, false},
		{"quota exhausted", &apiError{Code: "04000024", Message: "空间不足"}, false},
		{"plain transport error", errors.New("dial tcp: i/o timeout"), true},
		{"no error at all", nil, false},
	}
	for _, c := range cases {
		got, _ := shouldRetry(context.Background(), c.err)
		if got != c.want {
			t.Errorf("shouldRetry(%s) = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestShouldRetry_CancelledContextWins asserts that a cancelled context stops
// retrying even for a code that is on the transient list - otherwise a Ctrl-C
// during a walk keeps hammering the API.
func TestShouldRetry_CancelledContextWins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	retry, err := shouldRetry(ctx, &apiError{Code: "02010002", Message: "调用外部系统异常"})
	if retry {
		t.Errorf("a cancelled context must not schedule another attempt")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("shouldRetry lost the cancellation: got %v", err)
	}
}

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
