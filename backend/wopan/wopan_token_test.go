package wopan

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/rclone/rclone/backend/wopan/api"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testFreshAT = "ffffffffffffffffffffffffffffffff"
	testFreshRT = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

// envelopeRequest pulls the method and the decrypted param out of a dispatcher
// request so a test can assert on what the caller actually sent.
func envelopeRequest(r *http.Request) (method string, param []byte, err error) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return "", nil, err
	}
	var env struct {
		Header struct {
			Key string `json:"key"`
		} `json:"header"`
		Body struct {
			Param string `json:"param"`
		} `json:"body"`
	}
	if err = json.Unmarshal(raw, &env); err != nil {
		return "", nil, err
	}
	plain, err := aesDecrypt(env.Body.Param, aesKeyFor(chanAPIUser, ""))
	if err != nil {
		return "", nil, fmt.Errorf("decrypt param: %w", err)
	}
	return env.Header.Key, plain, nil
}

func envelopeResponse(body string) *http.Response {
	return &http.Response{
		StatusCode:    http.StatusOK,
		Status:        "200 OK",
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Header:        http.Header{"Content-Type": []string{"application/json"}},
	}
}

func business(code, desc, data string) string {
	return `{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"` + code +
		`","RSP_DESC":"` + desc + `","DATA":` + data + `}}`
}

// rejectingTransport answers AppQueryUser with 1001 and AppRefreshToken with a
// caller-chosen business code, counting every request.
type rejectingTransport struct {
	mu             sync.Mutex
	refreshCode    string
	refreshes      int
	queryUserCalls int
}

func (t *rejectingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	method, _, err := envelopeRequest(r)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch method {
	case "AppRefreshToken":
		t.refreshes++
		return envelopeResponse(business(t.refreshCode, "登录失败", `""`)), nil
	default: // AppQueryUser and anything else: the token is not accepted
		t.queryUserCalls++
		return envelopeResponse(business(codeAuthInvalid, "无效的令牌", `""`)), nil
	}
}

// TestUnitRefreshFailureIsNoRetry locks the retry contract for a rejected
// refresh_token. A dead refresh_token answers RSP_CODE=8005 登录失败, which
// never turns valid on retry, yet before this was classified the error was
// wrapped with two %w - and asAPIError only walks single-Unwrap chains - so the
// pacer treated it as retryable and one `rclone lsf` fired ten failed logins
// against the shared account in five seconds (ten distinct LOGIDs observed on
// the live account on 2026-10-09).
func TestUnitRefreshFailureIsNoRetry(t *testing.T) {
	tr := &rejectingTransport{refreshCode: "8005"}
	f := newUnitTestFs(tr)

	_, err := f.queryUserID(context.Background())
	require.Error(t, err)
	assert.True(t, fserrors.IsNoRetryError(err), "a rejected refresh_token must be terminal, got: %v", err)

	retry, _ := shouldRetryCall(context.Background(), err)
	assert.False(t, retry, "shouldRetryCall must not retry a rejected refresh")

	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Equal(t, 1, tr.refreshes, "换票失败后不得再打第二次登录")
	assert.Equal(t, 1, tr.queryUserCalls, "一次失败就应当结束，不是 low-level-retries 次")
}

// recordingTransport is the probe for which access token a call actually
// carries: AppQueryUser is rejected while the body still holds the stale token
// and succeeds once it holds the one minted by AppRefreshToken.
type recordingTransport struct {
	mu        sync.Mutex
	sentAT    []string
	refreshes int
}

func (t *recordingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	method, param, err := envelopeRequest(r)
	if err != nil {
		return nil, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	switch method {
	case "AppRefreshToken":
		t.refreshes++
		return envelopeResponse(business("0000", "ok",
			`{"access_token":"`+testFreshAT+`","refresh_token":"`+testFreshRT+
				`","token_type":"bearer","expires_in":604799}`)), nil
	default:
		var q api.QueryUserRequest
		if err := json.Unmarshal(param, &q); err != nil {
			return nil, err
		}
		t.sentAT = append(t.sentAT, q.AccessToken)
		if q.AccessToken == testFreshAT {
			return envelopeResponse(business("0000", "ok", `{"userId":"u-1","userName":"tester"}`)), nil
		}
		return envelopeResponse(business(codeAuthInvalid, "无效的令牌", `""`)), nil
	}
}

// TestUnitQueryUserSendsRefreshedToken is the regression lock for the failure
// that motivated this round: AppRefreshToken succeeded and the rotated pair was
// even written back to the config, yet the run still died with RSP_CODE=1001 -
// because queryUserID built its param once, before the refresh, and the retried
// request therefore kept asking the server about the token that had just been
// invalidated. "刷出来了传不上去" is this bug, not a token problem.
func TestUnitQueryUserSendsRefreshedToken(t *testing.T) {
	tr := &recordingTransport{}
	f := newUnitTestFs(tr)

	userID, err := f.queryUserID(context.Background())
	require.NoError(t, err, "换票成功后必须用新令牌重试到通过")
	assert.Equal(t, "u-1", userID)

	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Equal(t, 1, tr.refreshes, "换票只应发生一次")
	require.Len(t, tr.sentAT, 2, "一次带旧令牌、一次带新令牌")
	assert.Equal(t, testAccessToken, tr.sentAT[0], "首次应带配置里那枚")
	assert.Equal(t, testFreshAT, tr.sentAT[1], "刷新之后的重试必须带上换出来的那枚")
	assert.Equal(t, testFreshAT, f.tok.accessTokenNow(), "内存态也要跟上换出来的新令牌")
}
