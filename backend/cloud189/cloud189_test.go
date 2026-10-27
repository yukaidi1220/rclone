package cloud189

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloud189/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/require"
)

// The expectations in this file are literals, copied from the measurements in
// the capture notes (D:\天翼云抓包\189抓包接口清单.md), not computed from the
// code under test. Changing a rule here means the server's rule changed.

// TestChunkPlan pins the part-size ladder the server demands.
//
// Four of these points are measured directly against the live server by
// declaring a size and reading whether initMultiUpload accepted the part size:
// 20Mi -> 10Mi accepted / 20Mi refused, 2Gi -> 10Mi, 11Gi -> 20Mi accepted,
// 30Gi -> 50Mi accepted.
func TestChunkPlan(t *testing.T) {
	const (
		mib  int64 = 1024 * 1024
		gib        = 1024 * mib
		base       = 10 * mib
	)
	tests := []struct {
		name string
		size int64
		want int64
	}{{
		name: "empty",
		size: 0,
		want: base,
	}, {
		name: "one part",
		size: base,
		want: base,
	}, {
		name: "20Mi measured",
		size: 20 * mib,
		want: base,
	}, {
		name: "2Gi measured",
		size: 2 * gib,
		want: base,
	}, {
		// 999 parts of 10Mi is the last size that still uses one base, and the
		// byte past it is the first that does not.
		name: "top of the first step",
		size: 999 * base,
		want: base,
	}, {
		name: "one byte past it",
		size: 999*base + 1,
		want: 2 * base,
	}, {
		name: "11Gi measured",
		size: 11 * gib,
		want: 2 * base,
	}, {
		name: "top of the second step",
		size: 999 * 2 * base,
		want: 2 * base,
	}, {
		// The third step never drops below five bases, so the second step ends
		// at 1998 parts of one base, not at 1999.
		name: "one byte past that",
		size: 999*2*base + 1,
		want: 5 * base,
	}, {
		name: "30Gi measured",
		size: 30 * gib,
		want: 5 * base,
	}, {
		name: "1000Gi",
		size: 1000 * gib,
		want: 52 * base,
	}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.EqualValues(t, tc.want, chunkPlan(tc.size, base), "size %d", tc.size)
		})
	}
}

// TestChunkPlanPartCount asserts the invariant the ladder exists to hold: the
// part size it picks keeps the part count inside what the server accepts. This
// is checked against the function's own output over a sweep of sizes, so a
// ladder that miscomputes a step cannot pass the pinned table above by accident
// at the sizes nobody probed.
func TestChunkPlanPartCount(t *testing.T) {
	const base = int64(10 * 1024 * 1024)
	for _, size := range []int64{1, base - 1, base, base + 1, 999 * base, 999*base + 1,
		9986 * base, 9995 * base, 30 * 1024 * 1024 * 1024, 500 * 1024 * 1024 * 1024} {
		part := chunkPlan(size, base)
		parts := (size + part - 1) / part
		limit := int64(ladderPartsLow)
		if part > 2*base {
			limit = ladderPartsHigh
		}
		require.LessOrEqual(t, parts, limit, "size %d with part size %d", size, part)
		// And the step below would have overflowed the same limit, so the plan
		// is not needlessly generous with the day's transfer count.
		if parts > limit/2 && part > base {
			require.Greater(t, (size+part/2-1)/(part/2), limit, "size %d could use a smaller step", size)
		}
	}
}

// TestChunkPlanOtherBase covers the ladder with a non-default base, which is
// what the chunk_size option lets a user ask for.
func TestChunkPlanOtherBase(t *testing.T) {
	const base = int64(20 * 1024 * 1024)
	require.EqualValues(t, base, chunkPlan(11*1024*1024*1024, base))
	require.EqualValues(t, 2*base, chunkPlan(999*base+1, base))
	require.EqualValues(t, 5*base, chunkPlan(999*2*base+1, base))
}

// cloud189DamagedRunes is the hardcoded roster from the measured character
// table: the characters this server does not store as sent. Each one is either
// deleted or rewritten with an inserted backslash, with no error raised, so the
// original name cannot be recovered afterwards.
var cloud189DamagedRunes = []rune{'"', '*', ':', '<', '>', '?', '\\', '|', '\''}

// cloud189VerbatimRunes is the other half of the same table: characters the
// server stores exactly as received. Escaping any of these would invent a
// restriction the server does not have.
var cloud189VerbatimRunes = []rune{'#', '%', '&', '=', '+', ',', ';', '@', '(', ')', '[', ']', '{', '}', '$', '!', '^', '~'}

func TestDefaultEncodingEscapesExactlyTheDamagedSet(t *testing.T) {
	enc := cloud189DefaultEncoding

	for _, r := range cloud189DamagedRunes {
		wire := enc.FromStandardName("a" + string(r) + "b")
		require.NotEqual(t, "a"+string(r)+"b", wire, "character %q is stored damaged by the server, so it must not reach it verbatim", r)
		require.Equal(t, "a"+string(r)+"b", enc.ToStandardName(wire), "escaping %q must round-trip back", r)
	}

	// Two spot checks pin the form of the escape: the fullwidth substitute, not
	// a percent or a control-picture escape. These literals are what the server
	// was measured to store unchanged.
	require.Equal(t, "a＂b", enc.FromStandardName(`a"b`))
	require.Equal(t, "a＊b", enc.FromStandardName("a*b"))

	for _, r := range cloud189VerbatimRunes {
		wire := enc.FromStandardName("a" + string(r) + "b")
		require.Equal(t, "a"+string(r)+"b", wire, "the table shows %q is stored verbatim, so escaping it is wrong", r)
	}

	// A leading or trailing space is stripped by the drive, so it is escaped:
	// measured 2026-10-23, the name reaches the drive without the space and the
	// rename that installs it is then refused as already existing. A space inside
	// the name is stored as sent and stays unescaped.
	for _, name := range []string{" lead", "trail "} {
		wire := enc.FromStandardName(name)
		require.NotEqual(t, name, wire, "%q begins or ends with a space, which the drive strips, so it must be escaped", name)
		require.Equal(t, name, enc.ToStandardName(wire), "escaping %q must round-trip back", name)
	}
	require.Equal(t, "mid dle", enc.FromStandardName("mid dle"), "an interior space is stored as sent")

	// These flags are off on purpose, each one because the table shows the
	// server stores the characters it would escape.
	for _, off := range []encoder.MultiEncoder{
		encoder.EncodeHashPercent,
		encoder.EncodeLeftPeriod, encoder.EncodeLeftTilde,
		encoder.EncodeLeftCrLfHtVt, encoder.EncodeRightCrLfHtVt,
	} {
		require.Zerof(t, enc&off, "flag %v escapes characters this server stores verbatim", off)
	}
	require.NotZero(t, enc&encoder.EncodeLeftSpace, "a leading space is stripped by the drive")
	require.NotZero(t, enc&encoder.EncodeRightSpace, "a trailing space is stripped by the drive")
}

// TestValidateNameRefusesNonBMP covers the local pre-refusal for characters the
// server only rejects after the whole file has been transferred.
func TestValidateNameRefusesNonBMP(t *testing.T) {
	// Measured: these are stored, so they must not be refused.
	for _, name := range []string{
		"", "plain.bin", "雪.bin", "☃.bin", "❤.bin", "❤️.bin",
		"zero\u200bwidth.bin", "rtl\u200f.bin", "combining\u1e4d.bin",
		"long-" + strings.Repeat("a", 300), strings.Repeat("汉", 200) + ".bin",
	} {
		require.NoErrorf(t, validateName(name), "validateName(%q) rejected a name the server stores", name)
	}

	for _, name := range []string{"😀.bin", "file-🐧.bin", "𝓊nicode.bin"} {
		err := validateName(name)
		require.Errorf(t, err, "validateName(%q) accepted a name the server refuses", name)
		// The refusal has to stop rclone from retrying the file and from
		// spending the day's transfer allowance on it.
		require.True(t, fserrors.IsNoRetryError(err), "a name the server cannot store will not change between retries")
		// Only this branch prints this half-sentence, so the assertion cannot be
		// fed by another rejection path.
		require.Contains(t, err.Error(), "rename it locally")
	}
}

func TestParseUploadZone(t *testing.T) {
	got, err := parseUploadZone("")
	require.NoError(t, err)
	require.Equal(t, "https://upload.cloud.189.cn", got)

	for _, zone := range []string{"https://proxy.example.com", "https://proxy.example.com:8443", "https://proxy.example.com/"} {
		got, err := parseUploadZone(zone)
		require.NoErrorf(t, err, "%q", zone)
		require.Equal(t, strings.TrimSuffix(zone, "/"), got, "%q", zone)
	}

	for _, tc := range []struct {
		zone, want string
	}{
		{"http://proxy.example.com", "must be an https:// URL"},
		{"proxy.example.com", "must be an https:// URL"},
		{"https://", "must be an https:// URL"},
		{"https://proxy.example.com/relay", "may only name a host"},
		{"https://user:pw@proxy.example.com", "may only name a host"},
		{"https://proxy.example.com?a=b", "may only name a host"},
		{"https://proxy.example.com#f", "may only name a host"},
	} {
		_, err := parseUploadZone(tc.zone)
		require.Errorf(t, err, "%q was accepted", tc.zone)
		require.Contains(t, err.Error(), tc.want, "%q", tc.zone)
	}
}

func TestFatalIfNoSpace(t *testing.T) {
	makeFs := func(fatal bool) *Fs {
		f := &Fs{}
		f.opt.FatalIfNoSpace = fatal
		return f
	}
	for _, code := range []string{codeNoSpace, codeDayFlowOver} {
		err := &apiError{Status: 403, Code: code}

		got := makeFs(false).fatalIfNoSpace(err)
		require.Equal(t, err, got, "without the option the answer must stay an ordinary error")
		require.False(t, fserrors.IsFatalError(got))

		got = makeFs(true).fatalIfNoSpace(err)
		require.True(t, fserrors.IsFatalError(got), "code %s with fatal_if_no_space must cancel the run", code)
	}

	// An unrelated refusal is never fatal, whatever the option says.
	for _, fatal := range []bool{false, true} {
		other := &apiError{Status: 400, Code: codeFileAlreadyExists}
		require.False(t, fserrors.IsFatalError(makeFs(fatal).fatalIfNoSpace(other)))
	}
	require.NoError(t, makeFs(true).fatalIfNoSpace(nil))
}

// TestNoRefreshStopsAtTheRejectedSession is the guard on the one branch that
// talks to the login endpoint on someone else's behalf.
//
// A rejected session is normally re-exchanged, and the account may be shared
// with the official client and with OpenList, so with no_refresh set rclone has
// to fail where it stands: exactly one request, no exchange, and an answer
// rclone will not retry.
func TestNoRefreshStopsAtTheRejectedSession(t *testing.T) {
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"errorCode":"InvalidSessionKey","errorMsg":"userSessionBO is null or fail to get sessionsecret by sessionkey"}`))
	}))
	defer srv.Close()

	f := &Fs{}
	// A token unique to this test, so the shared session registry cannot be
	// reached by another remote in the same process.
	f.opt.AccessToken = "offline-test-token-no-refresh"
	f.opt.NoRefresh = true
	f.httpClient = srv.Client()
	ts := f.session()
	ts.mu.Lock()
	ts.sessionKey = "offline-test-key"
	ts.sessionSecret = strings.Repeat("a", api.AESKeyLen*2)
	ts.mu.Unlock()
	f.pacer = fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Microsecond), pacer.MaxSleep(time.Millisecond), pacer.DecayConstant(1)))

	_, err := f.call(context.Background(), srv.URL, http.MethodGet, "/listFiles.action", "rand=1", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "no_refresh is set")
	require.True(t, fserrors.IsNoRetryError(err), "a session this server will not accept again must not be retried")
	require.EqualValues(t, 1, atomic.LoadInt64(&hits), "one request only: neither a retry nor an exchange")
}

// TestNoSpaceErrorStopsRetries: the same two refusals must stop the low level
// retries even when fatal_if_no_space is off, or a run walks its whole list
// again per retry.
func TestNoSpaceErrorStopsRetries(t *testing.T) {
	for _, code := range []string{codeNoSpace, codeDayFlowOver} {
		got := noSpaceError(&apiError{Status: 403, Code: code})
		require.True(t, fserrors.IsNoRetryError(got), "code %s", code)
	}
	other := &apiError{Status: 400, Code: codeInvalidArgument}
	require.False(t, fserrors.IsNoRetryError(noSpaceError(other)))
	require.NoError(t, noSpaceError(nil))

	// shouldRetry must not hand a NoRetry answer back to the pacer.
	should, err := shouldRetry(context.Background(), noSpaceError(&apiError{Status: 403, Code: codeNoSpace}))
	require.False(t, should)
	require.Error(t, err)
}

// TestNewFsRejectsBadOptions: a chunk_size the server can never accept, a
// concurrency of zero and a malformed upload_zone must be refused when the
// remote is opened, not on the first upload of a run.
//
// These cases stop before the session exchange, so they need no network and no
// real token.
func TestNewFsRejectsBadOptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		key     string
		value   string
		wantMsg string
	}{
		{"chunk too small", "chunk_size", "9Mi", "chunk_size must be between"},
		{"chunk zero", "chunk_size", "0", "chunk_size must be between"},
		{"chunk absurd", "chunk_size", "2048Ti", "chunk_size must be between"},
		{"concurrency zero", "upload_concurrency", "0", "upload_concurrency must be 1 or more"},
		{"zone not https", "upload_zone", "http://proxy.example.com", "upload_zone must be an https:// URL"},
		{"zone with path", "upload_zone", "https://proxy.example.com/relay", "upload_zone may only name a host"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// configstruct.Set does not fill in the registered defaults, so the
			// options that are not under test have to be given explicitly.
			m := configmap.Simple{
				"access_token":       "0123456789abcdef0123456789abcdef",
				"space":              spacePersonal,
				"chunk_size":         "10Mi",
				"upload_concurrency": "4",
				tc.key:               tc.value,
			}
			_, err := newFs(context.Background(), "test", "", m)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

// TestRegisteredDefaults checks the option table NewFs reads, since a default
// that does not match the constants is a remote nobody tested.
func TestRegisteredDefaults(t *testing.T) {
	reg, err := fs.Find("cloud189")
	require.NoError(t, err)
	defaults := map[string]interface{}{}
	for _, opt := range reg.Options {
		defaults[opt.Name] = opt.Default
	}
	require.Equal(t, defaultChunkSize, defaults["chunk_size"], "the registered default must be the ladder's own base")
	require.Equal(t, 4, defaults["upload_concurrency"])
	require.Equal(t, fs.SizeSuffix(0), defaults["max_file_size"])
	require.Equal(t, false, defaults["fatal_if_no_space"])
	require.Equal(t, false, defaults["no_quota_check"])
	require.Equal(t, false, defaults["hard_delete"])
	require.Equal(t, false, defaults["no_refresh"])
	require.Equal(t, false, defaults["disable_http2"])
	require.Equal(t, cloud189DefaultEncoding, defaults["encoding"])
	require.Equal(t, spacePersonal, defaults["space"])

	// Every field of Options has to be registered, or configstruct.Set never
	// fills it and the user cannot turn the feature on at all.
	var probe Options
	for _, f := range reflect.VisibleFields(reflect.TypeOf(probe)) {
		tag := strings.TrimSuffix(f.Tag.Get("config"), ",omitempty")
		if tag == "" || tag == "-" {
			t.Errorf("Options field %s has no config tag", f.Name)
			continue
		}
		_, ok := defaults[tag]
		require.Truef(t, ok, "option %q (field %s) is not registered", tag, f.Name)
	}
}

// TestRedactLoginNameKeepsTheLastTwoDigits: what the account logs in with reaches a
// debug log, and a phone number there is as good as a name and a contact detail. The
// last two digits stay so two accounts can be told apart; an email is not a secret
// and is left alone.
func TestRedactLoginNameKeepsTheLastTwoDigits(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"17263275626", "*********26"},
		{"+86 172 6327 5626", "+** *** **** **26"},
		{"17263275626@189.cn", "17263275626@189.cn"},
		{"someone@example.com", "someone@example.com"},
		{"fixture-account", "fixture-account"},
		{"123456", "123456"},
		{"", ""},
	} {
		require.Equal(t, tc.want, redactLoginName(tc.in), "input %q", tc.in)
	}
}

// The drive has been measured echoing its own sessionKey back inside an error
// body (InfoSecurityErrorCode, 2026-10-26), and an error string goes to the log -
// so a quoted body must keep its shape without keeping its credentials.
const fixtureSessionKey = "abcdefghij1234567890ABCD" // 24, the length the captures show

func TestAnUnclassifiedErrorBodyNeverCarriesASessionKey(t *testing.T) {
	// No errorCode, no code, no res_code: this is the shape whose error text IS a
	// quote of the body ("unexpected answer (HTTP %d): %s"), which is where a key
	// the drive echoes would travel. A recognised shape is covered by the next
	// test and prints no body at all.
	body := []byte(`{"res_message":"boom","sessionKey":"` + fixtureSessionKey + `"}`)
	err := refuse(http.StatusBadRequest, body)
	msg := err.Error()
	require.Contains(t, msg, "unexpected answer", "an answer with no known field is the quoting branch: %q", msg)
	require.NotContains(t, msg, fixtureSessionKey, "the value must not survive into the error text")
	require.Contains(t, msg, "sessionKey", "the key name stays, or the log says nothing")
	require.Contains(t, msg, "<masked len=24>", "and the value is reported by length")
}

// A classified answer is the shape that actually echoed the key. Its Body field is
// what a later debug line could print, so it is scrubbed at construction.
func TestAClassifiedErrorKeepsItsCodeAndMasksTheKeyInItsBody(t *testing.T) {
	body := []byte(`{"errorCode":"InfoSecurityErrorCode","errorMsg":"security check","sessionKey":"` + fixtureSessionKey + `"}`)
	err := refuse(http.StatusOK, body)
	var ae *apiError
	require.True(t, asAPIError(err, &ae), "an answer carrying errorCode is a known shape: %v", err)
	require.Equal(t, "InfoSecurityErrorCode", ae.Code)
	require.NotContains(t, ae.Body, fixtureSessionKey, "the stored body is the one thing that could still leak")
	require.Contains(t, ae.Body, "<masked len=24>")
}

func TestAServerErrorResponseIsQuotedWithItsSecretsMasked(t *testing.T) {
	body := []byte(`<html>Bad Gateway? sessionKey=` + fixtureSessionKey + `&x=1</html>`)
	_, err := classify(http.StatusBadGateway, body)
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 502")
	require.NotContains(t, err.Error(), fixtureSessionKey)
	require.Contains(t, err.Error(), "<masked len=24>")
}

// The mask must not become a censor: a body with no credential in it is still
// worth quoting verbatim, and that is what makes the four cases above meaningful.
func TestABodyWithoutSecretsIsStillQuotedAsItIs(t *testing.T) {
	body := []byte(`{"errorCode":"WeirdCode","errorMsg":"the drive said this exactly"}`)
	err := refuse(http.StatusBadGateway, body)
	require.Contains(t, err.Error(), "the drive said this exactly")
	require.NotContains(t, err.Error(), "masked")
}

// The doc names the key set the mask covers; this is what makes that sentence a
// claim with a test behind it. The negatives matter as much as the positives - a
// mask that swallows ordinary short fields would look safe and destroy the only
// diagnostic left in the line.
func TestTheMaskCoversTheKeyNamesTheCapturesShow(t *testing.T) {
	const value = "abcdefghijklmnopqrstuvwx" // 24 chars, the length a real key has
	for _, tc := range []struct {
		name    string
		in      string
		masked  bool
		survive string
	}{
		{"json sessionKey", `{"sessionKey":"` + value + `"}`, true, "sessionKey"},
		{"query sk", `sk=` + value + `&x=1`, true, "x=1"},
		{"query signature", `signature=` + value, true, "signature"},
		{"json accessToken", `{"accessToken":"` + value + `"}`, true, "accessToken"},
		{"cookie header shaped", `Cookie: ` + value, true, "Cookie"},
		{"short value stays", `sign=abc12`, false, "sign=abc12"},
		{"plain text stays", `nothing of the sort here`, false, "nothing of the sort here"},
	} {
		got := describeBody([]byte(tc.in), 200)
		if tc.masked {
			require.NotContains(t, got, value, tc.name)
			require.Contains(t, got, "<masked len=24>", tc.name)
		} else {
			require.Equal(t, tc.in, got, tc.name)
		}
		require.Contains(t, got, tc.survive, tc.name+": the rest of the line must survive")
	}
}
