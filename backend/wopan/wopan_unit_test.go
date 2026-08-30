package wopan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rclone/rclone/backend/wopan/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	fsobject "github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Synthetic access token from the protocol spec §2.5: its first 16 bytes form
// the wohome AES key.
const testAccessToken = "0123456789abcdef0123456789abcdef"

func TestUnitAESVectors(t *testing.T) {
	cases := []struct {
		name      string
		channel   string
		plaintext string
		cipher    string
	}{
		{"empty string", chanWoHome, "", "IehVCgspCQA0uinb5LGpDw=="},
		{"list request", chanWoHome,
			`{"spaceType":"0","parentDirectoryId":"0","pageNum":0,"pageSize":100,"sortRule":0,"clientId":"1001000021"}`,
			"4FfmRJPZjxHxe/hjAH52MfDiP5GXNzso1Wa7riRmQ4BFSu82QLTesRXpaqb9lEZ1vs1U77Mttsvf6eKIE4JIbv8yFJervMGrN9kCE9rbsMGx+tXixejUASe/FMxdCUrg7Zw+f8tQ+Q7o3DfweGN5Ow=="},
		{"CJK and emoji", chanWoHome, `{"name":"世界Test😀"}`, "J0R3T7ucV3jpSVGc/nI6/Pyj95MQc9mILchM5E63zKk="},
		{"api-user channel", chanAPIUser,
			`{"refreshToken":"fedcba98-7654-3210-fedc-ba9876543210","clientSecret":"XFmi9GS2hzk98jGX"}`,
			"r9J3oKWUtWa4ySsAo8AHDI2xZpqZVBYCgg2jZ+Yz/X0OoKubx0a/87yhbHOhEQLio/zAh0yJV7GsGEndlKEWtZ8UjnudCd/ku45lQ1JulSbeF0Fk0KlQsL065GSv8frC"},
		{"exactly one block", chanWoHome, "0123456789abcdef", "4vLGrZK6y8CIBS1GpZzP3PGfpNHYEpedQ7+IUT7wtlo="},
		{"multi block", chanWoHome,
			"The quick brown fox jumps over the lazy dog 0123456789",
			"0osFzPcEDKnJHex7awhZvmZ9IxuKgQNlZa10lZ5E2ZXooHRAXEV7/18DQBeNVSZ9K7E36TLecybjavBrM0Q3NA=="},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := aesKeyFor(tc.channel, testAccessToken)
			if tc.channel == chanAPIUser {
				require.Equal(t, clientSecret, string(key))
			} else {
				require.Equal(t, "0123456789abcdef", string(key))
			}

			got, err := aesEncrypt([]byte(tc.plaintext), key)
			require.NoError(t, err)
			assert.Equal(t, tc.cipher, got, "encrypt vector mismatch")

			dec, err := aesDecrypt(got, key)
			require.NoError(t, err)
			assert.Equal(t, tc.plaintext, string(dec), "round-trip decrypt mismatch")
		})
	}
}

func TestUnitSignatureVectors(t *testing.T) {
	cases := []struct {
		method  string
		resTime int64
		reqSeq  int
		channel string
		want    string
	}{
		{"QueryAllFiles", 1690000000000, 104231, chanWoHome, "29a77879f12e5ca0406a86831e4b980a"},
		{"AppRefreshToken", 1690000000000, 100001, chanAPIUser, "ba86fd65e04a84e27da131d23398840d"},
		{"CreateDirectory", 1690000000001, 100002, chanWoHome, "e7e7729d17f33b70ff5ab0aa1061361d"},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			got := sign(tc.method, tc.resTime, tc.reqSeq, tc.channel)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestUnitDecodeDataThreeBranches(t *testing.T) {
	key := aesKeyFor(chanWoHome, testAccessToken)
	plain := `{"hello":"world"}`
	enc, err := aesEncrypt([]byte(plain), key)
	require.NoError(t, err)

	t.Run("quoted ciphertext", func(t *testing.T) {
		out, err := decodeData(json.RawMessage(fmt.Sprintf(`"%s"`, enc)), chanWoHome, testAccessToken)
		require.NoError(t, err)
		assert.JSONEq(t, plain, string(out))
	})

	t.Run("quoted empty string", func(t *testing.T) {
		out, err := decodeData(json.RawMessage(`""`), chanWoHome, testAccessToken)
		require.NoError(t, err)
		assert.Equal(t, "null", string(out))
	})

	t.Run("plaintext object", func(t *testing.T) {
		out, err := decodeData(json.RawMessage(`{"userId":"x"}`), chanWoHome, testAccessToken)
		require.NoError(t, err)
		assert.JSONEq(t, `{"userId":"x"}`, string(out))
	})
}

func TestUnitSpaceParams(t *testing.T) {
	t.Run("personal space omits familyId", func(t *testing.T) {
		p := spaceParams("0", "")
		_, ok := p["familyId"]
		assert.False(t, ok, "personal space must not contain familyId key")
		assert.Equal(t, "0", p["spaceType"])
		assert.Equal(t, clientID, p["clientId"])
	})

	t.Run("family space includes familyId", func(t *testing.T) {
		p := spaceParams("1", "577026")
		assert.Equal(t, "577026", p["familyId"])
	})
}

func TestUnitPartPlan(t *testing.T) {
	const mb = int64(8 * 1024 * 1024)
	cases := []struct {
		size     int64
		total    int64
		partSize []int64
	}{
		{0, 1, []int64{0}},
		{1, 1, []int64{1}},
		{mb, 1, []int64{mb}},
		{mb + 1, 1, []int64{mb + 1}},
		{2 * mb, 2, []int64{mb, mb}},                         // 16 MiB
		{20 * 1024 * 1024, 2, []int64{mb, 12 * 1024 * 1024}}, // 20 MiB
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("size=%d", tc.size), func(t *testing.T) {
			assert.Equal(t, tc.total, api.TotalParts(tc.size, mb))
			plans := api.PlanParts(tc.size, mb)
			require.Equal(t, len(tc.partSize), len(plans))
			var sum int64
			for i, p := range plans {
				assert.Equal(t, tc.partSize[i], p.PartSize, "part %d size", i+1)
				assert.Equal(t, sum, p.Offset, "offset continuity")
				sum += p.PartSize
			}
			assert.Equal(t, tc.size, sum, "parts cover exactly size")
		})
	}
}

func TestUnitContentETag(t *testing.T) {
	assert.Equal(t, "abc", api.ContentETag("abc"))
	assert.Equal(t, "", api.ContentETag("abc-3"))
	assert.Equal(t, "", api.ContentETag("99c4e8c18c5842beb099ab0444670083-3"))
}

func TestUnitParseTime(t *testing.T) {
	// Parsing must be independent of the host's local time zone.
	tm, err := api.ParseTime("20260829135749")
	require.NoError(t, err)
	assert.Equal(t, 2026, tm.Year())
	assert.Equal(t, time.August, tm.Month())
	assert.Equal(t, 29, tm.Day())
	assert.Equal(t, 13, tm.Hour())
	assert.Equal(t, 57, tm.Minute())
	assert.Equal(t, 49, tm.Second())
	assert.Equal(t, "UTC+8", tm.Location().String())

	// Round-trip: format back to the same 14-digit string.
	assert.Equal(t, "20260829135749", tm.Format(api.TimeFormat))

	// Empty string yields the zero time.
	zero, err := api.ParseTime("")
	require.NoError(t, err)
	assert.True(t, zero.IsZero())
}

func TestUnitValidateName(t *testing.T) {
	t.Run("within limit", func(t *testing.T) {
		assert.NoError(t, validateName(strings.Repeat("a", 100)))
	})

	t.Run("too long 101 runes", func(t *testing.T) {
		err := validateName(strings.Repeat("a", 101))
		require.Error(t, err)
		assert.ErrorIs(t, err, fs.ErrorFileNameTooLong)
		assert.True(t, fserrors.IsNoRetryError(err), "must be wrapped in NoRetryError")
	})

	t.Run("emoji non-BMP", func(t *testing.T) {
		err := validateName("emoji😀.txt")
		require.Error(t, err)
		assert.NotErrorIs(t, err, fs.ErrorFileNameTooLong)
		assert.True(t, fserrors.IsNoRetryError(err), "must be wrapped in NoRetryError")
	})
}

func TestUnitRecycleItemParse(t *testing.T) {
	data := []byte(`[{
		"deleteNo": "198bcff8e8c9481ea597a537946d58b4",
		"deleteTime": "20260829144132",
		"fid": "d8216a0a7e3f44dc8818cbcb81476a0f",
		"id": "d8216a0a7e3f44dc8818cbcb81476a0f",
		"fileSize": 0, "fileType": "", "name": "rclone-probe-len",
		"keepDays": 60, "thumbUrl": "", "type": "0"
	}]`)
	var items api.RecycleListResponse
	require.NoError(t, json.Unmarshal(data, &items))
	require.Len(t, items, 1)
	it := items[0]
	assert.Equal(t, "0", it.Type, "type must be a string")
	assert.Equal(t, "198bcff8e8c9481ea597a537946d58b4", it.DeleteNo)
	assert.Equal(t, "d8216a0a7e3f44dc8818cbcb81476a0f", it.ID)
	assert.Equal(t, 60, it.KeepDays)
	assert.Equal(t, int64(0), it.FileSize)
}

func TestUnitUsageParse(t *testing.T) {
	data := []byte(`{
		"code": "0000",
		"usageInfo": {
			"totalSize": "10485760",
			"usedSize": 3124,
			"byteUsedSize": 3199878,
			"byteTotalSize": "10737418240"
		},
		"vipLevel": "3"
	}`)
	var u api.UsageResponse
	require.NoError(t, json.Unmarshal(data, &u))
	assert.Equal(t, "10737418240", u.UsageInfo.ByteTotalSize, "byteTotalSize must be a string")
	assert.Equal(t, int64(3199878), u.UsageInfo.ByteUsedSize)
	assert.Equal(t, int64(3124), u.UsageInfo.UsedSize)
}

func TestUnitRspCodeClassification(t *testing.T) {
	cases := []struct {
		code        string
		authInvalid bool
	}{
		{"1001", true},
		{"9999", false},
		{"1000", false},
		{"130012", false},
		{"500010006", false},
	}
	for _, tc := range cases {
		t.Run(tc.code, func(t *testing.T) {
			err := &apiError{Code: tc.code, Desc: "desc", LogID: "log"}
			assert.Equal(t, tc.authInvalid, isAuthInvalid(err))
		})
	}
}

func TestUnitConstants(t *testing.T) {
	assert.Equal(t, "1001000021", clientID)
	assert.Equal(t, "XFmi9GS2hzk98jGX", clientSecret)
	assert.Equal(t, "10000001", appID)
	assert.Equal(t, "https://panservice.mail.wo.cn", baseURL)
	assert.Equal(t, "wNSOYIB1k1DjY5lA", fixedIV)
	assert.Equal(t, int64(8*1024*1024), partSize)
	assert.Equal(t, "api-user", chanAPIUser)
	assert.Equal(t, "wohome", chanWoHome)
	assert.Equal(t, "wocloud", chanWoCloud)
	assert.Equal(t, "0000", successCode)
}

func TestUnitCallEndToEnd(t *testing.T) {
	newServer := func(status, code, data string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"STATUS":%q,"MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":%q,"RSP_DESC":"d","DATA":%s}}`, status, code, data)
		}))
	}

	t.Run("success 0000 returns decoded data", func(t *testing.T) {
		srv := newServer("200", "0000", `""`)
		defer srv.Close()
		out, err := call(context.Background(), srv.Client(), srv.URL, testAccessToken, chanWoHome, "QueryAllFiles", nil, map[string]any{"secret": true})
		require.NoError(t, err)
		assert.Equal(t, "null", string(out))
	})

	t.Run("business error 9999 yields apiError", func(t *testing.T) {
		srv := newServer("200", "9999", `""`)
		defer srv.Close()
		_, err := call(context.Background(), srv.Client(), srv.URL, testAccessToken, chanWoHome, "QueryAllFiles", nil, nil)
		require.Error(t, err)
		var ae *apiError
		require.True(t, asAPIError(err, &ae))
		assert.Equal(t, "9999", ae.Code)
		assert.False(t, isAuthInvalid(err), "9999 must not be treated as auth failure")
	})

	t.Run("auth failure 1001 is flagged", func(t *testing.T) {
		srv := newServer("200", "1001", `""`)
		defer srv.Close()
		_, err := call(context.Background(), srv.Client(), srv.URL, testAccessToken, chanWoHome, "QueryAllFiles", nil, nil)
		require.Error(t, err)
		assert.True(t, isAuthInvalid(err), "1001 must be treated as auth failure")
	})
}

func TestUnitShootingTime(t *testing.T) {
	// 13:57:49 UTC is 21:57:49 in UTC+8.
	utc := time.Date(2026, time.August, 29, 13, 57, 49, 0, time.UTC)
	assert.Equal(t, "20260829215749", formatShootingTime(utc))

	// The same instant already expressed in UTC+8 formats identically, proving
	// the conversion is fixed-zone rather than host-local.
	beijing := time.FixedZone("UTC+8", 8*3600)
	inBeijing := time.Date(2026, time.August, 29, 21, 57, 49, 0, beijing)
	assert.Equal(t, "20260829215749", formatShootingTime(inBeijing))

	// A zero time is omitted.
	assert.Equal(t, "", formatShootingTime(time.Time{}))
}

func TestUnitTempNameTruncate(t *testing.T) {
	suffix := ".rclone-tmp-12345678" // 20 runes

	// A short leaf passes through unchanged.
	assert.Equal(t, "a.txt"+suffix, truncateName("a.txt", suffix))

	// A long leaf is truncated so the total is exactly the 100-rune limit.
	long := strings.Repeat("名", 200) // 200 CJK runes
	got := truncateName(long, suffix)
	assert.Equal(t, 100, utf8.RuneCountInString(got), "total must be at most 100 runes")
	assert.True(t, strings.HasSuffix(got, suffix), "suffix must be preserved")
}

// stepClock is an injected clock: it advances only when purgeRecycle sleeps, so
// the backoff budget can be exercised deterministically.
type stepClock struct {
	now   time.Time
	slept []time.Duration
}

func newStepClock() *stepClock {
	return &stepClock{now: time.Unix(0, 0)}
}

func (c *stepClock) Now() time.Time { return c.now }

func (c *stepClock) Sleep(d time.Duration) {
	c.slept = append(c.slept, d)
	c.now = c.now.Add(d)
}

func TestUnitPurgeRecycleNoOp(t *testing.T) {
	// snap already covers every deleted id: nothing is pending, so the purge
	// must return immediately without scanning, sleeping or deleting (the
	// idempotent-delete recovery path must not block for the full budget).
	scans := 0
	clock := newStepClock()
	opts := purgeRecycleOpts{
		listAll: func(ctx context.Context) ([]api.RecycleItem, error) {
			scans++
			return nil, nil
		},
		now:   clock.Now,
		sleep: clock.Sleep,
		delete: func(ctx context.Context, nos []string) error {
			t.Error("must not delete on a no-op purge")
			return nil
		},
		budget: recycleBudget,
	}
	err := purgeRecycle(context.Background(), opts, map[string][]string{"id1": {"no1"}}, []string{"id1"})
	require.NoError(t, err)
	assert.Equal(t, 0, scans, "no-op purge must not scan the recycle bin")
	assert.Empty(t, clock.slept, "no-op purge must not back off")
}

func TestUnitPurgeRecycleVisibilityDelay(t *testing.T) {
	// The entry appears only on the second scan: the first scan misses it, one
	// backoff later the retry finds it, and its deleteNo is purged.
	scans := 0
	clock := newStepClock()
	var deleted [][]string
	opts := purgeRecycleOpts{
		listAll: func(ctx context.Context) ([]api.RecycleItem, error) {
			scans++
			if scans < 2 {
				return nil, nil // first scan: not visible yet
			}
			return []api.RecycleItem{{ID: "id1", DeleteNo: "no1"}}, nil
		},
		now:   clock.Now,
		sleep: clock.Sleep,
		delete: func(ctx context.Context, nos []string) error {
			deleted = append(deleted, nos)
			return nil
		},
		budget: recycleBudget,
	}
	err := purgeRecycle(context.Background(), opts, map[string][]string{}, []string{"id1"})
	require.NoError(t, err)
	assert.Equal(t, 2, scans, "one scan before the entry appeared, one after")
	assert.Equal(t, []time.Duration{time.Second}, clock.slept)
	assert.Equal(t, [][]string{{"no1"}}, deleted)
}

func TestUnitPurgeRecyclePerIDAccounting(t *testing.T) {
	// Several entries for one pending id count as that ONE id being found: the
	// completion check compares id sets, not entry counts, so a duplicate-heavy
	// bin cannot make the purge pass early and skip entries.
	clock := newStepClock()
	var deleted [][]string
	opts := purgeRecycleOpts{
		listAll: func(ctx context.Context) ([]api.RecycleItem, error) {
			return []api.RecycleItem{
				{ID: "id1", DeleteNo: "no1"},
				{ID: "id1", DeleteNo: "no2"},
			}, nil
		},
		now:   clock.Now,
		sleep: clock.Sleep,
		delete: func(ctx context.Context, nos []string) error {
			deleted = append(deleted, nos)
			return nil
		},
		budget: recycleBudget,
	}
	err := purgeRecycle(context.Background(), opts, map[string][]string{}, []string{"id1"})
	require.NoError(t, err)
	assert.Empty(t, clock.slept, "both entries were on the first scan, no backoff expected")
	assert.Equal(t, [][]string{{"no1", "no2"}}, deleted, "every entry of the found id must be purged")
}

func TestUnitPurgeRecycleSnapshotEntriesSkipped(t *testing.T) {
	// id1 was already in the bin before the delete (user's own entry) and must
	// be left alone; only id2 is pending and only its entry is purged.
	clock := newStepClock()
	var deleted [][]string
	opts := purgeRecycleOpts{
		listAll: func(ctx context.Context) ([]api.RecycleItem, error) {
			return []api.RecycleItem{
				{ID: "id1", DeleteNo: "user-no"},
				{ID: "id2", DeleteNo: "ours"},
			}, nil
		},
		now:   clock.Now,
		sleep: clock.Sleep,
		delete: func(ctx context.Context, nos []string) error {
			deleted = append(deleted, nos)
			return nil
		},
		budget: recycleBudget,
	}
	err := purgeRecycle(context.Background(), opts,
		map[string][]string{"id1": {"user-no"}}, []string{"id1", "id2"})
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"ours"}}, deleted, "the user's own entry must never be purged")
}

func TestUnitPurgeRecycleBudgetExhausted(t *testing.T) {
	// The entry never becomes visible: the purge must keep backing off until the
	// budget runs out and then report an error, never a silent success.
	clock := newStepClock()
	deleted := false
	opts := purgeRecycleOpts{
		listAll: func(ctx context.Context) ([]api.RecycleItem, error) {
			return nil, nil // never visible
		},
		now:   clock.Now,
		sleep: clock.Sleep,
		delete: func(ctx context.Context, nos []string) error {
			deleted = true
			return nil
		},
		budget: 16 * time.Second,
	}
	err := purgeRecycle(context.Background(), opts, map[string][]string{}, []string{"id1"})
	require.Error(t, err)
	assert.False(t, deleted, "nothing may be deleted when the entry never appears")
	assert.Equal(t, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second},
		clock.slept, "backoff doubles and caps at 8s until the budget is spent")
}

// TestUnitShouldRetryCall is the regression lock for the T2 production
// incident: shouldRetryCall(nil) used to return (true, nil), so the pacer
// wrapped a successful delete in RetryError(nil) ("needs retry") and repeated
// it 30 times before reporting failure.
func TestUnitShouldRetryCall(t *testing.T) {
	// nil: success is never retryable.
	retry, err := shouldRetryCall(nil)
	assert.False(t, retry)
	assert.NoError(t, err)

	// A business error (HTTP 200 + RSP_CODE) is terminal.
	be := &apiError{Code: "1000", Desc: "param"}
	retry, err = shouldRetryCall(be)
	assert.False(t, retry)
	assert.Same(t, be, err)

	// A transport error is retryable.
	te := errors.New("connection reset")
	retry, err = shouldRetryCall(te)
	assert.True(t, retry)
	assert.Same(t, te, err)
}

// TestUnitUploadSingleNonSeekable1001 is the regression lock for the sixth
// round X1: after a 1001 the re-encryption succeeds and leaves err nil, so the
// non-seekable branch used to return (zero, nil) - a silent success for an
// upload that never happened.
func TestUnitUploadSingleNonSeekable1001(t *testing.T) {
	f := &Fs{
		opt:       Options{},
		spaceType: spacePersonal,
		tok: &tokenState{
			accessToken:  testAccessToken,
			refreshToken: "rt-old",
			mappers:      map[string]configmap.Mapper{},
		},
		pacer:      fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Millisecond))),
		httpClient: &http.Client{Transport: routingTransport{}},
		zoneMu:     new(sync.Mutex),
		zoneURL:    "https://zone.example",
		zoneLoaded: true,
	}
	// A bytes.Reader IS a Seeker, so wrap it to hide the Seek method.
	in := struct{ io.Reader }{strings.NewReader("content")}
	src := fsobject.NewStaticObjectInfo("dir/x.bin", time.Now(), int64(len("content")), true, nil, nil)

	_, err := f.uploadSingle(context.Background(), in, "dirID", "x.bin", int64(len("content")), src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not seekable")
	// The refresh must have been committed before the seek check: X1's
	// silent-success regression would leave the old token in place.
	assert.NotEqual(t, "rt-old", f.tok.refreshToken, "the refreshed token must be committed")
}

// routingTransport serves a 1001 business rejection for wohome uploads and a
// successful plaintext refresh response for api-user, so uploadSingle's 1001
// retry path can be exercised without a live server.
type routingTransport struct{}

func (routingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	body := `{"code":"1001","msg":"login expired"}`
	if strings.Contains(r.URL.Path, "api-user") {
		// AppRefreshToken: dispatcher envelope whose DATA is the plaintext
		// refresh JSON (no encryption).
		body = `{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"0000","RSP_DESC":"ok",` +
			`"DATA":{"access_token":"` + strings.Repeat("n", 32) +
			`","refresh_token":"` + strings.Repeat("r", 32) +
			`","token_type":"bearer","expires_in":604799}}}`
	}
	res := &http.Response{
		StatusCode:    http.StatusOK,
		Status:        "200 OK",
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Header:        http.Header{"Content-Type": []string{"application/json"}},
	}
	return res, nil
}

// TestUnitMoveLeafComparison is the regression lock for the fifth-round B-1:
// the move must compare leaves in STANDARD form and encode only for the
// rename. The collision is real for prefix-escaped names: the encoder doubles
// a literal ‛ to ‛‛, so encode("a‛？b.bin") == "a‛‛？b.bin" collides with a
// source literally named "a‛‛？b.bin" - comparing encoded forms would skip the
// rename for names that differ in standard form.
func TestUnitMoveLeafComparison(t *testing.T) {
	enc := encoder.Standard | encoder.EncodeInvalidUtf8
	srcLeaf := "a‛‛？b.bin"   // literal double prefix: the source's standard name
	dstLeafStd := "a‛？b.bin" // single prefix: the destination's standard name
	dstLeafEnc := enc.FromStandardName(dstLeafStd)
	assert.NotEqual(t, srcLeaf, dstLeafStd, "the names differ in standard form")
	assert.Equal(t, srcLeaf, dstLeafEnc, "the encoded destination collides with the source name - the old comparison skipped the rename here")
}

// TestUnitFlexString covers the three JSON shapes the server sends for
// inconsistently quoted numeric fields.
func TestUnitFlexString(t *testing.T) {
	var s api.FlexString

	// Quoted string.
	require.NoError(t, json.Unmarshal([]byte(`"7"`), &s))
	assert.Equal(t, api.FlexString("7"), s)

	// Bare number: the raw literal is kept as the text.
	require.NoError(t, json.Unmarshal([]byte(`7`), &s))
	assert.Equal(t, api.FlexString("7"), s)

	// null and empty input yield the empty string.
	require.NoError(t, json.Unmarshal([]byte(`null`), &s))
	assert.Equal(t, api.FlexString(""), s)
}

// TestUnitFileVersionMixed proves one listing page can carry fileVersion both
// quoted and unquoted (T2 acceptance smoke) without breaking the decode.
func TestUnitFileVersionMixed(t *testing.T) {
	var page struct {
		Files []api.File `json:"files"`
	}
	body := `{"files":[` +
		`{"id":"1","name":"a","fileVersion":3},` +
		`{"id":"2","name":"b","fileVersion":"4"}` +
		`]}`
	require.NoError(t, json.Unmarshal([]byte(body), &page))
	require.Len(t, page.Files, 2)
	assert.Equal(t, api.FlexString("3"), page.Files[0].FileVersion)
	assert.Equal(t, api.FlexString("4"), page.Files[1].FileVersion)
}

// TestUnitRefreshFromUpload verifies the receiver refresh is pure memory (no
// network) and leaves no stale download link behind.
func TestUnitRefreshFromUpload(t *testing.T) {
	o := &Object{
		remote:    "dir/a.bin",
		id:        "old-id",
		fid:       "old-fid",
		url:       "https://old/download",
		urlExpiry: time.Now().Add(time.Hour),
	}
	src := fsobject.NewStaticObjectInfo("dir/a.bin", time.Now(), 123, true, nil, nil)
	data := api.UploadData{Fid: "new-fid", FileVersion: "2", WcFileID: "new-id"}

	o.refreshFromUpload(context.Background(), src, data, 123)

	assert.Equal(t, "new-id", o.id)
	assert.Equal(t, "new-fid", o.fid)
	assert.Equal(t, int64(123), o.size)
	assert.Empty(t, o.url, "the old fid's download link must be dropped")
	assert.True(t, o.urlExpiry.IsZero())
	assert.False(t, o.uploadedAt.IsZero(), "the Hash grace period needs an upload timestamp")
	// shootingTime is truncated to second precision to match the server.
	assert.Equal(t, src.ModTime(context.Background()).Truncate(time.Second), o.shootingTime)
}

// newUnitTestFs builds an Fs wired to the given transport with an empty-root
// dircache; callers seed directories with dirCache.Put to stay offline.
func newUnitTestFs(transport http.RoundTripper) *Fs {
	f := &Fs{
		opt:       Options{},
		spaceType: spacePersonal,
		tok: &tokenState{
			accessToken:  testAccessToken,
			refreshToken: "rt-old",
			mappers:      map[string]configmap.Mapper{},
		},
		pacer:      fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Millisecond))),
		httpClient: &http.Client{Transport: transport},
		zoneMu:     new(sync.Mutex),
	}
	f.dirCache = dircache.New("", "root-id", f)
	return f
}

// hashTransport serves HEADs with a fixed status/ETag and rejects every
// dispatcher POST with a non-retryable business error, so Hash()'s two
// failure layers (link fetch, HEAD) can be driven independently.
type hashTransport struct {
	headStatus int
	headETag   string
}

func (t hashTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodHead {
		res := &http.Response{
			StatusCode: t.headStatus,
			Status:     fmt.Sprintf("%d %s", t.headStatus, http.StatusText(t.headStatus)),
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{},
		}
		if t.headETag != "" {
			res.Header.Set("ETag", t.headETag)
		}
		return res, nil
	}
	return okResp(`{"STATUS":"200","MSG":"forced","RSP":{"RSP_CODE":"9999","RSP_DESC":"forced failure"}}`), nil
}

// okResp builds a 200 JSON response carrying the given body.
func okResp(body string) *http.Response {
	return &http.Response{
		StatusCode:    http.StatusOK,
		Status:        "200 OK",
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Header:        http.Header{"Content-Type": []string{"application/json"}},
	}
}

// okRespTransport answers every dispatcher POST with a generic success
// envelope (RSP_CODE 0000, empty DATA) - enough for calls whose result is
// only checked for the business code, like RenameFileOrDirectory.
type okRespTransport struct{}

func (okRespTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return okResp(`{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"0000","RSP_DESC":"ok","DATA":""}}`), nil
}

// TestUnitHashGraceBothPaths locks Hash()'s grace-period contract: within
// hashGracePeriod of the upload both failure layers (GetDownloadUrlV2, HEAD)
// collapse to "no hash" instead of an error, past the window they surface the
// error - verify must never delete a freshly uploaded file, and must not hide
// a permanent failure either.
func TestUnitHashGraceBothPaths(t *testing.T) {
	newObj := func(fresh, cachedURL bool) *Object {
		f := newUnitTestFs(hashTransport{headStatus: http.StatusInternalServerError})
		uploadedAt := time.Now().Add(-2 * hashGracePeriod)
		if fresh {
			uploadedAt = time.Now()
		}
		o := &Object{fs: f, remote: "dir/a.bin", id: "id", fid: "fid", uploadedAt: uploadedAt}
		if cachedURL {
			o.url = "https://download.example/a"
			o.urlExpiry = time.Now().Add(time.Hour)
		}
		return o
	}

	// Fresh object, link fetch fails: grace swallows the error.
	sum, err := newObj(true, false).Hash(context.Background(), hash.MD5)
	assert.NoError(t, err)
	assert.Empty(t, sum)

	// Stale object, link fetch fails: the error surfaces.
	_, err = newObj(false, false).Hash(context.Background(), hash.MD5)
	assert.Error(t, err)

	// Fresh object, HEAD fails: grace swallows the error.
	sum, err = newObj(true, true).Hash(context.Background(), hash.MD5)
	assert.NoError(t, err)
	assert.Empty(t, sum)

	// Stale object, HEAD fails: the error surfaces.
	_, err = newObj(false, true).Hash(context.Background(), hash.MD5)
	assert.Error(t, err)

	// Success paths: a plain ETag is the content MD5, an S3-style multipart
	// digest ("...-N") is not a hash and yields "no hash".
	f := newUnitTestFs(hashTransport{headStatus: http.StatusOK, headETag: `"abc"`})
	o := &Object{fs: f, remote: "dir/a.bin", id: "id", fid: "fid",
		url: "https://download.example/a", urlExpiry: time.Now().Add(time.Hour)}
	sum, err = o.Hash(context.Background(), hash.MD5)
	assert.NoError(t, err)
	assert.Equal(t, "abc", sum)

	f = newUnitTestFs(hashTransport{headStatus: http.StatusOK, headETag: `"abc-3"`})
	o = &Object{fs: f, remote: "dir/a.bin", id: "id", fid: "fid",
		url: "https://download.example/a", urlExpiry: time.Now().Add(time.Hour)}
	sum, err = o.Hash(context.Background(), hash.MD5)
	assert.NoError(t, err)
	assert.Empty(t, sum)

	// Non-MD5 requests are unsupported.
	_, err = o.Hash(context.Background(), hash.SHA1)
	assert.Equal(t, hash.ErrUnsupported, err)
}

// TestUnitMoveFieldAssertions locks Move's return-object contract: the
// destination is a field-by-field clone of the source (id, fid, size and
// timestamps survive move + rename), carries the requested remote, starts
// with an empty link cache and is NOT the source object (a struct copy would
// duplicate the URL mutex).
func TestUnitMoveFieldAssertions(t *testing.T) {
	f := newUnitTestFs(okRespTransport{})
	// FindRoot short-circuits on the empty root (no network) and must come
	// first: _findRoot flushes the cache, so seed "dir" only afterwards.
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("dir", "dir-id")
	createTime := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	shootingTime := createTime.Add(time.Minute)
	src := &Object{
		fs:           f,
		remote:       "dir/a.bin",
		id:           "wc-1",
		fid:          "fid-1",
		size:         42,
		createTime:   createTime,
		shootingTime: shootingTime,
		thumbURL:     "https://thumb.example/a",
	}

	dst, err := f.Move(context.Background(), src, "dir/b.bin")
	require.NoError(t, err)
	assert.Equal(t, "wc-1", dst.(*Object).id)
	assert.Equal(t, "fid-1", dst.(*Object).fid)
	assert.Equal(t, int64(42), dst.(*Object).size)
	assert.Equal(t, createTime, dst.(*Object).createTime)
	assert.Equal(t, shootingTime, dst.(*Object).shootingTime)
	assert.Equal(t, "https://thumb.example/a", dst.(*Object).thumbURL)
	assert.Equal(t, "dir/b.bin", dst.Remote())
	assert.Same(t, f, dst.(*Object).fs)
	assert.NotSame(t, src, dst, "the clone must be a new object, not the receiver")
	assert.Empty(t, dst.(*Object).url, "the clone starts with a fresh link cache")
}

// TestUnitCopySameDirFallback locks the same-directory Copy guard: CopyFile
// takes no target name, so a same-dir copy collides with the source name and
// the server auto-renames the result - the backend must return the BARE
// ErrorCantCopy sentinel (copy.go compares with ==) so rclone falls back to a
// bandwidth copy instead of orphaning a z(1).txt-style entry.
func TestUnitCopySameDirFallback(t *testing.T) {
	f := newUnitTestFs(okRespTransport{})
	// FindRoot short-circuits on the empty root (no network) and must come
	// first: _findRoot flushes the cache, so seed "dir" only afterwards.
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("dir", "dir-id")
	src := &Object{fs: f, remote: "dir/x.bin", id: "wc-1", fid: "fid-1"}

	dst, err := f.Copy(context.Background(), src, "dir/y.bin")
	assert.Nil(t, dst)
	assert.Equal(t, fs.ErrorCantCopy, err)
}

// TestUnitDirMoveOverlapGuard locks the overlap guard on DirMove: a
// destination inside the source directory (the degenerate shape a moveto
// degrades to when its source file is not yet listing-visible) must be
// refused with the BARE ErrorCantDirMove before any server call, so the
// server can never partially apply a self-nested directory move. The guard
// fires on path shapes alone, before any dircache access, so bare Fs values
// suffice here.
func TestUnitDirMoveOverlapGuard(t *testing.T) {
	f := &Fs{root: "manual/b4"}
	src := &Fs{root: "manual"}

	// Destination inside the source directory.
	err := f.DirMove(context.Background(), src, "b4", "sub")
	assert.Equal(t, fs.ErrorCantDirMove, err)

	// Identity: a directory moved onto itself, expressed with the source as
	// the destination parent. The fstests contract (fstests.go FsDirMove)
	// requires fs.ErrorDirExists when the destination already exists.
	fSelf := &Fs{root: "manual"}
	err = fSelf.DirMove(context.Background(), src, "b4", "b4")
	assert.Equal(t, fs.ErrorDirExists, err)
}

// TestUnitRetryFindDir locks retryFindDir's contract: only ErrorDirNotFound
// retries (a directory created moments ago can be missing from its parent
// listing for seconds), every other error surfaces immediately, and the
// attempt count is a hard bound.
func TestUnitRetryFindDir(t *testing.T) {
	ctx := context.Background()

	// Two visibility-window misses, then success.
	calls := 0
	id, err := retryFindDir(ctx, 4, time.Millisecond, func() (string, error) {
		calls++
		if calls < 3 {
			return "", fs.ErrorDirNotFound
		}
		return "id-3", nil
	})
	require.NoError(t, err)
	assert.Equal(t, "id-3", id)
	assert.Equal(t, 3, calls)

	// The directory never becomes visible: ErrorDirNotFound after exactly
	// attempts lookups.
	calls = 0
	_, err = retryFindDir(ctx, 3, time.Millisecond, func() (string, error) {
		calls++
		return "", fs.ErrorDirNotFound
	})
	assert.Equal(t, fs.ErrorDirNotFound, err)
	assert.Equal(t, 3, calls)

	// Any other error surfaces on the first attempt.
	calls = 0
	_, err = retryFindDir(ctx, 4, time.Millisecond, func() (string, error) {
		calls++
		return "", errors.New("boom")
	})
	assert.EqualError(t, err, "boom")
	assert.Equal(t, 1, calls)
}

// TestUnitHashGraceBySize locks the size-tiered hash grace (manual test B3):
// files of 8 MiB or more are stored in server-side shards whose ETag takes
// minutes to settle to the content MD5, so their grace window outlives the
// small-file one - verify must not treat a freshly uploaded big file as
// hashless-with-error in between.
func TestUnitHashGraceBySize(t *testing.T) {
	newObj := func(size int64, age time.Duration) *Object {
		f := newUnitTestFs(hashTransport{headStatus: http.StatusInternalServerError})
		return &Object{fs: f, remote: "dir/a.bin", id: "id", fid: "fid",
			size: size, uploadedAt: time.Now().Add(-age)}
	}

	// Small file past the small grace: the HEAD error surfaces.
	_, err := newObj(4*1024*1024, hashGracePeriod+time.Minute).Hash(context.Background(), hash.MD5) // 4 MiB: small file
	assert.Error(t, err)

	// Large file of the same age is still inside the large grace: no-hash,
	// no error.
	sum, err := newObj(24*1024*1024, hashGracePeriod+time.Minute).Hash(context.Background(), hash.MD5) // 24 MiB: multi-shard
	assert.NoError(t, err)
	assert.Empty(t, sum)

	// Large file past even the large grace: the error surfaces.
	_, err = newObj(24*1024*1024, largeHashGracePeriod+time.Minute).Hash(context.Background(), hash.MD5)
	assert.Error(t, err)
}

// statusTransport answers every request with a bare HTTP status and an
// empty body.
type statusTransport struct{ code int }

func (t statusTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: t.code,
		Status:     fmt.Sprintf("%d %s", t.code, http.StatusText(t.code)),
		Body:       io.NopCloser(strings.NewReader("")),
		Header:     http.Header{},
	}, nil
}

// TestUnitSameDir locks the absolute-directory comparison behind Move and
// Copy (manual test BUG-D): the source remote is relative to the SOURCE Fs
// root while the destination remote is relative to the DESTINATION Fs root,
// so two different directories of the same backend compare equal as bare
// relative paths and a --backup-dir move used to be skipped entirely while
// reporting success.
func TestUnitSameDir(t *testing.T) {
	srcObj := &Object{fs: &Fs{root: "manual/c3"}, remote: "old.bin"}
	assert.False(t, sameDir(srcObj, "manual/backup", "old.bin"), "different dirs of different Fs instances must not read as same-dir")
	assert.True(t, sameDir(srcObj, "manual/c3", "old.bin"), "the object's own directory is same-dir")
	assert.False(t, sameDir(srcObj, "manual/c3", "sub/old.bin"), "a nested destination is a different directory")

	// Top-level objects of an account-rooted Fs: both directories are the
	// account root and do read as same-dir.
	rootObj := &Object{fs: &Fs{root: ""}, remote: "old.bin"}
	assert.True(t, sameDir(rootObj, "", "old.bin"))
	assert.False(t, sameDir(rootObj, "", "sub/old.bin"))
}

// TestUnitRecyclePagination locks collectRecyclePages: short page = done,
// distinct full pages are appended, a repeated full page means the server
// ignores pageNum (truncated, further entries unreachable), and fetch errors
// surface.
func TestUnitRecyclePagination(t *testing.T) {
	item := func(no string) api.RecycleItem {
		return api.RecycleItem{DeleteNo: no, ID: "id-" + no}
	}
	ctx := context.Background()

	// Short first page: bin exhausted, single fetch.
	fetches := 0
	items, truncated, err := collectRecyclePages(ctx, func(page int) ([]api.RecycleItem, error) {
		fetches++
		return []api.RecycleItem{item("a")}, nil
	}, 2, 10)
	require.NoError(t, err)
	assert.False(t, truncated)
	assert.Equal(t, []string{"a"}, []string{items[0].DeleteNo})
	assert.Equal(t, 1, fetches)

	// Distinct full pages then a short page: everything is collected and the
	// server is judged to honour paging.
	fetches = 0
	items, truncated, err = collectRecyclePages(ctx, func(page int) ([]api.RecycleItem, error) {
		fetches++
		switch page {
		case 0:
			return []api.RecycleItem{item("a"), item("b")}, nil // "full" for pageSize 2
		case 1:
			return []api.RecycleItem{item("c")}, nil
		default:
			t.Fatalf("unexpected fetch of page %d", page)
			return nil, nil
		}
	}, 2, 10)
	require.NoError(t, err)
	assert.False(t, truncated)
	assert.Equal(t, 3, len(items))
	assert.Equal(t, 2, fetches)

	// The server ignores pageNum: page 1 repeats page 0, the scan stops and
	// reports truncated so hard_delete can warn about invisible entries.
	items, truncated, err = collectRecyclePages(ctx, func(page int) ([]api.RecycleItem, error) {
		return []api.RecycleItem{item("a"), item("b")}, nil
	}, 2, 10)
	require.NoError(t, err)
	assert.True(t, truncated)
	assert.Equal(t, 2, len(items), "repeated page content must not be appended twice")

	// Fetch errors surface immediately.
	_, _, err = collectRecyclePages(ctx, func(page int) ([]api.RecycleItem, error) {
		return nil, errors.New("boom")
	}, 2, 10)
	assert.EqualError(t, err, "boom")
}

// TestUnitUploadHTTP500NoRetry locks the upload error mapping (manual test
// F-GH-1): a bare HTTP 500 from upload2C is deterministic for the given file
// name, so it must carry the no-retry mark instead of burning the low-level
// retry ladder.
func TestUnitUploadHTTP500NoRetry(t *testing.T) {
	// A transport that answers every request with a bare HTTP 500 - the
	// upload2C rejection shape observed for unstoreable file names.
	f := newUnitTestFs(statusTransport{code: http.StatusInternalServerError})
	f.zoneURL = "https://zone.example"
	f.zoneLoaded = true
	in := struct{ io.Reader }{strings.NewReader("content")}
	src := fsobject.NewStaticObjectInfo("dir/x.bin", time.Now(), 7, true, nil, nil)

	_, err := f.uploadSingle(context.Background(), in, "dirID", "x.bin", 7, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "http 500")
	assert.True(t, fserrors.IsNoRetryError(err), "the 500 must be marked non-retryable, got: %v", err)
}
