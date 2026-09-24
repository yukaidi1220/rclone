package wopan

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/rclone/rclone/backend/wopan/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
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

	t.Run("BMP emoji rejected", func(t *testing.T) {
		// Live probe (2026-09-19): the server answers RSP_CODE 1009 for every
		// sampled BMP Emoji=Yes codepoint. Each must be rejected before any
		// server call.
		for _, tc := range []struct {
			name string
			r    rune
		}{
			{"copyright", 0x00A9}, {"registered", 0x00AE},
			{"trademark", 0x2122}, {"info", 0x2139},
			{"left-right arrow", 0x2194}, {"sun", 0x2600},
			{"warning", 0x26A0}, {"plane", 0x2708},
			{"sparkles", 0x2728}, {"wavy dash", 0x3030},
			{"heart", 0x2764},
		} {
			t.Run(tc.name, func(t *testing.T) {
				err := validateName("英特尔" + string(tc.r) + "XTU.txt")
				require.Error(t, err, "U+%04X must be rejected", tc.r)
				assert.NotErrorIs(t, err, fs.ErrorFileNameTooLong)
				assert.True(t, fserrors.IsNoRetryError(err), "must be wrapped in NoRetryError")
				assert.Contains(t, err.Error(), "emoji character")
			})
		}
	})

	t.Run("non-emoji BMP symbols accepted", func(t *testing.T) {
		// Live probe: these sampled non-emoji symbols round-trip fine.
		for _, tc := range []struct {
			name string
			r    rune
		}{
			{"vulgar half", 0x00BD}, {"check mark", 0x2713},
			{"degree", 0x00B0}, {"plus-minus", 0x00B1},
			{"quater", 0x00BC}, {"pen nib", 0x2711},
			{"command", 0x2318}, {"white circle", 0x25CC},
			{"circled one", 0x2460}, {"natural", 0x266E},
			{"CJK ideograph", 0x4E2D},
		} {
			t.Run(tc.name, func(t *testing.T) {
				assert.NoError(t, validateName("ok"+string(tc.r)+".txt"), "U+%04X must be accepted", tc.r)
			})
		}
	})

	t.Run("keycap bases not rejected", func(t *testing.T) {
		// # * and digits are Emoji=Yes only as keycap-sequence bases and must
		// not be blocked, or names like "photo#1.jpg" would fail.
		for _, s := range []string{"photo#1.jpg", "a*b.txt", "2026-09.txt"} {
			assert.NoError(t, validateName(s), "%q must be accepted", s)
		}
	})

	t.Run("isWopanBMPEmoji boundaries", func(t *testing.T) {
		assert.True(t, isWopanBMPEmoji(0x00A9), "first range")
		assert.True(t, isWopanBMPEmoji(0x3299), "last range")
		assert.True(t, isWopanBMPEmoji(0x2648), "inside a range")
		assert.True(t, isWopanBMPEmoji(0x2648+5), "inside a range")
		assert.False(t, isWopanBMPEmoji(0x0041), "ASCII letter")
		assert.False(t, isWopanBMPEmoji(0x0041-1), "before table")
		assert.False(t, isWopanBMPEmoji(0x3299+1), "after table")
		assert.False(t, isWopanBMPEmoji(0x00A8), "gap before first range")
		assert.False(t, isWopanBMPEmoji(0x2713), "gap inside dingbats (check mark)")
		assert.False(t, isWopanBMPEmoji(0x0023), "keycap base")
		assert.False(t, isWopanBMPEmoji(0x1F600), "non-BMP is handled separately")
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

	t.Run("illegal name 1009 carries NoRetryError", func(t *testing.T) {
		srv := newServer("200", "1009", `""`)
		defer srv.Close()
		_, err := call(context.Background(), srv.Client(), srv.URL, testAccessToken, chanWoHome, "CreateDirectory", nil, nil)
		require.Error(t, err)
		// The rejection is deterministic for the given name, so --retries must
		// not re-run the whole sync round; the message must say why instead of
		// dumping a bare RSP_CODE.
		assert.True(t, fserrors.IsNoRetryError(err), "1009 must be wrapped in NoRetryError, got: %v", err)
		assert.Contains(t, err.Error(), "name is rejected by the server")
		// The apiError must stay reachable through the wrapper.
		var ae *apiError
		require.True(t, asAPIError(err, &ae))
		assert.Equal(t, "1009", ae.Code)
		assert.False(t, isAuthInvalid(err), "1009 must not be treated as auth failure")
	})

	t.Run("sensitive word 4444 carries NoRetryError", func(t *testing.T) {
		srv := newServer("200", "4444", `""`)
		defer srv.Close()
		_, err := call(context.Background(), srv.Client(), srv.URL, testAccessToken, chanWoHome, "CreateDirectory", nil, nil)
		require.Error(t, err)
		assert.True(t, fserrors.IsNoRetryError(err), "4444 must be wrapped in NoRetryError, got: %v", err)
		var ae *apiError
		require.True(t, asAPIError(err, &ae))
		assert.Equal(t, "4444", ae.Code)
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
	enc := encoder.Standard | encoder.EncodeInvalidUtf8
	suffix := ".rclone-tmp-12345678" // 20 runes

	// A short leaf passes through unchanged.
	assert.Equal(t, "a.txt"+suffix, truncateName(enc, "a.txt", suffix))

	// A long leaf is truncated so the total is exactly the 100-rune limit.
	long := strings.Repeat("名", 200) // 200 CJK runes
	got := truncateName(enc, long, suffix)
	assert.Equal(t, 100, utf8.RuneCountInString(got), "total must be at most 100 runes")
	assert.True(t, strings.HasSuffix(got, suffix), "suffix must be preserved")

	// A leaf whose encoded form has an escape group straddling the cut point:
	// 78 chars plus one escaped byte encodes to 81 runes and the body is cut at
	// 80, so a naive rune-slice would leave half an escape group behind. This
	// is what makes the round-trip assertion below non-vacuous - a CJK-only
	// leaf is a no-op for this encoder, so the check would be trivially true.
	straddle := enc.FromStandardName(strings.Repeat("x", 78) + "\xff")
	require.Equal(t, 81, utf8.RuneCountInString(straddle), "escape group layout changed")

	// tempName itself must respect the limit, not just truncateName: the suffix
	// carries a random 8-rune id, so the helper's own arithmetic is what keeps
	// a long leaf uploadable.
	tmp := tempName(enc, straddle)
	assert.LessOrEqual(t, utf8.RuneCountInString(tmp), 100,
		"tempName must fit the 100-rune limit, got %d", utf8.RuneCountInString(tmp))
	assert.Contains(t, tmp, tempSuffix)
	assert.Equal(t, tmp, enc.FromStandardName(enc.ToStandardName(tmp)),
		"tempName must not split an escape group")

	bak := backupName(enc, straddle)
	assert.LessOrEqual(t, utf8.RuneCountInString(bak), 100,
		"backupName must fit the 100-rune limit, got %d", utf8.RuneCountInString(bak))
	assert.Contains(t, bak, backupSuffix)
	assert.Equal(t, bak, enc.FromStandardName(enc.ToStandardName(bak)),
		"backupName must not split an escape group")
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
	ctx := context.Background()

	// nil: success is never retryable.
	retry, err := shouldRetryCall(ctx, nil)
	assert.False(t, retry)
	assert.NoError(t, err)

	// A business error (HTTP 200 + RSP_CODE) is terminal.
	be := &apiError{Code: "1000", Desc: "param"}
	retry, err = shouldRetryCall(ctx, be)
	assert.False(t, retry)
	assert.Same(t, be, err)

	// A transport error is retryable.
	te := errors.New("connection reset")
	retry, err = shouldRetryCall(ctx, te)
	assert.True(t, retry)
	assert.Same(t, te, err)

	// A transport error with a dead ctx is terminal: the caller gave up and
	// every retry would fail instantly (the hash-check abort storm).
	deadCtx, cancel := context.WithCancel(context.Background())
	cancel()
	retry, err = shouldRetryCall(deadCtx, te)
	assert.False(t, retry)
	assert.Same(t, te, err)
}

// TestUnitPacerNoRetryOnNameRejection locks the low-level retry contract for
// name rejections end to end: a CreateDirectory answering RSP_CODE 1009 or
// 4444 must reach the server exactly once. The wopan pacer callback routes
// through shouldRetryCall, which treats every business error as terminal, so
// --low-level-retries must never re-fire the request; and the NoRetryError
// wrapper keeps it visible to the high-level --retries too.
func TestUnitPacerNoRetryOnNameRejection(t *testing.T) {
	for _, code := range []string{"1009", "4444"} {
		t.Run(code, func(t *testing.T) {
			var hits int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&hits, 1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, `{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":%q,"RSP_DESC":"d","DATA":""}}`, code)
			}))
			defer srv.Close()
			ctx := context.Background()
			p := fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond)))
			err := p.Call(func() (bool, error) {
				_, err := call(ctx, srv.Client(), srv.URL, testAccessToken, chanWoHome, "CreateDirectory", nil, nil)
				return shouldRetryCall(ctx, err)
			})
			require.Error(t, err)
			assert.Equal(t, int32(1), atomic.LoadInt32(&hits), "a name rejection must not be low-level retried")
			assert.True(t, fserrors.IsNoRetryError(err), "must stay NoRetryError through the pacer, got: %v", err)
		})
	}
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
	// refreshFromUpload reads only f.opt.UploadCutoff (no network), so a
	// bare Fs with the split defaults suffices.
	f := newUnitTestFs(nil)
	o := &Object{
		fs:        f,
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
		// The upload split defaults mirror what newFs would produce from an
		// empty config; unit tests bypass newFs so they are set here.
		opt: Options{
			UploadCutoff:      64 * 1024 * 1024,
			ChunkSize:         8 * 1024 * 1024,
			UploadConcurrency: 4,
			// The real default, so tests exercise the encoded-name paths
			// (a zero encoder would make encoding a no-op).
			Enc: encoder.Standard | encoder.EncodeInvalidUtf8,
		},
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

// zoneRecorder records request URLs and serves the GetZoneInfo dispatcher
// response with a server-assigned zone, so uploadZone's two paths (configured
// override, lazy fetch with cache) can be driven without a live server.
// upload2C requests are additionally parsed into their multipart form fields
// (parts) for multipart assertions; intermediate parts are answered with an
// empty data object, the last part with a fid, mirroring the server.
type zoneRecorder struct {
	zoneURL string
	// failPart, if non-zero, makes that partIndex answer with a business
	// error so the failure propagation of the concurrent pipeline can be
	// driven.
	failPart int
	// failPart5xx, if non-zero, makes that partIndex answer with a bare
	// HTTP 500, driving the transport-level failure path of the chunked
	// upload.
	failPart5xx int
	mu          sync.Mutex
	urls        []string
	parts       []partValues
}

type partValues struct {
	url.Values
	partBytes   int64
	fileContent []byte
}

func (z *zoneRecorder) RoundTrip(r *http.Request) (*http.Response, error) {
	z.mu.Lock()
	defer z.mu.Unlock()
	z.urls = append(z.urls, r.URL.String())
	body := `{"code":"0000","data":{"fid":"F","wcFileId":"W"},"msg":"ok"}`
	if mr, err := r.MultipartReader(); err == nil {
		pv := partValues{Values: url.Values{}}
		for {
			p, err := mr.NextPart()
			if err != nil {
				break
			}
			b, _ := io.ReadAll(p)
			if p.FileName() != "" {
				pv.partBytes = int64(len(b))
				pv.fileContent = b
			} else {
				pv.Set(p.FormName(), string(b))
			}
		}
		z.parts = append(z.parts, pv)
		if idx, err := strconv.Atoi(pv.Get("partIndex")); err == nil && idx == z.failPart5xx {
			return &http.Response{
				StatusCode:    http.StatusInternalServerError,
				Status:        "500 Internal Server Error",
				Body:          io.NopCloser(strings.NewReader("server error")),
				ContentLength: int64(len("server error")),
				Header:        http.Header{"Content-Type": []string{"text/plain"}},
			}, nil
		}
		if idx, err := strconv.Atoi(pv.Get("partIndex")); err == nil && idx == z.failPart {
			body = `{"code":"9999","msg":"boom"}`
		} else if pv.Get("partIndex") != "" && pv.Get("partIndex") != pv.Get("totalPart") {
			body = `{"code":"0000","data":{},"msg":"ok"}`
		}
	} else if strings.HasSuffix(r.URL.Path, "/wohome/dispatcher") {
		data, err := aesEncrypt([]byte(`{"url":"`+z.zoneURL+`"}`), aesKeyFor(chanWoHome, testAccessToken))
		if err != nil {
			return nil, err
		}
		body = `{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"0000","RSP_DESC":"ok","DATA":"` + data + `"}}`
	}
	return &http.Response{
		StatusCode:    http.StatusOK,
		Status:        "200 OK",
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
		Header:        http.Header{"Content-Type": []string{"application/json"}},
	}, nil
}

// TestUnitUploadZoneOverride is the regression lock for upload_zone: a
// configured endpoint is returned verbatim and GetZoneInfo is never sent,
// even though the lazy cache is still empty.
func TestUnitUploadZoneOverride(t *testing.T) {
	rec := &zoneRecorder{zoneURL: "https://zone-from-server.example"}
	f := newUnitTestFs(rec)
	f.opt.UploadZone = "https://override.example"

	zone, err := f.uploadZone(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "https://override.example", zone)
	assert.Empty(t, rec.urls, "upload_zone override must not issue any request")
}

// TestUnitUploadZoneLazyCache covers the default path: the first uploadZone
// call fetches the zone from GetZoneInfo, later calls reuse the cached value.
func TestUnitUploadZoneLazyCache(t *testing.T) {
	rec := &zoneRecorder{zoneURL: "https://zone-from-server.example"}
	f := newUnitTestFs(rec)

	zone, err := f.uploadZone(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "https://zone-from-server.example", zone)
	zone, err = f.uploadZone(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "https://zone-from-server.example", zone)
	assert.Len(t, rec.urls, 1, "GetZoneInfo must be sent exactly once, then cached")
}

// TestUnitUploadMultipart drives the chunked path: a file above
// upload_cutoff goes as several 8 MiB POSTs sharing one uniqueId, and the
// fid from the last part is the one returned.
func TestUnitUploadMultipart(t *testing.T) {
	rec := &zoneRecorder{zoneURL: "https://zone-from-server.example"}
	f := newUnitTestFs(rec)
	f.zoneURL = "https://zone-from-server.example"
	f.zoneLoaded = true

	size := int64(65<<20) + 1 // floor(65MiB+1 / 8MiB) = 8 parts, last 9MiB+1
	src := fsobject.NewStaticObjectInfo("a", time.Now(), size, true, nil, nil)
	_, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, size)), "dirID", "big.bin", size, src)
	require.NoError(t, err)

	require.Len(t, rec.parts, 8, "expected 8 part POSTs")
	// Parts complete concurrently, so sort them by partIndex before asserting.
	byIndex := make(map[int]partValues, len(rec.parts))
	for _, pv := range rec.parts {
		idx, err := strconv.Atoi(pv.Get("partIndex"))
		require.NoError(t, err)
		byIndex[idx] = pv
	}
	var uniqueID string
	totalSent := int64(0)
	for i := 1; i <= 8; i++ {
		pv := byIndex[i]
		require.NotNil(t, pv.Values, "part %d missing", i)
		if i == 1 {
			uniqueID = pv.Get("uniqueId")
			require.NotEmpty(t, uniqueID)
		} else {
			assert.Equal(t, uniqueID, pv.Get("uniqueId"), "uniqueId must stay stable across parts")
		}
		assert.Equal(t, strconv.Itoa(i), pv.Get("partIndex"))
		assert.Equal(t, "8", pv.Get("totalPart"))
		assert.Equal(t, strconv.FormatInt(size, 10), pv.Get("fileSize"))
		if i < 8 {
			assert.Equal(t, "8388608", pv.Get("partSize"))
		} else {
			// SDK split: the last part absorbs 7*8MiB plus the remainder,
			// i.e. 65MiB+1 - 7*8MiB = 9MiB+1.
			assert.Equal(t, "9437185", pv.Get("partSize"), "the last part absorbs the remainder")
		}
		totalSent += pv.partBytes
	}
	assert.Equal(t, size, totalSent, "parts must cover the file exactly")
}

// TestUnitUploadPartFailure drives a business rejection in the middle of the
// concurrent pipeline: the upload fails as a whole and must not be reported
// as success.
func TestUnitUploadPartFailure(t *testing.T) {
	rec := &zoneRecorder{zoneURL: "https://zone-from-server.example", failPart: 3}
	f := newUnitTestFs(rec)
	f.zoneURL = "https://zone-from-server.example"
	f.zoneLoaded = true
	// A serial worker pins the schedule so the early-stop count below is
	// deterministic: with several workers the remaining in-flight parts
	// would race the cancellation and the part count would be flaky. The
	// concurrent success path is covered by TestUnitUploadMultipart.
	f.opt.UploadConcurrency = 1

	size := int64(65<<20) + 1 // 8 parts
	src := fsobject.NewStaticObjectInfo("a", time.Now(), size, true, nil, nil)
	_, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, size)), "dirID", "big.bin", size, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Equal(t, 3, len(rec.parts), "parts 4+ must never reach the server after part 3 failed")
}

// TestUnitUploadPart5xxRetryable locks the retry semantics of transport-level
// failures: a bare HTTP 500 in the chunked path must NOT be marked NoRetry,
// so --retries still re-runs the whole file (probe E4 saw transient 500s
// there), while a 4xx and the single-part path keep their deterministic
// NoRetry protection.
func TestUnitUploadPart5xxRetryable(t *testing.T) {
	rec := &zoneRecorder{zoneURL: "https://zone-from-server.example", failPart5xx: 3}
	f := newUnitTestFs(rec)
	f.zoneURL = "https://zone-from-server.example"
	f.zoneLoaded = true
	// Serial worker, same determinism reasoning as TestUnitUploadPartFailure.
	f.opt.UploadConcurrency = 1

	size := int64(65<<20) + 1 // 8 parts
	src := fsobject.NewStaticObjectInfo("a", time.Now(), size, true, nil, nil)
	_, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, size)), "dirID", "big.bin", size, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500 Internal Server Error")
	assert.False(t, fserrors.IsNoRetryError(err), "a chunked-path 5xx must stay retryable")
	assert.Equal(t, 3, len(rec.parts), "parts 4+ must never reach the server after part 3 failed")
}

// TestUnitNewFsOptionValidation checks the fail-fast validation of the
// chunked-upload options before any network call.
func TestUnitNewFsOptionValidation(t *testing.T) {
	for _, tc := range []struct {
		key, value, wantErr string
	}{
		{"chunk_size", "1Ki", "chunk_size"},
		// 4Mi chunks reproduced a bare HTTP 500 at upload completion twice
		// (probe E4), so the floor sits at 5Mi.
		{"chunk_size", "4Mi", "chunk_size"},
		{"upload_concurrency", "0", "upload_concurrency"},
	} {
		// configstruct.Set does not fill option defaults, so the sibling
		// option must be given a valid value explicitly.
		m := configmap.Simple{"chunk_size": "8Mi", "upload_concurrency": "4"}
		m[tc.key] = tc.value
		_, err := newFs(context.Background(), "wopan-test", "", m)
		require.Error(t, err, tc.key)
		assert.Contains(t, err.Error(), tc.wantErr)
	}
}

// TestUnitChunkWriterOpen checks the OpenChunkWriter contract: unknown
// sizes and empty files are rejected, a file below chunk_size collapses to
// a single-part plan, opening a writer issues no upload requests, and
// Close without any posted part fails rather than faking success.
func TestUnitChunkWriterOpen(t *testing.T) {
	rec := &zoneRecorder{zoneURL: "https://zone-from-server.example"}
	f := newUnitTestFs(rec)
	f.zoneURL = "https://zone-from-server.example"
	f.zoneLoaded = true
	ctx := context.Background()

	_, _, err := f.OpenChunkWriter(ctx, "u.bin", fsobject.NewStaticObjectInfo("u.bin", time.Now(), -1, true, nil, nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown size")

	_, _, err = f.OpenChunkWriter(ctx, "e.bin", fsobject.NewStaticObjectInfo("e.bin", time.Now(), 0, true, nil, nil))
	require.ErrorIs(t, err, fs.ErrorCantUploadEmptyFiles)

	src := fsobject.NewStaticObjectInfo("s.bin", time.Now(), 100, true, nil, nil)
	info, w, err := f.OpenChunkWriter(ctx, "s.bin", src)
	require.NoError(t, err)
	require.NotNil(t, w)
	assert.Equal(t, int64(100), info.ChunkSize, "a file below chunk_size is one part")
	assert.Equal(t, 4, info.Concurrency)
	require.Error(t, w.Close(ctx), "Close without a fid must fail")
	assert.Empty(t, rec.urls, "opening a writer must not issue upload requests")
}

// TestUnitChunkWriterOutOfOrder posts the three parts out of order through
// the ChunkWriter contract the multi-thread engine uses, and locks the
// session semantics: one stable uniqueId, exact per-part boundaries and
// content, and a successful Close driven by the fid of the final part even
// though it was posted first.
func TestUnitChunkWriterOutOfOrder(t *testing.T) {
	rec := &zoneRecorder{zoneURL: "https://zone-from-server.example"}
	f := newUnitTestFs(rec)
	f.zoneURL = "https://zone-from-server.example"
	f.zoneLoaded = true
	f.opt.ChunkSize = 8

	payload := []byte("0123456789ABCDEFGHIJ") // 20 bytes -> parts 8, 8, 4
	src := fsobject.NewStaticObjectInfo("oo.bin", time.Now(), int64(len(payload)), true, nil, nil)
	info, w0, err := f.OpenChunkWriter(context.Background(), "oo.bin", src)
	require.NoError(t, err)
	w := w0.(*wopanChunkWriter)
	assert.Equal(t, int64(8), info.ChunkSize)

	ctx := context.Background()
	n, err := w.WriteChunk(ctx, 2, bytes.NewReader(payload[16:]))
	require.NoError(t, err)
	assert.Equal(t, int64(4), n)
	n, err = w.WriteChunk(ctx, 1, bytes.NewReader(payload[8:16]))
	require.NoError(t, err)
	assert.Equal(t, int64(8), n)
	n, err = w.WriteChunk(ctx, 0, bytes.NewReader(payload[:8]))
	require.NoError(t, err)
	assert.Equal(t, int64(8), n)

	require.NoError(t, w.Close(ctx))
	require.NoError(t, w.Abort(ctx), "abort is a no-op: orphaned parts expire on their own")

	require.Len(t, rec.parts, 3)
	byIndex := make(map[int]partValues, 3)
	var uniqueID string
	for _, pv := range rec.parts {
		idx, err := strconv.Atoi(pv.Get("partIndex"))
		require.NoError(t, err)
		byIndex[idx] = pv
		if uniqueID == "" {
			uniqueID = pv.Get("uniqueId")
			require.NotEmpty(t, uniqueID)
		} else {
			assert.Equal(t, uniqueID, pv.Get("uniqueId"), "one session spans the whole file")
		}
		assert.Equal(t, "3", pv.Get("totalPart"))
		assert.Equal(t, strconv.Itoa(len(payload)), pv.Get("fileSize"))
	}
	assert.Equal(t, payload[:8], byIndex[1].fileContent, "part 1 content")
	assert.Equal(t, payload[8:16], byIndex[2].fileContent, "part 2 content")
	assert.Equal(t, payload[16:], byIndex[3].fileContent, "part 3 content")
	assert.Equal(t, "8", byIndex[1].Get("partSize"))
	assert.Equal(t, "8", byIndex[2].Get("partSize"))
	assert.Equal(t, "4", byIndex[3].Get("partSize"), "the final part absorbs the remainder")
}

// TestUnitChunkWriterPartFailure drives a business rejection through the
// ChunkWriter path: the error propagates, no fid is recorded and Close
// refuses to report success.
func TestUnitChunkWriterPartFailure(t *testing.T) {
	rec := &zoneRecorder{zoneURL: "https://zone-from-server.example", failPart: 2}
	f := newUnitTestFs(rec)
	f.zoneURL = "https://zone-from-server.example"
	f.zoneLoaded = true
	f.opt.ChunkSize = 8

	payload := []byte("0123456789ABCDEFGHIJ")
	src := fsobject.NewStaticObjectInfo("pf.bin", time.Now(), int64(len(payload)), true, nil, nil)
	_, w0, err := f.OpenChunkWriter(context.Background(), "pf.bin", src)
	require.NoError(t, err)
	w := w0.(*wopanChunkWriter)

	n, err := w.WriteChunk(context.Background(), 1, bytes.NewReader(payload[8:16]))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Equal(t, int64(0), n)
	require.Error(t, w.Close(context.Background()), "without a fid Close must fail")
	require.Len(t, rec.parts, 1)
}

// TestUnitDisableHTTP2Transport locks the disable_http2 option to the transport
// wiring: when set, the wopan client's transport carries a non-nil empty
// TLSNextProto map (Go's documented way to disable HTTP/2); by default the
// transport must never carry that marker.
func TestUnitDisableHTTP2Transport(t *testing.T) {
	ctx := context.Background()

	enabled := newHTTPClient(ctx, &Options{DisableHTTP2: true})
	tr, ok := enabled.Transport.(*fshttp.Transport)
	require.True(t, ok, "wopan client must use the fshttp transport")
	require.NotNil(t, tr.TLSNextProto, "disable_http2 must set TLSNextProto to a non-nil empty map")
	assert.Empty(t, tr.TLSNextProto, "disable_http2 must leave no h2 slot configured")

	def := newHTTPClient(ctx, &Options{})
	tr2, ok := def.Transport.(*fshttp.Transport)
	require.True(t, ok)
	// http.Transport lazily fills TLSNextProto with the h2 slots on first
	// use, and the copy of http.DefaultTransport's map can make that state
	// observable before use depending on test order. Either way the default
	// client must never carry the empty map that disables HTTP/2.
	if tr2.TLSNextProto != nil {
		_, hasH2 := tr2.TLSNextProto["h2"]
		assert.True(t, hasH2, "default transport must not disable HTTP/2")
	}
}

// TestUnitDisableHTTP2Negotiation proves the disabled client really negotiates
// HTTP/1.1 against a local HTTP/2-capable TLS server. The default side is left
// to net/http's own behaviour (an empty TLSNextProto map disables HTTP/2), so
// only the enabled direction - the behaviour this option controls - is asserted.
func TestUnitDisableHTTP2Negotiation(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	ci := fs.GetConfig(context.Background())
	oldSkip := ci.InsecureSkipVerify
	ci.InsecureSkipVerify = true
	defer func() { ci.InsecureSkipVerify = oldSkip }()

	c := newHTTPClient(context.Background(), &Options{DisableHTTP2: true})
	res, err := c.Get(srv.URL)
	require.NoError(t, err)
	defer res.Body.Close()
	assert.Equal(t, 1, res.ProtoMajor, "disable_http2 must negotiate HTTP/1.1")
}

// ------------------------------------------------- lossless Update ----------

// updateTransport records the calls Update makes and can be told to fail one of
// them, so every recovery branch can be driven without a server.
//
// Renames are counted by call order because the update issues up to three of
// them (park, install, restore) and only their sequence distinguishes the
// branches under test.
type updateTransport struct {
	mu sync.Mutex
	// ops is the call order, each entry prefixed by its kind.
	ops []string
	// renames/uploads/deletes hold the name (or id) of each call of that kind.
	renames []string
	// renameIDs holds the object id each rename targeted, in call order. The
	// lossless flow renames two different objects (old, then new), so a test
	// that only checks the target NAMES cannot tell a correct update from one
	// that renames the wrong object and then deletes the file at the real name.
	renameIDs []string
	uploads   []string
	deletes   []string

	// failUpload makes the upload answer with a business error.
	failUpload bool
	// failRenameAt holds 1-based rename ordinals that answer with a business
	// error.
	failRenameAt []int
	// failRenameTransport reports whether the rename of this object fails at
	// the transport level, i.e. with a bare error rather than an RSP_CODE. It
	// is a predicate rather than an ordinal because the pacer retries
	// internally: an ordinal would only fail the first attempt and the retry
	// would succeed, so the error would never surface. The pacer turns these
	// into fserrors.RetryError, the shape the code under test must strip
	// before marking the error NoLowLevelRetry.
	failRenameTransport func(id, name string) bool
	// failDelete makes the backup delete answer with a business error.
	failDelete bool

	// names maps a directory-visible name to the object id that holds it, so
	// the listing can answer leafHolds. Renames update it: a rename moves the
	// id, and the server never overwrites, so a rename onto a held name fails
	// with the name-occupied code unless the holder is the id being moved.
	names map[string]string
	// listFails makes every listing answer with a business error.
	listFails bool
	// renameLandsButFails models a lost response: the rename takes effect on
	// the server but the call reports an error. The server state must still be
	// updated, because that is the whole point of the case.
	renameLandsButFails map[string]bool
	// dirEntries maps a name to the id of a DIRECTORY holding it, so a test can
	// put a same-named directory beside a file: only the file may answer
	// leafHolds.
	dirEntries map[string]string
	// hideEntry maps a name to the number of page-0 listings that omit it,
	// modelling the directory index's visibility delay. The counter drops by
	// one per listing served, so the entry appears once it reaches zero.
	hideEntry map[string]int
	// failRenameOnce maps "id->name" to the number of leading rename attempts
	// that answer with the transient name-occupied code - what the server
	// returns while a just-freed name's index is still being released. The
	// counter drops by one per attempt, after which the rename succeeds.
	failRenameOnce map[string]int
	// listCalls counts the listings served, so a test can prove leafHolds
	// polled rather than trusting a single answer.
	listCalls int
}

func (t *updateTransport) renameOrdinalFail(n int) bool {
	for _, f := range t.failRenameAt {
		if f == n {
			return true
		}
	}
	return false
}

// applyRename moves id to name in the modelled server state, dropping whatever
// name held it before.
func (t *updateTransport) applyRename(id, name string) {
	if t.names == nil {
		t.names = map[string]string{}
	}
	for n, held := range t.names {
		if held == id {
			delete(t.names, n)
		}
	}
	t.names[name] = id
}

// listResp answers a listing with the current modelled server state, encrypted
// the way the real DATA field is. Only page 0 carries the entries: listAll pages
// until it sees an empty page, so returning them again would never terminate.
func (t *updateTransport) listResp(fields map[string]any) *http.Response {
	if t.listFails {
		return okResp(`{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"9999","RSP_DESC":"boom","DATA":""}}`)
	}
	t.listCalls++
	pageNum := 0
	if v, ok := fields["pageNum"].(float64); ok {
		pageNum = int(v)
	}
	files := []map[string]any{}
	if pageNum == 0 {
		for n, id := range t.names {
			// hideEntry models the index's visibility delay: the entry is
			// absent for the first N listings, then appears.
			if left, ok := t.hideEntry[n]; ok && left > 0 {
				t.hideEntry[n] = left - 1
				continue
			}
			files = append(files, map[string]any{"id": id, "name": n, "type": fileTypeFile})
		}
		for n, id := range t.dirEntries {
			files = append(files, map[string]any{"id": id, "name": n, "type": fileTypeDir})
		}
	}
	payload, _ := json.Marshal(map[string]any{"files": files})
	enc, err := aesEncrypt(payload, aesKeyFor(chanWoHome, testAccessToken))
	if err != nil {
		return okResp(`{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"9999","RSP_DESC":"encrypt","DATA":""}}`)
	}
	return okResp(`{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"0000","RSP_DESC":"ok","DATA":"` + enc + `"}}`)
}

func (t *updateTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Upload parts carry the target name as a multipart form field.
	if strings.HasSuffix(r.URL.Path, "/openapi/client/upload2C") {
		name := ""
		if mr, err := r.MultipartReader(); err == nil {
			for {
				p, err := mr.NextPart()
				if err != nil {
					break
				}
				b, _ := io.ReadAll(p)
				if p.FormName() == "fileName" {
					name = string(b)
				}
			}
		}
		t.uploads = append(t.uploads, name)
		t.ops = append(t.ops, "upload:"+name)
		if t.failUpload {
			return okResp(`{"code":"9999","msg":"boom"}`), nil
		}
		return okResp(`{"code":"0000","data":{"fid":"new-fid","wcFileId":"new-id"},"msg":"ok"}`), nil
	}

	raw, _ := io.ReadAll(r.Body)
	var req struct {
		Header struct {
			Key string `json:"key"`
		} `json:"header"`
		Body map[string]any `json:"body"`
	}
	_ = json.Unmarshal(raw, &req)
	ok := `{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"0000","RSP_DESC":"ok","DATA":""}}`
	boom := `{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"9999","RSP_DESC":"boom","DATA":""}}`
	// nameOccupied is the real "target name already taken" code, which is what
	// the server answers when a rename would overwrite another object.
	nameOccupied := `{"STATUS":"200","MSG":"ok","LOGID":"L","RSP":{"RSP_CODE":"` + codeNameOccupied + `","RSP_DESC":"name occupied","DATA":""}}`

	// The operation parameters travel AES-encrypted in body.param, so the
	// recorded name/id has to be decrypted before it can be asserted on.
	fields := map[string]any{}
	if enc, ok := req.Body["param"].(string); ok && enc != "" {
		if plain, err := aesDecrypt(enc, aesKeyFor(chanWoHome, testAccessToken)); err == nil {
			_ = json.Unmarshal(plain, &fields)
		}
	}

	switch req.Header.Key {
	case "QueryAllFiles":
		return t.listResp(fields), nil
	case "RenameFileOrDirectory":
		name, _ := fields["name"].(string)
		id, _ := fields["id"].(string)
		t.renames = append(t.renames, name)
		t.renameIDs = append(t.renameIDs, id)
		t.ops = append(t.ops, "rename:"+name)
		// A transient name-occupied answer: the server does this while a
		// just-freed name's index is still being released, and a retry by id
		// heals it. Checked before the state below so the attempt leaves the
		// server untouched, exactly like the real transient window.
		if left, ok := t.failRenameOnce[id+"->"+name]; ok && left > 0 {
			t.failRenameOnce[id+"->"+name] = left - 1
			return okResp(nameOccupied), nil
		}
		// The server never overwrites: a rename onto a name another object
		// holds fails with the name-occupied code, which is what makes the
		// listing a trustworthy oracle for a lost response.
		if held, ok := t.names[name]; ok && held != id {
			return okResp(nameOccupied), nil
		}
		if t.renameLandsButFails[id+"->"+name] {
			t.applyRename(id, name)
			return nil, errors.New("simulated lost response")
		}
		if t.failRenameTransport != nil && t.failRenameTransport(id, name) {
			// A transport-level failure: the handler returns no response at
			// all, which is what makes f.call fail with a bare error and the
			// pacer wrap it in fserrors.RetryError. Nothing changed server-side.
			return nil, errors.New("simulated transport failure")
		}
		if t.renameOrdinalFail(len(t.renames)) {
			return okResp(boom), nil
		}
		t.applyRename(id, name)
	case "DeleteFile":
		id := ""
		if list, ok := fields["fileList"].([]any); ok && len(list) > 0 {
			id, _ = list[0].(string)
		}
		t.deletes = append(t.deletes, id)
		t.ops = append(t.ops, "delete:"+id)
		if t.failDelete {
			return okResp(boom), nil
		}
		for n, held := range t.names {
			if held == id {
				delete(t.names, n)
			}
		}
	}
	return okResp(ok), nil
}

// newUpdateFixture builds an Fs with dir/a.bin cached, an object for it, and the
// matching source info, so Update runs entirely offline.
func newUpdateFixture(t *testing.T, tr *updateTransport, newSize int64) (*Fs, *Object, fs.ObjectInfo) {
	t.Helper()
	f := newUnitTestFs(tr)
	// A configured upload_zone keeps GetZoneInfo (and its own dispatcher call)
	// out of the recorded sequence.
	f.opt.UploadZone = "https://zone.example"
	// Keep a failing-rename test fast: the production budget is 60s.
	f.renameDeadlineOverride = 50 * time.Millisecond
	// And a missing listing entry fast: the production budget is 20s.
	f.listBudgetOverride = 50 * time.Millisecond
	// Model the server's name index: the old object currently holds "a.bin".
	if tr.names == nil {
		tr.names = map[string]string{}
	}
	tr.names["a.bin"] = "old-id"
	// FindRoot short-circuits on the empty root (no network) and must come
	// first: _findRoot flushes the cache, so seed "dir" only afterwards.
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("dir", "dir-id")
	o := &Object{fs: f, remote: "dir/a.bin", id: "old-id", fid: "old-fid", size: 10}
	src := fsobject.NewStaticObjectInfo("dir/a.bin", time.Now(), newSize, true, nil, nil)
	return f, o, src
}

// TestUnitUpdateLosslessHappyPath locks the order that makes the update
// lossless: the new content is uploaded first, the old object is only parked
// (never deleted) once that succeeded, the new object is installed under the
// real name, and only then is the backup dropped.
func TestUnitUpdateLosslessHappyPath(t *testing.T) {
	tr := &updateTransport{}
	_, o, src := newUpdateFixture(t, tr, 99)

	require.NoError(t, o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src))

	require.Len(t, tr.ops, 4, "expected upload, park, install, drop-backup: %v", tr.ops)
	assert.True(t, strings.HasPrefix(tr.ops[0], "upload:"), "the upload must come first, got %v", tr.ops)
	require.Len(t, tr.uploads, 1)
	assert.True(t, strings.HasPrefix(tr.uploads[0], "a.bin.rclone-tmp-"),
		"the new content goes to a temp name, got %q", tr.uploads[0])
	require.Len(t, tr.renames, 2)
	assert.True(t, strings.HasPrefix(tr.renames[0], "a.bin.rclone-old-"),
		"the old object is parked under a backup name, got %q", tr.renames[0])
	assert.Equal(t, "a.bin", tr.renames[1], "the new object must be installed under the real name")
	assert.Equal(t, []string{"old-id"}, tr.deletes, "only the old object may be deleted")

	// The IDs matter as much as the names: renaming the OLD object onto the real
	// name (instead of the new one) produces exactly the same name sequence and
	// then deletes the file that is actually at the real name - silent data
	// loss. Assert which object each rename targeted.
	assert.Equal(t, []string{"old-id", "new-id"}, tr.renameIDs,
		"the park must move the OLD object and the install must move the NEW one")

	// The receiver must describe the new content.
	assert.Equal(t, "new-id", o.id)
	assert.Equal(t, "new-fid", o.fid)
	assert.Equal(t, int64(99), o.size)
}

// TestUnitUpdateUploadFailureLeavesOldObject is the core data-safety lock: when
// the upload fails, the old object must not be touched at all - no rename, no
// delete - so the file is still readable under its own name.
func TestUnitUpdateUploadFailureLeavesOldObject(t *testing.T) {
	tr := &updateTransport{failUpload: true}
	_, o, src := newUpdateFixture(t, tr, 99)

	err := o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.False(t, fserrors.IsNoLowLevelRetryError(err),
		"an upload failure leaves the old object intact, so a whole-file retry is safe")
	assert.Len(t, tr.ops, 1, "nothing may happen after a failed upload: %v", tr.ops)
	require.Len(t, tr.uploads, 1)
	assert.True(t, strings.HasPrefix(tr.uploads[0], "a.bin.rclone-tmp-"), "got %q", tr.uploads[0])
	assert.Empty(t, tr.renames, "the old object must not be parked before the new content is on the server")
	assert.Empty(t, tr.deletes, "the old object must never be deleted before the new one is in place")
	assert.Equal(t, "old-id", o.id, "the receiver must keep describing the old object")
}

// TestUnitUpdateParkFailureKeepsOldObject covers a failure of the park rename:
// the error must stop the whole-file retry (a retry would upload another temp)
// and must not delete anything, since the park's outcome is uncertain.
func TestUnitUpdateParkFailureKeepsOldObject(t *testing.T) {
	tr := &updateTransport{failRenameAt: []int{1}}
	_, o, src := newUpdateFixture(t, tr, 99)

	err := o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src)
	require.Error(t, err)
	assert.True(t, fserrors.IsNoLowLevelRetryError(err), "got: %v", err)
	assert.Contains(t, err.Error(), "backup name")
	assert.Equal(t, "old-id", o.id)
	assert.Empty(t, tr.deletes, "an uncertain park must not trigger any delete")
	assert.Len(t, tr.renames, 1, "the install must not be attempted after a failed park")
}

// TestUnitUpdateInstallFailureRestoresOldObject covers the recoverable branch:
// installing the new object fails, so the old object is renamed back to the
// real name and the file stays reachable.
func TestUnitUpdateInstallFailureRestoresOldObject(t *testing.T) {
	tr := &updateTransport{failRenameAt: []int{2}}
	_, o, src := newUpdateFixture(t, tr, 99)

	err := o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src)
	require.Error(t, err)
	assert.True(t, fserrors.IsNoLowLevelRetryError(err), "got: %v", err)
	require.Len(t, tr.renames, 3, "park, install, restore: %v", tr.renames)
	assert.Equal(t, "a.bin", tr.renames[2], "the restore must put the OLD object back under the real name")
	assert.Equal(t, "old-id", tr.renameIDs[2], "the restore must target the OLD object, not the temp")
	// The temp is deliberately NOT deleted: the listing may have been wrong or
	// raced a concurrent writer, in which case it holds the only copy of the
	// new content. Assert the invariant rather than only the delete count.
	assert.Empty(t, tr.deletes, "nothing may be deleted when the install failed")
	assert.Equal(t, "old-id", o.id, "the receiver still describes the old object")
	assert.Equal(t, "old-id", tr.names["a.bin"], "the old content must own the real name again")
	assert.NotContains(t, tr.deletes, "new-id", "the temp must never be deleted on this path")
}

// TestUnitUpdateTransportFailureSuppressesWholeFileRetry locks the interaction
// that makes NoLowLevelRetryError a no-op on its own: f.pacer.Call wraps an
// exhausted-retry transport failure in fserrors.RetryError, and copy.go checks
// IsRetryError BEFORE ShouldRetry (the only place the NoLowLevelRetry marker is
// honoured). A transport-level rename failure must therefore come back with the
// pacer's marker already stripped, or the whole file is re-uploaded each round.
func TestUnitUpdateTransportFailureSuppressesWholeFileRetry(t *testing.T) {
	// Fail the park at the transport level: no RSP_CODE, just a dead connection.
	tr := &updateTransport{failRenameTransport: func(id, _ string) bool { return id == "old-id" }}
	_, o, src := newUpdateFixture(t, tr, 99)

	err := o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src)
	require.Error(t, err)
	assert.True(t, fserrors.IsNoLowLevelRetryError(err), "got: %v", err)
	assert.False(t, fserrors.IsRetryError(err),
		"the pacer's RetryError marker must be stripped, or copy.go retries the whole file")
	assert.False(t, fserrors.ShouldRetry(err),
		"ShouldRetry must see the NoLowLevelRetry marker, got: %v", err)
	assert.Equal(t, "old-id", o.id, "the receiver must still describe the old object")
	assert.Empty(t, tr.deletes, "a transport-failed park must not delete anything")
	// The park never succeeded, so nothing may have been moved onto the real
	// name, and the temp upload must not have been repeated.
	assert.Len(t, tr.uploads, 1, "a whole-file retry would re-upload the temp")
	for i, id := range tr.renameIDs {
		assert.Equal(t, "old-id", id, "rename %d must target the old object", i)
	}
}

// TestUnitUpdateTransportFailureOnInstallRestores locks the same interaction on
// the install step, where the consequence is worse: the temp object exists, so a
// whole-file retry would upload another temp every round while the real name
// stays empty.
func TestUnitUpdateTransportFailureOnInstallRestores(t *testing.T) {
	// Fail only the install (new object) at the transport level, so the restore
	// of the old object can still succeed.
	tr := &updateTransport{failRenameTransport: func(id, _ string) bool { return id == "new-id" }}
	_, o, src := newUpdateFixture(t, tr, 99)

	err := o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src)
	require.Error(t, err)
	assert.True(t, fserrors.IsNoLowLevelRetryError(err), "got: %v", err)
	assert.False(t, fserrors.IsRetryError(err), "the pacer's RetryError marker must be stripped")
	assert.Len(t, tr.uploads, 1, "a whole-file retry would re-upload the temp")
	assert.Empty(t, tr.deletes, "nothing may be deleted when the install failed")
	// The last rename must be the restore: the OLD object back under the real
	// name. Without it the file would be left only under the backup name.
	require.NotEmpty(t, tr.renames)
	last := len(tr.renames) - 1
	assert.Equal(t, "a.bin", tr.renames[last], "the last rename must restore the real name")
	assert.Equal(t, "old-id", tr.renameIDs[last], "the restore must target the OLD object")
}

// TestUnitUpdateRestoreRetriesTransientName locks the retry at the restore call
// site. The restore targets the name the park just vacated, which is exactly the
// window in which the server answers with the transient name-occupied code, so
// a single-shot restore would leave the file only under the backup name.
func TestUnitUpdateRestoreRetriesTransientName(t *testing.T) {
	// The install fails terminally (a plain business error, not the transient
	// code), and the first restore attempt hits the name-release window.
	tr := &updateTransport{
		failRenameAt:   []int{2},
		failRenameOnce: map[string]int{"old-id->a.bin": 1},
	}
	_, o, src := newUpdateFixture(t, tr, 99)

	err := o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src)
	require.Error(t, err, "the install failure must still be reported")

	// park, install, restore attempt 1 (transient), restore attempt 2.
	require.Len(t, tr.renames, 4, "the transient restore must have been retried: %v", tr.renames)
	assert.Equal(t, "a.bin", tr.renames[3], "the last rename must restore the real name")
	assert.Equal(t, "old-id", tr.renameIDs[3], "the restore must target the OLD object")
	assert.Equal(t, "old-id", tr.names["a.bin"], "the old content must own the real name again")
}

// TestUnitUpdateInstallAndRestoreFailureNamesTheBackup is the worst case: the
// install fails and the restore fails too. The old bytes are still under the
// backup name, so the error must name it - that is the only pointer the user
// gets to recover the file by hand.
func TestUnitUpdateInstallAndRestoreFailureNamesTheBackup(t *testing.T) {
	tr := &updateTransport{failRenameAt: []int{2, 3}}
	_, o, src := newUpdateFixture(t, tr, 99)

	err := o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src)
	require.Error(t, err)
	assert.True(t, fserrors.IsNoLowLevelRetryError(err), "got: %v", err)
	assert.Contains(t, err.Error(), "a.bin.rclone-old-", "the error must name the backup holding the old content")
	assert.Contains(t, err.Error(), "still intact")
	assert.Empty(t, tr.deletes)
}

// TestUnitUpdateLostResponseInstallCountsAsSuccess locks the case where the
// install rename LANDS but its response is lost. Reporting failure there is
// wrong twice over: the update actually succeeded, and the restore that would
// otherwise follow burns a second 60s deadline and then tells the user the
// wrong thing. The listing is the oracle that tells the two apart.
func TestUnitUpdateLostResponseInstallCountsAsSuccess(t *testing.T) {
	// The install takes effect server-side but always reports an error.
	tr := &updateTransport{renameLandsButFails: map[string]bool{"new-id->a.bin": true}}
	_, o, src := newUpdateFixture(t, tr, 99)

	err := o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src)
	require.NoError(t, err, "a landed install must not be reported as a failure")

	// The park and the install are the only renames: no restore may follow.
	for i, id := range tr.renameIDs {
		want := "old-id"
		if i > 0 {
			want = "new-id"
		}
		assert.Equal(t, want, id, "rename %d", i)
	}
	// The backup is still dropped, and the receiver describes the new content.
	assert.Equal(t, []string{"old-id"}, tr.deletes, "the backup must still be dropped")
	assert.Equal(t, "new-id", o.id)
	assert.Equal(t, int64(99), o.size)
}

// TestUnitUpdateInstallFailureRestoresWhenListingSaysNotLanded is the other half
// of the oracle: when the name does NOT hold the new id, the install genuinely
// did not land and the old object must go back.
func TestUnitUpdateInstallFailureRestoresWhenListingSaysNotLanded(t *testing.T) {
	// The install fails with a business error and does not touch the server.
	tr := &updateTransport{failRenameAt: []int{2}}
	_, o, src := newUpdateFixture(t, tr, 99)

	err := o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src)
	require.Error(t, err)
	require.Len(t, tr.renames, 3, "park, install, restore: %v", tr.renames)
	assert.Equal(t, "a.bin", tr.renames[2], "the old object must be restored")
	assert.Equal(t, "old-id", tr.renameIDs[2])
	assert.Equal(t, "old-id", o.id)
}

// TestUnitUpdateInstallListingUnavailableStillRestores proves the conservative
// default: if the confirming listing itself fails, the code must assume the
// install did not land and restore, because the opposite mistake would leave the
// real name empty.
func TestUnitUpdateInstallListingUnavailableStillRestores(t *testing.T) {
	tr := &updateTransport{failRenameAt: []int{2}, listFails: true}
	_, o, src := newUpdateFixture(t, tr, 99)

	err := o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src)
	require.Error(t, err)
	require.Len(t, tr.renames, 3, "park, install, restore: %v", tr.renames)
	assert.Equal(t, "old-id", tr.renameIDs[2], "a failed listing must default to restoring")
	assert.Equal(t, "old-id", o.id)
}

// TestUnitLeafHoldsMatchesEncodedNames is the regression lock for the encoding
// mismatch: the rename is issued with an ENCODED leaf, while listDirEntries
// returns STANDARD names, so leafHolds must convert before comparing. Without
// that, a name holding a reserved character (which the encoder escapes) would
// never match and every lost-response install would be misread as not landed.
func TestUnitLeafHoldsMatchesEncodedNames(t *testing.T) {
	enc := encoder.Standard | encoder.EncodeInvalidUtf8
	// A name the encoder rewrites, so the encoded and standard forms differ.
	// The \xff is a raw invalid UTF-8 byte, which the encoder escapes.
	const standard = "a*b\xffc"
	encoded := enc.FromStandardName(standard)
	require.NotEqual(t, standard, encoded, "the encoder must rewrite this name for the test to mean anything")
	require.Contains(t, encoded, string(encoder.QuoteRune), "the encoder must have escaped a byte")

	// The listing serves STANDARD names.
	tr := &updateTransport{names: map[string]string{standard: "id-1"}}
	f := newUnitTestFs(tr)
	f.listBudgetOverride = 20 * time.Millisecond

	// The caller passes the encoded form, exactly as Update does.
	got, err := f.leafHolds(context.Background(), "dir-id", encoded, "id-1")
	require.NoError(t, err)
	assert.True(t, got, "an encoded leaf must match the standard name from the listing")

	// And it must still reject a different holder.
	got, err = f.leafHolds(context.Background(), "dir-id", encoded, "id-2")
	require.NoError(t, err)
	assert.False(t, got)
}

// TestUnitLeafHolds locks the oracle itself: it reports the id that holds a name,
// treats a different holder as false, and reports false for a name nobody holds.
func TestUnitLeafHolds(t *testing.T) {
	tr := &updateTransport{names: map[string]string{"a.bin": "id-1", "b.bin": "id-2"}}
	f := newUnitTestFs(tr)
	f.listBudgetOverride = 20 * time.Millisecond

	got, err := f.leafHolds(context.Background(), "dir-id", "a.bin", "id-1")
	require.NoError(t, err)
	assert.True(t, got, "the name holds this id")

	got, err = f.leafHolds(context.Background(), "dir-id", "a.bin", "id-9")
	require.NoError(t, err)
	assert.False(t, got, "a different object holds the name")

	got, err = f.leafHolds(context.Background(), "dir-id", "missing.bin", "id-1")
	require.NoError(t, err)
	assert.False(t, got, "nobody holds the name")

	// A listing failure is reported, so the caller can pick the safe default.
	f2 := newUnitTestFs(&updateTransport{listFails: true})
	f2.listBudgetOverride = 20 * time.Millisecond
	_, err = f2.leafHolds(context.Background(), "dir-id", "a.bin", "id-1")
	require.Error(t, err, "a listing failure must be surfaced")

	// Names are case-insensitive, so a listing that reports a different case
	// than the name the rename was issued with must still match - the server
	// may normalise the case, and a miss here would misread a landed install
	// as "did not land" and report a successful update as failed.
	tr.names["C.BIN"] = "id-3"
	got, err = f.leafHolds(context.Background(), "dir-id", "c.bin", "id-3")
	require.NoError(t, err)
	assert.True(t, got, "the comparison must ignore case")
}

// TestUnitLeafHoldsIgnoresDirectories locks the type filter: only a FILE entry
// may answer the oracle. The listing holds both files and directories, and a
// directory can never be the object a rename moved, so matching one would make
// the oracle report a landed install that never happened.
func TestUnitLeafHoldsIgnoresDirectories(t *testing.T) {
	// A directory that (contrived, but this is the invariant being locked)
	// holds the very id being looked for, with no file of that name present.
	tr := &updateTransport{dirEntries: map[string]string{"a.bin": "id-1"}}
	f := newUnitTestFs(tr)
	f.listBudgetOverride = 20 * time.Millisecond

	got, err := f.leafHolds(context.Background(), "dir-id", "a.bin", "id-1")
	require.NoError(t, err)
	assert.False(t, got, "a directory entry must never answer for a file")
}

// TestUnitLeafHoldsWaitsForVisibility locks the polling loop: the directory
// index is eventually consistent, so an entry that is not there yet must be
// waited for rather than read as "absent". Without the wait the oracle would
// report a landed install as not landed whenever the listing lagged - exactly
// the case it exists to disambiguate.
func TestUnitLeafHoldsWaitsForVisibility(t *testing.T) {
	// The entry is missing from the first listing and appears in the second.
	tr := &updateTransport{
		names:     map[string]string{"a.bin": "id-1"},
		hideEntry: map[string]int{"a.bin": 1},
	}
	f := newUnitTestFs(tr)
	// Long enough for one backoff interval (the loop starts at a second).
	f.listBudgetOverride = 5 * time.Second

	got, err := f.leafHolds(context.Background(), "dir-id", "a.bin", "id-1")
	require.NoError(t, err)
	assert.True(t, got, "the oracle must wait out the visibility delay")
	// At least two page-0 listings: the first omits the entry, the second
	// serves it. Assert the poll count loosely - the exact number also depends
	// on how listAll pages, and pinning it would make this fail for a
	// pagination change rather than a real regression.
	assert.GreaterOrEqual(t, tr.listCalls, 2, "the oracle must have polled more than once")
	assert.Equal(t, 0, tr.hideEntry["a.bin"], "the visibility counter must have been consumed")
}

// TestUnitRenameWithBackoffRetriesTransient locks the retry loop: a rename onto
// a name whose index has just been released answers with the transient
// name-occupied code, and only a retry by id heals it. A single-shot rename
// would surface that as a failed update, and the restore path in particular
// depends on it - it targets the name the park just vacated.
func TestUnitRenameWithBackoffRetriesTransient(t *testing.T) {
	// The first attempt answers with the transient code, the second succeeds.
	tr := &updateTransport{failRenameOnce: map[string]int{"old-id->b.bin": 1}}
	f := newUnitTestFs(tr)
	f.renameDeadlineOverride = 5 * time.Second

	require.NoError(t, f.renameWithBackoff(context.Background(), "old-id", "b.bin"))
	assert.Equal(t, []string{"old-id", "old-id"}, tr.renameIDs,
		"the transient failure must be retried, not surfaced")
	assert.Equal(t, "old-id", tr.names["b.bin"], "the retry must have landed the rename")

	// A transient answer must not move the id: that is what makes retrying
	// safe, and what the real name-release window looks like. Drive every
	// attempt into the transient branch so the final state is observable -
	// with a retry that succeeds, the name ends up held either way and the
	// invariant would be invisible.
	tr3 := &updateTransport{failRenameOnce: map[string]int{"old-id->b.bin": 100}}
	f3 := newUnitTestFs(tr3)
	f3.renameDeadlineOverride = 500 * time.Millisecond
	err := f3.renameWithBackoff(context.Background(), "old-id", "b.bin")
	require.Error(t, err, "a persistently transient rename must eventually give up")
	assert.Empty(t, tr3.names, "a transient answer must never move the id")
	// The transient branch must actually have been taken. This also guards the
	// assertion above from passing trivially: if the branch never fired, the
	// first attempt would have succeeded and left the name held. Asserted via
	// the counter rather than the attempt count, which would depend on how
	// long the first attempt happened to take against the deadline.
	assert.Less(t, tr3.failRenameOnce["old-id->b.bin"], 100,
		"the transient branch must have been exercised")

	// A terminal business error must NOT be retried: the deadline exists to
	// heal the transient window, not to hammer a real rejection.
	tr2 := &updateTransport{failRenameAt: []int{1}}
	f2 := newUnitTestFs(tr2)
	f2.renameDeadlineOverride = 5 * time.Second
	require.Error(t, f2.renameWithBackoff(context.Background(), "old-id", "b.bin"))
	assert.Len(t, tr2.renameIDs, 1, "a terminal error must fail on the first attempt")
}

// TestUnitUpdateBackupDeleteFailureStillSucceeds locks the step-4 contract: the
// new content already owns the name, so failing to drop the backup only leaks
// quota and must never be reported as a failed update (a retry would upload the
// file again).
func TestUnitUpdateBackupDeleteFailureStillSucceeds(t *testing.T) {
	tr := &updateTransport{failDelete: true}
	_, o, src := newUpdateFixture(t, tr, 99)

	require.NoError(t, o.Update(context.Background(), bytes.NewReader(make([]byte, 99)), src))
	assert.Equal(t, "new-id", o.id, "the update must still be applied to the receiver")
	assert.Equal(t, int64(99), o.size)
	require.Len(t, tr.ops, 4)
	assert.Equal(t, "delete:old-id", tr.ops[3])
}

// TestUnitBackupNameTruncate locks the backup name helper: the old object's name
// is truncated so that the suffix keeps the total inside the 100-rune limit the
// server enforces - a too-long backup name would fail the park and abort the
// update of a file whose name was otherwise acceptable.
func TestUnitBackupNameTruncate(t *testing.T) {
	enc := encoder.Standard | encoder.EncodeInvalidUtf8
	got := backupName(enc, strings.Repeat("名", 200))
	assert.Equal(t, 100, utf8.RuneCountInString(got), "the backup name must fit the 100-rune limit")
	assert.Contains(t, got, ".rclone-old-")

	// A short leaf keeps its full name so the backup stays recognisable.
	assert.True(t, strings.HasPrefix(backupName(enc, "a.bin"), "a.bin.rclone-old-"))
}

// TestUnitTruncateNameKeepsEscapeIntact locks the encoder interaction in
// truncateName: the encoder escapes a reserved character as QuoteRune followed
// by more runes, so a cut landing inside an escape group leaves a name whose
// tail decodes to something else entirely. A truncated name must always be a
// well-formed encoded name, i.e. it must survive a decode/re-encode round trip.
func TestUnitTruncateNameKeepsEscapeIntact(t *testing.T) {
	enc := encoder.Standard | encoder.EncodeInvalidUtf8
	suffix := ".rclone-old-12345678" // 20 runes, so the body is cut at 80

	// The encoder expands an invalid byte to QuoteRune + two hex digits, so a
	// name of 78 x's plus one invalid byte encodes to 81 runes and the 80-rune
	// cut lands inside the escape group.
	leaf := enc.FromStandardName(strings.Repeat("x", 78) + "\xff")
	require.Equal(t, 81, utf8.RuneCountInString(leaf), "escape group layout changed")

	got := truncateName(enc, leaf, suffix)
	// The cut lands mid-escape: 80 runes = 78 x's + QuoteRune + one hex digit.
	// The hex digit is dropped (not a well-formed tail), then the now-dangling
	// QuoteRune, leaving the 78 x's + the 20-rune suffix.
	assert.Equal(t, 98, utf8.RuneCountInString(got),
		"the malformed escape tail must be dropped entirely")
	assert.Equal(t, strings.Repeat("x", 78)+suffix, got)
	assert.Equal(t, got, enc.FromStandardName(enc.ToStandardName(got)),
		"a truncated name must survive a decode/re-encode round trip")

	// The general invariant across every cut position and escape shape.
	tails := []string{"x", "\xff", "\xff\xff", " ", "\xff "}
	for n := 60; n <= 95; n++ {
		for _, tail := range tails {
			s := strings.Repeat("x", n) + tail
			encLeaf := enc.FromStandardName(s)
			got := truncateName(enc, encLeaf, suffix)
			assert.LessOrEqual(t, utf8.RuneCountInString(got), 100,
				"n=%d tail=%q exceeds the limit", n, tail)
			assert.True(t, strings.HasSuffix(got, suffix),
				"n=%d tail=%q lost the suffix: %q", n, tail, got)
			assert.Equal(t, got, enc.FromStandardName(enc.ToStandardName(got)),
				"n=%d tail=%q is not a well-formed encoded name: %q", n, tail, got)
			// Truncation must not drop more than the malformed tail: the kept
			// body has to stay a prefix of the encoded leaf.
			body := strings.TrimSuffix(got, suffix)
			assert.True(t, strings.HasPrefix(encLeaf, body),
				"n=%d tail=%q kept %q, which is not a prefix of %q", n, tail, body, encLeaf)
		}
	}
}
