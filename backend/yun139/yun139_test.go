package yun139

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/yun139/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPlanParts verifies the byte ranges produced by planParts for a few
// canonical sizes. The function is the heart of the multi-part upload
// pipeline; any off-by-one here corrupts the SHA-256 server-side.
func TestPlanParts(t *testing.T) {
	cases := []struct {
		name       string
		size       int64
		partSize   int64
		wantParts  int
		wantLastSz int64 // size of the last part
	}{
		{"empty", 0, 100, 1, 0},
		{"exact_one", 100, 100, 1, 100},
		{"one_and_bit", 250, 100, 3, 50},
		{"single_part_below_cutoff", 50, 100, 1, 50},
		{"two_parts", 150, 100, 2, 50},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			plans := planParts(c.size, c.partSize)
			if len(plans) != c.wantParts {
				t.Fatalf("len(plans) = %d, want %d", len(plans), c.wantParts)
			}
			if got := plans[len(plans)-1].partSize; got != c.wantLastSz {
				t.Errorf("last part size = %d, want %d", got, c.wantLastSz)
			}
			// First part starts at 0, every part indexed from 1.
			if plans[0].offset != 0 || plans[0].index != 1 {
				t.Errorf("first plan = %+v, want offset=0 index=1", plans[0])
			}
			// Indexes are contiguous 1..N.
			for i, p := range plans {
				if p.index != int64(i+1) {
					t.Errorf("plan[%d].index = %d, want %d", i, p.index, i+1)
				}
			}
		})
	}
}

// TestSrvPathCache covers the family-side id -> server-path map.
func TestSrvPathCache(t *testing.T) {
	c := newSrvPathCache()
	if _, ok := c.get("missing"); ok {
		t.Fatal("get on empty cache returned ok")
	}
	c.put("root", "root:/")
	c.put("child", "root:/abc")
	if v, ok := c.get("root"); !ok || v != "root:/" {
		t.Errorf("get(root) = %q, %v; want root:/, true", v, ok)
	}
	if v, ok := c.get("child"); !ok || v != "root:/abc" {
		t.Errorf("get(child) = %q, %v; want root:/abc, true", v, ok)
	}
	// Overwrite.
	c.put("child", "root:/def")
	if v, _ := c.get("child"); v != "root:/def" {
		t.Errorf("overwrite failed: got %q", v)
	}
}

// TestMd5hex pins md5hex to a known value so that x-yun-device-id stays
// stable across builds.
func TestMd5hex(t *testing.T) {
	// md5("hello") = 5d41402abc4b2a76b9719d911017c592
	got := md5hex("hello")
	want := "5d41402abc4b2a76b9719d911017c592"
	if got != want {
		t.Errorf("md5hex(hello) = %q, want %q", got, want)
	}
}

// TestQuotaToUsage pins the MiB->bytes conversion against a real
// captured quota response (2026-09-03: 701440 MiB total, 367136 MiB
// free for a ~685 GiB free-tier account).
func TestQuotaToUsage(t *testing.T) {
	u := quotaToUsage(701440, 367136)
	if u.Total == nil || u.Free == nil {
		t.Fatal("nil Total/Free")
	}
	if *u.Total != 701440*1024*1024 {
		t.Errorf("Total = %d, want %d", *u.Total, int64(701440)*1024*1024)
	}
	if *u.Free != 367136*1024*1024 {
		t.Errorf("Free = %d, want %d", *u.Free, int64(367136)*1024*1024)
	}
}

// TestQuotaToUsage_ZeroAccount pins the degenerate case: a brand-new
// account with zero quota must not produce negative/overflow values.
func TestQuotaToUsage_ZeroAccount(t *testing.T) {
	u := quotaToUsage(0, 0)
	if u.Total == nil || *u.Total != 0 {
		t.Errorf("Total = %v, want 0", u.Total)
	}
	if u.Free == nil || *u.Free != 0 {
		t.Errorf("Free = %v, want 0", u.Free)
	}
}

// TestSrvPathCache_FamilyAndPersonal pins the id->path cache used to
// translate family-space directory ids to server paths.
func TestSrvPathCache_FamilyAndPersonal(t *testing.T) {
	c := newSrvPathCache()
	c.put("fam1", "root:/a/b")
	if got, ok := c.get("fam1"); !ok || got != "root:/a/b" {
		t.Errorf("get(fam1) = %q, %v; want root:/a/b, true", got, ok)
	}
	c.put("fam1", "root:/a/b/c")
	if got, _ := c.get("fam1"); got != "root:/a/b/c" {
		t.Errorf("after update get(fam1) = %q, want root:/a/b/c", got)
	}
	if _, ok := c.get("nope"); ok {
		t.Error("get(nope) should miss")
	}
}

// TestParsePath_CleansDotDot pins BUG-5: "a/../b" must resolve to "b",
// not create a literal "．．" (full-width dot) directory on the server.
func TestParsePath_CleansDotDot(t *testing.T) {
	cases := map[string]string{
		"":          "",
		"a":         "a",
		"a/":        "a",
		"a//b":      "a/b",
		"a/../b":    "b",
		"a/./b":     "a/b",
		"../b":      "b",
		"a/b/..":    "a",
		"a/b/../..": "",
		`a\b`:       "a/b",
	}
	for in, want := range cases {
		if got := parsePath(in); got != want {
			t.Errorf("parsePath(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestValidateName pins the client-side name guard: the 139 server returns
// RSP_CODE 04000002 for the eight reserved ASCII characters
// `" * : < > ? \ |` (probe 2026-09-19). The default encoding escapes all
// eight to fullwidth substitutes the server stores verbatim, so a standard
// name is never refused; validateName guards the bytes that actually go on
// the wire and must still refuse a raw reserved character, with NoRetryError.
// Everything the encoder handles (control chars, leading/trailing space·dot,
// leading tilde) and everything the server accepts (CJK, é, symbols, emoji)
// must pass.
func TestValidateName(t *testing.T) {
	// Raw wire bytes: still refused, since the server rejects them.
	rejected := []string{
		`a"b`, `a*b`, `a:b`, `a<b>`, `a?b`, `a\b`, `a|b`,
		`qu"ote`, `"lead`, `trail"`, `star*`,
	}
	for _, n := range rejected {
		if err := validateName(n); err == nil {
			t.Errorf("validateName(%q) accepted, want NoRetryError", n)
		} else if !fserrors.IsNoRetryError(err) {
			t.Errorf("validateName(%q) = %v, want NoRetryError-wrapped", n, err)
		}
	}

	accepted := []string{
		"plain.txt",
		"世界.txt", "Ünïcødé.txt", "中文 文件",
		"emoji😀.txt", "©®™✓", "°±²³", "→↔★☀",
		" leading space", "trailing space ",
		".hidden", "trailing dot.",
		"a	b.txt", "a\nb.txt", "~tilde", // encoder handles these
	}
	for _, n := range accepted {
		if err := validateName(n); err != nil {
			t.Errorf("validateName(%q) rejected: %v", n, err)
		}
	}

	// Empty and pure-path variants.
	if err := validateName(""); err != nil {
		t.Errorf("validateName(\"\") rejected: %v", err)
	}
	enc := encoder.MultiEncoder(yun139DefaultEncoding)
	f := &Fs{opt: Options{Enc: enc}}
	// (f *Fs).validateName encodes before checking, so a reserved character in
	// the leaf is escaped and accepted, while a reserved character in an
	// intermediate directory is not part of the leaf and is left alone.
	if err := f.validateName("dir/a*name.txt"); err != nil {
		t.Errorf("(f).validateName rejected a reserved char in the leaf, which the encoding escapes: %v", err)
	}
	if err := f.validateName("a:dir/name.txt"); err != nil {
		t.Errorf("(f).validateName rejected reserved char in parent dir: %v", err)
	}
	// The default encoding must escape every reserved character, or the guard
	// above would start refusing names the server can in fact store.
	for _, r := range yun139RejectedRunes {
		wire := enc.FromStandardName("a" + string(r) + "b")
		if err := validateName(wire); err != nil {
			t.Errorf("default encoding left reserved character %q on the wire as %q: %v", r, wire, err)
		}
	}
}

// TestSha256MidstateSinglePass proves the single-forward-pass midstate
// computation (one running SHA-256, captured at each part boundary) produces
// byte-identical H registers to the old per-part re-hash from offset 0. This
// is the safety net for the O(n^2)->O(n) refactor of the parallel upload
// midstate path; the server signs each part's context, so a drift here would
// silently break every multi-part upload.
func TestSha256MidstateSinglePass(t *testing.T) {
	data := make([]byte, 12*1024*1024)
	for i := range data {
		data[i] = byte(i * 31)
	}
	r := bytes.NewReader(data)
	buf := make([]byte, 1<<20)

	partSizes := []int64{5 * 1024 * 1024, 1 * 1024 * 1024, 7 * 1024 * 1024, 12 * 1024 * 1024}
	for _, partSize := range partSizes {
		parts := planParts(int64(len(data)), partSize)
		if len(parts) < 2 {
			continue
		}
		// Single forward pass, mirroring uploadFromRandom.
		h := sha256.New()
		fed := int64(0)
		single := make([][]uint32, len(parts)+1)
		for _, p := range parts {
			if p.offset > 0 {
				if err := sha256Feed(h, r, fed, p.offset, buf); err != nil {
					t.Fatalf("partSize %d: sha256Feed(%d,%d): %v", partSize, fed, p.offset, err)
				}
				fed = p.offset
				regs, _, err := api.Sha256Midstate(h)
				if err != nil {
					t.Fatalf("partSize %d: midstate: %v", partSize, err)
				}
				single[p.index] = regs
			}
		}
		// Reference: fresh hash from offset 0 to each part boundary.
		for _, p := range parts {
			if p.offset == 0 {
				continue
			}
			h2 := sha256.New()
			off := int64(0)
			for off < p.offset {
				want := p.offset - off
				if want > int64(len(buf)) {
					want = int64(len(buf))
				}
				nr, _ := r.ReadAt(buf[:want], off)
				h2.Write(buf[:nr])
				off += int64(nr)
			}
			ref, _, err := api.Sha256Midstate(h2)
			if err != nil {
				t.Fatalf("partSize %d part %d: ref midstate: %v", partSize, p.index, err)
			}
			if !reflect.DeepEqual(single[p.index], ref) {
				t.Errorf("partSize %d part %d: single-pass midstate != per-part midstate", partSize, p.index)
			}
		}
	}
}

// linkTransport answers the two calls PublicLink makes for a personal-space
// object: /hcy/file/list (to resolve it) and /hcy/file/getDownloadUrl (to mint
// the link). It records the expireSec the backend asked for.
type linkTransport struct {
	mu        sync.Mutex
	url       string
	expireSec float64
	minted    int
}

func (t *linkTransport) expiry() (float64, int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.expireSec, t.minted
}

func (t *linkTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	t.mu.Lock()
	defer t.mu.Unlock()
	reply := func(v any) (*http.Response, error) {
		b, _ := json.Marshal(v)
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(string(b))),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
		}, nil
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/hcy/file/list"):
		return reply(map[string]any{
			"success": true,
			"data":    map[string]any{"items": []map[string]any{fileEntry("file-1", "a.bin", 42)}},
		})
	case strings.HasSuffix(r.URL.Path, "/hcy/file/getDownloadUrl"):
		var req struct {
			ExpireSec float64 `json:"expireSec"`
		}
		_ = json.Unmarshal(body, &req)
		t.expireSec = req.ExpireSec
		t.minted++
		return reply(map[string]any{"success": true, "data": map[string]any{"url": t.url}})
	}
	return reply(map[string]any{"success": false, "code": "9999", "message": "unexpected " + r.URL.Path})
}

// TestUnitPublicLink locks the rclone link contract for yun139: a personal-space
// file resolves to the getDownloadUrl link, the requested expiry reaches the
// API (capped at the 24h the official client asks for), and a directory path
// fails with the bare sentinel without touching the network.
func TestUnitPublicLink(t *testing.T) {
	const want = "https://dl.example/a.bin?token=abc"
	tr := &linkTransport{url: want}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("dir", "dir-id")

	link, err := f.PublicLink(context.Background(), "dir/a.bin", fs.DurationOff, false)
	require.NoError(t, err)
	assert.Equal(t, want, link)
	secs, minted := tr.expiry()
	assert.Equal(t, 1, minted)
	assert.Equal(t, float64(defaultLinkExpireSec), secs, "no --expire must ask for the client's default")

	// --expire is passed through, and an over-long one is capped at 24h.
	_, err = f.PublicLink(context.Background(), "dir/a.bin", fs.Duration(time.Hour), false)
	require.NoError(t, err)
	secs, _ = tr.expiry()
	assert.Equal(t, float64(3600), secs)

	_, err = f.PublicLink(context.Background(), "dir/a.bin", fs.Duration(48*time.Hour), false)
	require.NoError(t, err)
	secs, _ = tr.expiry()
	assert.Equal(t, float64(maxLinkExpireSec), secs, "an expiry beyond 24h must be capped")

	// A directory path returns before NewObject, so the transport is never hit.
	fDir := newMoveTestFs(&linkTransport{url: want})
	_, err = fDir.PublicLink(context.Background(), "dir/sub/", fs.DurationOff, false)
	assert.Equal(t, fs.ErrorCantShareDirectories, err)
}

// TestUnitDownloadURLTTL locks the per-space cache windows: the personal cloud
// only grants a 15-minute link (whatever expireSec asks for), so its window must
// stay well under that, while the family cloud grants 24h.
func TestUnitDownloadURLTTL(t *testing.T) {
	f := newMoveTestFs(&linkTransport{})
	assert.Equal(t, personalDownloadURLTTL, f.downloadURLTTL())
	f.space = spaceFamily
	assert.Equal(t, familyDownloadURLTTL, f.downloadURLTTL())
	assert.Less(t, personalDownloadURLTTL, 15*time.Minute, "personal links only live 15 minutes")
	assert.Less(t, familyDownloadURLTTL, 24*time.Hour, "family links live 24 hours")
}

// familyLinkTransport answers the two family-cloud calls PublicLink makes:
// queryContentListV3 resolves the object and getFileDownLoadURLV2 mints the
// link.
type familyLinkTransport struct {
	mu     sync.Mutex
	url    string
	minted int
}

func (t *familyLinkTransport) calls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.minted
}

func (t *familyLinkTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	_ = r.Body.Close()
	t.mu.Lock()
	defer t.mu.Unlock()
	reply := func(v any) (*http.Response, error) {
		b, _ := json.Marshal(v)
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(string(b))),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
		}, nil
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "queryContentListV3"):
		return reply(map[string]any{
			"result":           map[string]any{"resultCode": "0", "resultDesc": "ok"},
			"path":             "root:/",
			"totalCount":       1,
			"cloudContentList": []map[string]any{familyFileEntry("file-1", "a.bin", 42)},
		})
	case strings.HasSuffix(r.URL.Path, "getFileDownLoadURLV2"):
		t.minted++
		return reply(map[string]any{
			"result":      map[string]any{"resultCode": "0", "resultDesc": "ok"},
			"downloadURL": t.url,
		})
	}
	return reply(map[string]any{"success": false, "code": "9999", "message": "unexpected " + r.URL.Path})
}

// TestUnitPublicLinkFamily covers the other space: the family cloud's link comes
// from getFileDownLoadURLV2, which has no expiry parameter, so a requested
// expire is ignored (with a warning) and the link is still minted per call.
func TestUnitPublicLinkFamily(t *testing.T) {
	const want = "https://dl.example/fam?a=1"
	tr := &familyLinkTransport{url: want}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	link, err := f.PublicLink(context.Background(), "a.bin", fs.DurationOff, false)
	require.NoError(t, err)
	assert.Equal(t, want, link)
	assert.Equal(t, 1, tr.calls())

	link, err = f.PublicLink(context.Background(), "a.bin", fs.Duration(time.Hour), false)
	require.NoError(t, err)
	assert.Equal(t, want, link)
	assert.Equal(t, 2, tr.calls(), "each link call must mint from the API")
}
