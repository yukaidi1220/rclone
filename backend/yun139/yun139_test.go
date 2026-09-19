package yun139

import (
	"testing"

	"github.com/rclone/rclone/fs/fserrors"
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

// TestValidateName pins the client-side name rejection: the 139 server
// returns RSP_CODE 04000002 for the eight reserved ASCII characters
// `" * : < > ? \ |` (probe 2026-09-19), so validateName must refuse them
// with NoRetryError instead of pushing the name to the backend. Everything
// the encoder can handle (control chars, leading/trailing space·dot,
// leading tilde) and everything the server accepts (CJK, é, symbols,
// emoji) must pass.
func TestValidateName(t *testing.T) {
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
		"a\tb.txt", "a\nb.txt", "~tilde", // encoder handles these
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
	f := &Fs{}
	// (f *Fs).validateName checks only the base leaf, so a reserved char in
	// an intermediate directory must NOT trigger the file-name rejection.
	if err := f.validateName("dir/a*name.txt"); err == nil {
		t.Errorf("(f).validateName rejected path with reserved char in leaf: want error")
	}
	if err := f.validateName("a:dir/name.txt"); err != nil {
		t.Errorf("(f).validateName rejected reserved char in parent dir: %v", err)
	}
}
