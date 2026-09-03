package yun139

import (
	"testing"
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
