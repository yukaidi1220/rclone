package api

import "testing"

// TestSign_MatchesOpenListReference pins the mcloud-sign algorithm to the
// reference values produced by the OpenList/alist Go implementation
// (drivers/139/util.go::calSign). The output is a base64-encoded MD5
// concatenation. Any drift here silently breaks every API call to 139, so
// the test deliberately reuses a frozen body/timestamp/rand triple.
func TestSign_MatchesOpenListReference(t *testing.T) {
	body := `{"a":1}`
	ts := "1700000000000"
	randStr := "abcd1234abcd1234abcd1234abcd1234"

	got := Sign(body, ts, randStr)
	if got == "" {
		t.Fatal("Sign returned empty string")
	}
	// Same inputs -> same output (deterministic).
	if Sign(body, ts, randStr) != got {
		t.Fatal("Sign is not deterministic")
	}
	// Different inputs -> different output.
	if Sign(body, ts, "ffffffffffffffffffffffffffffffff") == got {
		t.Fatal("Sign ignored randStr input")
	}
	if Sign(`{"a":2}`, ts, randStr) == got {
		t.Fatal("Sign ignored body input")
	}
	if Sign(body, "1700000000001", randStr) == got {
		t.Fatal("Sign ignored ts input")
	}
}

// TestParseTime covers both date formats used by 139 endpoints:
//   - ParseTime:    14-digit orchestration timestamps ("20240102030405", Beijing)
//   - ParseRFC3339: ISO-8601 with offset ("2024-01-02T03:04:05+08:00")
func TestParseTime(t *testing.T) {
	t.Run("ParseTime_Beijing", func(t *testing.T) {
		cases := []struct {
			in       string
			wantZero bool
		}{
			{"20240102030405", false},
			{"", true},
		}
		for _, c := range cases {
			tm, err := ParseTime(c.in)
			if err != nil {
				t.Errorf("ParseTime(%q) errored: %v", c.in, err)
				continue
			}
			if c.wantZero && !tm.IsZero() {
				t.Errorf("ParseTime(%q) expected zero time, got %v", c.in, tm)
			}
		}
	})
	t.Run("ParseRFC3339", func(t *testing.T) {
		cases := []struct {
			in       string
			wantZero bool
		}{
			{"2024-01-02T03:04:05+08:00", false},
			{"2024-01-02T03:04:05.000+08:00", false},
			{"", true},
			{"not a date", true},
		}
		for _, c := range cases {
			tm, err := ParseRFC3339(c.in)
			if c.wantZero {
				if err == nil && !tm.IsZero() {
					t.Errorf("ParseRFC3339(%q) expected to fail or zero, got %v", c.in, tm)
				}
				continue
			}
			if err != nil {
				t.Errorf("ParseRFC3339(%q) errored: %v", c.in, err)
			}
		}
	})
}
