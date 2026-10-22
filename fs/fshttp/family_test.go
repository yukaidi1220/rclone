package fshttp

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeClock is a manually advanced clock for the health table.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

// dualListeners starts a v4 and a v6 listener on the same port and returns the
// port plus a counter of accepted connections per family.
func dualListeners(t *testing.T) (port string, v4, v6 *int) {
	t.Helper()
	l4, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l4.Close() })
	_, p, err := net.SplitHostPort(l4.Addr().String())
	require.NoError(t, err)

	l6, err := net.Listen("tcp6", "[::1]:"+p)
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	t.Cleanup(func() { _ = l6.Close() })

	var n4, n6 int
	go func() {
		for {
			c, err := l4.Accept()
			if err != nil {
				return
			}
			n4++
			_ = c.Close()
		}
	}()
	go func() {
		for {
			c, err := l6.Accept()
			if err != nil {
				return
			}
			n6++
			_ = c.Close()
		}
	}()
	return p, &n4, &n6
}

// newTestDialer builds a Dialer that shares the given health table and dials
// through a fake resolver returning ::1 first (as the real resolver usually
// does for a dual stack host).
func newTestDialer(t *testing.T, mode familyMode, h *familyHealth) *Dialer {
	t.Helper()
	oldLookup := lookupIPAddr
	lookupIPAddr = func(ctx context.Context, host string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("::1")}, {IP: net.ParseIP("127.0.0.1")}}, nil
	}
	t.Cleanup(func() { lookupIPAddr = oldLookup })
	return &Dialer{
		Dialer:   net.Dialer{Timeout: 2 * time.Second},
		timeout:  2 * time.Second,
		cooldown: 5 * time.Minute,
		family:   mode,
		health:   h,
	}
}

// dialOnce dials and returns the remote IP family.
func dialOnce(t *testing.T, d *Dialer, port string) bool {
	t.Helper()
	c, err := d.DialContext(context.Background(), "tcp", net.JoinHostPort("dualstack.test", port))
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	ip := net.ParseIP(c.RemoteAddr().(*net.TCPAddr).IP.String())
	require.NotNil(t, ip)
	return ip.To4() == nil // true when v6
}

// TestFamilySwitchesWhenV6Stalls is the core contract: the first dial takes v6,
// a stall on v6 demotes it, the next dial takes v4, and the family comes back
// once the cooldown expires.
func TestFamilySwitchesWhenV6Stalls(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1700000000, 0)}
	h := newFamilyHealth(clock.now, 2)
	d := newTestDialer(t, familyPreferV6, h)
	port, _, _ := dualListeners(t)

	assert.True(t, dialOnce(t, d, port), "first dial should use IPv6 under prefer-v6")

	// A read blocked for longer than the idle timeout and then failing is what
	// a dark v6 path looks like: record it the way familyConn.noteError does.
	require.True(t, h.noteStall("dualstack.test", true, d.cooldown))

	assert.False(t, dialOnce(t, d, port),
		"after v6 stalls the next dial must fall back to IPv4")

	assert.True(t, h.blocked("dualstack.test", true))
	clock.advance(4 * time.Minute)
	assert.True(t, h.blocked("dualstack.test", true), "cooldown must last")
	clock.advance(2 * time.Minute)
	assert.False(t, h.blocked("dualstack.test", true), "cooldown must expire")

	assert.True(t, dialOnce(t, d, port), "v6 must be preferred again after the cooldown")
}

// TestFamilyDialFailureThreshold checks that a transient dial failure does not
// demote, but two in a row do.
func TestFamilyDialFailureThreshold(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1700000000, 0)}
	h := newFamilyHealth(clock.now, 2)
	assert.False(t, h.noteDialFailure("h", true, time.Minute))
	assert.True(t, h.blocked("h", true) == false)
	assert.True(t, h.noteDialFailure("h", true, time.Minute))
	assert.True(t, h.blocked("h", true), "two dial failures must demote")

	// A success forgets the dial failures but must not clear a cooldown.
	h.noteDialSuccess("h", true)
	assert.True(t, h.blocked("h", true), "success must not clear a cooldown")

	clock.advance(61 * time.Second)
	assert.False(t, h.blocked("h", true))
	assert.False(t, h.noteDialFailure("h", true, time.Minute), "counter restarts after the cooldown")
}

// TestFamilyFailOpenWhenAllBlocked makes sure we never refuse to dial just
// because every family is cooling down.
func TestFamilyFailOpenWhenAllBlocked(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1700000000, 0)}
	h := newFamilyHealth(clock.now, 1)
	h.noteStall("dualstack.test", true, time.Hour)
	h.noteStall("dualstack.test", false, time.Hour)
	d := newTestDialer(t, familyOnlyV4, h)

	port, v4, _ := dualListeners(t)
	assert.False(t, dialOnce(t, d, port), "must still dial IPv4 even though it is cooling down")
	assert.Equal(t, 1, *v4)
}

// TestFamilyOnlyV4IgnoresV6 checks --connect-family=v4 never touches v6.
func TestFamilyOnlyV4IgnoresV6(t *testing.T) {
	h := newFamilyHealth(time.Now, 2)
	d := newTestDialer(t, familyOnlyV4, h)
	port, v4, v6 := dualListeners(t)

	for i := 0; i < 3; i++ {
		assert.False(t, dialOnce(t, d, port), "only-v4 dial must land on IPv4")
	}
	assert.Equal(t, 3, *v4)
	assert.Equal(t, 0, *v6, "only-v4 must never dial the v6 address")
}

// TestParseFamilyMode documents the flag surface, including the fact that the
// empty value is the old behaviour.
func TestParseFamilyMode(t *testing.T) {
	for in, want := range map[string]familyMode{
		"":          familyAuto,
		"prefer-v4": familyPreferV4,
		"prefer-v6": familyPreferV6,
		"v4":        familyOnlyV4,
		"v6":        familyOnlyV6,
	} {
		got, err := parseFamilyMode(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	_, err := parseFamilyMode("v5")
	assert.Error(t, err)
}

// TestSplitCandidates covers the only-v4/only-v6 "no such family" errors.
func TestSplitCandidates(t *testing.T) {
	onlyV6 := []net.IPAddr{{IP: net.ParseIP("::1")}}
	onlyV4 := []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}

	v4, v6, err := splitCandidates(familyPreferV4, onlyV6)
	require.NoError(t, err)
	assert.Empty(t, v4, "no IPv4 in the answer")
	assert.Len(t, v6, 1, "the v6 candidate must survive so the dial can fall through to it")

	_, _, err = splitCandidates(familyOnlyV4, onlyV6)
	assert.Error(t, err)
	_, _, err = splitCandidates(familyOnlyV6, onlyV4)
	assert.Error(t, err)
}
