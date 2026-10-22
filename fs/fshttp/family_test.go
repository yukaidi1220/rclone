package fshttp

import (
	"context"
	"net"
	"os"
	"sync/atomic"
	"syscall"
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
func dualListeners(t *testing.T) (port string, v4, v6 *int32) {
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

	n4, n6 := new(int32), new(int32)
	go func() {
		for {
			c, err := l4.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(n4, 1)
			_ = c.Close()
		}
	}()
	go func() {
		for {
			c, err := l6.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(n6, 1)
			_ = c.Close()
		}
	}()
	return p, n4, n6
}

// newTestDialer builds a Dialer that shares the given health table and dials
// through a fake resolver returning ::1 first (as the real resolver usually
// does for a dual stack host).
func newTestDialer(t *testing.T, mode familyMode, h *familyHealth) *Dialer {
	return newTestDialerWithAddrs(t, mode, h,
		[]net.IPAddr{{IP: net.ParseIP("::1")}, {IP: net.ParseIP("127.0.0.1")}})
}

func newTestDialerWithAddrs(t *testing.T, mode familyMode, h *familyHealth, addrs []net.IPAddr) *Dialer {
	t.Helper()
	oldLookup := lookupIPAddr
	lookupIPAddr = func(ctx context.Context, host string) ([]net.IPAddr, error) {
		return addrs, nil
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

// dialOnce dials and reports whether the connection landed on IPv6.
func dialOnce(t *testing.T, d *Dialer, port string) bool {
	t.Helper()
	c, err := d.DialContext(context.Background(), "tcp", net.JoinHostPort("dualstack.test", port))
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	ip := net.ParseIP(c.RemoteAddr().(*net.TCPAddr).IP.String())
	require.NotNil(t, ip)
	return ip.To4() == nil
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

	require.True(t, h.noteStall("dualstack.test", true, d.cooldown))
	assert.False(t, dialOnce(t, d, port), "after v6 stalls the next dial must fall back to IPv4")

	assert.True(t, h.blocked("dualstack.test", true))
	clock.advance(4 * time.Minute)
	assert.True(t, h.blocked("dualstack.test", true), "cooldown must last")
	clock.advance(2 * time.Minute)
	assert.False(t, h.blocked("dualstack.test", true), "cooldown must expire")

	assert.True(t, dialOnce(t, d, port), "v6 must be preferred again after the cooldown")
}

// scriptedConn returns a configurable result after a configurable delay, which
// is enough to exercise familyConn's attribution rules.
type scriptedConn struct {
	block time.Duration
	err   error
}

func (c *scriptedConn) Read(b []byte) (int, error)  { time.Sleep(c.block); return 0, c.err }
func (c *scriptedConn) Write(b []byte) (int, error) { time.Sleep(c.block); return 0, c.err }
func (c *scriptedConn) Close() error                { return nil }
func (c *scriptedConn) LocalAddr() net.Addr         { return &net.TCPAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr        { return &net.TCPAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error {
	return nil
}
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

func newScriptedFamilyConn(h *familyHealth, block time.Duration, err error) *familyConn {
	return &familyConn{
		Conn:     &scriptedConn{block: block, err: err},
		host:     "stall.test",
		v6:       true,
		timeout:  time.Minute,
		cooldown: time.Minute,
		health:   h,
	}
}

// TestFamilyConnOnlyDemotesOnRealStall pins the attribution rule: only an idle
// deadline that fired while the read was blocked counts. A prompt failure, even
// a deadline error from a pooled connection, must not demote the family.
func TestFamilyConnOnlyDemotesOnRealStall(t *testing.T) {
	h := newFamilyHealth(time.Now, 2)

	c := newScriptedFamilyConn(h, 1100*time.Millisecond, os.ErrDeadlineExceeded)
	_, err := c.Read(make([]byte, 1))
	require.Error(t, err)
	assert.True(t, h.blocked("stall.test", true), "a blocked read that hit the idle deadline must demote")

	h2 := newFamilyHealth(time.Now, 2)
	prompt := newScriptedFamilyConn(h2, 10*time.Millisecond, os.ErrDeadlineExceeded)
	_, err = prompt.Read(make([]byte, 1))
	require.Error(t, err)
	assert.False(t, h2.blocked("stall.test", true), "a deadline that was already expired must not demote")

	h3 := newFamilyHealth(time.Now, 2)
	reset := newScriptedFamilyConn(h3, 1100*time.Millisecond, syscall.ECONNRESET)
	_, err = reset.Read(make([]byte, 1))
	require.Error(t, err)
	assert.False(t, h3.blocked("stall.test", true), "a reset is not a stall")
}

// TestFamilyDialFailureThreshold checks that a transient dial failure does not
// demote, but two in a row do.
func TestFamilyDialFailureThreshold(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1700000000, 0)}
	h := newFamilyHealth(clock.now, 2)
	assert.False(t, h.noteDialFailure("h", true, time.Minute))
	assert.False(t, h.blocked("h", true), "one failure is not enough")
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
	require.Eventually(t, func() bool { return atomic.LoadInt32(v4) == 1 }, 2*time.Second, 10*time.Millisecond)
}

// TestFamilyOnlyV4IgnoresV6 checks --connect-family=v4 never touches v6.
func TestFamilyOnlyV4IgnoresV6(t *testing.T) {
	h := newFamilyHealth(time.Now, 2)
	d := newTestDialer(t, familyOnlyV4, h)
	port, v4, v6 := dualListeners(t)

	for i := 0; i < 3; i++ {
		assert.False(t, dialOnce(t, d, port), "only-v4 dial must land on IPv4")
	}
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(v4) == 3 && atomic.LoadInt32(v6) == 0
	}, 2*time.Second, 10*time.Millisecond, "only-v4 must dial IPv4 and never IPv6")
}

// TestFamilyOnlyModeIsEnforced makes sure a pin is not silently downgraded to
// the other family when the host has no address of the pinned family.
func TestFamilyOnlyModeIsEnforced(t *testing.T) {
	h := newFamilyHealth(time.Now, 2)
	aaaaOnly := []net.IPAddr{{IP: net.ParseIP("::1")}}
	d := newTestDialerWithAddrs(t, familyOnlyV4, h, aaaaOnly)

	_, err := d.DialContext(context.Background(), "tcp", "v6only.test:443")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connect-family=v4")

	// An IP literal of the wrong family must be refused as well, otherwise a
	// redirect to a literal would slip past the pin.
	_, err = d.DialContext(context.Background(), "tcp", "[::1]:443")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IPv6 address")
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
