package fshttp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// Dialer structure contains default dialer and timeout, tclass support
type Dialer struct {
	net.Dialer
	timeout  time.Duration
	tclass   int
	family   familyMode
	cooldown time.Duration
	health   *familyHealth
}

// health is shared by every Dialer so that the lesson learnt by one request
// applies to the next one (a new Dialer is made for every dial).
var health = newFamilyHealth(time.Now, 2)

var familyWarnOnce sync.Once

// NewDialer creates a Dialer structure with Timeout, Keepalive,
// LocalAddr and DSCP set from rclone flags.
func NewDialer(ctx context.Context) *Dialer {
	ci := fs.GetConfig(ctx)
	dialer := &Dialer{
		Dialer: net.Dialer{
			Timeout:   time.Duration(ci.ConnectTimeout),
			KeepAlive: 30 * time.Second,
		},
		timeout:  time.Duration(ci.Timeout),
		tclass:   int(ci.TrafficClass),
		cooldown: time.Duration(ci.ConnectFamilyCooldown),
		health:   health,
	}
	mode, err := parseFamilyMode(ci.ConnectFamily)
	if err != nil {
		familyWarnOnce.Do(func() {
			fs.Logf(nil, "fshttp: %v; using the default dialler", err)
		})
		mode = familyAuto
	}
	dialer.family = mode
	if ci.BindAddr != nil {
		dialer.Dialer.LocalAddr = &net.TCPAddr{IP: ci.BindAddr}
	}
	return dialer
}

// Dial connects to the network address.
func (d *Dialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

var warnDSCPFail, warnDSCPWindows sync.Once

// applyTrafficClass sets the DSCP/TOS bits on a fresh connection.
func (d *Dialer) applyTrafficClass(c net.Conn) {
	if d.tclass == 0 {
		return
	}
	// IPv6 addresses must have two or more ":"
	var err error
	if strings.Count(c.RemoteAddr().String(), ":") > 1 {
		err = ipv6.NewConn(c).SetTrafficClass(d.tclass)
	} else {
		err = ipv4.NewConn(c).SetTOS(d.tclass)
		// Warn of silent failure on Windows (IPv4 only, IPv6 caught by error handler)
		if runtime.GOOS == "windows" {
			warnDSCPWindows.Do(func() {
				fs.LogLevelPrintf(fs.LogLevelWarning, nil, "dialer: setting DSCP on Windows/IPv4 fails silently; see https://github.com/golang/go/issues/42728")
			})
		}
	}
	if err != nil {
		warnDSCPFail.Do(func() {
			fs.LogLevelPrintf(fs.LogLevelWarning, nil, "dialer: failed to set DSCP socket options: %v", err)
		})
	}
}

// DialContext connects to the network address using the provided context.
func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	// If local address is 0.0.0.0 or ::0 force IPv4 or IPv6
	// This works around https://github.com/golang/go/issues/48723
	// Which means 0.0.0.0 and ::0 both bind to both IPv4 and IPv6
	if ip, ok := d.Dialer.LocalAddr.(*net.TCPAddr); ok && ip.IP.IsUnspecified() && (network == "tcp" || network == "udp") {
		if ip.IP.To4() != nil {
			network += "4" // IPv4 address
		} else {
			network += "6" // IPv6 address
		}
	}

	// An explicit --bind pins the family, so the family preference below (which
	// only reorders what the resolver returned) must stay out of the way.
	if d.family != familyAuto && network == "tcp" && d.Dialer.LocalAddr == nil {
		if c, err, handled := d.dialFamily(ctx, address); handled {
			return c, err
		}
	}

	c, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil {
		return c, err
	}

	d.applyTrafficClass(c)

	t := &timeoutConn{
		Conn:    c,
		timeout: d.timeout,
	}
	return t, t.nudgeDeadline()
}

// dialFamily dials address trying the resolved addresses in the order chosen by
// --connect-family, skipping any host/family pair that is cooling down.
//
// handled is false when the caller should use the standard dialler instead:
// the address is already an IP literal, or the lookup failed (in which case the
// standard path will surface the same error).
func (d *Dialer) dialFamily(ctx context.Context, address string) (c net.Conn, err error, handled bool) {
	host, port, splitErr := net.SplitHostPort(address)
	if splitErr != nil {
		return nil, nil, false
	}
	if net.ParseIP(host) != nil {
		// A literal has a fixed family: nothing to choose, and nothing to learn.
		return nil, nil, false
	}
	h := d.health
	if h == nil {
		h = health
	}
	addrs, lookupErr := lookupIPAddr(ctx, host)
	if lookupErr != nil || len(addrs) == 0 {
		return nil, nil, false
	}
	v4, v6, splitErr := splitCandidates(d.family, addrs)
	if splitErr != nil {
		return nil, nil, false
	}

	// Order the families, then drop the ones cooling down. If that would leave
	// nothing to try, ignore the cooldown: refusing to dial would deadlock a
	// host whose only address just failed.
	var order []struct {
		ip net.IP
		v6 bool
	}
	add := func(list []net.IPAddr, isV6 bool) {
		for _, a := range list {
			order = append(order, struct {
				ip net.IP
				v6 bool
			}{a.IP, isV6})
		}
	}
	switch d.family {
	case familyOnlyV4:
		add(v4, false)
	case familyOnlyV6:
		add(v6, true)
	case familyPreferV4:
		add(v4, false)
		add(v6, true)
	case familyPreferV6:
		add(v6, true)
		add(v4, false)
	}

	keep := make([]int, 0, len(order))
	for i, cand := range order {
		if !h.blocked(host, cand.v6) {
			keep = append(keep, i)
		}
	}
	if len(keep) == 0 {
		fs.Debugf(nil, "fshttp: %s: every candidate family is cooling down, ignoring the cooldown", host)
		for i := range order {
			keep = append(keep, i)
		}
	}

	var lastErr error
	for _, i := range keep {
		cand := order[i]
		network := "tcp4"
		if cand.v6 {
			network = "tcp6"
		}
		target := net.JoinHostPort(cand.ip.String(), port)
		conn, dialErr := d.Dialer.DialContext(ctx, network, target)
		if dialErr != nil {
			lastErr = dialErr
			if h.noteDialFailure(host, cand.v6, d.cooldown) {
				fs.Logf(nil, "fshttp: %s: IPv%d failed %d times in a row (%v), avoiding that family for %v",
					host, familyNum(cand.v6), health.dialThreshold, dialErr,
					d.cooldown.Round(time.Second))
				closeIdleConnections()
			}
			continue
		}
		h.noteDialSuccess(host, cand.v6)
		d.applyTrafficClass(conn)

		tc := &timeoutConn{Conn: conn, timeout: d.timeout}
		if err := tc.nudgeDeadline(); err != nil {
			_ = conn.Close()
			return nil, err, true
		}
		return &familyConn{
			Conn:     tc,
			host:     host,
			v6:       cand.v6,
			timeout:  d.timeout,
			cooldown: d.cooldown,
			health:   h,
		}, nil, true
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no address family left to try for %s", host)
	}
	return nil, lastErr, true
}

// A net.Conn that sets deadline for every Read/Write operation
type timeoutConn struct {
	net.Conn
	timeout time.Duration
}

// Nudge the deadline for an idle timeout on by c.timeout if non-zero
func (c *timeoutConn) nudgeDeadline() error {
	if c.timeout > 0 {
		return c.SetDeadline(time.Now().Add(c.timeout))
	}
	return nil
}

// Read bytes with rate limiting and idle timeouts
func (c *timeoutConn) Read(b []byte) (n int, err error) {
	// Ideally we would LimitBandwidth(len(b)) here and replace tokens we didn't use
	n, err = c.Conn.Read(b)
	accounting.TokenBucket.LimitBandwidth(accounting.TokenBucketSlotTransportRx, n)
	if err == nil && n > 0 && c.timeout > 0 {
		err = c.nudgeDeadline()
	}
	return n, err
}

// Write bytes with rate limiting and idle timeouts
func (c *timeoutConn) Write(b []byte) (n int, err error) {
	accounting.TokenBucket.LimitBandwidth(accounting.TokenBucketSlotTransportTx, len(b))
	n, err = c.Conn.Write(b)
	if err == nil && n > 0 && c.timeout > 0 {
		err = c.nudgeDeadline()
	}
	return n, err
}

// familyConn attributes a stalled connection to the host/family it was dialled
// for, so the next dial can prefer the other family.
type familyConn struct {
	net.Conn
	host     string
	v6       bool
	timeout  time.Duration
	cooldown time.Duration
	health   *familyHealth
}

func (c *familyConn) Read(b []byte) (int, error) {
	start := time.Now()
	n, err := c.Conn.Read(b)
	if err != nil {
		c.noteError(err, time.Since(start))
	}
	return n, err
}

func (c *familyConn) Write(b []byte) (int, error) {
	start := time.Now()
	n, err := c.Conn.Write(b)
	if err != nil {
		c.noteError(err, time.Since(start))
	}
	return n, err
}

// noteError demotes the host/family pair when a read or write was actually
// blocked before failing, which is what an idle deadline expiry during a
// transfer looks like.
//
// A pooled connection whose deadline expired while it was idle fails instantly
// when it is used again; blockedFor is then tiny and nothing is recorded.
func (c *familyConn) noteError(err error, blockedFor time.Duration) {
	if c.health == nil || err == nil || errors.Is(err, io.EOF) {
		return
	}
	if blockedFor < time.Second {
		return
	}
	if c.health.noteStall(c.host, c.v6, c.cooldown) {
		fs.Logf(nil, "fshttp: %s: IPv%d stopped making progress (blocked %v), avoiding that family for %v",
			c.host, familyNum(c.v6), blockedFor.Round(time.Second), c.cooldown.Round(time.Second))
		closeIdleConnections()
	}
}
