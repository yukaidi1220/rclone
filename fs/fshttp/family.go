package fshttp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"
)

// familyMode is how the dialer should order the resolved addresses.
//
// familyAuto keeps Go's default behaviour (Happy Eyeballs over both
// families) and disables every check in this file.
type familyMode int

const (
	familyAuto familyMode = iota
	familyPreferV4
	familyPreferV6
	familyOnlyV4
	familyOnlyV6
)

func parseFamilyMode(s string) (familyMode, error) {
	switch s {
	case "":
		return familyAuto, nil
	case "prefer-v4":
		return familyPreferV4, nil
	case "prefer-v6":
		return familyPreferV6, nil
	case "v4":
		return familyOnlyV4, nil
	case "v6":
		return familyOnlyV6, nil
	}
	return familyAuto, fmt.Errorf("unknown --connect-family %q", s)
}

// familyKey identifies one host's one address family.
//
// The key is per host, not per IP: several of the hosts we dial share CDN
// edges, and a v6 path that has gone dark is a property of the host we are
// talking to (it may resolve to a different address on the next lookup
// anyway). Keying per IP would also let one transient failure on a shared edge
// poison unrelated hosts.
type familyKey struct {
	host string
	v6   bool
}

// familyHealth remembers which host/family pairs have recently failed.
//
// Two failure kinds are counted separately:
//
//   - dial failures (connect refused/unreachable/timeout): a single one can be
//     a blip, so demotion needs dialThreshold of them in a row.
//   - an idle deadline firing while a read/write was actually blocked: by
//     definition no byte moved for the whole timeout, so a single one demotes.
//
// A successful dial forgets the accumulated dial failures; it does not clear a
// cooldown, otherwise a healthy parallel transfer could immediately re-enable a
// family that just stalled.
type familyHealth struct {
	mu        sync.Mutex
	dialFails map[familyKey]int
	blockedTo map[familyKey]time.Time
	now       func() time.Time

	dialThreshold int
}

func newFamilyHealth(now func() time.Time, dialThreshold int) *familyHealth {
	return &familyHealth{
		dialFails:     map[familyKey]int{},
		blockedTo:     map[familyKey]time.Time{},
		now:           now,
		dialThreshold: dialThreshold,
	}
}

// blocked reports whether the pair is currently cooling down.
func (h *familyHealth) blocked(host string, v6 bool) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := familyKey{host, v6}
	until, ok := h.blockedTo[key]
	if !ok {
		return false
	}
	if !h.now().Before(until) {
		delete(h.blockedTo, key)
		return false
	}
	return true
}

// noteDialFailure records one failed connection attempt. It returns true when
// this failure demoted the pair, so the caller can log and drop pooled
// connections to it.
func (h *familyHealth) noteDialFailure(host string, v6 bool, cooldown time.Duration) bool {
	if cooldown <= 0 {
		return false
	}
	key := familyKey{host, v6}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dialFails[key]++
	if h.dialFails[key] < h.dialThreshold {
		return false
	}
	delete(h.dialFails, key)
	h.blockedTo[key] = h.now().Add(cooldown)
	return true
}

// noteStall records an idle deadline that fired while data was expected. One is
// enough: it means nothing moved for the whole timeout.
func (h *familyHealth) noteStall(host string, v6 bool, cooldown time.Duration) bool {
	if cooldown <= 0 {
		return false
	}
	key := familyKey{host, v6}
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.dialFails, key)
	until := h.now().Add(cooldown)
	if cur, ok := h.blockedTo[key]; ok && cur.After(until) {
		until = cur
	}
	h.blockedTo[key] = until
	return true
}

// noteDialSuccess forgets the accumulated dial failures for the pair.
func (h *familyHealth) noteDialSuccess(host string, v6 bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.dialFails, familyKey{host, v6})
}

// transports keeps every transport built by NewTransportCustom so a demotion
// can drop the pooled connections that would otherwise keep carrying requests
// to the address family we just gave up on.
var transports sync.Map // closer -> struct{}

func registerTransport(t interface{ CloseIdleConnections() }) {
	if t != nil {
		transports.Store(t, struct{}{})
	}
}

// closeIdleConnections drops the idle connections of every transport we made.
//
// http.Transport has no per-host equivalent, so this is all-or-nothing; it only
// happens on a demotion, which is a rare event.
func closeIdleConnections() {
	transports.Range(func(k, _ any) bool {
		if t, ok := k.(interface{ CloseIdleConnections() }); ok {
			t.CloseIdleConnections()
		}
		return true
	})
}

// splitCandidates splits resolved addresses into the two families.
//
// An "only" mode with no address of that family returns an error; a "prefer"
// mode simply ends up with an empty preferred list and falls through to the
// other family.
func splitCandidates(mode familyMode, addrs []net.IPAddr) (v4, v6 []net.IPAddr, err error) {
	for _, a := range addrs {
		if a.IP.To4() != nil {
			v4 = append(v4, a)
		} else if a.IP.To16() != nil {
			v6 = append(v6, a)
		}
	}
	if mode == familyOnlyV4 && len(v4) == 0 {
		return nil, nil, fmt.Errorf("no IPv4 address")
	}
	if mode == familyOnlyV6 && len(v6) == 0 {
		return nil, nil, fmt.Errorf("no IPv6 address")
	}
	return v4, v6, nil
}

func familyNum(v6 bool) int {
	if v6 {
		return 6
	}
	return 4
}

// lookupIPAddr is a variable so tests can inject addresses.
var lookupIPAddr = func(ctx context.Context, host string) ([]net.IPAddr, error) {
	return net.DefaultResolver.LookupIPAddr(ctx, host)
}
