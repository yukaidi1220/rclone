package wopan

// Upload dial targets.
//
// The server assigns one upload endpoint per account (GetZoneInfo), so an
// account's whole upload volume leaves through one source IP. The operator can
// put self-built relays in front of that endpoint to spread it. This file owns
// that choice; wopan.go keeps the protocol.
//
// Two rules come from the operator, not from the protocol:
//   - Every relay's IPv4 is tried before any relay's IPv6. Measured 2026-10-23,
//     the relay path stalled 398 times on IPv6 against 8 on IPv4, so interleaving
//     the families would make each part pay an IPv6 probe first.
//   - A dial target is dead when an attempt fails before a complete HTTP response
//     arrived: no route, TLS broken, no response headers within --timeout, a
//     stalled write, or the per-attempt wall clock. An HTTP status, including 5xx,
//     says nothing about the target - a relay that forwarded a bare 500 answered.
//     Three shapes are not evidence against it either: the wall clock expiring
//     while the part was still being fed, the part's own source having failed, and
//     a pooled connection the far end had already closed.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rclone/rclone/fs"
)

// Address families a request can be pinned to at dial time.
const (
	familyAny = 0 // no pin: the endpoint's own family selection applies
	familyV4  = 4
	familyV6  = 6
)

// defaultRelayCooldown is the fallback when --connect-family-cooldown is unset.
// The same 5 minutes the address-family table uses; no new knob.
const defaultRelayCooldown = 5 * time.Minute

// defaultRelayPartDeadline bounds one attempt's wall clock for one part of
// relayPartFloorBytes, which is the default --wopan-chunk-size.
//
// --timeout is an idle clock that renews on every byte, so a target that trickles
// never trips it: measured 2026-10-21 a single part held a worker for 91 minutes.
// The bound is a constant, so it is applied per 8 MiB of part - the bandwidth it
// demands is the same 0.22 Mbit/s whether the part is 8 MiB or the 64 MiB a
// default --wopan-upload_cutoff single-part upload can reach, and a deployment
// that raises chunk_size far above 8 MiB gets a longer clock rather than an
// unreachable demand. There is no user-facing knob for it
// (relayPartDeadlineOverride is test-only).
const defaultRelayPartDeadline = 5 * time.Minute

// relayPartFloorBytes is the part size defaultRelayPartDeadline is stated for.
const relayPartFloorBytes = 8 << 20

// dialTarget is one place an upload part can be POSTed.
type dialTarget struct {
	host   string // host[:port] used in the request URL
	family int    // familyV4 or familyV6 for a relay, familyAny for direct
	direct bool   // the endpoint itself, the last resort
}

// deadKey identifies a relay plus the family that failed.
func (t dialTarget) deadKey() string {
	return t.host + "|" + strconv.Itoa(t.family)
}

// describe is the target as it appears in logs.
func (t dialTarget) describe() string {
	if t.direct {
		return t.host + " (direct)"
	}
	return fmt.Sprintf("%s IPv%d", t.host, t.family)
}

// normalizeHostPort validates one side of an "endpoint=relay" item and fills in
// the default port so that "x.pan.wo.cn" and "x.pan.wo.cn:443" are one key.
func normalizeHostPort(raw, what string) (string, error) {
	s := strings.ToLower(strings.TrimSpace(raw))
	s = strings.TrimSuffix(s, ".")
	if s == "" {
		return "", fmt.Errorf("%s is empty", what)
	}
	// A host is a host: anything that belongs to a URL, a credential or a second
	// value is a malformed item, not something to dial.
	if strings.ContainsAny(s, "=@/\\ \t\r\n\"'") || strings.Contains(s, "://") {
		return "", fmt.Errorf("%s %q: expected host or host:port, without scheme, path or credentials", what, raw)
	}
	if !strings.Contains(s, ":") {
		return s + ":443", nil
	}
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return "", fmt.Errorf("%s %q: %w", what, raw, err)
	}
	if host == "" {
		return "", fmt.Errorf("%s %q: empty host", what, raw)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", fmt.Errorf("%s %q: port %q is not 1-65535", what, raw, port)
	}
	return net.JoinHostPort(host, strconv.Itoa(n)), nil
}

// parseRelayConfig turns the option items into an ordered relay list per
// endpoint. The port is part of an endpoint's identity: bjupload.pan.wo.cn and
// bjupload.pan.wo.cn:30443 are different servers.
func parseRelayConfig(items []string) (map[string][]string, error) {
	out := make(map[string][]string, len(items))
	seen := make(map[string]bool, len(items))
	for i, item := range items {
		endpoint, relay, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("wopan: upload_relays item %d %q: expected \"endpoint=relay\"", i+1, item)
		}
		epKey, err := normalizeHostPort(endpoint, fmt.Sprintf("upload_relays item %d endpoint", i+1))
		if err != nil {
			return nil, fmt.Errorf("wopan: %w", err)
		}
		relayKey, err := normalizeHostPort(relay, fmt.Sprintf("upload_relays item %d relay", i+1))
		if err != nil {
			return nil, fmt.Errorf("wopan: %w", err)
		}
		if epKey == relayKey {
			return nil, fmt.Errorf("wopan: upload_relays item %d: endpoint and relay are the same host %q", i+1, epKey)
		}
		dupKey := epKey + "=" + relayKey
		if seen[dupKey] {
			// Refusing a repeat is the cheap way to keep "N relays" meaning N
			// source IPs.
			return nil, fmt.Errorf("wopan: upload_relays: %q is listed twice", dupKey)
		}
		seen[dupKey] = true
		out[epKey] = append(out[epKey], relayKey)
	}
	return out, nil
}

// buildCandidates returns the attempt order for one endpoint: every relay's IPv4
// in written order, then every relay's IPv6, then the endpoint itself.
func buildCandidates(origin string, relays []string) []dialTarget {
	cands := make([]dialTarget, 0, 2*len(relays)+1)
	for _, r := range relays {
		cands = append(cands, dialTarget{host: r, family: familyV4})
	}
	for _, r := range relays {
		cands = append(cands, dialTarget{host: r, family: familyV6})
	}
	cands = append(cands, dialTarget{host: origin, family: familyAny, direct: true})
	return cands
}

// pickTarget chooses the candidate to use now.
//
// IPv4 candidates are preferred as a group, so a relay whose IPv4 is dead does
// not pull traffic onto its IPv6 while another relay's IPv4 is still healthy.
// rotate spreads the steady state across the live candidates instead of parking
// every session on the first one. ok is false when cands holds no direct entry.
func pickTarget(cands []dialTarget, isDead func(dialTarget) bool, rotate int) (int, bool) {
	for _, fam := range []int{familyV4, familyV6} {
		var live []int
		for i, c := range cands {
			if c.family == fam && !isDead(c) {
				live = append(live, i)
			}
		}
		if len(live) > 0 {
			return live[rotate%len(live)], true
		}
	}
	for i, c := range cands {
		if c.direct {
			return i, true
		}
	}
	return 0, false
}

// uploadRoute is the dial target a single upload session is sending through.
//
// It is shared by every part of the session: the session's uniqueId spans the
// whole file, and whether the server cares that a session's parts arrived from
// different source IPs has not been measured, so a part does not move on its own
// accord. It moves only when an attempt failed.
//
// failed is the session's own list of targets not to come back to. The shared
// cooldown table cannot play that role: its window can expire while a long
// session is still running, and the session would then POST to the same
// host-and-family a second time.
type uploadRoute struct {
	// originURL is the endpoint exactly as it was resolved. The direct target
	// keeps using it verbatim, so a remote with no relay table behaves
	// byte-for-byte as it did before relays existed.
	originURL string
	scheme    string
	// escapedPath is the endpoint's own path as it was written, empty for the usual
	// scheme://host form. The upload2C suffix is appended to it, and a relay carries
	// the same prefix, so an endpoint behind a mount point works on both branches
	// and an escaped character is not decoded on one of them only.
	escapedPath string
	cands       []dialTarget
	mu          sync.Mutex
	cur         int
	rotate      int
	relaysSet   bool            // an entry existed in the relay table for this endpoint
	failed      map[string]bool // deadKeys this session must not retry
}

func (r *uploadRoute) target() dialTarget {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cands[r.cur]
}

// urlFor is the request origin for a target.
func (r *uploadRoute) urlFor(t dialTarget) string {
	if t.direct {
		return r.originURL
	}
	return r.scheme + "://" + t.host + r.escapedPath
}

// hasRelays reports whether the table offered anything beyond the endpoint.
func (r *uploadRoute) hasRelays() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.relaysSet && len(r.cands) > 1
}

// attemptCount is how many POSTs a session may make for one part.
func (r *uploadRoute) attemptCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.cands)
}

// unavailable is the predicate pickTarget uses for this session: a target is out
// if the shared table is cooling it or this session already failed on it.
func (r *uploadRoute) unavailable(f *Fs, now time.Time) func(dialTarget) bool {
	return func(target dialTarget) bool {
		if target.direct {
			return false
		}
		if r.failed[target.deadKey()] {
			return true
		}
		return f.relayDead.coolingAt(target, now)
	}
}

// observedTarget is the dial target the failure can be charged to.
//
// The pin only binds new connections, so an attempt can ride an idle pooled
// connection of another family. When it did, the family on the wire is the one
// that failed; the pinned family never got a packet and must stay available to
// this session and out of the cooldown table.
func observedTarget(failed dialTarget, observedFamily int) dialTarget {
	if failed.direct || observedFamily == familyAny || observedFamily == failed.family {
		return failed
	}
	return dialTarget{host: failed.host, family: observedFamily}
}

// noteDead records that the attempt on failed did not complete, and returns the
// target to try next.
//
// The failed target is passed in rather than read from the cursor: several
// workers post parts of one session at the same time, so by the time a failure
// is reported the cursor may already have moved to a healthy relay. Blaming the
// cursor would park a relay that never failed.
//
// Such a stale report is still recorded, but it must not move the cursor again:
// a worker that was still POSTing while the session failover ran would otherwise
// kick the session off the relay that is serving it now.
//
// observedFamily is the family seen on the wire when a connection existed, and
// familyAny when the dial never produced one: a relay whose IPv6 carried the
// request must not cost its IPv4 the cooldown.
//
// blame is false for a failure that cannot be pinned on the target - the wall
// clock expiring while the source was still feeding the part, the source itself
// erroring, and a pooled connection the far end had already closed. The session
// still moves off the first two, they just do not take the shared table down.
//
// The direct endpoint never enters the table. It is the last resort, so cooling
// it down would leave nothing to fall back to.
func (r *uploadRoute) noteDead(f *Fs, failed dialTarget, observedFamily int, blame bool, cooldown time.Duration, cause error, now time.Time) (next dialTarget, moved bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	charged := observedTarget(failed, observedFamily)
	r.failed[charged.deadKey()] = true
	announced := false
	if blame && !charged.direct {
		announced = f.relayDead.parkRelay(charged, cooldown, now)
	}
	next = r.cands[r.cur]
	if failed == next {
		if idx, ok := pickTarget(r.cands, r.unavailable(f, now), r.rotate+1); ok && idx != r.cur {
			r.cur = idx
			r.rotate++
			next, moved = r.cands[idx], true
		}
	}
	if announced {
		fs.Logf(f, "wopan: relay %s parked for %v after a failed attempt (%v); next attempt goes to %s",
			charged.describe(), cooldown, cause, next.describe())
	} else if !charged.direct {
		fs.Debugf(f, "wopan: part moves off %s after a failed attempt (%v); next attempt goes to %s",
			failed.describe(), cause, next.describe())
	}
	return next, moved
}

// relayCooldownTable parks relay+family targets until their cooldown expires.
// It hangs off Fs as a pointer for the same reason zoneMu does: newFs copies the
// Fs value once the userId is known, and a lock inside a copied Fs is a vet
// failure waiting to happen.
type relayCooldownTable struct {
	mu       sync.Mutex
	deadline map[string]time.Time
}

func newRelayCooldownTable() *relayCooldownTable {
	return &relayCooldownTable{deadline: map[string]time.Time{}}
}

// parkRelay puts a target out of rotation until now+cooldown. announced is true
// only on the transition, so a burst of failures against one relay logs one line
// instead of one per part.
func (t *relayCooldownTable) parkRelay(target dialTarget, cooldown time.Duration, now time.Time) (announced bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := target.deadKey()
	if until, ok := t.deadline[key]; ok && until.After(now) {
		return false
	}
	t.deadline[key] = now.Add(cooldown)
	return true
}

// coolingAt reports whether a target is inside its cooldown window at now.
func (t *relayCooldownTable) coolingAt(target dialTarget, now time.Time) bool {
	if target.direct {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	until, ok := t.deadline[target.deadKey()]
	return ok && until.After(now)
}

// coolingTargets is the isDead predicate pickTarget wants, bound to one instant.
func (t *relayCooldownTable) coolingTargets(now time.Time) func(dialTarget) bool {
	return func(target dialTarget) bool { return t.coolingAt(target, now) }
}

// parkedByKey reads the table by its raw key. The tests need this because the
// property they check is "nothing was recorded for this target", and coolingAt
// answers that for a direct target without ever reading the table - it would
// pass for the wrong reason.
func (t *relayCooldownTable) parkedByKey(key string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	until, ok := t.deadline[key]
	return ok && until.After(now)
}

// newUploadRoute resolves the dial targets for one endpoint and picks the target
// this session starts on. rotate comes from the session counter, so the steady
// state spreads across the live relays instead of parking on the first one.
func (f *Fs) newUploadRoute(endpoint string, rotate int) (*uploadRoute, error) {
	origin, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("wopan: parse upload endpoint %q: %w", endpoint, err)
	}
	if origin.Host == "" || origin.Scheme == "" {
		return nil, fmt.Errorf("wopan: upload endpoint %q is not a scheme://host URL", endpoint)
	}
	// upload_zone may carry a path: rclone used to keep it and append the
	// upload2C suffix, so an endpoint behind a mount point has to keep working.
	// A query cannot compose with that suffix and is refused.
	if origin.RawQuery != "" {
		return nil, fmt.Errorf("wopan: upload endpoint %q must not carry a query", endpoint)
	}
	escapedPath := strings.TrimSuffix(origin.EscapedPath(), "/")
	if escapedPath == "/" {
		escapedPath = ""
	}
	hostKey, err := normalizeHostPort(origin.Host, "upload endpoint")
	if err != nil {
		return nil, fmt.Errorf("wopan: %w", err)
	}
	r := &uploadRoute{
		originURL:   strings.TrimSuffix(endpoint, "/"),
		scheme:      origin.Scheme,
		escapedPath: escapedPath,
		rotate:      rotate,
		failed:      map[string]bool{},
	}
	relays := f.relayMap[hostKey]
	if len(relays) > 0 {
		r.relaysSet = true
	} else {
		f.noteRelayMiss(hostKey, len(f.relayMap))
	}
	r.cands = buildCandidates(hostKey, relays)
	// The fallback candidate is labelled with the host that actually goes on the
	// wire, so a log line cannot name a port the request never used.
	r.cands[len(r.cands)-1].host = origin.Host
	idx, ok := pickTarget(r.cands, f.relayDead.coolingTargets(time.Now()), rotate)
	if !ok {
		return nil, fmt.Errorf("wopan: no upload target for %q", endpoint)
	}
	r.cur = idx
	return r, nil
}

// noteRelayMiss says once per remote that the relay table names endpoints this
// account was not given. Without that, a mistyped endpoint uploads direct and
// the log looks like everything is working.
func (f *Fs) noteRelayMiss(hostKey string, configured int) {
	if configured == 0 || f.relayMiss == nil || !f.relayMiss.CompareAndSwap(false, true) {
		return
	}
	fs.Logf(f, "wopan: upload_relays has %d entries and none matches upload endpoint %q - uploads there leave through this machine's own address",
		configured, hostKey)
}

// uploadRouteFor opens a session's route: the endpoint the server named, spread
// across relays by the session counter.
func (f *Fs) uploadRouteFor(endpoint string) (*uploadRoute, error) {
	// An Fs without the session counter stays on the first live candidate; newFs
	// always sets it, and a test that cares about the spread sets it too.
	rotate := 0
	if f.relayRound != nil {
		rotate = int(atomic.AddUint64(f.relayRound, 1) - 1)
	}
	r, err := f.newUploadRoute(endpoint, rotate)
	if err != nil {
		return nil, err
	}
	t := r.target()
	switch {
	case !t.direct:
		fs.Debugf(f, "wopan: upload session starts on %s for endpoint %s", t.describe(), endpoint)
	case r.relaysSet:
		// A session that opens on the endpoint itself never went through the
		// failover path, so nothing else would say it: every relay is cooling and
		// this file is leaving through the single source IP again.
		fs.Logf(f, "wopan: every relay for %s is cooling, this session goes direct to %s", endpoint, t.describe())
	}
	return r, nil
}

// relayCooldownFrom is the window a dead target stays out of rotation. It reuses
// --connect-family-cooldown rather than adding a knob that does the same job.
func relayCooldownFrom(ctx context.Context) time.Duration {
	return relayCooldown(time.Duration(fs.GetConfig(ctx).ConnectFamilyCooldown))
}

func relayCooldown(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultRelayCooldown
	}
	return d
}

// relayPartDeadline is the per-attempt wall clock for a part of partSize: the
// constant bound per relayPartFloorBytes, so the bandwidth it asks for does not
// depend on how big the part is. The override field exists so tests do not have
// to sleep for five minutes.
func (f *Fs) relayPartDeadline(partSize int64) time.Duration {
	if f.relayPartDeadlineOverride > 0 {
		return f.relayPartDeadlineOverride
	}
	if partSize <= relayPartFloorBytes {
		return defaultRelayPartDeadline
	}
	return defaultRelayPartDeadline * time.Duration((partSize+relayPartFloorBytes-1)/relayPartFloorBytes)
}

// familyPin carries the address family a request must dial, through the context
// to the transport's DialContext.
type familyPinKey struct{}

func withFamilyPin(ctx context.Context, family int) context.Context {
	if family == familyAny {
		return ctx
	}
	return context.WithValue(ctx, familyPinKey{}, family)
}

func familyPin(ctx context.Context) int {
	if v, ok := ctx.Value(familyPinKey{}).(int); ok {
		return v
	}
	return familyAny
}

// wrapFamilyDial rewrites the network of a pinned dial to that family alone.
//
// fshttp is deliberately not asked to do this: inside one dial it falls back to
// the other family after a failure (fs/fshttp/dialer.go) and ignores the
// cooldown entirely when every candidate is cooling, so "this relay's IPv4 is
// dead" could be concluded from a failure its IPv6 actually survived. Rewriting
// the network makes the pin one family, and fshttp's own --connect-family
// behaviour is left untouched for the control plane and for direct attempts.
//
// The pin only binds new connections. A request that rides an idle pooled
// connection keeps that connection's family, so failures are attributed to the
// family observed on the wire when there is one, and to the pin only when the
// dial itself never produced a connection.
func wrapFamilyDial(inner func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	if inner == nil {
		return nil
	}
	return func(dialCtx context.Context, network, address string) (net.Conn, error) {
		if strings.HasPrefix(network, "tcp") {
			switch familyPin(dialCtx) {
			case familyV4:
				network = "tcp4"
			case familyV6:
				network = "tcp6"
			}
		}
		return inner(dialCtx, network, address)
	}
}

// relayUnavailableStatus reports an HTTP status that condemns the path to the
// relay rather than being a rejection from the upload service behind it.
//
// A 5xx that the relay *forwarded* is not evidence about the relay - the existing
// rule for that stands. These two come from the gateway in front of the relay and
// say it could not reach the service at all, so the bytes were not stored and a
// retry against the same host is a wasted part. 503 is deliberately not here: it
// is the usual answer for rate limiting or a briefly overloaded upstream, where
// the right response is to come back later rather than to take a whole relay out
// of rotation for a cooldown window.
func relayUnavailableStatus(code int) bool {
	switch code {
	case 502, 504: // Bad Gateway, Gateway Timeout
		return true
	}
	return false
}

// uploadFailure is a failure that happened before a part's answer was read. It
// carries what the attempt had already achieved, because the two ways an attempt
// can run out of time blame different parties.
type uploadFailure struct {
	err error
	// gotConn is true once a connection existed. A failure before that is the
	// target refusing or the resolver failing, never the source.
	gotConn bool
	// bodySent is true once the whole request body, part bytes included, had
	// been handed to the connection. Until then the attempt was still being fed,
	// so the target cannot be blamed for the pace.
	bodySent bool
	// deadline marks the per-attempt wall clock going off: the one signal the
	// operator set by hand rather than the protocol.
	deadline bool
	// reused is the connection having come out of the pool. A failure on such a
	// connection before any of the body went out is the far end having closed an
	// idle connection, which says nothing about the target: Go will not replay a
	// POST body on a fresh connection by itself.
	reused bool
	// sourceErr is the part's own reader having failed, so the bytes never left
	// this machine for a reason that has nothing to do with the target.
	sourceErr bool
	// rejected marks an answer from the gateway in front of the relay saying it
	// could not reach the upload service. The body did go out, so this is not a
	// transient connection problem, but nothing was stored either - the target's
	// path is what failed.
	rejected bool
	// peerFamily is the family observed on the wire, familyAny when no
	// connection was reached.
	peerFamily int
}

func (e *uploadFailure) Error() string { return e.err.Error() }
func (e *uploadFailure) Unwrap() error { return e.err }

// transient is a failure a retry on the same target is expected to clear, so the
// session should not move off it and nothing should be recorded.
func (e *uploadFailure) transient() bool {
	return e.reused && !e.bodySent && !e.sourceErr
}

// blameTarget says whether this failure is evidence against the dial target.
//
// Three shapes are not. The source failing while we were still reading it. A
// pooled connection the far end had already closed, which is the ordinary shape
// of "wsarecv: An existing connection was forcibly closed" and did occur against
// the live service during the outage this feature is for. And the wall clock
// running out while the body was still being fed, where a stalled pipe and a
// slow source look identical because a blocked write stops the reads too -
// guessing wrong there parks every relay for the length of a bad afternoon on
// the source side.
//
// Everything else is on the target: no connection at all (no route, no DNS
// answer, TLS refused mid-handshake), the idle clock, a clock that expired
// after the whole part was already on the wire, or a gateway 502/504 that
// says the relay could not reach the upload service.
func (e *uploadFailure) blameTarget() bool {
	if e.sourceErr {
		return false
	}
	if !e.gotConn {
		return true
	}
	if e.reused && !e.bodySent {
		return false
	}
	return !(e.deadline && !e.bodySent)
}

// reOpenReader reads a part again from the source after a failed attempt. The
// caller that asked for it closes it.
type reOpenReader struct {
	io.Reader
	rc   io.ReadCloser
	once sync.Once
}

// Close releases the reopened source. It is idempotent because an attempt that
// ended early is closed by the loop and the reader may also have hit EOF.
func (r *reOpenReader) Close() error {
	r.once.Do(func() { _ = r.rc.Close() })
	return nil
}

// partReader watches what the part's source actually delivered.
//
// Two shapes are the source's fault rather than the target's: the read itself
// erroring, and an EOF arriving before the declared part size. In the second one
// the request cannot have been stored anywhere - the body ran short of the
// ContentLength we declared - so a target that hung up once we stopped sending
// is not a dead target. Charging it would park relay after relay over one shrunk
// file.
//
// The multipart pump reads this in its own goroutine while the attempt's goroutine
// asks for the verdict, so the counters are atomic.
type partReader struct {
	r     io.Reader
	want  int64
	got   atomic.Int64
	broke atomic.Bool
	short atomic.Bool
}

func (p *partReader) Read(buf []byte) (int, error) {
	n, err := p.r.Read(buf)
	p.got.Add(int64(n))
	switch {
	case errors.Is(err, io.EOF):
		if p.got.Load() < p.want {
			p.short.Store(true)
		}
	case err != nil:
		p.broke.Store(true)
	}
	return n, err
}

// sourceFault reports whether the part's own bytes are what went wrong.
func (p *partReader) sourceFault() bool {
	return p.broke.Load() || p.short.Load()
}
