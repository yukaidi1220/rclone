package wopan

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	fsobject "github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// directRoute builds the single-candidate route a remote without any relay
// resolves to. The older tests used a bare zone URL string where a route is now
// required; this keeps that shape explicit.
func directRoute(t *testing.T, f *Fs, endpoint string) *uploadRoute {
	t.Helper()
	r, err := f.newUploadRoute(endpoint, 0)
	require.NoError(t, err)
	require.False(t, r.hasRelays(), "directRoute is only for the no-relay shape")
	return r
}

func TestUnitNormalizeHostPort(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want string
		bad  bool
	}{
		{in: "tjupload.pan.wo.cn", want: "tjupload.pan.wo.cn:443"},
		{in: "BJUpload.Pan.WO.CN:30443", want: "bjupload.pan.wo.cn:30443"},
		{in: " hyupload.pan.wo.cn. ", want: "hyupload.pan.wo.cn:443"},
		{in: "r.example:8443", want: "r.example:8443"},
		{in: "[::1]:443", want: "[::1]:443"},
		{in: "", bad: true},
		{in: "https://r.example", bad: true},
		{in: "r.example/path", bad: true},
		{in: "r.example:0", bad: true},
		{in: "r.example:70000", bad: true},
		{in: "r.example:notaport", bad: true},
	} {
		got, err := normalizeHostPort(tc.in, "test")
		if tc.bad {
			assert.Error(t, err, "normalizeHostPort(%q) must be rejected", tc.in)
			continue
		}
		require.NoError(t, err, "normalizeHostPort(%q)", tc.in)
		assert.Equal(t, tc.want, got, "normalizeHostPort(%q)", tc.in)
	}
}

func TestUnitParseRelayConfig(t *testing.T) {
	got, err := parseRelayConfig([]string{
		"tjupload.pan.wo.cn=r1.example",
		"TJUpload.pan.wo.cn=r2.example:8443",
		"hyupload.pan.wo.cn=r3.example",
	})
	require.NoError(t, err)
	// Written order is the attempt order, so it must survive parsing.
	assert.Equal(t, map[string][]string{
		"tjupload.pan.wo.cn:443": {"r1.example:443", "r2.example:8443"},
		"hyupload.pan.wo.cn:443": {"r3.example:443"},
	}, got)

	// An endpoint is identified with its port: the bare host and :30443 are two
	// different servers and must not share a relay list.
	got, err = parseRelayConfig([]string{"bjupload.pan.wo.cn:30443=r4.example"})
	require.NoError(t, err)
	assert.Equal(t, map[string][]string{"bjupload.pan.wo.cn:30443": {"r4.example:443"}}, got)

	empty, err := parseRelayConfig(nil)
	require.NoError(t, err)
	assert.Empty(t, empty, "no items must mean no relays, not an error")

	for _, bad := range []string{
		"r1.example",
		"=r1.example",
		"ep.example=",
		"ep.example=r1.example=r2.example",
		"ep.example=ep.example",
	} {
		_, err := parseRelayConfig([]string{bad})
		assert.Error(t, err, "parseRelayConfig(%q) must be rejected", bad)
	}

	_, err = parseRelayConfig([]string{"ep.example=r1.example", "ep.example=r1.example"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "twice", "a repeated pair must not masquerade as a second source IP")

	_, err = parseRelayConfig([]string{"ep.example=r1.example", "bogus item"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "item 2", "the error must name the offending item")
}

// TestUnitBuildCandidatesOrder locks the operator's rule: every relay's IPv4
// first, then every relay's IPv6, then the endpoint itself.
func TestUnitBuildCandidatesOrder(t *testing.T) {
	got := buildCandidates("hyupload.pan.wo.cn:443", []string{"r1.example:443", "r2.example:443"})
	want := []dialTarget{
		{host: "r1.example:443", family: familyV4},
		{host: "r2.example:443", family: familyV4},
		{host: "r1.example:443", family: familyV6},
		{host: "r2.example:443", family: familyV6},
		{host: "hyupload.pan.wo.cn:443", family: familyAny, direct: true},
	}
	assert.Equal(t, want, got)

	// With no relay there is nothing but the endpoint, so a remote that never
	// configured relays keeps exactly one candidate.
	assert.Equal(t, []dialTarget{{host: "hyupload.pan.wo.cn:443", family: familyAny, direct: true}},
		buildCandidates("hyupload.pan.wo.cn:443", nil))
}

func TestUnitPickTargetPrefersIPv4Group(t *testing.T) {
	cands := buildCandidates("ep:443", []string{"r1:443", "r2:443"})
	alive := func(dialTarget) bool { return false }

	idx, ok := pickTarget(cands, alive, 0)
	require.True(t, ok)
	assert.Equal(t, 0, idx, "the first relay's IPv4 leads")

	// r1's IPv4 is dead but its IPv6 is not: the whole IPv4 group still has r2,
	// so the session must move to r2's IPv4 rather than drop to IPv6.
	deadR1V4 := func(c dialTarget) bool { return c.host == "r1:443" && c.family == familyV4 }
	idx, ok = pickTarget(cands, deadR1V4, 0)
	require.True(t, ok)
	assert.Equal(t, 1, idx, "an IPv4 relay is preferred over a live IPv6 of the same relay")
	assert.Equal(t, "r2:443", cands[idx].host)
	assert.Equal(t, familyV4, cands[idx].family)

	// Only once both IPv4 relays are dead does IPv6 get used.
	deadAllV4 := func(c dialTarget) bool { return c.family == familyV4 }
	idx, ok = pickTarget(cands, deadAllV4, 0)
	require.True(t, ok)
	assert.Equal(t, 2, idx, "all IPv4 cooling must fall to the first IPv6")
	assert.Equal(t, "r1:443", cands[idx].host)
	assert.Equal(t, familyV6, cands[idx].family)

	deadEverything := func(dialTarget) bool { return true }
	idx, ok = pickTarget(cands, func(c dialTarget) bool { return !c.direct && deadEverything(c) }, 0)
	require.True(t, ok)
	assert.True(t, cands[idx].direct, "a dead relay table must end on the endpoint itself")
	assert.Equal(t, 4, idx)

	// A candidate list without a direct entry cannot be resolved: silently
	// returning index 0 would send traffic to a relay that is known dead.
	noDirect := []dialTarget{{host: "r1:443", family: familyV4}}
	_, ok = pickTarget(noDirect, func(dialTarget) bool { return true }, 0)
	assert.False(t, ok, "every candidate cooling with no direct entry must report no target")
}

func TestUnitPickTargetRotatesAcrossLiveRelays(t *testing.T) {
	cands := buildCandidates("ep:443", []string{"r1:443", "r2:443", "r3:443"})
	alive := func(dialTarget) bool { return false }
	var hosts []string
	for rotate := 0; rotate < 4; rotate++ {
		idx, ok := pickTarget(cands, alive, rotate)
		require.True(t, ok)
		hosts = append(hosts, cands[idx].host)
	}
	// Three relays, so the fourth session wraps to the first: rotation is what
	// keeps the run from parking every upload on one source IP.
	assert.Equal(t, []string{"r1:443", "r2:443", "r3:443", "r1:443"}, hosts)

	// Rotation must only spread over the live ones.
	deadR2 := func(c dialTarget) bool { return c.host == "r2:443" }
	var live []string
	for rotate := 0; rotate < 3; rotate++ {
		idx, _ := pickTarget(cands, deadR2, rotate)
		live = append(live, cands[idx].host)
	}
	assert.Equal(t, []string{"r1:443", "r3:443", "r1:443"}, live)
}

func TestUnitParkRelayAnnouncesOnceAndExpires(t *testing.T) {
	f := &Fs{relayDead: newRelayCooldownTable()}
	target := dialTarget{host: "r1:443", family: familyV4}
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)

	assert.False(t, f.relayDead.coolingAt(target, now), "an untouched table must not block anything")
	require.True(t, f.relayDead.parkRelay(target, 5*time.Minute, now), "the first failure must be announced")
	assert.True(t, f.relayDead.coolingAt(target, now.Add(time.Minute)))
	assert.False(t, f.relayDead.parkRelay(target, 5*time.Minute, now.Add(time.Minute)),
		"a target already cooling must not announce again")

	// After the window the same target is eligible again, and re-parking is
	// announced: that is a new failure round, not a repeat of the log line.
	assert.False(t, f.relayDead.coolingAt(target, now.Add(6*time.Minute)))
	assert.True(t, f.relayDead.parkRelay(target, 5*time.Minute, now.Add(6*time.Minute)))

	// The family is part of the identity: parking IPv6 must leave IPv4 usable.
	f2 := &Fs{relayDead: newRelayCooldownTable()}
	require.True(t, f2.relayDead.parkRelay(dialTarget{host: "r1:443", family: familyV6}, 5*time.Minute, now))
	assert.True(t, f2.relayDead.coolingAt(dialTarget{host: "r1:443", family: familyV6}, now))
	assert.False(t, f2.relayDead.coolingAt(dialTarget{host: "r1:443", family: familyV4}, now))

	// The endpoint itself is never parked: it is the last resort.
	f3 := &Fs{relayDead: newRelayCooldownTable()}
	direct := dialTarget{host: "ep:443", family: familyAny, direct: true}
	assert.False(t, f3.relayDead.coolingAt(direct, now))
}

func TestUnitRouteNoteDeadParksTheFamilyThatCarriedIt(t *testing.T) {
	f := &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"ep:443": {"r1:443", "r2:443"}}}
	r, err := f.newUploadRoute("https://ep:443", 0)
	require.NoError(t, err)
	require.Equal(t, "r1:443", r.target().host)

	// The request rode a pooled IPv6 connection, so IPv6 - not the pinned IPv4 -
	// must take the cooldown.
	next, moved := r.noteDead(f, r.target(), familyV6, true, 5*time.Minute, errors.New("dial failed"), time.Now())
	assert.True(t, moved)
	assert.Equal(t, "r2:443", next.host, "the session must advance off the relay that failed")
	assert.Equal(t, familyV4, next.family, "a live IPv4 relay must be preferred over IPv6")

	require.True(t, f.relayDead.parkedByKey("r1:443|6", time.Now()), "the family seen on the wire must be the one parked")
	assert.False(t, f.relayDead.coolingAt(dialTarget{host: "r1:443", family: familyV4}, time.Now()),
		"the pinned family must not be blamed for a failure on the other one")

	// The same split has to hold inside the session's own blacklist: r1's IPv4
	// never got a packet, so refusing it for the rest of the session would take
	// a working address out of rotation for a failure it did not have.
	dead := r.unavailable(f, time.Now())
	assert.False(t, dead(dialTarget{host: "r1:443", family: familyV4}),
		"the pinned family that was never dialled must stay usable for this session")
	assert.True(t, dead(dialTarget{host: "r1:443", family: familyV6}),
		"the family that carried the failure is out for this session")
}

// A failure is reported by the goroutine that was POSTing, which can be well after
// another worker's failure already moved the session. Recording it is right;
// moving the session again from a stale report is not - it would kick the session
// off the relay that is answering now.
func TestUnitLateFailureDoesNotMoveTheSessionAgain(t *testing.T) {
	f := &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"ep:443": {"r1:443", "r2:443", "r3:443"}}}
	now := time.Now()
	r, err := f.newUploadRoute("https://ep:443", 0)
	require.NoError(t, err)
	first := r.target()
	require.Equal(t, "r1:443", first.host)

	_, moved := r.noteDead(f, first, familyV4, true, 5*time.Minute, errors.New("no such host"), now)
	require.True(t, moved)
	// Which relay it moved to is the rotation's business; that it moved is the
	// premise the rest of this test needs.
	movedTo := r.target()
	require.NotEqual(t, first.host, movedTo.host)

	// The second worker's failure against r1 only now comes back.
	_, moved = r.noteDead(f, first, familyV4, true, 5*time.Minute, errors.New("no such host"), now)
	assert.False(t, moved, "a report about a relay the session already left must not move it again")
	assert.Equal(t, movedTo.host, r.target().host, "the relay that is answering must stay the current one")
	for _, host := range []string{"r2:443", "r3:443"} {
		assert.False(t, f.relayDead.parkedByKey(host+"|4", now),
			"no relay that never failed may be parked, or pulled into rotation, by a stale report")
	}
}

func TestUnitRouteBlamesTheTargetItWasGivenNotTheCursor(t *testing.T) {
	// P0-1: several workers post parts of one session at the same time. By the
	// time the second failure is reported the cursor has already moved to a
	// healthy relay, and that relay must not be the one parked.
	f := &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"ep:443": {"r1:443", "r2:443"}}}
	r, err := f.newUploadRoute("https://ep:443", 0)
	require.NoError(t, err)
	first := r.target()
	require.Equal(t, "r1:443", first.host)

	// worker one fails on r1 and moves the session to r2
	_, moved := r.noteDead(f, first, familyV4, true, 5*time.Minute, errors.New("no such host"), time.Now())
	require.True(t, moved)
	require.Equal(t, "r2:443", r.target().host)

	// worker two was also sending on r1: it must charge r1, never the r2 it is
	// reporting after.
	_, moved = r.noteDead(f, first, familyV4, true, 5*time.Minute, errors.New("no such host"), time.Now())
	assert.False(t, moved, "r1 is already accounted for, so nothing moves again")
	assert.Equal(t, "r2:443", r.target().host, "the healthy relay must still be the current one")
	assert.False(t, f.relayDead.parkedByKey("r2:443|4", time.Now()), "r2 never failed and must not be parked")
}

func TestUnitRouteFallsToDirectWhenEveryRelayCools(t *testing.T) {
	f := &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"ep:443": {"r1:443"}}}
	r, err := f.newUploadRoute("https://ep:443", 0)
	require.NoError(t, err)

	// Both families of the only relay fail in turn, then the session must be on
	// the endpoint itself.
	_, moved := r.noteDead(f, r.target(), familyV4, true, 5*time.Minute, errors.New("dial failed"), time.Now())
	require.True(t, moved)
	assert.Equal(t, "r1:443", r.target().host)
	assert.Equal(t, familyV6, r.target().family)

	_, moved = r.noteDead(f, r.target(), familyV6, true, 5*time.Minute, errors.New("no such host"), time.Now())
	require.True(t, moved)
	assert.True(t, r.target().direct, "all relays cooling must fall back to the endpoint")
	assert.Equal(t, "https://ep:443", currentURL(r))

	// The endpoint never enters the table, so a direct failure cannot park it and
	// leave the session with nowhere to go.
	_, moved = r.noteDead(f, r.target(), familyAny, true, 5*time.Minute, errors.New("connection reset"), time.Now())
	assert.False(t, moved, "there is nothing left to move to")
	assert.False(t, f.relayDead.parkedByKey("ep:443|0", time.Now()), "the direct endpoint must not be parked")
	assert.Equal(t, "https://ep:443", currentURL(r), "the direct target keeps the endpoint verbatim")
}

func TestUnitRouteWithoutRelaysKeepsTheEndpointVerbatim(t *testing.T) {
	f := &Fs{relayDead: newRelayCooldownTable()}
	r, err := f.newUploadRoute("https://zone.example", 0)
	require.NoError(t, err)
	// No ":443" is appended to the request URL when no relay is configured: a
	// remote without the table must send exactly what it sent before.
	assert.Equal(t, "https://zone.example", currentURL(r))
	assert.Equal(t, "https://zone.example/openapi/client/upload2C", currentURL(r)+"/openapi/client/upload2C")
	assert.False(t, r.hasRelays())
	assert.Equal(t, 1, r.attemptCount())

	// An endpoint the table does not mention behaves the same way.
	f = &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"other.example:443": {"r1:443"}}}
	r, err = f.newUploadRoute("https://zone.example", 0)
	require.NoError(t, err)
	assert.Equal(t, "https://zone.example", currentURL(r))
	assert.False(t, r.hasRelays())

	// A table entry for the endpoint switches the origin to the relay and keeps
	// the endpoint reachable as the last candidate.
	f = &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"zone.example:443": {"r1:443"}}}
	r, err = f.newUploadRoute("https://zone.example", 0)
	require.NoError(t, err)
	assert.True(t, r.hasRelays())
	assert.Equal(t, "https://r1:443", currentURL(r))
	assert.Equal(t, "https://zone.example", r.urlFor(dialTarget{host: "zone.example:443", direct: true}),
		"the direct fallback must use the endpoint as configured")
}

func TestUnitNewUploadRouteRejectsUnusable(t *testing.T) {
	f := &Fs{relayDead: newRelayCooldownTable()}
	for _, bad := range []string{"not a url", "//zone.example", "zone.example"} {
		_, err := f.newUploadRoute(bad, 0)
		assert.Error(t, err, "newUploadRoute(%q) must fail loudly rather than upload somewhere unexpected", bad)
	}
}

func TestUnitFamilyPinRoundTrip(t *testing.T) {
	assert.Equal(t, familyAny, familyPin(context.Background()))

	base := context.Background()
	assert.Equal(t, base, withFamilyPin(base, familyAny), "no pin must not wrap the context")
	assert.Equal(t, familyV4, familyPin(withFamilyPin(base, familyV4)))
	assert.Equal(t, familyV6, familyPin(withFamilyPin(base, familyV6)))
}

func TestUnitWrapFamilyDialRewritesNetwork(t *testing.T) {
	var seen []string
	inner := func(ctx context.Context, network, address string) (net.Conn, error) {
		seen = append(seen, network+" "+address)
		return nil, errors.New("not dialed here")
	}
	wrapped := wrapFamilyDial(inner)
	base := context.Background()

	_, _ = wrapped(withFamilyPin(base, familyV4), "tcp", "r1:443")
	_, _ = wrapped(withFamilyPin(base, familyV6), "tcp", "r1:443")
	_, _ = wrapped(base, "tcp", "r1:443")

	// The pin has to reach the resolver: "tcp" would let Happy Eyeballs pick the
	// other family inside the same dial.
	assert.Equal(t, []string{"tcp4 r1:443", "tcp6 r1:443", "tcp r1:443"}, seen)

	// The pin is authoritative: a dial already naming a family is rewritten to
	// the pinned one, so an attempt can never report a family the route did not
	// choose.
	assert.Equal(t, "tcp4 r1:443", func() string {
		seen = nil
		_, _ = wrapped(withFamilyPin(base, familyV4), "tcp6", "r1:443")
		return seen[0]
	}())

	assert.Nil(t, wrapFamilyDial(nil), "a transport without a dialer must not gain one")
}

func TestUnitUploadFailureBlame(t *testing.T) {
	// The whole rule in one table. Three shapes are not evidence against a dial
	// target: the wall clock expiring while the part was still being fed, the
	// part's own source having gone wrong, and a pooled connection the far end had
	// already closed.
	for _, tc := range []struct {
		name  string
		f     uploadFailure
		blame bool
	}{
		{name: "no connection at all", f: uploadFailure{gotConn: false, bodySent: false, deadline: false}, blame: true},
		{name: "clock out before dial finished", f: uploadFailure{gotConn: false, deadline: true}, blame: true},
		{name: "TLS broken mid write", f: uploadFailure{gotConn: true, bodySent: false}, blame: true},
		{name: "idle clock on the response", f: uploadFailure{gotConn: true, bodySent: true}, blame: true},
		{name: "wall clock after the whole part went out", f: uploadFailure{gotConn: true, bodySent: true, deadline: true}, blame: true},
		{name: "wall clock while the source is still feeding", f: uploadFailure{gotConn: true, bodySent: false, deadline: true}, blame: false},
		{name: "source read broke", f: uploadFailure{gotConn: true, bodySent: false, sourceErr: true}, blame: false},
		{name: "source read broke after the part went out", f: uploadFailure{gotConn: true, bodySent: true, sourceErr: true}, blame: false},
		{name: "idle pooled connection refused", f: uploadFailure{gotConn: true, reused: true}, blame: false},
		{name: "idle pooled connection, clock out", f: uploadFailure{gotConn: true, reused: true, deadline: true}, blame: false},
		{name: "pooled connection that had taken the whole part", f: uploadFailure{gotConn: true, reused: true, bodySent: true}, blame: true},
	} {
		got := tc.f.blameTarget()
		assert.Equal(t, tc.blame, got, "%s", tc.name)
	}

	// transient is narrower than not-blame: the two other exonered shapes must not
	// be re-posted to the same target, because re-sending them cannot succeed.
	for _, tc := range []struct {
		name string
		f    uploadFailure
		want bool
	}{
		{name: "idle pooled connection", f: uploadFailure{gotConn: true, reused: true}, want: true},
		{name: "nothing of it left on a fresh connection", f: uploadFailure{gotConn: false}, want: false},
		{name: "the part was already on the wire", f: uploadFailure{gotConn: true, reused: true, bodySent: true}, want: false},
		{name: "the source is what broke", f: uploadFailure{gotConn: true, reused: true, sourceErr: true}, want: false},
		{name: "clock expired while being fed", f: uploadFailure{gotConn: true, deadline: true}, want: false},
	} {
		assert.Equal(t, tc.want, tc.f.transient(), "%s", tc.name)
	}

	plain := errors.New("plain")
	failure := &uploadFailure{err: plain, gotConn: true, bodySent: true}
	assert.Equal(t, plain.Error(), failure.Error(), "the message must not change shape")
	assert.True(t, errors.Is(failure, plain), "the wrapped error must stay unwrappable")

	// postPartRetry finds the failure through whatever the pacer and the copy
	// engine wrapped around it, so the classification has to survive %w.
	assert.True(t, errors.As(&wrappedMarker{err: failure}, new(*uploadFailure)))
	assert.False(t, errors.As(plain, new(*uploadFailure)))
}

func TestUnitPartReaderWatchesTheSource(t *testing.T) {
	// The want accounting is what tells "the source ended early" from "the target
	// stopped listening": both leave the same number of bytes delivered.
	shy := &partReader{r: strings.NewReader("12345"), want: 9}
	_, err := io.ReadAll(shy)
	require.NoError(t, err)
	assert.True(t, shy.sourceFault(), "an EOF before the declared part size is the source")

	whole := &partReader{r: strings.NewReader("123456789"), want: 9}
	got, err := io.ReadAll(whole)
	require.NoError(t, err)
	assert.False(t, whole.sourceFault(), "the part arriving whole says nothing about the source")
	assert.Equal(t, 9, len(got))

	broken := &partReader{r: io.MultiReader(strings.NewReader("12"), &brokenReader{}), want: 9}
	_, err = io.ReadAll(broken)
	require.Error(t, err)
	assert.True(t, broken.sourceFault(), "a source that errors is not the target")

	// A reader that ends exactly where the part ends must not be marked: io.Copy
	// delivers the final chunk and the EOF in one return.
	exact := &partReader{r: eofWithBytes{data: "123456789"}, want: 9}
	got, err = io.ReadAll(exact)
	require.NoError(t, err)
	assert.False(t, exact.sourceFault())
	assert.Equal(t, 9, len(got))
}

// brokenReader errors on any read.
type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, errors.New("disk on fire") }

// eofWithBytes returns its data and io.EOF together, the shape io.Copy stops on.
type eofWithBytes struct{ data string }

func (r eofWithBytes) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	return n, io.EOF
}

func TestUnitReOpenReaderClosesOnce(t *testing.T) {
	closes := 0
	rc := &countingCloser{onClose: func() { closes++ }}
	body := &reOpenReader{Reader: strings.NewReader("abc"), rc: rc}

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	assert.Equal(t, "abc", string(got))
	assert.Equal(t, 0, closes, "reading to EOF must not close it: the attempt that asked for it closes it")

	// A second EOF must not close it either; only Close does, and only once.
	_, _ = body.Read(make([]byte, 1))
	assert.Equal(t, 0, closes)

	require.NoError(t, body.Close())
	assert.NoError(t, body.Close(), "a second Close must be harmless")
	assert.Equal(t, 1, closes, "the reopened source must be closed exactly once")
}

// wrappedMarker is the shape the copy engine and the pacer add on top of what
// uploadPart returned: the classification has to survive it.
type wrappedMarker struct{ err error }

func (e *wrappedMarker) Error() string { return "wrapped: " + e.err.Error() }
func (e *wrappedMarker) Unwrap() error { return e.err }

type countingCloser struct {
	onClose func()
}

func (c *countingCloser) Read(p []byte) (int, error) { return 0, io.EOF }

func (c *countingCloser) Close() error {
	c.onClose()
	return nil
}

func TestUnitRelayCooldownAndDeadline(t *testing.T) {
	// 0 disables --connect-family-cooldown, which must not mean "cool down for
	// nothing" or the table would flip on every failure.
	assert.Equal(t, 5*time.Minute, relayCooldown(0))
	assert.Equal(t, 30*time.Second, relayCooldown(30*time.Second))

	f := &Fs{relayDead: newRelayCooldownTable()}
	assert.Equal(t, 5*time.Minute, f.relayPartDeadline(), "the default bounds one attempt")
	f.relayPartDeadlineOverride = time.Millisecond
	assert.Equal(t, time.Millisecond, f.relayPartDeadline())
}

func TestUnitDescribeTargets(t *testing.T) {
	// "(direct)" is the half only the fallback branch can print, and the tests
	// that assert the fallback happened match on it.
	assert.Equal(t, "ep:443 (direct)", dialTarget{host: "ep:443", direct: true}.describe())
	assert.Equal(t, "r1:443 IPv4", dialTarget{host: "r1:443", family: familyV4}.describe())
}

// The tests below drive the whole upload pipeline through zoneRecorder, so the
// host each part actually went to is observed rather than inferred.

const (
	relayZoneURL  = "https://zone-from-server.example"
	relayZoneHost = "zone-from-server.example"
	// relayZoneKey is that endpoint as parseRelayConfig would have written it:
	// the default port filled in.
	relayZoneKey = "zone-from-server.example:443"
)

// relayFs is a three-part upload Fs with relays hung off the endpoint the
// recorder serves.
func relayFs(rec *zoneRecorder, relays ...string) *Fs {
	f := threePartFs(rec)
	if len(relays) > 0 {
		f.relayMap = map[string][]string{relayZoneKey: relays}
	}
	// The session counter is what makes the second upload start on the second
	// relay; a nil counter would pin everything to the first one.
	f.relayRound = new(uint64)
	return f
}

// smallPartFs is the same shape for a file that stays in one part.
func smallPartFs(t *testing.T, rec *zoneRecorder, relays ...string) *Fs {
	t.Helper()
	f := newUnitTestFs(rec)
	f.zoneURL = relayZoneURL
	f.zoneLoaded = true
	if len(relays) > 0 {
		f.relayMap = map[string][]string{relayZoneKey: relays}
	}
	f.relayRound = new(uint64)
	return f
}

func uploadThreeParts(t *testing.T, f *Fs, name string) {
	t.Helper()
	src := fsobject.NewStaticObjectInfo(name, time.Now(), threePartSize, true, nil, nil)
	data, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, threePartSize)), "dirID", name, threePartSize, src)
	require.NoError(t, err)
	assert.NotEmpty(t, data.Fid, "the upload must finish")
}

func TestUnitRelayRewritesOnlyTheUploadHost(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL}
	f := relayFs(rec, "r1.example:443")
	uploadThreeParts(t, f, "big.bin")

	assert.Equal(t, []string{"r1.example:443", "r1.example:443", "r1.example:443"}, rec.uploadHosts())
	// The control plane keeps its own host. GetZoneInfo has to answer before any
	// relay is known, and the dispatcher carries the token.
	for _, u := range rec.urls {
		if strings.HasSuffix(u, "/openapi/client/upload2C") {
			continue
		}
		assert.NotContains(t, u, "r1.example", "a relay must not receive control-plane traffic")
	}
}

func TestUnitRelayTableAbsentKeepsEndpointVerbatim(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL}
	f := relayFs(rec)
	uploadThreeParts(t, f, "big.bin")

	// No ":443" appended, and exactly one post per part: a remote without the
	// table has to send what it sent before relays existed.
	assert.Equal(t, []string{
		"zone-from-server.example", "zone-from-server.example", "zone-from-server.example",
	}, rec.uploadHosts())
	assert.Equal(t, 1, rec.countOf(1))
}

func TestUnitTransportFailureMovesSessionToNextRelay(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, failHost: map[string]int{"r1.example:443": 1}}
	f := relayFs(rec, "r1.example:443", "r2.example:443")
	uploadThreeParts(t, f, "big.bin")

	// Part 1 leaves on r1, is refused before any answer, and comes back on r2.
	// The rest of the session stays on r2: parts of one uniqueId do not scatter
	// unless a target failed.
	assert.Equal(t, []string{
		"r1.example:443", "r2.example:443", "r2.example:443", "r2.example:443",
	}, rec.uploadHosts())
	assert.Equal(t, 2, rec.countOf(1), "only the failed part is re-posted")
	got := rec.contentOf(1)
	require.Len(t, got, 2)
	assert.Equal(t, got[0], got[1], "the re-post must carry the same bytes")
	assert.True(t, f.relayDead.parkedByKey("r1.example:443|4", time.Now()), "the refused relay must be parked on the family that was asked for")
}

func TestUnitRelaySessionsRotateAndStayPut(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL}
	f := relayFs(rec, "r1.example:443", "r2.example:443")
	uploadThreeParts(t, f, "a.bin")
	uploadThreeParts(t, f, "b.bin")

	hosts := rec.uploadHosts()
	require.Len(t, hosts, 6)
	assert.Equal(t, []string{"r1.example:443", "r1.example:443", "r1.example:443"}, hosts[:3],
		"one session must not scatter across relays")
	assert.Equal(t, []string{"r2.example:443", "r2.example:443", "r2.example:443"}, hosts[3:],
		"the next session must start on the next relay, or all volume leaves through one source IP")
}

// TestUnitRelayPartDeadlineAbandonsStalledTarget is about the clock being *there*
// at all: nothing else in this shape ever ends the attempt. Which side the clock
// blames is a different rule, and TestUnitUploadFailureBlame plus the two real
// transport tests own that.
func TestUnitRelayPartDeadlineAbandonsStalledTarget(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, blockHost: "r1.example:443"}
	f := relayFs(rec, "r1.example:443", "r2.example:443")
	// The idle clock would never fire here: the recorder is parked, not silent
	// about progress forever. This is the shape only a wall clock catches.
	f.relayPartDeadlineOverride = 20 * time.Millisecond
	uploadThreeParts(t, f, "big.bin")

	assert.Equal(t, []string{
		"r1.example:443", "r2.example:443", "r2.example:443", "r2.example:443",
	}, rec.uploadHosts())
	// The load-bearing pair: without these the test passes whenever *anything*
	// ends the stalled attempt, including the recorder's own thirty second
	// guard, which is what a missing deadline does.
	assert.Equal(t, 1, rec.deadlineFired, "the per-attempt clock must be what releases it")
	assert.Equal(t, 0, rec.neverGivenUp, "the recorder's guard firing means no deadline was watching")
}

func TestUnitAllRelaysCoolingFallsBackToEndpoint(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, failHost: map[string]int{"r1.example:443": 99}}
	f := relayFs(rec, "r1.example:443")
	uploadThreeParts(t, f, "big.bin")

	// Both families of the only relay fail inside part 1, so part 1 finishes on
	// the endpoint itself and the session stays there.
	assert.Equal(t, []string{
		"r1.example:443", "r1.example:443",
		"zone-from-server.example", "zone-from-server.example", "zone-from-server.example",
	}, rec.uploadHosts())
	assert.False(t, f.relayDead.parkedByKey(relayZoneHost+"|0", time.Now()), "the endpoint itself is the last resort and must never be parked")
}

func TestUnitSinglePartRepostsOntoAnotherRelayOnly(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, failHost: map[string]int{"r1.example:443": 1}}
	f := smallPartFs(t, rec, "r1.example:443", "r2.example:443")

	small := int64(64 << 10)
	src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
	data, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, small)), "dirID", "small.bin", small, src)
	require.NoError(t, err)
	assert.NotEmpty(t, data.Fid)

	// A single-part file is one POST of the whole file. The operator's rule is
	// that it may move to another relay, which means re-reading the bytes; it
	// may not POST the same request twice to the same host.
	assert.Equal(t, []string{"r1.example:443", "r2.example:443"}, rec.uploadHosts())
	got := rec.contentOf(1)
	require.Len(t, got, 2)
	assert.Equal(t, got[0], got[1], "the re-posted single part must carry the whole file again, unchanged")
}

func TestUnitSinglePartCannotRepostReportsTheFirstFailure(t *testing.T) {
	// A source that can neither be rewound nor reopened cannot be re-posted. The
	// failure the caller must see is the transport failure against the first
	// relay, not the reason the re-post could not be assembled.
	rec := &zoneRecorder{zoneURL: relayZoneURL, failHost: map[string]int{"r1.example:443": 1}}
	f := smallPartFs(t, rec, "r1.example:443", "r2.example:443")

	small := int64(64 << 10)
	src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
	in := struct{ io.Reader }{bytes.NewReader(make([]byte, small))}
	_, err := f.uploadSingle(context.Background(), in, "dirID", "small.bin", small, src)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no such host", "the original transport failure must survive")
	assert.Len(t, rec.uploadHosts(), 1, "the part is posted once and then given up on")
}

func TestUnitSinglePartWithoutRelaysPostsOnce(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, transportFailPart: 1, transportFailTimes: 99, hardCap: 6}
	f := smallPartFs(t, rec)

	small := int64(64 << 10)
	src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
	_, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, small)), "dirID", "small.bin", small, src)
	require.Error(t, err)
	// No relay table, so nothing to move to: the single attempt is still the only
	// one, and the whole-file ladder in the copy engine stays responsible.
	assert.Equal(t, 1, rec.countOf(1), "a remote without relays must not gain a re-post it never had")
}

func TestUnitSinglePartHTTPRejectionIsNotReposted(t *testing.T) {
	withPartRetries(t, 3)
	// The server answers a bare 500 for names it cannot store. That is an HTTP
	// answer, so it neither moves the session nor earns a second POST - even
	// though this remote has relays to move to.
	rec := &zoneRecorder{zoneURL: relayZoneURL, failPart5xx: 1}
	f := smallPartFs(t, rec, "r1.example:443", "r2.example:443")

	small := int64(64 << 10)
	src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
	_, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, small)), "dirID", "small.bin", small, src)
	require.Error(t, err)
	assert.Equal(t, []string{"r1.example:443"}, rec.uploadHosts())
	assert.False(t, f.relayDead.parkedByKey("r1.example:443|4", time.Now()), "an HTTP status says nothing about the relay")
}

func TestUnitRelayFiveHundredAnswerDoesNotParkTheRelay(t *testing.T) {
	withPartRetries(t, 3)
	// A 500 in the chunked path stays retryable, but it must not be read as the
	// relay being dead: the relay is the thing that answered.
	rec := &zoneRecorder{zoneURL: relayZoneURL, failPart5xx: 1}
	f := relayFs(rec, "r1.example:443", "r2.example:443")
	src := fsobject.NewStaticObjectInfo("big.bin", time.Now(), threePartSize, true, nil, nil)
	_, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, threePartSize)), "dirID", "big.bin", threePartSize, src)
	require.Error(t, err)

	for _, h := range []string{"r1.example:443|4", "r2.example:443|4"} {
		assert.False(t, f.relayDead.parkedByKey(h, time.Now()), "a forwarded 5xx must not park %s", h)
	}
	hosts := rec.uploadHosts()
	require.GreaterOrEqual(t, len(hosts), 3, "the first part must have been posted three times")
	assert.Equal(t, []string{"r1.example:443", "r1.example:443", "r1.example:443"}, hosts[:3],
		"the part is re-posted to the same relay that answered")
}

func TestUnitRelayFailoverIsAnnounced(t *testing.T) {
	withPartRetries(t, 3)
	var buf bytes.Buffer
	fs.SetLogger(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	defer fs.SetLogger(slog.Default().Handler())

	rec := &zoneRecorder{zoneURL: relayZoneURL, failHost: map[string]int{"r1.example:443": 99}}
	f := relayFs(rec, "r1.example:443", "r2.example:443")
	uploadThreeParts(t, f, "big.bin")

	assert.Equal(t, []string{
		"r1.example:443", "r2.example:443", "r2.example:443", "r2.example:443",
	}, rec.uploadHosts())

	logged := buf.String()
	assert.Contains(t, logged, "wopan: relay r1.example:443 IPv4 parked for",
		"a target taken out of rotation has to say so")
	assert.Equal(t, 1, strings.Count(logged, "parked for"),
		"one notice per cooldown window, or a stalled relay buries the log")
}

func TestUnitDirectFallbackIsVisibleInLogs(t *testing.T) {
	withPartRetries(t, 3)
	var buf bytes.Buffer
	fs.SetLogger(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	defer fs.SetLogger(slog.Default().Handler())

	rec := &zoneRecorder{zoneURL: relayZoneURL, failHost: map[string]int{"r1.example:443": 99}}
	f := relayFs(rec, "r1.example:443")
	uploadThreeParts(t, f, "big.bin")

	// "(direct)" is the half only the fallback branch can print, so matching on
	// it proves the session really moved off the relays instead of passing on a
	// word some other line also says.
	logged := buf.String()
	assert.Contains(t, logged, "next attempt goes to "+relayZoneHost+" (direct)")
	assert.Equal(t, 2, strings.Count(logged, "parked for"),
		"both families of the only relay being refused is two events, and both moves are announced")
}

func TestUnitRelayFamilyPinReachesTheUploadClient(t *testing.T) {
	// uploadPart only puts the pin on the context. This checks the client the
	// backend uploads with actually honours it, using an IPv4-only server as the
	// probe: if the pin never reached the dialer, the IPv6 attempt would succeed
	// and the whole "IPv4 first" rule would be decoration.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	client := newHTTPClient(context.Background(), &Options{})
	dialErr := func(family int) error {
		req, err := http.NewRequestWithContext(withFamilyPin(context.Background(), family), http.MethodGet, srv.URL, nil)
		require.NoError(t, err)
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		_ = res.Body.Close()
		return nil
	}

	require.NoError(t, dialErr(familyAny), "the probe server must be reachable at all")
	require.NoError(t, dialErr(familyV4), "a pinned IPv4 request must reach an IPv4 server")
	err := dialErr(familyV6)
	require.Error(t, err, "the same server must be unreachable when pinned to IPv6")
	assert.Contains(t, err.Error(), "tcp6", "the failure must name the family the pin chose")
}

// currentURL is the request origin uploadPart would POST to for the route's
// current target.
func currentURL(r *uploadRoute) string { return r.urlFor(r.target()) }

// A mistyped endpoint would otherwise upload direct with nothing in the log to
// say so, and the operator would believe the volume was spread.
func TestUnitRelayTableThatMatchesNothingIsAnnouncedOnce(t *testing.T) {
	var buf bytes.Buffer
	fs.SetLogger(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	defer fs.SetLogger(slog.Default().Handler())

	f := &Fs{
		relayDead: newRelayCooldownTable(),
		relayMap:  map[string][]string{"tjupload.pan.wo.cn:443": {"r1:443"}},
		relayMiss: new(atomic.Bool),
	}
	r1, err := f.newUploadRoute("https://hyupload.pan.wo.cn", 0)
	require.NoError(t, err)
	assert.Equal(t, "https://hyupload.pan.wo.cn", currentURL(r1), "an unmatched endpoint still uploads direct")

	// A second session must not repeat the line: it is a property of the config,
	// not of each file.
	_, err = f.newUploadRoute("https://hyupload.pan.wo.cn", 1)
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(buf.String(), "none matches upload endpoint"),
		"one notice per remote, not one per session")
	assert.Contains(t, buf.String(), "hyupload.pan.wo.cn:443", "and it must name the endpoint that did not match")

	// An empty table is the ordinary case and must stay silent.
	var quiet bytes.Buffer
	fs.SetLogger(slog.NewTextHandler(&quiet, &slog.HandlerOptions{Level: slog.LevelDebug}))
	defer fs.SetLogger(slog.Default().Handler())
	empty := &Fs{relayDead: newRelayCooldownTable(), relayMiss: new(atomic.Bool)}
	_, err = empty.newUploadRoute("https://hyupload.pan.wo.cn", 0)
	require.NoError(t, err)
	assert.NotContains(t, quiet.String(), "none matches upload endpoint")
}

// The config file form is one line with comma separated pairs; that split is the
// shape production writes, so it is what startup has to accept.
func TestUnitNewFsAcceptsCommaSeparatedRelays(t *testing.T) {
	m := configmap.Simple{
		"chunk_size": "8Mi", "upload_concurrency": "4", "access_token": testAccessToken,
		"upload_relays": "hyupload.pan.wo.cn=r1.example, hyupload.pan.wo.cn=r2.example:8443",
	}
	// newFs goes on to validate the token against the service, which unit tests
	// have no business doing: the error it returns must therefore be about the
	// network, not about the relay line.
	f, err := newFs(context.Background(), "wopan-test", "", m)
	if err != nil {
		assert.NotContains(t, err.Error(), "upload_relays",
			"two comma separated pairs must parse")
		return
	}
	// A remote that came up despite the token check must have parsed the line:
	// without this the assertion below is vacuous on the success path.
	assert.Equal(t, map[string][]string{"hyupload.pan.wo.cn:443": {"r1.example:443", "r2.example:8443"}}, f.relayMap)
}

func TestUnitWrapFamilyDialLeavesOtherNetworksAlone(t *testing.T) {
	// Only tcp dials may be rewritten. A unix or other socket reaching this
	// wrapper must keep its network, or the pin would break transports it has no
	// business touching.
	var seen []string
	inner := func(ctx context.Context, network, address string) (net.Conn, error) {
		seen = append(seen, network)
		return nil, errors.New("not dialed here")
	}
	wrapped := wrapFamilyDial(inner)
	base := context.Background()
	_, _ = wrapped(withFamilyPin(base, familyV4), "unix", "/tmp/x")
	_, _ = wrapped(withFamilyPin(base, familyV6), "udp", "1.2.3.4:53")
	assert.Equal(t, []string{"unix", "udp"}, seen)
}

func TestUnitRelayConfigFailsTheRemoteAtStartup(t *testing.T) {
	// A malformed item has to stop the remote instead of quietly uploading
	// direct: silent success here is the failure mode the whole table exists to
	// avoid.
	_, err := parseRelayConfig([]string{"ep.example", "relay with spaces=x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "item 1")

	items, err := parseRelayConfig([]string{"ep.example=r1.example"})
	require.NoError(t, err)
	require.Len(t, items, 1)
}

// The table has to be parsed by the code that builds the remote, not only by the
// function that parses it: a startup that swallows the error would leave every
// upload going through one source IP in silence.
func TestUnitNewFsRejectsMalformedRelays(t *testing.T) {
	for _, tc := range []struct{ value, wantErr string }{
		{"nonsense-without-equals", "upload_relays item 1"},
		{"ep.example=bad host", "relay"},
	} {
		m := configmap.Simple{
			"chunk_size": "8Mi", "upload_concurrency": "4",
			// Long enough to satisfy the AES-key length check, so newFs
			// reaches the relay parsing instead of failing earlier.
			"access_token":  testAccessToken,
			"upload_relays": tc.value,
		}
		_, err := newFs(context.Background(), "wopan-test", "", m)
		require.Error(t, err, tc.value)
		assert.Contains(t, err.Error(), tc.wantErr)
	}
}

// An endpoint that carries its own path would build one URL for the direct
// branch and another for a relay branch. Refusing beats being inconsistent.
// TestUnitNewUploadRouteKeepsTheEndpointPath: upload_zone used to be usable with
// a path (`https://proxy.example/wopan`), and the upload2C suffix was appended to
// it. That has to keep working, on the direct branch and through a relay alike.
func TestUnitNewUploadRouteKeepsTheEndpointPath(t *testing.T) {
	f := &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"ep:443": {"r1:443"}}}
	r, err := f.newUploadRoute("https://ep:443/wopan", 0)
	require.NoError(t, err)
	assert.Equal(t, "https://r1:443/wopan", currentURL(r), "a relay carries the same mount point")
	assert.Equal(t, "https://ep:443/wopan", r.urlFor(dialTarget{host: "ep:443", direct: true}))

	// A trailing slash must not become a doubled separator.
	r, err = f.newUploadRoute("https://ep:443/wopan/", 0)
	require.NoError(t, err)
	assert.Equal(t, "https://r1:443/wopan", currentURL(r))
	assert.Equal(t, "https://ep:443/wopan", r.urlFor(dialTarget{host: "ep:443", direct: true}))

	// A query cannot compose with an appended path and is still refused.
	_, err = f.newUploadRoute("https://ep:443?x=1", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "query")
}

func TestUnitSessionOpenSkipsCoolingRelays(t *testing.T) {
	f := &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"ep:443": {"r1:443", "r2:443"}}}
	now := time.Now()
	require.True(t, f.relayDead.parkRelay(dialTarget{host: "r1:443", family: familyV4}, time.Hour, now))

	r, err := f.newUploadRoute("https://ep:443", 0)
	require.NoError(t, err)
	assert.Equal(t, "r2:443", r.target().host, "a session must not open on a relay that is cooling")

	// The rotation index must be taken over the live ones only: r1's IPv4 is out,
	// so a session with rotate=1 also has to land on r2 rather than wrap onto the
	// cooling r1.
	r2, err := f.newUploadRoute("https://ep:443", 1)
	require.NoError(t, err)
	assert.Equal(t, "r2:443", r2.target().host)
}

func TestUnitSessionNeverReturnsToATargetItFailedOn(t *testing.T) {
	// The shared cooldown window is measured from the failure, so a long session
	// can outlive it. The session must still not POST twice to the same
	// host-and-family: that is the rule the single-attempt path was built on.
	f := &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"ep:443": {"r1:443", "r2:443"}}}
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	r, err := f.newUploadRoute("https://ep:443", 0)
	require.NoError(t, err)

	first := r.target()
	require.Equal(t, "r1:443|4", first.deadKey())
	_, moved := r.noteDead(f, first, familyV4, true, time.Minute, errors.New("no such host"), now)
	require.True(t, moved)

	later := now.Add(time.Hour)
	assert.False(t, f.relayDead.coolingAt(first, later), "the shared window has long expired")
	assert.True(t, r.unavailable(f, later)(first), "the session still refuses the target it failed on")

	idx, ok := pickTarget(r.cands, r.unavailable(f, later), 0)
	require.True(t, ok)
	assert.NotEqual(t, first.deadKey(), r.cands[idx].deadKey(),
		"no selection may land back on a target this session already posted to")
}

func TestUnitUnblamedFailureMovesTheSessionWithoutParking(t *testing.T) {
	// The wall clock running out before the part left us is the one shape that
	// moves the session but charges nobody: the next attempt goes elsewhere and
	// the relay stays in rotation for everyone else.
	f := &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"ep:443": {"r1:443", "r2:443"}}}
	r, err := f.newUploadRoute("https://ep:443", 0)
	require.NoError(t, err)

	next, moved := r.noteDead(f, r.target(), familyV4, false, time.Minute, errors.New("context deadline exceeded"), time.Now())
	require.True(t, moved)
	assert.Equal(t, "r2:443", next.host, "the session still moves off the silent target")
	assert.False(t, f.relayDead.parkedByKey("r1:443|4", time.Now()), "and parks nothing")
}

func TestUnitSessionOpeningDirectIsAnnounced(t *testing.T) {
	// A session that starts on the endpoint never went through the failover path,
	// so nothing else in the log would say that all relays are cooling and this
	// file is leaving through the single source IP again.
	var buf bytes.Buffer
	fs.SetLogger(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	defer fs.SetLogger(slog.Default().Handler())

	f := &Fs{relayDead: newRelayCooldownTable(), relayMap: map[string][]string{"ep:443": {"r1:443"}}}
	now := time.Now()
	require.True(t, f.relayDead.parkRelay(dialTarget{host: "r1:443", family: familyV4}, time.Hour, now))
	require.True(t, f.relayDead.parkRelay(dialTarget{host: "r1:443", family: familyV6}, time.Hour, now))

	r, err := f.uploadRouteFor("https://ep:443")
	require.NoError(t, err)
	require.True(t, r.target().direct)
	assert.Contains(t, buf.String(), "every relay for https://ep:443 is cooling, this session goes direct",
		"falling back to one source IP has to be visible without -vv")
}

// The multi-thread engine is a second entry into the same pipeline, and it is the
// one production big files take. Its route must behave like the chunked path's.
func TestUnitChunkWriterFollowsTheRelayRoute(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, failHost: map[string]int{"r1.example:443": 1}}
	f := relayFs(rec, "r1.example:443", "r2.example:443")

	w := newChunkWriter(t, f, mustRelayRoute(t, f, relayZoneURL))

	reader := bytes.NewReader(make([]byte, w.chunk))
	_, err := w.WriteChunk(context.Background(), 0, reader)
	require.NoError(t, err)
	_, err = w.WriteChunk(context.Background(), 1, bytes.NewReader(make([]byte, w.size-8<<20)))
	require.NoError(t, err)

	// The first chunk fails on r1 and is re-posted on r2; the second chunk stays
	// on r2 because the session moved, not because each part chooses afresh.
	assert.Equal(t, []string{
		"r1.example:443", "r2.example:443", "r2.example:443",
	}, rec.uploadHosts())
}

func mustRelayRoute(t *testing.T, f *Fs, endpoint string) *uploadRoute {
	t.Helper()
	r, err := f.uploadRouteFor(endpoint)
	require.NoError(t, err)
	require.True(t, r.hasRelays(), "this helper is for the relayed shape")
	return r
}

// Two workers of one session fail on the same relay at the same time. The
// recorder makes them meet inside one round trip, so the shape this test is named
// for is a fact rather than a hope: neither can have reported its failure before
// both were posted. Neither failure may be charged to the relay the other one
// moved the session onto.
func TestUnitConcurrentPartFailuresBlameOnlyTheRelayThatFailed(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{
		zoneURL: relayZoneURL,
		// failHost counts per host, so two means both POSTs that met on r1 refuse.
		failHost: map[string]int{"r1.example:443": 2},
		meetHost: "r1.example:443",
		meetWant: 2,
	}
	f := relayFs(rec, "r1.example:443", "r2.example:443")
	f.opt.UploadConcurrency = 2

	// Two parts, so the two workers each own one and both start on r1.
	size := int64(16 << 20)
	src := fsobject.NewStaticObjectInfo("big.bin", time.Now(), size, true, nil, nil)
	data, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, size)), "dirID", "big.bin", size, src)
	require.NoError(t, err, "r2 is healthy, so the upload must finish there")
	assert.NotEmpty(t, data.Fid)

	require.Equal(t, int32(2), rec.meetArrived.Load(),
		"both workers must have been POSTing to r1 at the same time, or this proves nothing about concurrency")
	perHost := map[string]int{}
	for _, h := range rec.uploadHosts() {
		perHost[h]++
	}
	assert.Equal(t, 2, perHost["r1.example:443"],
		"each worker lost exactly one POST on r1, and the session never went back there")
	assert.Equal(t, 2, rec.countOf(2), "the fid-carrying part failed on r1 and finished on r2")
	assert.LessOrEqual(t, rec.countOf(1), 2, "part 1 is posted at most twice")
	assert.Contains(t, []int{1, 2}, perHost["r2.example:443"],
		"r2 receives the moved session; whether part 1 is still re-posted there depends on "+
			"which part gets the fid first, and a part is skipped once the session has one")
	assert.True(t, f.relayDead.parkedByKey("r1.example:443|4", time.Now()), "the relay that refused is parked")
	assert.False(t, f.relayDead.parkedByKey("r2.example:443|4", time.Now()),
		"a relay that only received the moved session must never be parked")
}

// An object that can be reopened, so the single-part re-post can take the branch
// that re-reads from the source instead of rewinding.
type reopenableObject struct {
	fs.ObjectInfo
	size   int64
	opens  *int
	closes *int
}

func (o *reopenableObject) Open(context.Context, ...fs.OpenOption) (io.ReadCloser, error) {
	*o.opens++
	return &reopenStream{
		Reader: bytes.NewReader(make([]byte, o.size)),
		onClose: func() {
			*o.closes++
		},
	}, nil
}

func (o *reopenableObject) SetModTime(context.Context, time.Time) error { return nil }
func (o *reopenableObject) Update(context.Context, io.Reader, fs.ObjectInfo, ...fs.OpenOption) error {
	return nil
}
func (o *reopenableObject) Remove(context.Context) error { return nil }

type reopenStream struct {
	io.Reader
	onClose func()
	closed  bool
}

func (s *reopenStream) Close() error {
	if !s.closed {
		s.closed = true
		s.onClose()
	}
	return nil
}

// The re-post that has to reopen the source is the one branch of the single-part
// path with no coverage: the source here is an fs.Object, and the reader handed
// to the copy is deliberately not seekable.
func TestUnitSinglePartRepostReopensSourceAndClosesIt(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, failHost: map[string]int{"r1.example:443": 1}}
	f := smallPartFs(t, rec, "r1.example:443", "r2.example:443")

	small := int64(1 << 20)
	opens, closes := 0, 0
	base := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
	src := &reopenableObject{ObjectInfo: base, size: small, opens: &opens, closes: &closes}

	in := struct{ io.Reader }{bytes.NewReader(make([]byte, small))}
	data, err := f.uploadSingle(context.Background(), in, "dirID", "small.bin", small, src)
	require.NoError(t, err)
	assert.NotEmpty(t, data.Fid)

	assert.Equal(t, []string{"r1.example:443", "r2.example:443"}, rec.uploadHosts())
	assert.Equal(t, 1, opens, "the re-post must read the part again from the source")
	assert.Equal(t, opens, closes, "every reader the attempt opened must be closed again")
}

// TestUnitRealTransportChargesTheTargetThatTookTheWholePart is the shape where the
// part went out and only the answer came back broken. It is charged to the target,
// because nothing about the source was involved.
func TestUnitRealTransportChargesTheTargetThatTookTheWholePart(t *testing.T) {
	silent := startRawTarget(t, rawCloseWithoutAnswer)
	healthy := startAnsweringTarget(t)
	// No clock override: the default five minutes must not be what ends these
	// attempts. The target dropping the connection after taking the whole part is.
	f, relayKey := relayedTransportFs(t, healthy, silent.URL)

	size := int64(4 << 20)
	src := fsobject.NewStaticObjectInfo("a.bin", time.Now(), size, true, nil, nil)
	// 1 MiB parts over a 4 MiB file, so this is the chunked path: a part may be
	// re-posted wherever the session has moved.
	f.opt.ChunkSize = fs.SizeSuffix(1 << 20)
	f.opt.UploadCutoff = fs.SizeSuffix(1 << 20)
	// One worker, so "how many attempts did this target get" is a number the test
	// can pin instead of a range depending on scheduling.
	f.opt.UploadConcurrency = 1

	data, err := f.uploadSingle(probeCtx(t), bytes.NewReader(make([]byte, size)), "dirID", "a.bin", size, src)
	require.NoError(t, err, "the upload must finish after moving to the endpoint")
	assert.NotEmpty(t, data.Fid)
	assert.True(t, f.relayDead.parkedByKey(relayKey+"|4", time.Now()),
		"a target that was fed the whole part and then lost the answer is parked")
	assert.Equal(t, int32(1), silent.hits.Load(), "the parked target got exactly one attempt")
	assert.Equal(t, int32(4), healthy.hits.Load(), "all four parts finished on the endpoint")
}

// TestUnitRealTransportSparesTheTargetStillBeingFed: a relay that accepts the
// connection and then stops reading looks, from here, exactly like a source
// feeding slowly - a blocked write stops the reads too. Charging it would park
// every relay for the length of a bad afternoon on the source side, which is the
// failure mode this feature exists to avoid.
//
// The part is 16 MiB against a two second clock and the target reads one kilobyte
// of it. No socket buffer on any supported platform holds that, so "the body had
// not gone out when the clock fired" is arithmetic rather than a guess about how
// fast this machine is.
func TestUnitRealTransportSparesTheTargetStillBeingFed(t *testing.T) {
	dark := startRawTarget(t, rawNeverReadsBody)
	healthy := startAnsweringTarget(t)
	f, relayKey := relayedTransportFs(t, healthy, dark.URL)
	f.relayPartDeadlineOverride = 2 * time.Second

	size := int64(32 << 20)
	src := fsobject.NewStaticObjectInfo("a.bin", time.Now(), size, true, nil, nil)
	f.opt.ChunkSize = fs.SizeSuffix(16 << 20)
	f.opt.UploadCutoff = fs.SizeSuffix(16 << 20)
	f.opt.UploadConcurrency = 1

	data, err := f.uploadSingle(probeCtx(t), bytes.NewReader(make([]byte, size)), "dirID", "a.bin", size, src)
	require.NoError(t, err, "the session still moves off a silent target")
	assert.NotEmpty(t, data.Fid)
	assert.False(t, f.relayDead.parkedByKey(relayKey+"|4", time.Now()),
		"an attempt that never finished writing the part must not park the target")
	assert.Equal(t, int32(1), dark.hits.Load(), "the unresponsive target got exactly one attempt")
	assert.Equal(t, int32(2), healthy.hits.Load(), "both parts finished on the endpoint")
}

// One part holds the whole file here, and the whole file had already gone out
// before the answer was lost. POSTing it again to another relay could register
// the object twice, and whether the drive then holds one entry or two has never
// been measured, so the attempt ends where it is and the whole-file ladder owns
// the retry.
func TestUnitSinglePartWithWholeObjectSentIsNotReposted(t *testing.T) {
	cut := startRawTarget(t, rawTruncatedAnswer)
	healthy := startAnsweringTarget(t)
	f, relayKey := relayedTransportFs(t, healthy, cut.URL)

	size := int64(4 << 20)
	src := fsobject.NewStaticObjectInfo("a.bin", time.Now(), size, true, nil, nil)
	f.opt.UploadCutoff = fs.SizeSuffix(size)
	_, err := f.uploadSingle(probeCtx(t), bytes.NewReader(make([]byte, size)), "dirID", "a.bin", size, src)
	require.Error(t, err, "the lost answer must be reported, not papered over")
	assert.Equal(t, int32(1), cut.hits.Load(), "the object was posted once to the relay")
	assert.Equal(t, int32(0), healthy.hits.Load(), "and it must not be posted a second time anywhere")
	assert.True(t, f.relayDead.parkedByKey(relayKey+"|4", time.Now()),
		"the relay that dropped the answer is still parked")
}

// uploadTarget is a real listener that behaves like a working upload endpoint: it
// takes the part and answers 0000 with a fid. Real listeners are what make the
// httptrace flags the blame rules read be more than values a test wrote itself.
type uploadTarget struct {
	*httptest.Server
	hits atomic.Int32
}

func startAnsweringTarget(t *testing.T) *uploadTarget {
	t.Helper()
	target := &uploadTarget{}
	target.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target.hits.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":"0000","data":{"fid":"F","wcFileId":"W"},"msg":"ok"}`))
	}))
	t.Cleanup(target.Server.Close)
	return target
}

// rawAnswer is what a raw target does with a request after taking - or not taking
// - all of it.
type rawAnswer int

const (
	// rawCloseWithoutAnswer takes the whole body and hangs up without a byte of
	// response: the client's Do fails after the part went out.
	rawCloseWithoutAnswer rawAnswer = iota
	// rawTruncatedAnswer takes the whole body, starts a 200 with a JSON body, and
	// hangs up mid-body: the client's read of the answer fails.
	rawTruncatedAnswer
	// rawNeverReadsBody takes one kilobyte and stops reading, so the client's
	// writes back up and only a clock can end the attempt.
	rawNeverReadsBody
)

// rawTarget speaks just enough HTTP/1 to be an upload relay. It is not httptest:
// its handler machinery buffers responses and, when a handler aborts, can leave
// the client with a socket-level error that says nothing about how much of the
// part had gone out - and that is the one fact these tests are about.
type rawTarget struct {
	URL  string
	hits atomic.Int32
	ln   net.Listener
	quit chan struct{}
}

func startRawTarget(t *testing.T, answer rawAnswer) *rawTarget {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	target := &rawTarget{URL: "http://" + ln.Addr().String(), ln: ln, quit: make(chan struct{})}
	t.Cleanup(func() { close(target.quit) })
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			target.hits.Add(1)
			go target.serve(conn, answer)
		}
	}()
	return target
}

func (rt *rawTarget) serve(conn net.Conn, answer rawAnswer) {
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	req, err := http.ReadRequest(bufio.NewReader(conn))
	if err != nil {
		return
	}
	if answer == rawNeverReadsBody {
		_, _ = io.CopyN(io.Discard, req.Body, 1<<10)
		<-rt.quit
		return
	}
	_, _ = io.Copy(io.Discard, req.Body)
	// Let the client's own write loop finish before the answer, however broken the
	// answer is. WroteRequest fires on the transport's write goroutine and the
	// response can be delivered before it, so without this margin "how much of the
	// part had gone out" is a scheduling question - and the single-part path
	// decides whether to POST again on exactly that.
	time.Sleep(200 * time.Millisecond)
	if answer == rawCloseWithoutAnswer {
		return
	}
	const head = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n"
	_, _ = conn.Write([]byte(head + `{"code":"0000","data":{"fid":"`))
}

// relayedTransportFs is an Fs that uploads through a real listener standing in for
// a relay, with a second real listener as the endpoint it falls back to.
func relayedTransportFs(t *testing.T, endpoint *uploadTarget, relayURL string) (*Fs, string) {
	t.Helper()
	f := newUnitTestFs(nil)
	f.httpClient = newHTTPClient(context.Background(), &Options{})
	f.relayRound = new(uint64)
	f.zoneURL = endpoint.URL
	f.zoneLoaded = true
	key, err := normalizeHostPort(strings.TrimPrefix(endpoint.URL, "http://"), "test endpoint")
	require.NoError(t, err)
	relayKey, err := normalizeHostPort(strings.TrimPrefix(relayURL, "http://"), "test relay")
	require.NoError(t, err)
	f.relayMap = map[string][]string{key: {relayKey}}
	return f, relayKey
}

// probeCtx bounds a transport test from above. Without it, a mutation that
// removes the per-attempt wall clock would leave these tests waiting on a real
// server forever, and the package timeout would report a panic instead of the
// assertion that is actually missing.
func probeCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// The blame rules read httptrace events, which the fake RoundTripper does not
// produce on its own. These three tests fire them by hand, because each one is a
// shape that must not cost a working relay anything: a connection the far end had
// already closed, a peer of the other family, and a part whose own bytes failed.

// TestUnitIdlePooledConnectionRepostsWhereItFailed is the pooled-connection case.
// net/http will not replay a POST body on a fresh connection by itself, so an
// attempt that dies on an idle connection before any byte of the request went out
// has to be re-sent - and the relay that handed back a dead socket is not dead.
func TestUnitIdlePooledConnectionRepostsWhereItFailed(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, failTrace: map[string]hostFault{
		"r1.example:443": {attempts: 1, peer: "198.51.100.7", reused: true},
	}}
	f := relayFs(rec, "r1.example:443", "r2.example:443")
	uploadThreeParts(t, f, "big.bin")

	// Every part stays on the relay that only lost an idle connection: moving the
	// session here would spread upload volume by a signal that is not about any
	// relay.
	assert.Equal(t, []string{
		"r1.example:443", "r1.example:443", "r1.example:443", "r1.example:443",
	}, rec.uploadHosts())
	assert.Equal(t, 2, rec.countOf(1), "the part is re-posted on the same relay, on a fresh connection")
	assert.False(t, f.relayDead.parkedByKey("r1.example:443|4", time.Now()),
		"a connection the far end closed while idle must not park the relay")
	assert.False(t, f.relayDead.parkedByKey("r1.example:443|6", time.Now()))
}

// The same rule has to hold on the single-part path, where the second rule of the
// ladder is "never POST the same request twice to the same host". An idle
// connection that died is not that case: none of the request went out, so the
// re-post is still the first send.
func TestUnitSinglePartIdleConnectionRepostsSameRelay(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, failTrace: map[string]hostFault{
		"r1.example:443": {attempts: 1, peer: "198.51.100.7", reused: true},
	}}
	f := smallPartFs(t, rec, "r1.example:443", "r2.example:443")

	small := int64(64 << 10)
	src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
	data, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, small)), "dirID", "small.bin", small, src)
	require.NoError(t, err, "the re-post on the same relay must finish the file")
	assert.NotEmpty(t, data.Fid)
	assert.Equal(t, []string{"r1.example:443", "r1.example:443"}, rec.uploadHosts(),
		"the session does not move for a connection that never carried the request")
	got := rec.contentOf(1)
	require.Len(t, got, 2)
	assert.Equal(t, got[0], got[1], "the re-post carries the same whole file")
	assert.False(t, f.relayDead.parkedByKey("r1.example:443|4", time.Now()))
}

// TestUnitParkFollowsTheFamilyOnTheWire: the pin decides how a *new* connection is
// dialled, so a request can ride an idle one of the other family. The cooldown has
// to land on the family that was actually used or IPv4 gets parked for IPv6's
// failure - and the operator's rule is that IPv4 is the family worth keeping.
func TestUnitParkFollowsTheFamilyOnTheWire(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, failTrace: map[string]hostFault{
		"r1.example:443": {attempts: 1, peer: "2001:db8::1"},
	}}
	f := relayFs(rec, "r1.example:443", "r2.example:443")
	uploadThreeParts(t, f, "big.bin")

	assert.True(t, f.relayDead.parkedByKey("r1.example:443|6", time.Now()),
		"the IPv6 peer on the wire is what failed, so IPv6 is what is parked")
	assert.False(t, f.relayDead.parkedByKey("r1.example:443|4", time.Now()),
		"the IPv4 side of that relay never failed and must stay in rotation for everyone")
	// The part itself did not complete, so the session still moves.
	assert.Equal(t, []string{
		"r1.example:443", "r2.example:443", "r2.example:443", "r2.example:443",
	}, rec.uploadHosts())
}

// TestUnitTruncatedAnswerDecidesOnWhatWasSent covers the answer that stops in the
// middle. Whether the part itself had gone out is the only thing that separates
// "POSTing again would register the object twice" from "nothing was stored yet",
// and it must come from what the transport reported, not from a guess.
func TestUnitTruncatedAnswerDecidesOnWhatWasSent(t *testing.T) {
	small := int64(64 << 10)
	src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)

	t.Run("whole object already sent", func(t *testing.T) {
		withPartRetries(t, 3)
		rec := &zoneRecorder{zoneURL: relayZoneURL, halfAnswerHost: map[string]hostFault{
			"r1.example:443": {attempts: 1, peer: "198.51.100.7", bodySent: true},
		}}
		f := smallPartFs(t, rec, "r1.example:443", "r2.example:443")
		_, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, small)), "dirID", "small.bin", small, src)
		require.Error(t, err, "a lost answer must be reported, not papered over")
		assert.Equal(t, []string{"r1.example:443"}, rec.uploadHosts(),
			"the object went out once, so it must not be posted a second time anywhere")
		assert.True(t, f.relayDead.parkedByKey("r1.example:443|4", time.Now()),
			"the relay that swallowed the answer is still the one at fault")
	})

	t.Run("answer began before the part was through", func(t *testing.T) {
		withPartRetries(t, 3)
		rec := &zoneRecorder{zoneURL: relayZoneURL, halfAnswerHost: map[string]hostFault{
			"r1.example:443": {attempts: 1, peer: "198.51.100.7"},
		}}
		f := smallPartFs(t, rec, "r1.example:443", "r2.example:443")
		data, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, small)), "dirID", "small.bin", small, src)
		require.NoError(t, err, "nothing was stored on r1, so the other relay is a fresh chance")
		assert.NotEmpty(t, data.Fid)
		assert.Equal(t, []string{"r1.example:443", "r2.example:443"}, rec.uploadHosts())
		assert.True(t, f.relayDead.parkedByKey("r1.example:443|4", time.Now()))
	})
}

// TestUnitGatewayFailureParksTheRelay covers the answer that comes from the
// gateway in front of a relay rather than from the upload service. Measured
// 2026-10-30: one relay's IPv6 leg answered 504 to 264 parts in eleven minutes
// while the other relays were healthy, and because an HTTP status used to mean
// "the relay is alive", every one of those parts was re-sent to the same host.
func TestUnitGatewayFailureParksTheRelay(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{
		zoneURL:       relayZoneURL,
		gatewayStatus: 504,
		gatewayPart:   1,
		gatewayTimes:  1,
		gatewayHost:   "r1.example:443",
	}
	f := relayFs(rec, "r1.example:443", "r2.example:443")
	uploadThreeParts(t, f, "big.bin")

	assert.Equal(t, []string{
		"r1.example:443", "r2.example:443", "r2.example:443", "r2.example:443",
	}, rec.uploadHosts(), "the part is re-posted on the relay the session moved to")
	assert.Equal(t, 2, rec.countOf(1))
	assert.True(t, f.relayDead.parkedByKey("r1.example:443|4", time.Now()),
		"a gateway 504 is the relay failing to reach the service, not an answer from it")
}

// TestUnitGatewayFailureOnASinglePartRepostsElsewhere: one part holds the whole
// file, so the usual rule is "never POST the same request twice". A gateway 504
// means nothing was stored upstream, so the second POST is the first one that can
// succeed - and staying on the same relay would just waste the file.
func TestUnitGatewayFailureOnASinglePartRepostsElsewhere(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{
		zoneURL:       relayZoneURL,
		gatewayStatus: 504,
		gatewayPart:   1,
		gatewayTimes:  1,
		gatewayHost:   "r1.example:443",
	}
	f := smallPartFs(t, rec, "r1.example:443", "r2.example:443")

	small := int64(64 << 10)
	src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
	data, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, small)), "dirID", "small.bin", small, src)
	require.NoError(t, err, "the file must finish on the relay the session moved to")
	assert.NotEmpty(t, data.Fid)
	assert.Equal(t, []string{"r1.example:443", "r2.example:443"}, rec.uploadHosts())
	assert.True(t, f.relayDead.parkedByKey("r1.example:443|4", time.Now()))
}

// TestUnitRelayUnavailableStatus pins which statuses condemn the path. 500 and
// the 4xx rejections are answers from the upload service and must keep their old
// meaning; 503 is the usual shape of rate limiting and only means "come back
// later", so it must not take a relay out of rotation for a whole cooldown.
func TestUnitRelayUnavailableStatus(t *testing.T) {
	for _, code := range []int{502, 504} {
		assert.True(t, relayUnavailableStatus(code), "%d comes from the gateway in front of the relay", code)
	}
	for _, code := range []int{200, 400, 401, 403, 404, 413, 429, 500, 501, 503, 505} {
		assert.False(t, relayUnavailableStatus(code), "%d is not a gateway path failure", code)
	}
}

// TestUnitGatewayFailureOnDirectPathPostsOnce: the session is already on the
// endpoint (every relay cooling), so the gateway answer has nowhere to move it.
// Re-posting would put the same request on the same host, which is exactly what
// the single-attempt rule prevents.
func TestUnitGatewayFailureOnDirectPathPostsOnce(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{
		zoneURL:       relayZoneURL,
		gatewayStatus: 502,
		gatewayPart:   1,
		gatewayTimes:  9,
		gatewayHost:   relayZoneHost,
	}
	f := smallPartFs(t, rec, "r1.example:443")
	now := time.Now()
	for _, target := range []dialTarget{
		{host: "r1.example:443", family: familyV4},
		{host: "r1.example:443", family: familyV6},
	} {
		require.True(t, f.relayDead.parkRelay(target, time.Hour, now))
	}

	small := int64(64 << 10)
	src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
	_, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, small)), "dirID", "small.bin", small, src)
	require.Error(t, err, "a gateway answer from the endpoint itself must be reported")
	assert.Equal(t, []string{relayZoneHost}, rec.uploadHosts(),
		"the cursor had nowhere to move, so exactly one POST is the rule")
	assert.False(t, f.relayDead.parkedByKey(relayZoneHost+"|0", now), "the endpoint itself is never parked")
}

// TestUnitDirectAttemptHasNoWallClock: the per-attempt clock exists so a stalled
// relay can be abandoned for the next one. A direct attempt has nowhere to go, so
// imposing one would abandon a legitimately slow upload of up to upload_cutoff
// bytes. The override makes the difference observable without waiting minutes.
func TestUnitDirectAttemptHasNoWallClock(t *testing.T) {
	withPartRetries(t, 3)
	// A real transport: the fake RoundTripper ignores the request context, so a
	// clock-wrapped attempt would still answer and the test could not tell the
	// difference.
	endpoint := startAnsweringTarget(t)
	f := newUnitTestFs(nil)
	f.httpClient = newHTTPClient(context.Background(), &Options{})
	f.zoneURL = endpoint.URL
	f.zoneLoaded = true
	// Far too short for anything real: if the clock were applied to a direct
	// attempt this upload could not finish.
	f.relayPartDeadlineOverride = time.Nanosecond

	small := int64(64 << 10)
	src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
	data, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, small)), "dirID", "small.bin", small, src)
	require.NoError(t, err, "a direct attempt must not be cut by the relay wall clock")
	assert.NotEmpty(t, data.Fid)
}

// TestUnitSinglePartOnDirectPathPostsOnce: with every relay cooling the session
// opens on the endpoint. The re-post ladder would otherwise let one part - which
// here is the whole file - be POSTed once per candidate, and the rule that a
// single request goes out once is the one that has to hold on that path too.
func TestUnitSinglePartOnDirectPathPostsOnce(t *testing.T) {
	withPartRetries(t, 3)
	rec := &zoneRecorder{zoneURL: relayZoneURL, failTrace: map[string]hostFault{
		relayZoneHost: {attempts: 9, peer: "198.51.100.9"},
	}}
	f := smallPartFs(t, rec, "r1.example:443", "r2.example:443")
	now := time.Now()
	for _, target := range []dialTarget{
		{host: "r1.example:443", family: familyV4},
		{host: "r1.example:443", family: familyV6},
		{host: "r2.example:443", family: familyV4},
		{host: "r2.example:443", family: familyV6},
	} {
		require.True(t, f.relayDead.parkRelay(target, time.Hour, now))
	}

	small := int64(64 << 10)
	src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
	_, err := f.uploadSingle(context.Background(), bytes.NewReader(make([]byte, small)), "dirID", "small.bin", small, src)
	require.Error(t, err)
	assert.Equal(t, []string{relayZoneHost}, rec.uploadHosts(),
		"the cursor had nowhere to move, so exactly one POST is the rule")
}

// TestUnitSourceFaultIsNotChargedToTheRelay uses a real transport, because the
// shape only exists once net/http is the one reading the part: a source that
// errors, and a source that ends short of the ContentLength declared, both leave
// the request unfinished through no fault of the address it was sent to.
func TestUnitSourceFaultIsNotChargedToTheRelay(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   io.Reader
		want string
	}{
		// A body that errors may or may not reach the relay at all: whether the
		// headers made it out of the transport's buffer before the read broke is
		// arithmetic about buffer sizes, not a property worth pinning. What is
		// pinning is that the relay is not blamed, and not asked twice.
		{name: "source errors", in: struct{ io.Reader }{&failingReader{}}, want: "disk on fire"},
		// Short by 4 KiB: the body ends below the declared length, so the server
		// could not have stored the object whatever it answered.
		{name: "source ends short", in: struct{ io.Reader }{&countingReader{left: 60 << 10}}, want: "Body length"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withPartRetries(t, 3)
			endpoint := startAnsweringTarget(t)
			relay := startAnsweringTarget(t)
			f, relayKey := relayedTransportFs(t, endpoint, relay.URL)

			small := int64(64 << 10)
			src := fsobject.NewStaticObjectInfo("small.bin", time.Now(), small, true, nil, nil)
			_, err := f.uploadSingle(probeCtx(t), tc.in, "dirID", "small.bin", small, src)
			require.Error(t, err, "a part whose own bytes failed must be reported")
			assert.Contains(t, err.Error(), tc.want)
			assert.LessOrEqual(t, relay.hits.Load(), int32(1), "the relay is not asked twice")
			assert.Equal(t, int32(0), endpoint.hits.Load(),
				"re-posting the same broken bytes elsewhere cannot succeed, and this reader cannot be rewound")
			assert.False(t, f.relayDead.parkedByKey(relayKey+"|4", time.Now()),
				"the source is what broke, so nothing may be parked")
		})
	}
}

// failingReader breaks after a little data, the way a source object that vanished
// mid-read does.
type failingReader struct{ sent int }

func (r *failingReader) Read(p []byte) (int, error) {
	if r.sent > 4<<10 {
		return 0, errors.New("disk on fire")
	}
	n := copy(p, make([]byte, 4<<10))
	r.sent += n
	return n, nil
}

// countingReader yields n bytes and then ends.
type countingReader struct{ left int }

func (r *countingReader) Read(p []byte) (int, error) {
	if r.left <= 0 {
		return 0, io.EOF
	}
	if r.left < len(p) {
		p = p[:r.left]
	}
	n := copy(p, make([]byte, len(p)))
	r.left -= n
	return n, nil
}
