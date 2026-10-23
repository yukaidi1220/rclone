package fshttp

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestNoteLookupShape locks the "log once per changed shape" rule: the point of
// the line is to make an AAAA-only lookup visible without flooding the log with
// one entry per dial.
func TestNoteLookupShape(t *testing.T) {
	var store sync.Map
	assert.True(t, noteLookupShape(&store, "relay.example", 14, 3), "the first lookup of a host is always news")
	assert.False(t, noteLookupShape(&store, "relay.example", 14, 3), "the same shape must not be logged again")
	assert.False(t, noteLookupShape(&store, "relay.example", 9, 2), "address-count drift inside the same shape is not news")
	assert.True(t, noteLookupShape(&store, "relay.example", 0, 3), "a name that stops resolving IPv4 is a change")
	assert.True(t, noteLookupShape(&store, "relay.example", 14, 3), "and so is recovering it")
	assert.True(t, noteLookupShape(&store, "other.example", 0, 3), "the shape is tracked per host, not globally")
}

// TestNoteFallbackLog keeps the fall-through line rate limited: one per host
// per interval, never suppressed forever.
func TestNoteFallbackLog(t *testing.T) {
	var store sync.Map
	t0 := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	assert.True(t, noteFallbackLog(&store, "relay.example", t0, time.Minute), "the first fall-through is logged")
	assert.False(t, noteFallbackLog(&store, "relay.example", t0.Add(30*time.Second), time.Minute), "a second one inside the window is not")
	assert.True(t, noteFallbackLog(&store, "relay.example", t0.Add(61*time.Second), time.Minute), "the window must expire")
	assert.True(t, noteFallbackLog(&store, "other.example", t0.Add(61*time.Second), time.Minute), "the window is per host")
}

// TestSummarizeAddrs keeps the one-line rendering bounded and honest about the
// part that was cut.
func TestSummarizeAddrs(t *testing.T) {
	ip := func(s string) net.IPAddr { return net.IPAddr{IP: net.ParseIP(s)} }
	assert.Equal(t, "none", summarizeAddrs(nil))
	assert.Equal(t, "10.0.0.1", summarizeAddrs([]net.IPAddr{ip("10.0.0.1")}))
	assert.Equal(t, "10.0.0.1,10.0.0.2", summarizeAddrs([]net.IPAddr{ip("10.0.0.1"), ip("10.0.0.2")}))
	assert.Equal(t,
		"10.0.0.1,10.0.0.2,10.0.0.3,+2 more",
		summarizeAddrs([]net.IPAddr{ip("10.0.0.1"), ip("10.0.0.2"), ip("10.0.0.3"), ip("10.0.0.4"), ip("10.0.0.5")}))
}
