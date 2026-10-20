package yun139

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/rclone/rclone/fs/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The shared-group listing carries no hash at all (digest:null, fileEtag:0),
// so Object.Hash for a group asks the EOS edge for it with a one-byte ranged
// GET. These tests pin that mechanism without touching the network: the range
// that must be sent, which header the value may come from, and the fact that a
// missing header degrades to "no hash" instead of failing a whole check run.

func hashProbeFs(t *testing.T, h http.Handler) (f *Fs, url string, closeSrv func()) {
	t.Helper()
	srv := httptest.NewServer(h)
	f = &Fs{
		space:      spaceGroup,
		auth:       "Zm9v",
		tokMu:      &sync.Mutex{},
		account:    "13900000000",
		httpClient: srv.Client(),
	}
	return f, srv.URL, srv.Close
}

func TestContentHashFromLink(t *testing.T) {
	const want = "d372e9039b3fe62575f792cf4038684cfa06e366b9703d9be31155322d86a174"

	t.Run("reads x-eos-hash-sha256 and asks for exactly one byte", func(t *testing.T) {
		var gotRange string
		f, url, closeSrv := hashProbeFs(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotRange = r.Header.Get("Range")
			// The server answers upper case; rclone must hand back lower case.
			w.Header().Set("x-eos-hash-sha256", strings.ToUpper(want))
			w.Header().Set("Content-Range", "bytes 0-0/1048576")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte{0x00})
		}))
		defer closeSrv()

		got, err := f.contentHashFromLink(context.Background(), url)
		require.NoError(t, err)
		assert.Equal(t, want, got)
		assert.Equal(t, "bytes=0-0", gotRange, "a wider range risks pulling the whole object down")
	})

	t.Run("falls back to x-amz-content-sha256", func(t *testing.T) {
		f, url, closeSrv := hashProbeFs(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("x-amz-content-sha256", want)
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte{0x00})
		}))
		defer closeSrv()
		got, err := f.contentHashFromLink(context.Background(), url)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("a 200 that ignored Range still yields the hash", func(t *testing.T) {
		f, url, closeSrv := hashProbeFs(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("x-eos-hash-sha256", want)
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, strings.Repeat("x", 4096))
		}))
		defer closeSrv()
		got, err := f.contentHashFromLink(context.Background(), url)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("no hash header degrades to empty, not error", func(t *testing.T) {
		f, url, closeSrv := hashProbeFs(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte{0x00})
		}))
		defer closeSrv()
		got, err := f.contentHashFromLink(context.Background(), url)
		require.NoError(t, err, "one object without a hash must not abort a whole check")
		assert.Empty(t, got)
	})

	t.Run("an error status is an error", func(t *testing.T) {
		f, url, closeSrv := hashProbeFs(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusForbidden)
		}))
		defer closeSrv()
		_, err := f.contentHashFromLink(context.Background(), url)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "403")
	})
}

// TestGroupHashIsCached asserts a hash already on the object is returned
// without any request: without that, every comparison in a long-running
// transfer would mint a fresh link per object.
func TestGroupHashIsCached(t *testing.T) {
	const want = "11453dcf79cbf6e231633ab04e4768f9c4d98f75fbc80ccd7ed950f37e6ddd55"
	calls := 0
	f, _, closeSrv := hashProbeFs(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusPartialContent)
	}))
	defer closeSrv()

	o := &Object{fs: f, remote: "x.bin", sha256: want, hashMu: &sync.Mutex{}, urlMu: &sync.Mutex{}}
	got, err := o.Hash(context.Background(), hash.SHA256)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, 0, calls, "a cached hash must not hit the network")

	_, err = o.Hash(context.Background(), hash.MD5)
	assert.ErrorIs(t, err, hash.ErrUnsupported, "MD5 is never offered")
}

// TestHashesAdvertises pins who claims SHA-256: personal gets it free from the
// listing, the group from the ranged probe, and the family claims nothing -
// advertising a hash it cannot produce would make --checksum fail on every
// file.
func TestHashesAdvertises(t *testing.T) {
	f := &Fs{tokMu: &sync.Mutex{}}
	f.space = spaceGroup
	assert.Equal(t, hash.Set(hash.SHA256), f.Hashes(), "group must advertise SHA-256 now that the probe exists")
	f.space = spacePersonal
	assert.Equal(t, hash.Set(hash.SHA256), f.Hashes())
	f.space = spaceFamily
	assert.Equal(t, hash.Set(0), f.Hashes(), "family has no verified source of SHA-256")
}
