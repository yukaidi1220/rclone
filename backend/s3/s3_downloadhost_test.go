// Tests for the download_host presigned-CDN download path.

package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/lib/bucket"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeDownloadHostFs builds an Fs wired to an unreachable origin (presigning
// is a purely local operation, so the origin is never contacted) and a fake
// CDN server that receives the actual GET.
func makeDownloadHostFs(t *testing.T, cdn *httptest.Server) *Fs {
	ctx, opt, client := SetupS3Test(t)
	opt.Endpoint = "http://origin.invalid" // unreachable on purpose; http so the rewritten URL matches the plain-HTTP fake CDN
	// httptest serves plain HTTP; keep scheme in sync with the fake CDN.
	opt.DownloadHost = cdn.URL[strings.Index(cdn.URL, "//")+2:]
	opt.AccessKeyID = "id"
	opt.SecretAccessKey = "secret"

	c, _, err := s3Connection(ctx, opt, client)
	require.NoError(t, err)

	f := &Fs{
		name:    "s3test",
		opt:     *opt,
		ctx:     ctx,
		c:       c,
		pacer:   fs.NewPacer(ctx, pacer.NewS3(pacer.MinSleep(minSleep))),
		cache:   bucket.NewCache(),
		srvRest: rest.NewClient(fshttp.NewClient(ctx)),
	}
	f.setRoot("bucket")
	return f
}

// TestDownloadPresignedHostRewrite checks that Open with download_host:
//   - never contacts the origin endpoint (it is unreachable),
//   - sends the GET to the CDN host,
//   - carries the SigV4 query parameters,
//   - passes Range as a plain header (not part of the signed query),
//   - maps response headers onto the object metadata.
func TestDownloadPresignedHostRewrite(t *testing.T) {
	var gotReq *http.Request
	var gotRange string
	var gotQuery url.Values
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReq = r
		gotRange = r.Header.Get("Range")
		gotQuery = r.URL.Query()
		w.Header().Set("ETag", `"5d41402abc4b2a76b9719d911017c592"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Amz-Meta-Foo", "bar")
		// A real CDN answers a ranged GET with 206 and the requested window.
		w.Header().Set("Content-Range", "bytes 0-3/10")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("0123"))
	}))
	defer cdn.Close()

	f := makeDownloadHostFs(t, cdn)
	o := &Object{fs: f, remote: "08887b268e7c8196b6a0baf5/dir/file.bin", bytes: 10}

	in, err := o.Open(context.Background(), &fs.RangeOption{Start: 0, End: 3})
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(in, buf)
	require.NoError(t, err)
	require.NoError(t, in.Close())
	assert.Equal(t, "0123", string(buf), "should read only the requested range")

	require.NotNil(t, gotReq, "CDN should have received the GET")
	assert.Equal(t, f.opt.DownloadHost, gotReq.Host, "request must go to the CDN host")
	assert.NotEmpty(t, gotQuery.Get("X-Amz-Signature"), "SigV4 signature must be present in query")
	assert.NotEmpty(t, gotQuery.Get("X-Amz-Credential"), "credential must be present in query")
	assert.Equal(t, "host", gotQuery.Get("X-Amz-SignedHeaders"), "only host should be signed")
	assert.Equal(t, "bytes=0-3", gotRange, "Range must travel as a plain header")

	assert.Equal(t, "5d41402abc4b2a76b9719d911017c592", o.md5, "MD5-format ETag should be applied to the object")
}

// TestDownloadPresignedRangeIgnored checks that a CDN answering a ranged GET
// with the full body (HTTP 200) surfaces fs.ErrorRangeIgnored instead of
// handing out misplaced bytes - the multi-thread engine keys its
// single-stream fallback on that error.
func TestDownloadPresignedRangeIgnored(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("0123456789")) // ignores Range, answers 200
	}))
	defer cdn.Close()

	f := makeDownloadHostFs(t, cdn)
	o := &Object{fs: f, remote: "08887b268e7c8196b6a0baf5/dir/file.bin", bytes: 10}

	_, err := o.Open(context.Background(), &fs.RangeOption{Start: 4, End: 7})
	require.ErrorIs(t, err, fs.ErrorRangeIgnored)
}

// TestDownloadPresignedRangeShifted checks that a mismatched Content-Range is
// rejected: a 206 with the wrong window would otherwise corrupt the assembled
// file without tripping any size or protocol error.
func TestDownloadPresignedRangeShifted(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 8-11/12")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("89ab"))
	}))
	defer cdn.Close()

	f := makeDownloadHostFs(t, cdn)
	o := &Object{fs: f, remote: "08887b268e7c8196b6a0baf5/dir/file.bin", bytes: 12}

	_, err := o.Open(context.Background(), &fs.RangeOption{Start: 4, End: 7})
	require.Error(t, err)
	assert.NotErrorIs(t, err, fs.ErrorRangeIgnored)
}

// TestDownloadPresignedExpiryClamp checks that Open succeeds for size
// extremes (0 and unknown), i.e. the expiry estimate stays within bounds.
func TestDownloadPresignedExpiryClamp(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("x"))
	}))
	defer cdn.Close()

	f := makeDownloadHostFs(t, cdn)

	for _, size := range []int64{0, -1, 100 << 30} {
		o := &Object{fs: f, remote: "file.bin", bytes: size}
		in, err := o.Open(context.Background())
		require.NoError(t, err, "size=%d", size)
		require.NoError(t, in.Close())
	}
}

// TestNewFsDownloadOptionsMutex checks the mutual exclusion errors.
func TestNewFsDownloadOptionsMutex(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name           string
		downloadURL    string
		downloadHost   string
		sseCustomerKey string
		wantErr        string
	}{
		{name: "url+host", downloadURL: "https://cdn.example/", downloadHost: "cdn.example", wantErr: "same time"},
		{name: "host+ssec", downloadHost: "cdn.example", sseCustomerKey: "key", wantErr: "sse_customer_key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := configmap.Simple{
				"type":              "s3",
				"provider":          "Alibaba",
				"region":            "eu-west-1",
				"access_key_id":     "id",
				"secret_access_key": "secret",
				"endpoint":          "https://oss-eu-west-1.yukaidi.com",
				"chunk_size":        "64Mi",
				"copy_cutoff":       "1Gi",
			}
			if test.downloadURL != "" {
				m["download_url"] = test.downloadURL
			}
			if test.downloadHost != "" {
				m["download_host"] = test.downloadHost
			}
			if test.sseCustomerKey != "" {
				m["sse_customer_key"] = test.sseCustomerKey
			}
			_, err := NewFs(ctx, "test", "bucket:", m)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

// TestHeadPresignedViaDownloadHost checks that Fs-level HeadObject with
// download_host:
//   - never contacts the origin endpoint (it is unreachable),
//   - uses a presigned first-byte GET instead of HEAD (some CDNs/workers
//     mishandle HEAD while serving GET fine),
//   - sends Range as a plain unsigned header,
//   - maps response headers (incl. Content-Range total) onto the output.
func TestHeadPresignedViaDownloadHost(t *testing.T) {
	var gotReq *http.Request
	var gotQuery url.Values
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotReq = r
		gotQuery = r.URL.Query()
		assert.Equal(t, "bytes=0-0", r.Header.Get("Range"), "metadata probe must request only the first byte")
		w.Header().Set("ETag", `"5d41402abc4b2a76b9719d911017c592"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Amz-Meta-Foo", "bar")
		w.Header().Set("Last-Modified", time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC).Format(http.TimeFormat))
		w.Header().Set("Content-Range", "bytes 0-0/10")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("x"))
	}))
	defer cdn.Close()

	f := makeDownloadHostFs(t, cdn)
	o := &Object{fs: f, remote: "08887b268e7c8196b6a0baf5/dir/file.bin"}

	bucketName, bucketPath := o.split()
	resp, err := f.headObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucketName, Key: &bucketPath})
	require.NoError(t, err)

	require.NotNil(t, gotReq, "CDN should have received the metadata GET")
	assert.Equal(t, http.MethodGet, gotReq.Method, "must be a GET, not HEAD - workers may rewrite HEAD to GET breaking the signature")
	assert.Equal(t, f.opt.DownloadHost, gotReq.Host, "request must go to the CDN host")
	assert.NotEmpty(t, gotQuery.Get("X-Amz-Signature"), "SigV4 signature must be present in query")
	assert.NotEmpty(t, gotQuery.Get("X-Amz-Credential"), "credential must be present in query")
	assert.Equal(t, "host", gotQuery.Get("X-Amz-SignedHeaders"), "only host should be signed")

	assert.NotNil(t, resp.ETag)
	assert.NotNil(t, resp.ContentLength)
	assert.Equal(t, int64(10), *resp.ContentLength, "size must come from the Content-Range total")
	assert.False(t, resp.LastModified.IsZero(), "modtime must come from the Last-Modified header")
	assert.Equal(t, map[string]string{"foo": "bar"}, resp.Metadata, "x-amz-meta-* headers must become metadata")
}

// TestHeadPresignedZeroLength checks that a zero-length object (any range is
// unsatisfiable -> HTTP 416) is reported as existing with size 0.
func TestHeadPresignedZeroLength(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	}))
	defer cdn.Close()

	f := makeDownloadHostFs(t, cdn)
	o := &Object{fs: f, remote: "empty.bin"}

	bucketName, bucketPath := o.split()
	resp, err := f.headObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucketName, Key: &bucketPath})
	require.NoError(t, err)
	assert.NotNil(t, resp.ContentLength)
	assert.Equal(t, int64(0), *resp.ContentLength, "416 on a first-byte range means the object has no bytes")
}

// TestHeadPresignedNotFound checks that a 404 through the CDN maps to
// fs.ErrorObjectNotFound so callers treat it as "object absent".
func TestHeadPresignedNotFound(t *testing.T) {
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer cdn.Close()

	f := makeDownloadHostFs(t, cdn)
	o := &Object{fs: f, remote: "missing.bin"}

	bucketName, bucketPath := o.split()
	_, err := f.headObject(context.Background(), &s3.HeadObjectInput{Bucket: &bucketName, Key: &bucketPath})
	assert.Equal(t, fs.ErrorObjectNotFound, err)
}

// keep the s3 import referenced if upstream tests change
var _ = s3.HeadObjectOutput{}
var _ = time.Hour
