package s3

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/bucket"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// versionHits counts the requests a test server saw by kind, so that both
// halves are assertable: "we did not touch versioning" and "the versioned path
// actually did the versioned work".
type versionHits struct {
	head       int
	versions   int
	versioning int
	deletes    int
	// bucketVersioned makes the fake answer GetBucketVersioning with Enabled.
	bucketVersioned bool
}

func newCountedTestFs(t *testing.T) (*Fs, *versionHits) {
	t.Helper()
	hits := &versionHits{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Query().Has("versioning"):
			hits.versioning++
			body := `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></VersioningConfiguration>`
			if hits.bucketVersioned {
				body = `<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>`
			}
			_, err := io.WriteString(w, body)
			assert.NoError(t, err)
		case r.URL.Query().Has("versions"):
			hits.versions++
			_, err := io.WriteString(w, `<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<IsTruncated>false</IsTruncated>
<Version><Key>prefix/file</Key><VersionId>version+id/1</VersionId><IsLatest>false</IsLatest>
<LastModified>2024-01-01T00:00:00Z</LastModified><Size>1</Size></Version>
</ListVersionsResult>`)
			assert.NoError(t, err)
		case r.Method == http.MethodHead:
			hits.head++
			if strings.HasSuffix(r.URL.Path, "/missing") {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", "1")
			w.Header().Set("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT")
		case r.Method == http.MethodDelete:
			hits.deletes++
			w.WriteHeader(http.StatusNoContent)
		default:
			// object listings, v1 or v2: always empty
			_, err := io.WriteString(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>bucket</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
</ListBucketResult>`)
			assert.NoError(t, err)
		}
	}))
	t.Cleanup(server.Close)

	ctx, opt, client := SetupS3Test(t)
	opt.Endpoint = server.URL
	opt.ForcePathStyle = true
	opt.Region = "us-east-1"
	opt.AccessKeyID = "test-access-key"
	opt.SecretAccessKey = "test-secret-key"
	opt.SessionToken = "test-token+/="
	c, _, err := s3Connection(ctx, opt, client)
	require.NoError(t, err)
	f := &Fs{
		name:  "s3test",
		opt:   *opt,
		ctx:   ctx,
		ci:    fs.GetConfig(ctx),
		c:     c,
		pacer: fs.NewPacer(ctx, pacer.NewS3(pacer.MinSleep(minSleep))),
		cache: bucket.NewCache(),
	}
	f.setRoot("bucket/prefix")
	return f, hits
}

// With no version switch set, signing a link must not spend a request on the
// object at all, and a missing object must not be discovered here.
func TestPublicLinkWithoutVersionsSendsNoRequest(t *testing.T) {
	f, hits := newCountedTestFs(t)

	link, err := f.PublicLink(context.Background(), "file", fs.DurationOff, false)
	require.NoError(t, err)
	assert.Contains(t, link, "/bucket/prefix/file")
	assert.NotContains(t, link, "versionId=", "an unversioned link must not name a version")
	assert.Equal(t, 0, hits.head, "link signing must not stat the object")

	_, err = f.PublicLink(context.Background(), "missing", fs.DurationOff, false)
	require.NoError(t, err, "the missing object is only visible when the link is opened")
	assert.Equal(t, 0, hits.head)
	assert.Equal(t, 0, hits.versions)
}

// Turning --s3-versions on buys back both halves of the old behaviour: the
// object is looked up, and a missing object is an error here rather than later.
// Pinning the link to a resolved version is covered by TestCommandLinkVersionAt.
func TestPublicLinkWithVersionsStatsTheObject(t *testing.T) {
	f, hits := newCountedTestFs(t)
	f.opt.Versions = true

	_, err := f.PublicLink(context.Background(), "file", fs.DurationOff, false)
	require.NoError(t, err)
	assert.Equal(t, 1, hits.head, "--s3-versions must still resolve the object")

	_, err = f.PublicLink(context.Background(), "missing", fs.DurationOff, false)
	assert.ErrorIs(t, err, fs.ErrorObjectNotFound)
}

// Purging a directory must not ask the bucket whether it is versioned unless
// --s3-versions says so, or unless the caller asked for old versions by name.
func TestPurgeWithoutVersionsDoesNotProbeVersioning(t *testing.T) {
	f, hits := newCountedTestFs(t)

	require.NoError(t, f.purge(context.Background(), "", false))
	assert.Equal(t, 0, hits.versioning, "plain purge must not read the versioning status")
	assert.Equal(t, 0, hits.versions, "plain purge must not use the versions lister")

	require.NoError(t, f.purge(context.Background(), "", true))
	assert.Equal(t, 1, hits.versioning, "removing old versions must still check whether the bucket is versioned")
	assert.Equal(t, 0, hits.deletes, "an unversioned bucket has no old versions to remove")
}

// The other side of the same gate: when the bucket really is versioned, the
// request must be acted on, not just made and discarded.
func TestPurgeWithVersionedBucketRemovesOldVersions(t *testing.T) {
	f, hits := newCountedTestFs(t)
	hits.bucketVersioned = true

	require.NoError(t, f.purge(context.Background(), "", true))
	assert.Equal(t, 1, hits.versioning)
	assert.Greater(t, hits.versions, 0, "a versioned bucket must be purged through the versions lister")
	assert.Equal(t, 1, hits.deletes, "the non-current version must be deleted by version id")
}
