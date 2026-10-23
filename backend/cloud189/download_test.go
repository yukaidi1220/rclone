package cloud189

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// Downloading is two calls: the drive mints a link for one object id, and the
// bytes are read from whatever storage host that link names. The tests below pin
// the three things that can go wrong in that handover - the link arrives
// HTML-escaped, it goes stale on its own schedule rather than rclone's, and it is
// a signed URL that must not end up in a message. The last of those is checked on
// the upload half too, because a part PUT carries a signature of its own.

// TestDownloadReadsThroughTheUnescapedLink is the escape rule. The link the API
// writes for the family space carries &amp; inside its own query, so the string
// that has to be requested is not the string that was sent.
func TestDownloadReadsThroughTheUnescapedLink(t *testing.T) {
	drive := &fakeDrive{t: t, readBody: []byte("0123456789")}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip-01.mp4", "900000000000000004")

	rc, err := obj.Open(context.Background())
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, []byte("0123456789"), got)

	require.Len(t, drive.readQueries, 1)
	require.NotContains(t, drive.readQueries[0], "amp", "the escaped ampersand was sent to the storage layer verbatim")
	require.Contains(t, drive.readQueries[0], "sig=LIVE-SIGNATURE-VALUE", "and the signature arrived intact")

	require.Equal(t, 1, drive.downloadHits, "the link was minted once")
	_, err = obj.Open(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, drive.downloadHits, "a link that is still inside its cache window is not minted again")
	require.Len(t, drive.readQueries, 2, "while the second read still went to the storage layer")
}

// TestExpiredLinkIsMintedAgainOnce covers the case the cache window cannot fully
// avoid: a link that aged out between being cached and being used. One fresh link
// is fetched and the read is retried with it - and nothing that was asked for with
// the dead link is reported as success.
func TestExpiredLinkIsMintedAgainOnce(t *testing.T) {
	drive := &fakeDrive{t: t, readBody: []byte("0123456789"), readStatus: []int{http.StatusForbidden}}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip-01.mp4", "900000000000000004")

	rc, err := obj.Open(context.Background())
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, []byte("0123456789"), got)

	require.Equal(t, 2, drive.downloadHits, "the dead link was dropped and a new one asked for")
	require.Len(t, drive.readQueries, 2)
}

// TestDownloadGivesUpAfterTheSecondRefusal: the retry is one deep. A link that is
// refused twice means something other than age is wrong, and looping here would
// spend requests on a read that cannot succeed.
func TestDownloadGivesUpAfterTheSecondRefusal(t *testing.T) {
	drive := &fakeDrive{t: t, readStatus: []int{http.StatusForbidden, http.StatusForbidden}}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip-01.mp4", "900000000000000004")

	_, err := obj.Open(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 403")
	require.Equal(t, 2, drive.downloadHits, "and no third link was minted")
	require.Len(t, drive.readQueries, 2)
}

// TestDownloadRefusalDoesNotQuoteTheSignature is the log-hygiene rule on the
// download path. The storage layer echoes the request it refused, URL and query
// included, and that query is a live signature for the object.
func TestDownloadRefusalDoesNotQuoteTheSignature(t *testing.T) {
	drive := &fakeDrive{t: t, readStatus: []int{http.StatusForbidden, http.StatusForbidden}}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip-01.mp4", "900000000000000004")

	_, err := obj.Open(context.Background())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "LIVE-SIGNATURE-VALUE", "the refusal echoed the signed URL into the message")
}

// TestDownloadTransportFailureDoesNotQuoteTheLink covers the other way the same
// secret travels: the transport itself quotes the URL it could not reach.
func TestDownloadTransportFailureDoesNotQuoteTheLink(t *testing.T) {
	drive := &fakeDrive{t: t,
		downloadLink: "http://127.0.0.1:1/store/file?a=1&amp;sig=LIVE-SIGNATURE-VALUE"}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip-01.mp4", "900000000000000004")

	_, err := obj.Open(context.Background())
	require.Error(t, err, "nothing is listening on that port, so the read has to fail")
	require.NotContains(t, err.Error(), "LIVE-SIGNATURE-VALUE")
	require.Contains(t, err.Error(), "<redacted>", "the URL is said to be redacted rather than shown")
}

// TestPartRefusalDoesNotQuoteTheSignature is the same rule on the way up. The
// presigned part URL is a signature for one part of one file, and the storage
// layer answers a refusal with the request it refused, query included.
func TestPartRefusalDoesNotQuoteTheSignature(t *testing.T) {
	noWaiting(t)
	drive := &fakeDrive{t: t, refuseAllPuts: true}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("clip.mp4", int64(len(content))))
	require.Error(t, err)
	require.Contains(t, err.Error(), "refused by the storage layer")
	require.NotContains(t, err.Error(), "PART-SIGNATURE-1", "the refusal echoed the presigned query into the message")
}
