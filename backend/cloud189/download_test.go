package cloud189

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"

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

// TestADownloadCutShortIsContinued: the storage host has been measured to drop a
// large transfer part way and answer the same object in full afterwards (2026-10-24:
// the standard suite's 21 MiB read failed with unexpected EOF three runs in a row
// while the object read back whole minutes later). An object cannot change under a
// reader - it is addressed by id - so the rest is asked for from the byte already
// delivered, with a fresh link.
func TestADownloadCutShortIsContinued(t *testing.T) {
	drive := &fakeDrive{t: t, readBody: []byte("0123456789"), readCut: []int64{4}}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip-01.mp4", "900000000000000004")
	obj.size = 10

	rc, err := obj.Open(context.Background())
	require.NoError(t, err)
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	require.Equal(t, []byte("0123456789"), got, "the half the host did not send came from a ranged read")

	require.Len(t, drive.readQueries, 2, "the transfer was continued, not restarted")
	require.Empty(t, drive.readRanges[0])
	require.Equal(t, "bytes=4-9", drive.readRanges[1], "asked for the rest from the byte already delivered")
	require.Equal(t, 2, drive.downloadHits, "on a link of its own")
}

// TestARangedDownloadIsLeftAsTheHostAnsweredIt: continuing a read that asked for
// part of an object is a different measurement from continuing the whole object,
// and nothing has measured the host cutting a window. So a ranged read is handed
// over as it arrived, and a cut in one is reported rather than guessed at.
func TestARangedDownloadIsLeftAsTheHostAnsweredIt(t *testing.T) {
	drive := &fakeDrive{t: t, readBody: []byte("0123456789"), readCut: []int64{2}}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip-01.mp4", "900000000000000004")
	obj.size = 10

	rc, err := obj.Open(context.Background(), &fs.RangeOption{Start: 4, End: -1})
	require.NoError(t, err)
	_, err = io.ReadAll(rc)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF, "a cut window surfaces as it is")
	require.NoError(t, rc.Close())

	require.Equal(t, "bytes=4-9", drive.readRanges[0])
	require.Len(t, drive.readQueries, 1, "nothing is asked for a second time")
}

// TestADownloadCutEveryTimeIsReported: a host that cuts every attempt is reported
// instead of being asked forever.
func TestADownloadCutEveryTimeIsReported(t *testing.T) {
	drive := &fakeDrive{t: t, readBody: []byte("0123456789"), readCut: []int64{0, 0, 0, 0, 0, 0, 0}}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip-01.mp4", "900000000000000004")
	obj.size = 10

	rc, err := obj.Open(context.Background())
	require.NoError(t, err)
	_, err = io.ReadAll(rc)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Contains(t, err.Error(), "after 4 reconnects", "the attempts are bounded and the message says where it stopped")
	require.NoError(t, rc.Close())

	require.Len(t, drive.readQueries, 1+maxResumes, "one read and the reconnects it allows, and no more")
}

// TestADownloadOfUnknownLengthIsNotContinued: without a length there is nothing to
// measure a short body against, so a cut is reported as it is.
func TestADownloadOfUnknownLengthIsNotContinued(t *testing.T) {
	drive := &fakeDrive{t: t, readBody: []byte("0123456789"), readCut: []int64{4}}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip-01.mp4", "900000000000000004")
	obj.size = 0

	rc, err := obj.Open(context.Background())
	require.NoError(t, err)
	_, err = io.ReadAll(rc)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NotContains(t, err.Error(), "stopped at", "there is no length to report progress against")
	require.NoError(t, rc.Close())
	require.Len(t, drive.readQueries, 1, "nothing was asked for a second time")
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

// TestPublicLinkMintsAFreshUnescapedLink is the contract `/operations/publiclink`
// hands out: a URL that works for whoever receives it. Two things can quietly
// break that. The link the API writes carries `&amp;` inside its own query, so
// the string that has to be handed over is not the string that was sent. And a
// link that was kept and handed out again is part way through a lifetime rclone
// cannot shorten - rclone re-mints when its own read dies, the recipient has no
// way to. The drive answers each request with a different link here, so the mint
// number says which one came out of rclone's hand.
func TestPublicLinkMintsAFreshUnescapedLink(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("7000000000000000007", "clip.mp4", rootID)}, nil)
	}

	require.NotNil(t, f.Features().PublicLink, "without the feature the rc endpoint says the remote cannot do this")

	link, err := f.PublicLink(context.Background(), "clip.mp4", fs.DurationOff, false)
	require.NoError(t, err)
	require.Contains(t, link, "sig=LIVE-SIGNATURE-VALUE", "the signature arrived intact")
	require.NotContains(t, link, "amp", "the escaped ampersand was unescaped before the link went out")
	require.Contains(t, link, "fileId=7000000000000000007", "and it is the id the drive holds, not the name")
	require.Contains(t, link, "mint=1", "one link asked for, one link handed over")
	require.Equal(t, 1, drive.downloadHits, "asking for one link did not cost two calls")

	second, err := f.PublicLink(context.Background(), "clip.mp4", fs.DurationOff, false)
	require.NoError(t, err)
	require.Contains(t, second, "mint=2", "the second caller got the link the drive just signed, not the first one")
	require.NotEqual(t, link, second)
	require.Equal(t, 2, drive.downloadHits)
}

// TestPublicLinkIgnoresAnExpiryItCannotAskFor locks the shape of the answer for
// --expiry. The endpoint has no expiry parameter, so the requested lifetime is
// reported and the link still comes back; refusing outright would take away a
// link the caller can use right now.
func TestPublicLinkIgnoresAnExpiryItCannotAskFor(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("7000000000000000007", "clip.mp4", rootID)}, nil)
	}

	link, err := f.PublicLink(context.Background(), "clip.mp4", fs.Duration(time.Hour), false)
	require.NoError(t, err)
	require.NotEmpty(t, link)

	_, err = f.PublicLink(context.Background(), "clip.mp4", fs.DurationOff, true)
	require.NoError(t, err, "there is no stored share to revoke, so unlink changes nothing")
}

// TestPublicLinkRefusesADirectory covers both ways a directory arrives: spelled
// with the trailing slash and without it. Without the second half the caller
// gets a bare "is a directory" from the rc endpoint instead of the reason rclone
// gives everywhere else for this command.
func TestPublicLinkRefusesADirectory(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON(nil, []string{folderJSON("7000000000000000008", "albums", rootID)})
	}

	_, err := f.PublicLink(context.Background(), "albums/", fs.DurationOff, false)
	require.ErrorIs(t, err, fs.ErrorCantShareDirectories)

	_, err = f.PublicLink(context.Background(), "albums", fs.DurationOff, false)
	require.ErrorIs(t, err, fs.ErrorCantShareDirectories)

	_, err = f.PublicLink(context.Background(), "missing.mp4", fs.DurationOff, false)
	require.ErrorIs(t, err, fs.ErrorObjectNotFound)

	require.Zero(t, drive.downloadHits, "none of the three reached the link endpoint")
}

// TestPublicLinkHandsBackTheAddressTheBytesComeFrom is why PublicLink follows the
// one redirect instead of quoting the drive's own endpoint. download.cloud.189.cn
// is a redirector: the address the object is really read from is behind it
// (measured 2026-10-27, a 302 to a presigned OBS URL), and that host is what a
// direct-link rule keys on. A client that does not follow redirects - or that has
// to decide the host before it asks - is handed nothing useful by the first form.
func TestPublicLinkHandsBackTheAddressTheBytesComeFrom(t *testing.T) {
	drive := &fakeDrive{t: t,
		readBody:     []byte("0123456789"),
		linkRedirect: "/store/obs?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=300&X-Amz-Signature=OBS-SIGNATURE-1",
	}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("7000000000000000007", "clip.mp4", rootID)}, nil)
	}

	link, err := f.PublicLink(context.Background(), "clip.mp4", fs.DurationOff, false)
	require.NoError(t, err)
	require.Contains(t, link, "X-Amz-Signature=OBS-SIGNATURE-1", "the storage address came back")
	require.NotContains(t, link, "/store/file", "not the redirector")
	require.NotContains(t, link, "sig=LIVE-SIGNATURE-VALUE", "and the drive's own signature is not part of what went out")
	require.Equal(t, 1, drive.downloadHits, "learning where the link goes must not cost a second link")

	require.Len(t, drive.readQueries, 2, "one request to the link, one to what it named")
	require.Equal(t, "bytes=0-0", drive.readRanges[0], "a byte is asked for, so following the link is not a download")
}

// TestPublicLinkFallsBackToTheRedirectingLink covers the resolution failing. The
// link the drive minted is valid and does serve anything that follows a redirect,
// so losing only that property is worth reporting rather than losing the link -
// and the address that refused is not passed on as if it had worked.
func TestPublicLinkFallsBackToTheRedirectingLink(t *testing.T) {
	drive := &fakeDrive{t: t,
		readBody:     []byte("0123456789"),
		readStatus:   []int{http.StatusOK, http.StatusForbidden},
		linkRedirect: "/store/obs?X-Amz-Expires=300&X-Amz-Signature=OBS-SIGNATURE-1",
	}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("7000000000000000007", "clip.mp4", rootID)}, nil)
	}

	link, err := f.PublicLink(context.Background(), "clip.mp4", fs.DurationOff, false)
	require.NoError(t, err, "the storage address refused, but the drive's own link is still in hand")
	require.Contains(t, link, "/store/file", "so what goes out is the form that redirects")
	require.NotContains(t, link, "X-Amz-Signature", "and the address that refused is not handed out as if it had worked")
	require.Len(t, drive.readRanges, 2, "the link was followed once before giving up")
}

// TestStorageURLDoesNotQuoteTheStorageSignatureOnTransportFailure is the log
// hygiene rule on the redirecting hop.
//
// When the transport itself fails, Go quotes the URL it was sending to - and
// after a redirect that is the last address in the chain, the presigned storage
// URL with its signature in the query, not the drive's own link. Scrubbing only
// the link would leave the signature in the message, which is the one thing that
// must not happen on this drive.
func TestStorageURLDoesNotQuoteTheStorageSignatureOnTransportFailure(t *testing.T) {
	drive := &fakeDrive{t: t,
		linkRedirectAbs: "http://127.0.0.1:1/store/obs?X-Amz-Signature=OBS-SIGNATURE-1&X-Amz-Credential=OBS-CREDENTIAL-1",
	}
	f := newFakeFs(t, drive)
	link := f.apiBase + "/store/file?fileId=1&amp;sig=LIVE-SIGNATURE-VALUE"

	_, err := f.storageURL(context.Background(), link)
	require.Error(t, err, "nothing listens at the far end of the redirect")
	require.NotContains(t, err.Error(), "OBS-SIGNATURE-1", "the transport quoted the storage URL and its signature went with it")
	require.NotContains(t, err.Error(), "OBS-CREDENTIAL-1", "and the credential that travels beside it")
	require.NotContains(t, err.Error(), "LIVE-SIGNATURE-VALUE", "nor the drive's own signature")
	require.Contains(t, err.Error(), "<redacted>", "the addresses are said to be redacted rather than shown")
}

// TestPublicLinkReportsADeadLinkInsteadOfHandingItOver is the other half of the
// fallback. When the drive's own endpoint refuses - no redirect happened, so the
// link itself is what answered badly - the link is dead, and returning it would
// give the caller something that does not work.
func TestPublicLinkReportsADeadLinkInsteadOfHandingItOver(t *testing.T) {
	drive := &fakeDrive{t: t, readStatus: []int{http.StatusForbidden}}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("7000000000000000007", "clip.mp4", rootID)}, nil)
	}

	link, err := f.PublicLink(context.Background(), "clip.mp4", fs.DurationOff, false)
	require.Error(t, err, "the drive refused the link it had just minted")
	require.Empty(t, link, "a link that was refused is not handed over as if it worked")
	require.Contains(t, err.Error(), "HTTP 403")
	require.Len(t, drive.readRanges, 1, "one attempt, and no redirect to follow")
}

// TestLinkValidityReadsTheGrant checks what the lifetime warning is computed
// from. X-Amz-Expires is a window counted from when the URL was signed, so
// X-Amz-Date has to come off it; without that a URL signed an hour ago would be
// reported as fresh for its whole window.
func TestLinkValidityReadsTheGrant(t *testing.T) {
	signedAt := time.Now().UTC().Add(-10 * time.Minute)
	date := signedAt.Format("20060102T150405Z")

	for _, test := range []struct {
		name string
		url  string
		want time.Duration
		ok   bool
	}{
		{"signed ten minutes ago, half-hour grant", "/f?X-Amz-Date=" + date + "&X-Amz-Expires=1800", 20 * time.Minute, true},
		{"no signing time, the window alone", "/f?X-Amz-Expires=300", 5 * time.Minute, true},
		{"already expired", "/f?X-Amz-Date=" + date + "&X-Amz-Expires=60", -9 * time.Minute, true},
		{"no expiry at all", "/f?sign=1", 0, false},
		{"not a number", "/f?X-Amz-Expires=soon", 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := linkValidity(test.url)
			require.Equal(t, test.ok, ok)
			if test.ok {
				// A second of slack for the clock moving between the two reads.
				require.InDelta(t, test.want.Seconds(), got.Seconds(), 2)
			}
		})
	}
}
