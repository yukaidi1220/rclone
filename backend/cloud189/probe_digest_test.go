package cloud189

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloud189/api"
	"github.com/stretchr/testify/require"
)

// What the server actually enforces about digests.
//
// "init accepted a wrong sliceMd5" is easy to over-read as "digests are not
// checked". There are three separate digests in this protocol and they are
// checked in different places:
//
//	fileMd5  - declared in init; keys rapid transfer; echoed back by commit
//	sliceMd5 - declared in init; the aggregate over the part digests
//	part     - declared in partInfo, and the server signs it into the
//	           Content-MD5 of the presigned storage PUT
//
// The part digest is the one a client would have to blank in order to skip
// hashing the file. If the server echoes our own declaration into that header,
// blanking does not remove the check - it moves it to the storage layer, which
// compares the header against the bytes it actually receives. These probes
// decide that by declaring wrong values one at a time while uploading real
// bytes.

// digestAttempt records what one upload attempt did.
type digestAttempt struct {
	initCode       string
	fileDataExists string
	contentMD5     string
	putStatus      int
	putBody        string
	commitCode     string
	commitFileMd5  string
	commitName     string
	commitID       string
}

// uploadSpec is one deliberately mis-declared upload. sliceSize forces the part
// count so a multi-part claim can be made with a small file, and the declared
// digests are separate from the real ones so each can be lied about alone.
type uploadSpec struct {
	name             string
	content          []byte
	sliceSize        int64
	declaredFileMd5  string
	declaredSliceMd5 string
	fakePartDigests  bool
}

// runUpload performs init -> (getMultiUploadUrls -> PUT)* -> commit, taking the
// rapid-transfer branch when the server declares the content already present.
func runUpload(t *testing.T, s probeSession, spec uploadSpec) digestAttempt {
	t.Helper()
	var out digestAttempt

	sliceSize := spec.sliceSize
	if sliceSize <= 0 {
		sliceSize = probeSliceSize
	}
	parts := api.PlanParts(int64(len(spec.content)), sliceSize)
	realDigests := make([][]byte, 0, len(parts))
	declaredDigests := make([][]byte, 0, len(parts))
	for _, p := range parts {
		raw := api.MD5Raw(spec.content[int64(p.Index-1)*sliceSize : minInt64(int64(p.Index)*sliceSize, int64(len(spec.content)))])
		realDigests = append(realDigests, raw)
		if spec.fakePartDigests {
			declaredDigests = append(declaredDigests, api.MD5Raw([]byte(fmt.Sprintf("not-part-%d", p.Index))))
		} else {
			declaredDigests = append(declaredDigests, raw)
		}
	}
	trueSlice := api.SliceAggMD5(realDigests, api.MD5Upper(spec.content))

	params := api.InitParams("-11", spec.name, spec.declaredFileMd5, int64(len(spec.content)),
		spec.declaredSliceMd5, sliceSize)
	enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams(params))
	require.NoError(t, err)
	status, body := signedCall(t, s, uploadHost, false, http.MethodPost, "/person/initMultiUpload", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m := decode(t, body)
	data := subMap(m, "data")
	out.initCode, out.fileDataExists = field(m, "code"), field(data, "fileDataExists")
	uploadFileID := field(data, "uploadFileId")
	t.Logf("init name=%q parts=%d declaredFileMd5=%s declaredSliceMd5=%s fakeParts=%v http=%d code=%q fileDataExists=%s uploadFileId=%q",
		spec.name, len(parts), trunc(spec.declaredFileMd5, 12), trunc(spec.declaredSliceMd5, 12), spec.fakePartDigests,
		status, out.initCode, out.fileDataExists, trunc(uploadFileID, 40))
	if uploadFileID == "" {
		return out
	}

	if out.fileDataExists != "1" {
		enc, err = api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
			{Key: "uploadFileId", Value: uploadFileID},
			{Key: "partInfo", Value: api.PartInfo(declaredDigests)},
		}))
		require.NoError(t, err)
		status, body = signedCall(t, s, uploadHost, false, http.MethodGet, "/person/getMultiUploadUrls", probeQuery("params="+enc), "")
		m = decode(t, body)
		for i := range parts {
			key := fmt.Sprintf("partNumber_%d", parts[i].Index)
			part := subMap(subMap(m, "uploadUrls"), key)
			partURL, partHeader := field(part, "requestURL"), field(part, "requestHeader")
			require.NotEmptyf(t, partURL, "no presigned URL for %s: http=%d %s", key, status, trunc(string(body), 240))
			headers := api.ParseRequestHeader(partHeader)
			out.contentMD5 = headers["Content-MD5"]
			t.Logf("%s server-issued Content-MD5=%s our declared digest=%s echo=%v",
				key, out.contentMD5, base64.StdEncoding.EncodeToString(declaredDigests[i]),
				out.contentMD5 == base64.StdEncoding.EncodeToString(declaredDigests[i]))

			start := int64(parts[i].Index-1) * sliceSize
			chunk := spec.content[start:minInt64(start+sliceSize, int64(len(spec.content)))]
			req, err := http.NewRequest(http.MethodPut, partURL, bytes.NewReader(chunk))
			require.NoError(t, err)
			for k, v := range headers {
				req.Header.Set(k, v)
			}
			req.Header.Set("user-agent", dartUserAgent)
			req.ContentLength = int64(len(chunk))
			res, err := doScrubbed(req)
			require.NoError(t, err)
			putBody, _ := io.ReadAll(res.Body)
			_ = res.Body.Close()
			out.putStatus, out.putBody = res.StatusCode, string(putBody)
			t.Logf("%s PUT bytes=%d http=%d body=%s (true digest of this chunk=%s)",
				key, len(chunk), out.putStatus, trunc(out.putBody, 200), base64.StdEncoding.EncodeToString(realDigests[i]))
			if out.putStatus != http.StatusOK {
				return out
			}
		}
	}

	enc, err = api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
	}))
	require.NoError(t, err)
	status, body = signedCall(t, s, uploadHost, false, http.MethodPost, "/person/commitMultiUploadFile", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m = decode(t, body)
	file := subMap(m, "file")
	out.commitCode, out.commitFileMd5 = field(m, "code"), field(file, "fileMd5")
	out.commitName, out.commitID = field(file, "fileName"), field(file, "userFileId")
	t.Logf("commit http=%d code=%q file.fileMd5=%q name=%q id=%s (true slice agg=%s)",
		status, out.commitCode, out.commitFileMd5, out.commitName, out.commitID, trueSlice)
	return out
}

// TestProbePartResendScope measures the retry radius: once one part's PUT is
// refused, can the same upload session still accept that part again, or is the
// whole file doomed to start over? The answer decides whether a part-level
// retry loop is possible at all, which is exactly what wopan and yun139 have.
//
// A corrupted part in flight looks like this: the presigned URL carries the
// server-issued Content-MD5, so sending the wrong bytes is refused by the
// storage layer. Re-sending the same part number on the same URL is what a
// retry would do.
func TestProbePartResendScope(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	require.NotEmpty(t, s.SessionSecret)

	status, code := getUserInfoAlive(t, s)
	if status != http.StatusOK || code != "0" {
		s = getSessionForPC(t, s)
	}

	content := bytes.Repeat([]byte("part-resend-probe-"), 100000) // 1,600,000 bytes, one part
	fileMd5 := api.MD5Upper(content)
	stamp := time.Now().Format("150405")
	name := "probe-resend-" + stamp + ".bin"

	params := api.InitParams("-11", name, fileMd5, int64(len(content)), fileMd5, probeSliceSize)
	enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams(params))
	require.NoError(t, err)
	status, body := signedCall(t, s, uploadHost, false, http.MethodPost, "/person/initMultiUpload", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m := decode(t, body)
	uploadFileID := field(subMap(m, "data"), "uploadFileId")
	require.NotEmptyf(t, uploadFileID, "init refused: http=%d %s", status, trunc(string(body), 200))
	t.Logf("init http=%d fileDataExists=%s uploadFileId=%q", status, field(subMap(m, "data"), "fileDataExists"), trunc(uploadFileID, 40))

	enc, err = api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
		{Key: "partInfo", Value: api.PartInfo([][]byte{api.MD5Raw(content)})},
	}))
	require.NoError(t, err)
	status, body = signedCall(t, s, uploadHost, false, http.MethodGet, "/person/getMultiUploadUrls", probeQuery("params="+enc), "")
	m = decode(t, body)
	part := subMap(subMap(m, "uploadUrls"), "partNumber_1")
	partURL, partHeader := field(part, "requestURL"), field(part, "requestHeader")
	require.NotEmptyf(t, partURL, "no presigned URL: http=%d %s", status, trunc(string(body), 200))
	headers := api.ParseRequestHeader(partHeader)

	put := func(payload []byte, label string) int {
		req, err := http.NewRequest(http.MethodPut, partURL, bytes.NewReader(payload))
		require.NoError(t, err)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		req.Header.Set("user-agent", dartUserAgent)
		req.ContentLength = int64(len(payload))
		res, err := doScrubbed(req)
		require.NoError(t, err)
		b, _ := io.ReadAll(res.Body)
		_ = res.Body.Close()
		t.Logf("%s PUT bytes=%d http=%d body=%s", label, len(payload), res.StatusCode, trunc(string(b), 300))
		return res.StatusCode
	}

	bad := bytes.Clone(content)
	bad[len(bad)-1] ^= 0xFF
	if put(bad, "corrupted-part") != http.StatusOK {
		t.Log("corrupted part refused as expected")
	}

	resend := put(content, "resend-same-part-same-URL")
	t.Logf("VERDICT resend after a refused part, same URL: http=%d", resend)

	// A second getMultiUploadUrls on the same session is what a retry loop
	// would do if it wanted a fresh signature rather than the stale URL.
	status, body = signedCall(t, s, uploadHost, false, http.MethodGet, "/person/getMultiUploadUrls", probeQuery("params="+enc), "")
	t.Logf("VERDICT re-request part URLs on the same session: http=%d resp=%s", status, trunc(string(body), 200))

	enc, err = api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
	}))
	require.NoError(t, err)
	status, body = signedCall(t, s, uploadHost, false, http.MethodPost, "/person/commitMultiUploadFile", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m = decode(t, body)
	t.Logf("VERDICT commit after the refused-then-resent part: http=%d code=%q storedMd5=%q trueMd5=%s",
		status, field(m, "code"), field(subMap(m, "file"), "fileMd5"), fileMd5)

	_, files, _ := listDir(t, s, false, "-11")
	for _, e := range files {
		if strings.HasPrefix(e.name, "probe-resend-") {
			t.Logf("listed %q size=%s md5=%s intact=%v", e.name, e.size, e.md5, e.md5 == fileMd5)
			deleteTargets(t, s, false, []probeEntry{{id: e.id, name: e.name, parent: "-11"}}, true)
			t.Log("probe object DELETEd, left in the recycle bin")
		}
	}
}

// TestProbePartURLStability settles which storage node serves a part, without
// moving a single byte: open a session, ask for the part URLs twice on the same
// uploadFileId, and compare what comes back.
//
// This replaces a claim I made from an unlogged value - one run printed only the
// second URL's host, and I compared it against a different session's node.
func TestProbePartURLStability(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	require.NotEmpty(t, s.SessionSecret)

	status, code := getUserInfoAlive(t, s)
	if status != http.StatusOK || code != "0" {
		s = getSessionForPC(t, s)
	}

	const declared = 30 << 20
	fileMd5 := api.MD5Upper([]byte("url-stability-claim"))
	params := api.InitParams("-11", "probe-url-stability.bin", fileMd5, declared, fileMd5, probeSliceSize)
	enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams(params))
	require.NoError(t, err)
	status, body := signedCall(t, s, uploadHost, false, http.MethodPost, "/person/initMultiUpload", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m := decode(t, body)
	uploadFileID := field(subMap(m, "data"), "uploadFileId")
	require.NotEmptyf(t, uploadFileID, "init refused http=%d %s", status, trunc(string(body), 200))

	parts := api.PlanParts(declared, probeSliceSize)
	digests := make([][]byte, 0, len(parts))
	for _, p := range parts {
		digests = append(digests, api.MD5Raw([]byte(fmt.Sprintf("url-stability-%d", p.Index))))
	}
	enc, err = api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
		{Key: "partInfo", Value: api.PartInfo(digests)},
	}))
	require.NoError(t, err)

	seen := map[int][]string{}
	for round := 1; round <= 3; round++ {
		status, body = signedCall(t, s, uploadHost, false, http.MethodGet, "/person/getMultiUploadUrls", probeQuery("params="+enc), "")
		m = decode(t, body)
		t.Logf("round %d http=%d code=%q uploadFileId=%q", round, status, field(m, "code"), trunc(uploadFileID, 28))
		for _, p := range parts {
			key := fmt.Sprintf("partNumber_%d", p.Index)
			part := subMap(subMap(m, "uploadUrls"), key)
			u := field(part, "requestURL")
			if u == "" {
				t.Logf("  part %d absent in this round", p.Index)
				continue
			}
			parsed, err := url.Parse(u)
			if err != nil {
				t.Logf("  %s unparseable: %s", key, trunc(u, 120))
				continue
			}
			line := fmt.Sprintf("host=%s path=%s uploadId=%s", parsed.Host, parsed.Path, parsed.Query().Get("uploadId"))
			seen[p.Index] = append(seen[p.Index], line)
			t.Logf("  %s %s", key, line)
		}
	}

	for idx, lines := range seen {
		same := true
		for _, l := range lines[1:] {
			if l != lines[0] {
				same = false
			}
		}
		t.Logf("VERDICT part %d across %d rounds: identical=%v", idx, len(lines), same)
	}
}

// TestProbePartSizeRule works out the shape of the InvalidPartSize rule rather
// than just its existence: is it "exactly 10 MiB", "a value that divides the
// file", "at least 10 MiB", or a limit on the part count? Every case is a
// declared size with no bytes uploaded, so the whole sweep is free.
//
// Declared sizes stay under the day's remaining transfer allowance, so a
// refusal here cannot be the flow gate talking.
func TestProbePartSizeRule(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	require.NotEmpty(t, s.SessionSecret)

	status, code := getUserInfoAlive(t, s)
	if status != http.StatusOK || code != "0" {
		s = getSessionForPC(t, s)
	}

	const miB = 1 << 20
	cases := []struct {
		declared  int64
		sliceSize int64
		why       string
	}{
		{100 * miB, 10 * miB, "the captured value"},
		{100 * miB, 11 * miB, "one MiB above it"},
		{100 * miB, 20 * miB, "a multiple of it"},
		{100 * miB, 25 * miB, "divides the file exactly, 4 parts"},
		{100 * miB, 50 * miB, "divides the file exactly, 2 parts"},
		{100 * miB, 10*miB + 1, "off by one byte"},
		{11 * miB, 10 * miB, "two parts with a small tail"},
		{300 * miB, 10 * miB, "30 parts"},
		{300 * miB, 1 * miB, "300 parts: is the count what fails, not the size"},
		{300 * miB, 2 * miB, "150 parts"},
	}
	for _, tc := range cases {
		code := claimInitCode(t, s, tc.declared, tc.sliceSize)
		t.Logf("VERDICT declared=%dMiB sliceSize=%d (%s) -> %s",
			tc.declared/miB, tc.sliceSize, tc.why, code)
	}
}

// claimInitCode opens a session for a declared size and slice size and returns
// the server's code, without uploading anything.
func claimInitCode(t *testing.T, s probeSession, declared, sliceSize int64) string {
	t.Helper()
	parts := api.PlanParts(declared, sliceSize)
	digests := make([][]byte, 0, len(parts))
	for _, p := range parts {
		digests = append(digests, api.MD5Raw([]byte(fmt.Sprintf("rule-%d-%d-%d", declared, p.Index, p.Size))))
	}
	fileMd5 := api.MD5Upper([]byte(fmt.Sprintf("rule-file-%d-%d", declared, sliceSize)))
	params := api.InitParams("-11", "probe-rule.bin", fileMd5, declared, api.SliceAggMD5(digests, fileMd5), sliceSize)
	enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams(params))
	require.NoError(t, err)
	status, body := signedCall(t, s, uploadHost, false, http.MethodPost, "/person/initMultiUpload", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m := decode(t, body)
	c := field(m, "code")
	if c == "" {
		c = fmt.Sprintf("http=%d %s", status, trunc(string(body), 120))
	}
	if field(subMap(m, "data"), "uploadFileId") != "" {
		c += " (parts=" + fmt.Sprint(len(parts)) + ")"
	}
	return c
}

// TestProbePartSizeLadder tells the two remaining hypotheses apart without
// declaring a file big enough to trip the day's transfer allowance:
//
//   - the server accepts any multiple of 10 MiB, or
//   - the server accepts 10 MiB only until the file is large enough that the
//     official client's own ladder would move off it (OpenList partSize():
//     20 MiB above ~9.75 GiB, and multiples of 10 MiB above ~19.5 GiB).
//
// Every declared size here is at most 300 MiB, so a refusal cannot be the flow
// gate, and nothing is uploaded.
func TestProbePartSizeLadder(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	require.NotEmpty(t, s.SessionSecret)

	status, code := getUserInfoAlive(t, s)
	if status != http.StatusOK || code != "0" {
		s = getSessionForPC(t, s)
	}

	const miB = 1 << 20
	cases := []struct {
		declared  int64
		sliceSize int64
		why       string
	}{
		{20 * miB, 20 * miB, "one part, slice equals the whole file"},
		{30 * miB, 30 * miB, "one part at 30 MiB"},
		{30 * miB, 20 * miB, "two parts at 20 MiB"},
		{100 * miB, 30 * miB, "four parts at 30 MiB"},
		{100 * miB, 40 * miB, "three parts at 40 MiB"},
		{300 * miB, 20 * miB, "15 parts at 20 MiB"},
		{300 * miB, 30 * miB, "10 parts at 30 MiB"},
		{100 * miB, 15 * miB, "not a multiple of 10 MiB at all"},
		{100 * miB, 10 * miB, "the baseline that already works"},
	}
	for _, tc := range cases {
		code := claimInitCode(t, s, tc.declared, tc.sliceSize)
		t.Logf("VERDICT declared=%dMiB sliceSize=%dMiB (%s) -> %s",
			tc.declared/miB, tc.sliceSize/miB, tc.why, code)
	}
}

// TestProbePartSizeLadderVIP repeats the ladder question now that the account's
// ceiling allows it: before the membership change every declaration past the
// day's 2 GiB transfer allowance was refused by the flow gate first, so the
// upper rungs of OpenList's partSize() ladder could not be reached at all.
//
// Still declared sizes only - no bytes move. The account now reports 4 TiB of
// space and int64max for both transFileSize and transDayFlow, so a refusal here
// can only be about the part sizing itself.
func TestProbePartSizeLadderVIP(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	require.NotEmpty(t, s.SessionSecret)

	status, code := getUserInfoAlive(t, s)
	if status != http.StatusOK || code != "0" {
		s = getSessionForPC(t, s)
	}

	const miB = 1 << 20
	cases := []struct {
		declared  int64
		sliceSize int64
		why       string
	}{
		{20 * miB, 20 * miB, "was refused before the upgrade: is it about membership?"},
		{20 * miB, 10 * miB, "control at the same declared size"},
		{2 * 1024 * miB, 10 * miB, "2 GiB, 205 parts at the captured slice"},
		{2 * 1024 * miB, 20 * miB, "2 GiB in 100 parts of 20 MiB"},
		{11 * 1024 * miB, 20 * miB, "OpenList's first rung: 20 MiB above ~9.75 GiB"},
		{11 * 1024 * miB, 10 * miB, "1127 parts: does the count break it"},
		{30 * 1024 * miB, 50 * miB, "OpenList's formula above ~19.5 GiB gives 50 MiB"},
		{30 * 1024 * miB, 10 * miB, "3072 parts"},
		{100 * 1024 * miB, 100 * miB, "100 MiB slices, 1024 parts"},
	}
	for _, tc := range cases {
		code := claimInitCode(t, s, tc.declared, tc.sliceSize)
		t.Logf("VERDICT declared=%dGiB sliceSize=%dMiB (%s) -> %s",
			tc.declared/(1024*miB), tc.sliceSize/miB, tc.why, code)
	}
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// TestProbePartSizeAcceptance maps which slice sizes initMultiUpload allows,
// using declared sizes only: no part URL is requested and no byte moves, so
// the whole sweep costs requests rather than quota.
//
// It exists because a 5.5 MiB slice size was refused with InvalidPartSize while
// the client's 10 MiB is accepted, and the rclone chunk_size option can only
// carry a default and a constraint that the server actually honours.
func TestProbePartSizeAcceptance(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	require.NotEmpty(t, s.SessionSecret)

	status, code := getUserInfoAlive(t, s)
	if status != http.StatusOK || code != "0" {
		s = getSessionForPC(t, s)
	}

	// 100 MiB declared file: big enough that every candidate slice size below
	// produces several parts, and small enough to stay inside the day's flow.
	const declared = 100 << 20
	fileMd5 := api.MD5Upper([]byte(fmt.Sprintf("sweep-%d", declared)))
	for _, sliceSize := range []int64{1 << 20, 2 << 20, 4 << 20, 5 << 20, 8 << 20, 10 << 20, 16 << 20, 32 << 20, 64 << 20} {
		parts := api.PlanParts(declared, sliceSize)
		digests := make([][]byte, 0, len(parts))
		for _, p := range parts {
			digests = append(digests, api.MD5Raw([]byte(fmt.Sprintf("sweep-%d-%d", p.Index, p.Size))))
		}
		spec := uploadSpec{
			name:             fmt.Sprintf("probe-sweep-%dMiB.bin", sliceSize>>20),
			content:          []byte("x"),
			sliceSize:        sliceSize,
			declaredFileMd5:  fileMd5,
			declaredSliceMd5: api.SliceAggMD5(digests, fileMd5),
		}
		// Only the init half of runUpload is wanted here, so ask for the URLs
		// and never PUT: an accepted slice size answers with an uploadFileId.
		params := api.InitParams("-11", spec.name, spec.declaredFileMd5, declared, spec.declaredSliceMd5, sliceSize)
		enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams(params))
		require.NoError(t, err)
		status, body := signedCall(t, s, uploadHost, false, http.MethodPost, "/person/initMultiUpload", probeQuery(""), "params="+enc,
			hdr{"decodefields", api.DecodeFields})
		m := decode(t, body)
		data := subMap(m, "data")
		t.Logf("sliceSize=%dMiB parts=%d http=%d code=%q uploadFileId=%q",
			sliceSize>>20, len(parts), status, field(m, "code"), trunc(field(data, "uploadFileId"), 32))
	}
}

// TestProbeDigestEnforcement decides whether a client may skip hashing.
func TestProbeDigestEnforcement(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	require.NotEmpty(t, s.SessionSecret)

	status, code := getUserInfoAlive(t, s)
	if status != http.StatusOK || code != "0" {
		t.Logf("captured session refused (http=%d %s), rotating", status, code)
		s = getSessionForPC(t, s)
	}
	reportAccountState(t, s)

	stamp := time.Now().Format("150405")
	honest := bytes.Repeat([]byte("digest-probe-"), 121400) // 1,335,400 bytes
	honestMd5 := api.MD5Upper(honest)
	lieMd5 := api.MD5Upper([]byte("a lie told to the server"))
	freshA := bytes.Repeat([]byte("fresh-content-A-"), 100000) // 1,600,000 bytes, never uploaded
	freshAMd5 := api.MD5Upper(freshA)
	freshB := bytes.Repeat([]byte("fresh-content-B-"), 100000) // differs only in content
	freshBMd5 := api.MD5Upper(freshB)
	multi := bytes.Repeat([]byte("multipart-probe-"), 901120) // 11,534,336 bytes: 5.5 MiB x 2
	multiMd5 := api.MD5Upper(multi)
	multiSlice := func() string {
		var digests [][]byte
		for _, start := range []int64{0, 5767168} {
			digests = append(digests, api.MD5Raw(multi[start:int64(len(multi))]))
		}
		return api.SliceAggMD5(digests, multiMd5)
	}
	t.Logf("digests honest=%s freshA=%s freshB=%s multi=%s", honestMd5, freshAMd5, freshBMd5, multiMd5)

	cases := []struct {
		name string
		spec uploadSpec
	}{
		{"honest-single-part", uploadSpec{name: "probe-d-honest-" + stamp + ".bin", content: honest,
			declaredFileMd5: honestMd5, declaredSliceMd5: honestMd5}},

		{"lie-file-md5", uploadSpec{name: "probe-d-liemd5-" + stamp + ".bin", content: honest,
			declaredFileMd5: lieMd5, declaredSliceMd5: lieMd5}},

		// Fake the per-part digests on content the server has never seen, so
		// rapid transfer cannot short-circuit the PUT and hide the answer.
		{"fake-part-digests", uploadSpec{name: "probe-d-fakepart-" + stamp + ".bin", content: freshA,
			declaredFileMd5: freshAMd5, declaredSliceMd5: freshAMd5, fakePartDigests: true}},

		// Declare one content's real digest while sending another's bytes: does
		// the server answer "already present" on the word alone?
		{"declare-A-digest-send-B", uploadSpec{name: "probe-d-spoof-" + stamp + ".bin", content: freshB,
			declaredFileMd5: freshAMd5, declaredSliceMd5: freshAMd5}},

		// The reference pair for multi-part: the joined digest first (this is
		// what the capture says the client sends), then the whole-file digest
		// in its place, which is what skipping the join would send.
		{"multipart-honest-slice-md5", uploadSpec{name: "probe-d-multihonest-" + stamp + ".bin", content: multi,
			sliceSize: 5767168, declaredFileMd5: multiMd5, declaredSliceMd5: multiSlice()}},
		{"multipart-wrong-slice-md5", uploadSpec{name: "probe-d-multislice-" + stamp + ".bin", content: multi,
			sliceSize: 5767168, declaredFileMd5: multiMd5, declaredSliceMd5: multiMd5}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := runUpload(t, s, tc.spec)
			t.Logf("VERDICT %s: init=%q rapid=%s put=%d commit=%q storedMd5=%q id=%s",
				tc.name, res.initCode, res.fileDataExists, res.putStatus, res.commitCode, res.commitFileMd5, trunc(res.commitID, 20))
		})
	}

	t.Run("what-the-listing-says", func(t *testing.T) {
		_, files, _ := listDir(t, s, false, "-11")
		for _, e := range files {
			if !strings.HasPrefix(e.name, "probe-d-") {
				continue
			}
			t.Logf("listed %q size=%s md5=%s id=%s", e.name, e.size, e.md5, trunc(e.id, 20))
		}
	})

	t.Run("cleanup", func(t *testing.T) {
		_, files, _ := listDir(t, s, false, "-11")
		var entries []probeEntry
		for _, e := range files {
			if strings.HasPrefix(e.name, "probe-d-") {
				entries = append(entries, probeEntry{id: e.id, name: e.name, parent: "-11"})
			}
		}
		if len(entries) == 0 {
			t.Log("nothing landed, nothing to clean")
			return
		}
		deleteTargets(t, s, false, entries, true)
		t.Logf("DELETE submitted for %d probe objects, left in the recycle bin", len(entries))
	})
}
