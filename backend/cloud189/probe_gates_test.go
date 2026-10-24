package cloud189

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloud189/api"
	"github.com/stretchr/testify/require"
)

// The remaining protocol questions that need real bytes, kept together because
// they share one uploader. Each one answers a gate the implementation has to
// make, so none of them is curiosity.

// multipartPlan drives init -> URLs -> PUTs -> commit with knobs that let a
// test mis-state exactly one thing at a time.
type multipartPlan struct {
	parentID     string
	name         string
	sliceSize    int64
	wrongSlice   bool // send the whole-file digest where the join is expected
	fakePartMD5  bool // send wrong per-part digests (the storage layer should catch this)
	skipPart     int  // do not upload this part number, then commit anyway
	resendPart   int  // upload this part number twice
	parallel     int  // concurrent PUTs, 1 means sequential
	declaredSize int64
}

type multipartResult struct {
	initCode       string
	fileDataExists string
	putStatuses    []int
	commitStatus   int
	commitCode     string
	commitMd5      string
	commitName     string
	commitID       string
	bytesSent      int64
	seconds        float64
}

func runMultipart(t *testing.T, s probeSession, p multipartPlan, content []byte) multipartResult {
	t.Helper()
	var res multipartResult
	slice := p.sliceSize
	if slice <= 0 {
		slice = probeSliceSize
	}
	fileMd5 := api.MD5Upper(content)
	declared := p.declaredSize
	if declared == 0 {
		declared = int64(len(content))
	}
	parts := api.PlanParts(declared, slice)
	digests := make([][]byte, 0, len(parts))
	for _, pt := range parts {
		start := int64(pt.Index-1) * slice
		end := start + pt.Size
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		if p.fakePartMD5 {
			digests = append(digests, api.MD5Raw([]byte(fmt.Sprintf("fake-part-%d", pt.Index))))
		} else {
			digests = append(digests, api.MD5Raw(content[start:end]))
		}
	}
	sliceMd5 := api.SliceAggMD5(digests, fileMd5)
	if p.wrongSlice {
		sliceMd5 = fileMd5
	}

	params := api.InitParams(p.parentID, p.name, fileMd5, declared, sliceMd5, slice)
	enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams(params))
	require.NoError(t, err)
	_, body := signedCall(t, s, uploadHost, false, http.MethodPost, "/person/initMultiUpload", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m := decode(t, body)
	data := subMap(m, "data")
	res.initCode, res.fileDataExists = field(m, "code"), field(data, "fileDataExists")
	uploadFileID := field(data, "uploadFileId")
	if uploadFileID == "" {
		return res
	}
	if res.fileDataExists == "1" {
		t.Log("rapid transfer hit; nothing uploaded")
	} else {
		enc, err = api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
			{Key: "uploadFileId", Value: uploadFileID},
			{Key: "partInfo", Value: api.PartInfo(digests)},
		}))
		require.NoError(t, err)
		status, body := signedCall(t, s, uploadHost, false, http.MethodGet, "/person/getMultiUploadUrls", probeQuery("params="+enc), "")
		m = decode(t, body)
		urls := subMap(m, "uploadUrls")
		require.NotEmptyf(t, urls, "no uploadUrls: http=%d %s", status, trunc(string(body), 200))

		send := func(idx int) (int, int64) {
			pt := parts[idx-1]
			start := int64(pt.Index-1) * slice
			chunk := content[start:minInt64(start+pt.Size, int64(len(content)))]
			part := subMap(urls, fmt.Sprintf("partNumber_%d", pt.Index))
			req, err := http.NewRequest(http.MethodPut, field(part, "requestURL"), bytes.NewReader(chunk))
			require.NoError(t, err)
			for k, v := range api.ParseRequestHeader(field(part, "requestHeader")) {
				req.Header.Set(k, v)
			}
			req.Header.Set("user-agent", dartUserAgent)
			req.ContentLength = int64(len(chunk))
			res2, err := doScrubbed(req)
			if err != nil {
				t.Logf("part %d transport error: %v", pt.Index, err)
				return 0, 0
			}
			pb, _ := io.ReadAll(res2.Body)
			_ = res2.Body.Close()
			if res2.StatusCode != http.StatusOK {
				t.Logf("part %d http=%d body=%s", pt.Index, res2.StatusCode, trunc(string(pb), 200))
			}
			return res2.StatusCode, int64(len(chunk))
		}

		started := time.Now()
		idxs := orderedParts(len(parts), p.skipPart)
		var mu sync.Mutex
		record := func(code2 int, n int64) {
			mu.Lock()
			res.putStatuses = append(res.putStatuses, code2)
			res.bytesSent += n
			mu.Unlock()
		}
		if p.parallel > 1 {
			var wg sync.WaitGroup
			sem := make(chan struct{}, p.parallel)
			for _, i := range idxs {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					sem <- struct{}{}
					defer func() { <-sem }()
					code2, n := send(i)
					record(code2, n)
				}(i)
			}
			wg.Wait()
		} else {
			for _, i := range idxs {
				code2, n := send(i)
				record(code2, n)
			}
		}
		if p.resendPart > 0 && p.resendPart <= len(parts) {
			code2, n := send(p.resendPart)
			t.Logf("resend of part %d -> http=%d", p.resendPart, code2)
			res.bytesSent += n
		}
		res.seconds = time.Since(started).Seconds()
	}

	enc, err = api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
	}))
	require.NoError(t, err)
	res.commitStatus, body = commitUpload(t, s, enc)
	m = decode(t, body)
	file := subMap(m, "file")
	res.commitCode, res.commitMd5 = field(m, "code"), field(file, "fileMd5")
	res.commitName, res.commitID = field(file, "fileName"), field(file, "userFileId")
	return res
}

// orderedParts lists the part numbers to send, skipping one when asked.
func orderedParts(total, skip int) []int {
	var out []int
	for i := 1; i <= total; i++ {
		if i != skip {
			out = append(out, i)
		}
	}
	return out
}

func commitUpload(t *testing.T, s probeSession, enc string) (int, []byte) {
	return signedCall(t, s, uploadHost, false, http.MethodPost, "/person/commitMultiUploadFile", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
}

// liveSession returns a usable session for the platinum account, rotating once
// if the captured pair has aged out.
func liveSession(t *testing.T) probeSession {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	status, code := getUserInfoAlive(t, s)
	if status != http.StatusOK || code != "0" {
		t.Logf("captured session refused (http=%d %s), rotating", status, code)
		s = getSessionForPC(t, s)
	}
	return s
}

// patternBytes builds deterministic content of the wanted size without holding
// a giant literal in the source.
func patternBytes(n int) []byte {
	unit := []byte("cloud189-throughput-probe-unit-")
	out := make([]byte, 0, n)
	for len(out) < n {
		out = append(out, unit...)
	}
	return out[:n]
}

// TestProbeUploadGates answers, in one run, the four questions the retry and
// integrity design depends on.
func TestProbeUploadGates(t *testing.T) {
	s := liveSession(t)
	const miB = 1 << 20
	stamp := time.Now().Format("150405")
	body20 := patternBytes(20 * miB)

	t.Run("multipart-commit-checks-slice-md5", func(t *testing.T) {
		honest := runMultipart(t, s, multipartPlan{parentID: "-11", name: "probe-h-" + stamp + ".bin", wrongSlice: false}, body20)
		t.Logf("VERDICT honest 20MiB/2 parts: init=%s commit=%s code=%q md5=%q", honest.initCode, honest.fileDataExists, honest.commitCode, honest.commitMd5)
		wrong := runMultipart(t, s, multipartPlan{parentID: "-11", name: "probe-w-" + stamp + ".bin", wrongSlice: true}, body20)
		t.Logf("VERDICT sliceMd5=fileMd5 on 2 parts: init=%s commit http=%d code=%q md5=%q", wrong.initCode, wrong.commitStatus, wrong.commitCode, wrong.commitMd5)
	})

	t.Run("commit-with-a-missing-part", func(t *testing.T) {
		res := runMultipart(t, s, multipartPlan{parentID: "-11", name: "probe-miss-" + stamp + ".bin", skipPart: 2}, body20)
		t.Logf("VERDICT 2 of 2 parts sent, part 2 skipped, commit anyway: http=%d code=%q name=%q md5=%q",
			res.commitStatus, res.commitCode, res.commitName, res.commitMd5)
	})

	t.Run("resend-a-part-that-already-landed", func(t *testing.T) {
		res := runMultipart(t, s, multipartPlan{parentID: "-11", name: "probe-dup-" + stamp + ".bin", resendPart: 1}, body20)
		trueMd5 := api.MD5Upper(body20)
		t.Logf("VERDICT part 1 sent twice: commit code=%q md5=%q (true %s) intact=%v", res.commitCode, res.commitMd5, trueMd5, res.commitMd5 == trueMd5)
	})

	t.Run("fake-part-digest-is-caught-where", func(t *testing.T) {
		res := runMultipart(t, s, multipartPlan{parentID: "-11", name: "probe-fake-" + stamp + ".bin", fakePartMD5: true}, body20)
		t.Logf("VERDICT fake part digests: statuses=%v commit http=%d code=%q", res.putStatuses, res.commitStatus, res.commitCode)
	})

	t.Run("awkward-file-names", func(t *testing.T) {
		small := patternBytes(4096)
		for _, name := range []string{"probe-a&b=c.bin", "probe-中文 名(1).bin", "probe-100%.bin"} {
			res := runMultipart(t, s, multipartPlan{parentID: "-11", name: stamp + "-" + name}, small)
			t.Logf("VERDICT name %q: init=%s commit code=%q landed=%q", name, res.initCode, res.commitCode, res.commitName)
			if res.commitCode != "SUCCESS" {
				continue
			}
			count, files, _ := listDir(t, s, false, "-11")
			found := ""
			for _, e := range files {
				if e.id == res.commitID {
					found = e.name
				}
			}
			t.Logf("  listing count=%s shows %q same-as-committed=%v", count, found, found == res.commitName)
		}
	})

	t.Run("cleanup", func(t *testing.T) {
		_, files, _ := listDir(t, s, false, "-11")
		var entries []probeEntry
		for _, e := range files {
			if strings.Contains(e.name, "probe-") {
				entries = append(entries, probeEntry{id: e.id, name: e.name, parent: "-11"})
			}
		}
		if len(entries) == 0 {
			t.Log("nothing to clean")
			return
		}
		deleteTargets(t, s, false, entries, true)
		t.Logf("DELETE submitted for %d objects, left in the recycle bin", len(entries))
	})
}

// TestProbeThroughputCeiling measures whether the transfer-speed privileges are
// enforced: transSpeed says 12800 KB/s and every part PUT carries the signed
// header x-amz-limit=rate=12800. A single stream cannot tell an unenforced
// limit from a per-connection one, so this runs the same bytes once serially and
// once with four parts in flight.
func TestProbeThroughputCeiling(t *testing.T) {
	if os.Getenv("CLOUD189_SPEEDPROBE") != "1" {
		t.Skip("moves real bytes over the home link; set CLOUD189_SPEEDPROBE=1 to run")
	}
	s := liveSession(t)
	const miB = 1 << 20
	content := patternBytes(100 * miB)
	stamp := time.Now().Format("150405")

	for _, tc := range []struct {
		parallel int
	}{
		{1}, {4},
	} {
		res := runMultipart(t, s, multipartPlan{
			parentID: "-11", name: fmt.Sprintf("probe-speed-%dx-%s.bin", tc.parallel, stamp),
			sliceSize: 10 * miB, parallel: tc.parallel,
		}, content)
		mb := float64(res.bytesSent) / miB
		t.Logf("VERDICT parallel=%d bytes=%d seconds=%.2f throughput=%.2f MiB/s statuses=%v commit=%q",
			tc.parallel, res.bytesSent, res.seconds, mb/res.seconds, res.putStatuses, res.commitCode)
	}

	_, files, _ := listDir(t, s, false, "-11")
	var entries []probeEntry
	for _, e := range files {
		if strings.HasPrefix(e.name, "probe-speed-") {
			entries = append(entries, probeEntry{id: e.id, name: e.name, parent: "-11"})
		}
	}
	if len(entries) > 0 {
		deleteTargets(t, s, false, entries, true)
		t.Logf("cleaned %d speed-probe objects into the recycle bin", len(entries))
	}
}

// TestProbeConflictDealWay enumerates the conflict-handling values the batch
// task API accepts, which is what decides how Copy and Move report a clash to
// rclone. Server-side work only: a COPY moves no bytes.
func TestProbeConflictDealWay(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.SecondAccount
	status, code := getUserInfoAlive(t, s)
	if status != http.StatusOK || code != "0" {
		s = getSessionForPC(t, s)
	}
	stamp := time.Now().Format("150405")
	srcID := ""
	_, files, _ := listDir(t, s, false, "-11")
	for _, e := range files {
		if strings.HasPrefix(e.name, "probe-conflict-") {
			srcID = e.id
			break
		}
	}
	if srcID == "" {
		small := patternBytes(4096)
		res := runMultipart(t, s, multipartPlan{parentID: "-11", name: "probe-conflict-" + stamp + ".bin"}, small)
		require.Equalf(t, "SUCCESS", res.commitCode, "could not create the conflict source: %s", res.commitCode)
		srcID = res.commitID
	}

	for _, dealWay := range []int{1, 2, 3, 4, 5} {
		infos := fmt.Sprintf(`[{"fileId":%q,"fileName":"probe-conflict-%s.bin","isFolder":0,"srcParentId":-11}]`, srcID, stamp)
		form := url.Values{}
		form.Set("type", "COPY")
		form.Set("taskInfos", infos)
		form.Set("targetFolderId", personalRoot)
		_, body := signedCall(t, s, apiHost, false, http.MethodPost, "/batch/createBatchTask.action", probeQuery(""), form.Encode())
		taskID := field(decode(t, body), "taskId")
		require.NotEmptyf(t, taskID, "create refused: %s", trunc(string(body), 200))

		// Poll once: a same-name COPY should stop at taskStatus 2, waiting on a
		// conflict decision.
		poll := url.Values{}
		poll.Set("taskId", taskID)
		poll.Set("type", "COPY")
		_, body = signedCall(t, s, apiHost, false, http.MethodPost, "/batch/checkBatchTask.action", probeQuery(""), poll.Encode())
		first := decode(t, body)
		st := field(first, "taskStatus")
		if st != "2" {
			t.Logf("VERDICT dealWay=%d never reached the conflict state: status=%s errorCode=%q", dealWay, st, field(first, "errorCode"))
			continue
		}
		_, body = signedCall(t, s, apiHost, false, http.MethodPost, "/batch/getConflictTaskInfo.action", probeQuery(""), poll.Encode())
		conf := decode(t, body)
		infos2 := fmt.Sprintf(`[{"fileId":%q,"fileName":"probe-conflict-%s.bin","isFolder":0,"srcParentId":-11,"dealWay":%d}]`, srcID, stamp, dealWay)
		manage := url.Values{}
		manage.Set("taskId", taskID)
		manage.Set("type", "COPY")
		manage.Set("taskInfos", infos2)
		manage.Set("targetFolderId", personalRoot)
		_, body = signedCall(t, s, apiHost, false, http.MethodPost, "/batch/manageBatchTask.action", probeQuery(""), manage.Encode())
		t.Logf("  dealWay=%d submitted, manage says %s", dealWay, trunc(string(body), 120))

		var last map[string]any
		for i := 0; i < 8; i++ {
			time.Sleep(1200 * time.Millisecond)
			_, body = signedCall(t, s, apiHost, false, http.MethodPost, "/batch/checkBatchTask.action", probeQuery(""), poll.Encode())
			last = decode(t, body)
			if field(last, "taskStatus") == "4" {
				break
			}
		}
		t.Logf("VERDICT dealWay=%d final status=%s successed=%s failed=%s skip=%s errorCode=%q conflictInfo=%s",
			dealWay, field(last, "taskStatus"), field(last, "successedCount"), field(last, "failedCount"),
			field(last, "skipCount"), field(last, "errorCode"), trunc(conflictNames(conf), 120))
	}

	_, files, _ = listDir(t, s, false, "-11")
	var names []string
	for _, e := range files {
		if strings.Contains(e.name, "probe-conflict") {
			names = append(names, e.name)
		}
	}
	t.Logf("surviving copies in the root: %s", strings.Join(names, " | "))
}

// conflictNames lists which rows a conflict report flagged.
func conflictNames(m map[string]any) string {
	var out []string
	for _, e := range subList(m, "taskInfos") {
		row, ok := e.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, fmt.Sprintf("%s(dealWay=%s isConflict=%s)", field(row, "fileName"), field(row, "dealWay"), field(row, "isConflict")))
	}
	return strings.Join(out, ",")
}
