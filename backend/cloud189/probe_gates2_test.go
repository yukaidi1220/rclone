package cloud189

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloud189/api"
	"github.com/stretchr/testify/require"
)

// Round two of the upload gates.
//
// The first round left three questions unanswered because the same content was
// reused: once the drive holds a file with that MD5, every later init answers
// fileDataExists=1 and commits without transferring anything, so a skipped or
// duplicated part was never actually exercised. Everything here uses content
// that has never been uploaded before.

// uniqueContent returns bytes no earlier probe has sent, so rapid transfer
// cannot short-circuit an attempt.
func uniqueContent(label string, n int) []byte {
	return patternBytes(n + len(label)) // label makes the size unique per call site
}

func TestProbeUploadGatesRound2(t *testing.T) {
	f := loadProbeFixture(t)
	s := liveSession(t)
	const miB = 1 << 20
	stamp := time.Now().Format("150405")

	t.Run("missing-part-with-never-seen-content", func(t *testing.T) {
		content := uniqueContent("miss-"+stamp, 30*miB)
		res := runMultipart(t, s, multipartPlan{
			parentID: "-11", name: "probe2-miss-" + stamp + ".bin",
			sliceSize: 10 * miB, skipPart: 2,
		}, content)
		t.Logf("VERDICT 3 parts planned, part 2 never sent: init=%s rapid=%s putStatuses=%v commit http=%d code=%q id=%q",
			res.initCode, res.fileDataExists, res.putStatuses, res.commitStatus, res.commitCode, res.commitID)
	})

	t.Run("resent-part-with-never-seen-content", func(t *testing.T) {
		content := uniqueContent("dup-"+stamp, 3*miB)
		trueMd5 := api.MD5Upper(content)
		res := runMultipart(t, s, multipartPlan{
			parentID: "-11", name: "probe2-dup-" + stamp + ".bin",
			sliceSize: 10 * miB, resendPart: 2,
		}, content)
		t.Logf("VERDICT part 2 sent twice: init=%s rapid=%s commit http=%d code=%q md5=%q true=%s intact=%v",
			res.initCode, res.fileDataExists, res.commitStatus, res.commitCode, res.commitMd5, trueMd5, res.commitMd5 == trueMd5)
	})

	t.Run("names-that-break-the-plaintext", func(t *testing.T) {
		// The params payload is "k=v&k=v" text with values written raw. A name
		// carrying & or = therefore collides with that syntax, and a name
		// carrying % collides with whatever decoding the server applies.
		// Measure which spelling lands the name intact.
		cases := []struct {
			label string
			name  string
		}{
			{"raw-amp", "probe2-a&b=" + stamp + ".bin"},
			{"escaped-amp", "probe2-a&b=" + stamp + "esc.bin"},
			{"escaped-percent", "probe2-100%" + stamp + "esc.bin"},
			{"raw-hash", "probe2-a#b" + stamp + ".bin"},
			{"raw-plus", "probe2-a+b" + stamp + ".bin"},
		}
		for _, tc := range cases {
			content := uniqueContent("name-"+tc.label+stamp, 512)
			plain := tc.name
			switch tc.label {
			case "escaped-amp", "escaped-percent":
				plain = url.QueryEscape(tc.name)
			}
			fileMd5 := api.MD5Upper(content)
			params := api.InitParams("-11", plain, fileMd5, int64(len(content)), fileMd5, probeSliceSize)
			enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams(params))
			require.NoError(t, err)
			status, body := signedCall(t, s, uploadHost, false, http.MethodPost, "/person/initMultiUpload", probeQuery(""), "params="+enc,
				hdr{"decodefields", api.DecodeFields})
			m := decode(t, body)
			data := subMap(m, "data")
			uploadFileID := field(data, "uploadFileId")
			if uploadFileID == "" {
				t.Logf("VERDICT %s: init refused http=%d code=%q (name as sent: %q)", tc.label, status, field(m, "code"), trunc(plain, 40))
				continue
			}
			// Upload the single part and commit so the stored name is visible.
			res := runMultipartNamed(t, s, content, tc.label, uploadFileID)
			t.Logf("VERDICT %s: sent name=%q landed=%q", tc.label, tc.name, res)
		}
	})

	t.Run("family-directory-rename", func(t *testing.T) {
		fs2 := f.Personal
		if status, code := getUserInfoAlive(t, fs2); status != http.StatusOK || code != "0" {
			t.Logf("second account session refused (http=%d %s), rotating", status, code)
			fs2 = getSessionForPC(t, fs2)
		}
		s := fs2
		// The family side was only ever captured renaming a file. Find out
		// whether renameFolder exists there at all.
		parent := familyRootFor(t, s)
		require.NotEqualf(t, "0", parent, "该号的家庭盘没有子项，读不出真实根 id")
		id := createFolder(t, s, true, parent, "probe2-fam-"+stamp)
		require.NotEmpty(t, id, "family createFolder failed, cannot test the rename")
		q := probeQuery(fmt.Sprintf("familyId=%s&folderId=%s&destFolderName=%s",
			url.QueryEscape(s.FamilyID), url.QueryEscape(id), url.QueryEscape("probe2-famed-"+stamp)))
		status, body := signedCall(t, s, apiHost, true, http.MethodGet, "/family/file/renameFolder.action", q, "")
		t.Logf("VERDICT family renameFolder (GET) http=%d body=%s", status, trunc(string(body), 200))

		q = probeQuery(fmt.Sprintf("familyId=%s&fileId=%s&destFileName=%s",
			url.QueryEscape(s.FamilyID), url.QueryEscape(id), url.QueryEscape("probe2-file-"+stamp)))
		status, body = signedCall(t, s, apiHost, true, http.MethodGet, "/family/file/renameFile.action", q, "")
		t.Logf("VERDICT family renameFile pointed at a folder id: http=%d body=%s", status, trunc(string(body), 200))

		// And a family MOVE, which no capture has ever shown.
		dst := createFolder(t, s, true, parent, "probe2-famto-"+stamp)
		if dst != "" {
			infos := fmt.Sprintf(`[{"fileId":%q,"fileName":"probe2-famed-%s","isFolder":1,"srcParentId":%s}]`, id, stamp, parent)
			form := url.Values{}
			form.Set("type", "MOVE")
			form.Set("taskInfos", infos)
			form.Set("targetFolderId", dst)
			form.Set("familyId", s.FamilyID)
			status, body := signedCall(t, s, apiHost, true, http.MethodPost, "/batch/createBatchTask.action", probeQuery(""), form.Encode())
			taskID := field(decode(t, body), "taskId")
			t.Logf("VERDICT family MOVE create http=%d taskId=%q", status, taskID)
			if taskID != "" {
				poll := url.Values{}
				poll.Set("taskId", taskID)
				poll.Set("type", "MOVE")
				for i := 0; i < 6; i++ {
					time.Sleep(1200 * time.Millisecond)
					q := probeQuery("familyId=" + url.QueryEscape(s.FamilyID))
					_, body = signedCall(t, s, apiHost, true, http.MethodPost, "/batch/checkBatchTask.action", q, poll.Encode())
					m := decode(t, body)
					if field(m, "taskStatus") == "4" {
						t.Logf("  MOVE final status=4 successed=%s failed=%s errorCode=%q",
							field(m, "successedCount"), field(m, "failedCount"), field(m, "errorCode"))
						break
					}
				}
			}
		}
	})

	t.Run("cleanup", func(t *testing.T) {
		_, files, _ := listDir(t, s, false, "-11")
		var entries []probeEntry
		for _, e := range files {
			if strings.HasPrefix(e.name, "probe2-") {
				entries = append(entries, probeEntry{id: e.id, name: e.name, parent: "-11"})
			}
		}
		if len(entries) > 0 {
			deleteTargets(t, s, false, entries, true)
			t.Logf("personal: DELETE submitted for %d objects", len(entries))
		}
		parent := familyRootFor(t, s)
		_, _, dirs := listDir(t, s, true, parent)
		var fentries []probeEntry
		for _, e := range dirs {
			if strings.HasPrefix(e.name, "probe2-") {
				fentries = append(fentries, probeEntry{id: e.id, name: e.name, parent: parent, isFolder: true})
			}
		}
		if len(fentries) > 0 {
			deleteTargets(t, s, true, fentries, true)
			t.Logf("family: DELETE submitted for %d directories", len(fentries))
		}
		t.Log("everything left in the recycle bin, nothing purged")
	})
}

// runMultipartNamed uploads the single part of an already-initialised session and
// returns the name the object landed under, so a name-mangling question can be
// read from the listing rather than from our own request.
func runMultipartNamed(t *testing.T, s probeSession, content []byte, label, uploadFileID string) string {
	t.Helper()
	digests := [][]byte{api.MD5Raw(content)}
	enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
		{Key: "partInfo", Value: api.PartInfo(digests)},
	}))
	require.NoError(t, err)
	_, body := signedCall(t, s, uploadHost, false, http.MethodGet, "/person/getMultiUploadUrls", probeQuery("params="+enc), "")
	m := decode(t, body)
	part := subMap(subMap(m, "uploadUrls"), "partNumber_1")
	req, err := http.NewRequest(http.MethodPut, field(part, "requestURL"), strings.NewReader(string(content)))
	require.NoError(t, err)
	for k, v := range api.ParseRequestHeader(field(part, "requestHeader")) {
		req.Header.Set(k, v)
	}
	req.Header.Set("user-agent", dartUserAgent)
	res, err := doScrubbed(req)
	require.NoError(t, err)
	_ = res.Body.Close()
	enc, err = api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
	}))
	require.NoError(t, err)
	status, body := signedCall(t, s, uploadHost, false, http.MethodPost, "/person/commitMultiUploadFile", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m = decode(t, body)
	t.Logf("  %s: put=%d commit http=%d code=%q", label, res.StatusCode, status, field(m, "code"))
	_, files, _ := listDir(t, s, false, "-11")
	want := field(subMap(m, "file"), "userFileId")
	for _, e := range files {
		if e.id == want {
			return e.name
		}
	}
	return "（提交后列表里找不到）"
}

// familyRootFor returns a directory id the family drive will accept as a parent.
func familyRootFor(t *testing.T, s probeSession) string {
	_, _, dirs := listDir(t, s, true, "")
	for _, e := range dirs {
		if e.parent != "" {
			return e.parent
		}
	}
	return "0"
}
