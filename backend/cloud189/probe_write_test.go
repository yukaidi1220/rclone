package cloud189

import (
	"bytes"
	"encoding/json"
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

// Probes for the behaviours no capture covers. Everything here runs against
// the session the official client already issued, so no ticket is rotated and
// the client's own session is untouched. Objects are named probe-* so they stay
// recognisable in the recycle bin afterwards.

const (
	// probeSliceSize is the slice size the client used in every capture.
	probeSliceSize = int64(10485760)

	// probeContentRepeats makes the upload fixture 1,331,200 bytes, which is
	// one slice at the client's slice size - the case no capture measured.
	probeContentRepeats = 102400
)

// probeEntry is the subset of a listing row a probe needs.
//
// Ids stay strings: they are 18 digits and would not survive a float64 round
// trip. isFolder and parent carry what a DELETE task needs to address the row.
type probeEntry struct {
	id       string
	name     string
	size     string
	md5      string
	parent   string
	isFolder bool
}

func probeQuery(extra string) string {
	return fmt.Sprintf("rand=%d&%s&%s", time.Now().UnixMilli(), clientSuffix, extra)
}

func probeContent() []byte {
	return bytes.Repeat([]byte("rclone-probe-"), probeContentRepeats)
}

// field renders a JSON value as text, or "" when absent.
func field(m map[string]any, k string) string {
	if m == nil {
		return ""
	}
	switch v := m[k].(type) {
	case json.Number:
		return v.String()
	case string:
		return v
	case bool:
		return fmt.Sprintf("%v", v)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

func subMap(m map[string]any, k string) map[string]any {
	if v, ok := m[k].(map[string]any); ok {
		return v
	}
	return nil
}

func subList(m map[string]any, k string) []any {
	if v, ok := m[k].([]any); ok {
		return v
	}
	return nil
}

func rows(m map[string]any, k string) []probeEntry {
	var out []probeEntry
	for _, e := range subList(m, k) {
		row, ok := e.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, probeEntry{
			id:       field(row, "id"),
			name:     field(row, "name"),
			size:     field(row, "size"),
			md5:      field(row, "md5"),
			parent:   field(row, "parentId"),
			isFolder: k == "folderList",
		})
	}
	return out
}

// createFolder makes a directory and returns its id.
//
// The personal space takes a POST with the parameters in the query; the family
// space takes a GET and calls the parent parameter parentId rather than
// parentFolderId. Both answer synchronously, so no task polling is involved.
func createFolder(t *testing.T, s probeSession, family bool, parentID, name string) string {
	t.Helper()
	if family {
		q := probeQuery(fmt.Sprintf("familyId=%s&parentId=%s&folderName=%s", s.FamilyID, url.QueryEscape(parentID), url.QueryEscape(name)))
		status, body := signedCall(t, s, apiHost, true, http.MethodGet, "/family/file/createFolder.action", q, "")
		m := decode(t, body)
		t.Logf("family createFolder name=%q parentId=%q http=%d id=%s resp=%s", name, parentID, status, field(m, "id"), trunc(string(body), 200))
		return field(m, "id")
	}
	q := probeQuery(fmt.Sprintf("parentFolderId=%s&folderName=%s", url.QueryEscape(parentID), url.QueryEscape(name)))
	status, body := signedCall(t, s, apiHost, false, http.MethodPost, "/createFolder.action", q, "")
	m := decode(t, body)
	t.Logf("personal createFolder name=%q parentFolderId=%q http=%d res_code=%s id=%s resp=%s",
		name, parentID, status, field(m, "res_code"), field(m, "id"), trunc(string(body), 200))
	return field(m, "id")
}

// listDir returns one directory's rows. An empty folderID omits the parameter,
// which is how the client asks for the family root.
func listDir(t *testing.T, s probeSession, family bool, folderID string) (string, []probeEntry, []probeEntry) {
	t.Helper()
	extra := fmt.Sprintf("pageNum=1&pageSize=200&fileType=0&mediaType=0&descending=true&orderBy=%s", map[bool]string{true: "1", false: "filename"}[family])
	if family {
		extra = "familyId=" + s.FamilyID + "&" + extra
	}
	if folderID != "" {
		extra = "folderId=" + url.QueryEscape(folderID) + "&" + extra
	}
	requestURI := "/listFiles.action"
	if family {
		requestURI = "/family/file/listFiles.action"
	}
	status, body := signedCall(t, s, apiHost, family, http.MethodGet, requestURI, probeQuery(extra), "")
	m := decode(t, body)
	ao := subMap(m, "fileListAO")
	require.NotNilf(t, ao, "http=%d no fileListAO: %s", status, trunc(string(body), 240))
	return field(ao, "count"), rows(ao, "fileList"), rows(ao, "folderList")
}

// taskInfos renders the JSON array the batch endpoints take. The fileId is
// quoted and srcParentId is not, exactly as the client sends it.
func taskInfos(entries []probeEntry, withSrcParent bool) string {
	var b strings.Builder
	b.WriteString("[")
	for i, e := range entries {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"fileId":%q,"fileName":%q,"isFolder":%d`, e.id, e.name, bool2int(e.isFolder))
		if withSrcParent {
			fmt.Fprintf(&b, `,"srcParentId":%s`, e.parent)
		}
		b.WriteString("}")
	}
	b.WriteString("]")
	return b.String()
}

func bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}

// batchCreate submits a task and returns its taskId.
func batchCreate(t *testing.T, s probeSession, family bool, taskType, infos, targetFolderID string) string {
	t.Helper()
	form := url.Values{}
	form.Set("type", taskType)
	form.Set("taskInfos", infos)
	if targetFolderID != "" {
		form.Set("targetFolderId", targetFolderID)
	}
	if family {
		form.Set("familyId", s.FamilyID)
	}
	status, body := signedCall(t, s, apiHost, family, http.MethodPost, "/batch/createBatchTask.action", probeQuery(""), form.Encode())
	m := decode(t, body)
	t.Logf("createBatchTask type=%s family=%v http=%d res_code=%s taskId=%s resp=%s",
		taskType, family, status, field(m, "res_code"), field(m, "taskId"), trunc(string(body), 240))
	return field(m, "taskId")
}

// batchPoll waits for a task to reach its terminal state.
//
// taskStatus 4 is terminal, and success is decided by failedCount and
// successedCount alone: res_code stays 0 and HTTP stays 200 even when the task
// failed. The client polls on a flat ~1.2s cadence with no backoff.
func batchPoll(t *testing.T, s probeSession, family bool, taskID, taskType string) map[string]any {
	t.Helper()
	q := probeQuery("")
	if family {
		q = probeQuery("familyId=" + s.FamilyID)
	}
	var last map[string]any
	for i := 0; i < 12; i++ {
		time.Sleep(1200 * time.Millisecond)
		form := url.Values{}
		form.Set("taskId", taskID)
		form.Set("type", taskType)
		status, body := signedCall(t, s, apiHost, family, http.MethodPost, "/batch/checkBatchTask.action", q, form.Encode())
		m := decode(t, body)
		last = m
		t.Logf("checkBatchTask %s poll=%d http=%d status=%s successed=%s failed=%s skip=%s sub=%s errorCode=%q resp=%s",
			taskType, i+1, status, field(m, "taskStatus"), field(m, "successedCount"), field(m, "failedCount"),
			field(m, "skipCount"), field(m, "subTaskCount"), field(m, "errorCode"), trunc(string(body), 240))
		if field(m, "taskStatus") == "4" || field(m, "taskStatus") == "2" {
			return m
		}
	}
	t.Logf("task %s did not terminate within the polling budget", taskType)
	return last
}

func deleteTargets(t *testing.T, s probeSession, family bool, entries []probeEntry, withSrcParent bool) map[string]any {
	t.Helper()
	taskID := batchCreate(t, s, family, "DELETE", taskInfos(entries, withSrcParent), "")
	require.NotEmptyf(t, taskID, "DELETE task was not accepted")
	return batchPoll(t, s, family, taskID, "DELETE")
}

// TestProbeWriteOperations answers, in order, the questions no capture
// settles: does a rename onto an existing name overwrite, does DELETE recurse
// into a non-empty directory, what does a one-part file send as sliceMd5, and
// does the rapid-transfer match consult sliceMd5 at all.
//
// The order is not cosmetic: the last two need a file on the drive, and this
// account's personal space is full, so the file is created in the family space.
func TestProbeWriteOperations(t *testing.T) {
	f := loadProbeFixture(t)
	s := f.Personal
	stamp := time.Now().Format("150405")

	var (
		dirA, dirB, famDir string
		nameA, nameB       string
		entryA, entryB     probeEntry
		landedFile         probeEntry
	)

	t.Run("rename_onto_existing_name", func(t *testing.T) {
		nameA, nameB = "probe-a-"+stamp, "probe-b-"+stamp
		dirA = createFolder(t, s, false, "-11", nameA)
		dirB = createFolder(t, s, false, "-11", nameB)
		require.NotEmpty(t, dirA, "probe-a was not created, cannot test the collision")
		require.NotEmpty(t, dirB, "probe-b was not created, cannot test the collision")
		entryA = probeEntry{id: dirA, name: nameA, parent: "-11", isFolder: true}
		entryB = probeEntry{id: dirB, name: nameB, parent: "-11", isFolder: true}

		q := probeQuery(fmt.Sprintf("folderId=%s&destFolderName=%s", url.QueryEscape(dirA), url.QueryEscape(nameB)))
		status, body := signedCall(t, s, apiHost, false, http.MethodPost, "/renameFolder.action", q, "")
		m := decode(t, body)
		t.Logf("COLLISION renameFolder id=%s dest=%q http=%d res_code=%s returned_name=%q id_after=%q resp=%s",
			dirA, nameB, status, field(m, "res_code"), field(m, "name"), field(m, "id"), trunc(string(body), 300))

		count, _, folders := listDir(t, s, false, "-11")
		t.Logf("root after the collision rename: count=%s probe rows:", count)
		for _, e := range folders {
			if strings.HasPrefix(e.name, "probe-") {
				t.Logf("  %s (%s)", e.name, e.id)
			}
		}
	})

	t.Run("delete_nonempty_directory", func(t *testing.T) {
		if dirA == "" {
			t.Skip("no probe directory to nest under")
		}
		childID := createFolder(t, s, false, dirA, "probe-child-"+stamp)
		if childID == "" {
			t.Skip("child directory was not created")
		}
		count, _, folders := listDir(t, s, false, dirA)
		t.Logf("parent %s holds count=%s folders=%d before DELETE", dirA, count, len(folders))

		res := deleteTargets(t, s, false, []probeEntry{entryA}, true)
		t.Logf("DELETE on a NON-EMPTY directory: successedCount=%s failedCount=%s errorCode=%q",
			field(res, "successedCount"), field(res, "failedCount"), field(res, "errorCode"))

		_, _, rootFolders := listDir(t, s, false, "-11")
		stillThere := false
		for _, e := range rootFolders {
			if e.id == dirA {
				stillThere = true
			}
		}
		t.Logf("parent still present in root after DELETE: %v", stillThere)

		// A nested child that was moved as part of the subtree shows up in the
		// recycle bin; so does the parent. This is what proves recursion either
		// way, since both rows carry their own names.
		q := probeQuery("pageNum=1&pageSize=200&iconOption=7&orderBy=lastOpTime&descending=true")
		status, body := signedCall(t, s, apiHost, false, http.MethodGet, "/listRecycleBinFilesV3.action", q, "")
		m := decode(t, body)
		// The recycle bin answers flat: count, fileList and folderList sit at
		// the top level, unlike listFiles where they are wrapped in fileListAO.
		t.Logf("recycle bin http=%d count=%s probe rows:", status, field(m, "count"))
		for _, key := range []string{"fileList", "folderList"} {
			for _, e := range rows(m, key) {
				if strings.HasPrefix(e.name, "probe-") {
					t.Logf("  recycle %s name=%q id=%s parentId=%q", key, e.name, e.id, e.parent)
				}
			}
		}
	})

	t.Run("one_part_upload_in_family_space", func(t *testing.T) {
		content := probeContent()
		fileMd5 := api.MD5Upper(content)
		name := "probe-1m.bin"
		t.Logf("upload fixture name=%s size=%d fileMd5=%s", name, len(content), fileMd5)

		famDir = createFolder(t, s, true, "0", "probe-up-"+stamp)
		if famDir == "" {
			// parentId=0 for a create is in no capture. Read the real root id
			// off a folder row and retry once.
			_, _, folders := listDir(t, s, true, "")
			require.NotEmpty(t, folders, "family root listed no folder to read its parentId from")
			t.Logf("family root id read from a folder row: %s", folders[0].parent)
			famDir = createFolder(t, s, true, folders[0].parent, "probe-up-"+stamp)
		}
		require.NotEmpty(t, famDir, "no family directory to upload into")

		initA := uploadInit(t, s, famDir, name, content, fileMd5)
		dataA := subMap(initA, "data")
		t.Logf("CANDIDATE-A sliceMd5=fileMd5 -> fileDataExists=%s uploadFileId=%q",
			field(dataA, "fileDataExists"), field(dataA, "uploadFileId"))

		alt := api.MD5Upper([]byte(fileMd5))
		initB := uploadInit(t, s, famDir, name, content, alt)
		t.Logf("CANDIDATE-B sliceMd5=MD5(hex(fileMd5)) -> fileDataExists=%s", field(subMap(initB, "data"), "fileDataExists"))

		if field(dataA, "fileDataExists") == "1" {
			t.Log("candidate A hit rapid transfer on a file that was never uploaded, so sliceMd5 alone keys the match")
			return
		}
		if field(dataA, "fileDataExists") != "0" {
			t.Skipf("init did not answer fileDataExists, cannot continue: %v", initA)
		}
		uploadRun(t, s, famDir, name, content, initA)
	})

	t.Run("rapid_transfer_key_fields", func(t *testing.T) {
		_, files, _ := listDir(t, s, true, famDir)
		if len(files) == 0 {
			t.Log("nothing landed in the family probe directory, skipping")
			return
		}
		landedFile = files[0]
		content := probeContent()
		t.Logf("landed name=%q size=%s md5=%s localDigest=%s", landedFile.name, landedFile.size, landedFile.md5, api.MD5Upper(content))
		for _, cand := range []struct {
			label string
			value string
		}{
			{"fileMd5", landedFile.md5},
			{"MD5(hex(fileMd5))", api.MD5Upper([]byte(landedFile.md5))},
		} {
			res := uploadInit(t, s, famDir, landedFile.name, content, cand.value)
			t.Logf("re-init sliceMd5=%s -> fileDataExists=%s", cand.label, field(subMap(res, "data"), "fileDataExists"))
		}
	})

	t.Run("cleanup", func(t *testing.T) {
		if famDir != "" {
			_, files, _ := listDir(t, s, true, famDir)
			var fs []probeEntry
			for _, e := range files {
				e.parent = famDir
				fs = append(fs, e)
			}
			if len(fs) > 0 {
				deleteTargets(t, s, true, fs, true)
			}
			deleteTargets(t, s, true, []probeEntry{{id: famDir, name: "probe-up-" + stamp, parent: famDirParent(t, s, famDir), isFolder: true}}, true)
		}
		var leftovers []probeEntry
		if dirA != "" {
			leftovers = append(leftovers, entryA)
		}
		if dirB != "" {
			leftovers = append(leftovers, entryB)
		}
		if len(leftovers) > 0 {
			deleteTargets(t, s, false, leftovers, true)
		}
		t.Log("cleanup DELETE submitted; the items land in the recycle bin and were not purged")
	})
}

// famDirParent reads the parent the family directory actually reports, so a
// DELETE addresses it the way the server records it.
func famDirParent(t *testing.T, s probeSession, id string) string {
	t.Helper()
	_, _, folders := listDir(t, s, true, "")
	for _, e := range folders {
		if e.id == id {
			return e.parent
		}
	}
	return "0"
}

// uploadInit opens an upload session in the family space. The encrypted
// payload goes in the form body and decodefields names what the server will
// find inside it, which is the client's shape rather than the older GET form.
func uploadInit(t *testing.T, s probeSession, parentID, name string, content []byte, sliceMd5 string) map[string]any {
	t.Helper()
	digest := api.MD5Upper(content)
	params := []api.Param{{Key: "familyId", Value: s.FamilyID}}
	params = append(params, api.InitParams(parentID, name, digest, int64(len(content)), sliceMd5, probeSliceSize)...)
	enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams(params))
	require.NoError(t, err)
	status, resp := signedCall(t, s, uploadHost, true, http.MethodPost, "/family/initMultiUpload", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	t.Logf("initMultiUpload http=%d resp=%s", status, trunc(string(resp), 220))
	return decode(t, resp)
}

// uploadRun fetches the part URLs, PUTs the single part and commits.
func uploadRun(t *testing.T, s probeSession, parentID, name string, content []byte, init map[string]any) {
	t.Helper()
	data := subMap(init, "data")
	uploadFileID := field(data, "uploadFileId")
	require.NotEmpty(t, uploadFileID, "init returned no uploadFileId")

	digest := api.MD5Raw(content)
	enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
		{Key: "partInfo", Value: api.PartInfo([][]byte{digest})},
	}))
	require.NoError(t, err)
	status, resp := signedCall(t, s, uploadHost, true, http.MethodGet, "/family/getMultiUploadUrls", probeQuery("params="+enc), "")
	t.Logf("getMultiUploadUrls http=%d resp=%s", status, trunc(string(resp), 500))
	m := decode(t, resp)
	part := subMap(subMap(m, "uploadUrls"), "partNumber_1")
	partURL := field(part, "requestURL")
	partHeader := field(part, "requestHeader")
	require.NotEmptyf(t, partURL, "no presigned URL for part 1: %s", trunc(string(resp), 300))

	req, err := http.NewRequest(http.MethodPut, partURL, bytes.NewReader(content))
	require.NoError(t, err)
	for k, v := range api.ParseRequestHeader(partHeader) {
		req.Header.Set(k, v)
	}
	req.Header.Set("user-agent", dartUserAgent)
	req.ContentLength = int64(len(content))
	res, err := doScrubbed(req)
	require.NoError(t, err)
	putBody, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	t.Logf("part PUT http=%d respBytes=%d body=%s", res.StatusCode, len(putBody), trunc(string(putBody), 200))
	require.Equal(t, http.StatusOK, res.StatusCode, "part PUT failed")

	// Which fields a commit carries is in no capture; the ciphertext was only
	// ever length-matched. Try the narrowest shape first and widen only if the
	// server declines it, so the accepted set becomes evidence rather than an
	// assumption carried over from another driver.
	candidates := []string{
		"uploadFileId=" + uploadFileID,
		"uploadFileId=" + uploadFileID + "&isLog=false",
		"uploadFileId=" + uploadFileID + "&isLog=false&opertype=1",
	}
	for i, plain := range candidates {
		enc, err := api.EncryptParams(s.SessionSecret, plain)
		require.NoError(t, err)
		status, resp := signedCall(t, s, uploadHost, true, http.MethodPost, "/family/commitMultiUploadFile", probeQuery(""), "params="+enc,
			hdr{"decodefields", api.DecodeFields})
		m := decode(t, resp)
		code := field(m, "code")
		t.Logf("commit attempt=%d plain=%q http=%d code=%q file.name=%q file.id=%s resp=%s",
			i+1, plain, status, code, field(subMap(m, "file"), "fileName"), field(subMap(m, "file"), "userFileId"), trunc(string(resp), 260))
		if strings.EqualFold(code, "SUCCESS") {
			return
		}
	}
	t.Log("no commit field set was accepted; the part stays uploaded but uncommitted")
}
