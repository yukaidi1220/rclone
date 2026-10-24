package cloud189

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloud189/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
	fsobject "github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/require"
)

// These tests drive the whole write path - staging, signing, the multipart
// session, the part PUTs, the commit and the name swap - against a scripted
// stand-in for the drive. Nothing here reaches China Telecom, and no real
// credential appears: the session key and secret are fixtures.
//
// The stand-in re-signs every request the way the server does and refuses a
// mismatch, so the signing rules that were measured the hard way (the params
// value comes from the query on a GET) are checked rather than assumed.

const (
	fakeSecret = "0123456789abcdef0123456789abcdef"
	fakeKey    = "fixture-session-key"
	// fakePartBase is 1MiB, a part size the real server refuses (measured:
	// InvalidPartSize). The tests use it to get several parts out of a small
	// file; the ladder itself is pinned against the measured values in
	// TestChunkPlan, so nothing here depends on the server agreeing.
	fakePartBase = int64(1 << 20)
)

type renameCall struct {
	id   string
	name string
}

type taskCall struct {
	taskType string
	infos    []taskInfo
}

type putCall struct {
	part          string // the URL the storage layer was addressed at, e.g. part2
	headers       map[string]string
	body          []byte
	contentLength int64
}

// fakeDrive is the stand-in. The fields below it are the knobs a test turns.
type fakeDrive struct {
	t *testing.T

	mu          sync.Mutex
	sequence    []string
	renames     []renameCall
	tasks       []taskCall
	puts        []putCall
	initPlains  []map[string]string
	urlPlains   []map[string]string
	commitPlain map[string]string

	fileDataExists string // "1" means the content is already on the drive
	commitName     string // the name the commit reports; empty means the requested one
	commitID       string
	commitIDBare   bool                         // answer the id as a bare number, the way the capture does
	failInstall    bool                         // the rename that should give the new object its name collides
	refuseFirstPut bool                         // the storage layer answers the first PUT 400, then accepts it
	refuseAllPuts  bool                         // and never accepts one after that
	listing        string                       // JSON the listing answers with
	listings       map[string]string            // per-directory listings, keyed by folderId
	echoWrongMD5   bool                         // the commit reports a digest that is not the file
	createBodies   []string                     // the raw form body of each createBatchTask
	createKeys     []string                     // the sessionkey header each create was signed with
	apiKeys        []string                     // "path sessionkey" for every signed API call
	listingFn      func(r *http.Request) string // a drive whose contents change as the test goes
	taskStates     []int64                      // the taskStatus values the polls answer, in order
	taskSubs       int                          // object count of the task the next task poll answers

	// The download link and the storage layer on the way down.
	downloadLink string            // what getFileDownloadUrl answers; empty means the escaped link below
	downloadHits int               // how many times a link was minted
	readStatus   []int             // status for each successive read; shorter than the reads means 200
	readBody     []byte            // bytes served on a read, default a short constant
	readQueries  []string          // the query each read actually arrived with
	readRanges   []string          // the Range header each read arrived with
	readCut      []int64           // bytes to send while announcing the whole body, for each read
	folders      map[string]string // directory name to id, for a createFolder
	mkdirs       []string          // the directory names the drive was asked to create

	// The account's own numbers. Left at zero the gates find nothing to gate on,
	// which is what a drive that did not answer looks like to the code.
	transDayFlow  int64
	transFileSize int64
	usedDayFlow   int64
	capacity      int64
	available     int64
	privReads     int
	capReads      int
	lateUsedFlow  int64 // what the day's usage reads after the first answer
	lateUsedSet   bool
	privResCode   json.RawMessage // a res_code that says the account refused
	capResCode    json.RawMessage
}

// resCode answers the field with a refusal when the test asked for one, and
// with the plain zero the accounts answer otherwise.
func resCode(raw json.RawMessage) any {
	if len(raw) == 0 {
		return 0
	}
	return raw
}

func (d *fakeDrive) note(verb, path string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if strings.HasPrefix(path, "/store/") {
		path = "/store/part"
	}
	d.sequence = append(d.sequence, verb+" "+path)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (d *fakeDrive) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t := d.t
	bodyBytes, _ := io.ReadAll(r.Body)
	params := r.URL.Query().Get("params")
	if r.Method == http.MethodPost {
		if v, err := url.ParseQuery(string(bodyBytes)); err == nil && v.Get("params") != "" {
			params = v.Get("params")
		}
	}
	d.note(r.Method, r.URL.Path)

	// A part PUT carries no 189 signature at all: the storage layer signed the
	// URL itself, and the client only replays the headers it was given. The
	// exchange carries none either - it is the call that mints the session the
	// later signatures are made from.
	if !strings.HasPrefix(r.URL.Path, "/store/") && r.URL.Path != "/getSessionForPC.action" {
		d.mu.Lock()
		// Which session signed what: the family drive has its own key, and the
		// signature check below cannot tell the two apart because the captured
		// secret is the same for both. This record can.
		d.apiKeys = append(d.apiKeys, r.URL.Path+" "+r.Header.Get("sessionkey"))
		d.mu.Unlock()
		// The signature covers the method, the path alone, the date header's
		// exact bytes and the encrypted params - and nothing of the rest of the
		// query.
		want := api.Sign(fakeSecret, r.Header.Get("sessionkey"), r.Method, r.URL.Path, r.Header.Get("date"), params)
		if got := r.Header.Get("signature"); got != want {
			t.Errorf("%s %s: signature is wrong; recomputed %s from the same pieces the server uses, client sent %s", r.Method, r.URL.Path, want, got)
			writeJSON(w, http.StatusForbidden, map[string]any{"code": "InvalidSignature", "msg": "the signature check is false"})
			return
		}
	}

	plain := map[string]string{}
	if params != "" {
		decoded, err := api.DecryptParams(fakeSecret, params)
		if err != nil {
			t.Fatalf("%s: the params block does not decrypt with the session secret: %v", r.URL.Path, err)
		}
		for _, kv := range strings.Split(decoded, "&") {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				t.Fatalf("%s: params block holds a field with no separator: %q", r.URL.Path, kv)
			}
			plain[k] = v
		}
	}

	switch r.URL.Path {
	case "/person/initMultiUpload":
		d.mu.Lock()
		d.initPlains = append(d.initPlains, plain)
		d.mu.Unlock()
		exists := d.fileDataExists
		if exists == "" {
			exists = "0"
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"code": "SUCCESS",
			"data": map[string]any{"uploadFileId": "900000000000002_3_1792658503979", "fileDataExists": exists},
		})

	case "/person/getMultiUploadUrls":
		d.mu.Lock()
		d.urlPlains = append(d.urlPlains, plain)
		d.mu.Unlock()
		n := partsFromInfo(plain["partInfo"])
		urls := map[string]any{}
		for i := 1; i <= n; i++ {
			urls[fmt.Sprintf("partNumber_%d", i)] = map[string]any{
				// A presigned URL of the shape the service returns: the signature
				// lives in the query. (The download link's query arrives HTML-
				// escaped; nothing measured says the same of these, so they are
				// served here unescaped and the code is not asked to guess.)
				"requestURL":    "http://" + r.Host + fmt.Sprintf("/store/part%d?X-Amz-Signature=PART-SIGNATURE-%d&a=1&b=2", i, i),
				"requestHeader": d.headerFor(plain["partInfo"], i),
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"code": "SUCCESS", "uploadUrls": urls})

	case "/person/commitMultiUploadFile":
		d.mu.Lock()
		d.commitPlain = plain
		name := d.commitName
		if name == "" && len(d.initPlains) > 0 {
			name = d.initPlains[len(d.initPlains)-1]["fileName"]
		}
		id := d.commitID
		if id == "" {
			id = "9000000000000001"
		}
		// The drive reports the digest it stored, which is the one the client
		// declared: it checked the bytes against it before answering.
		storedMD5 := ""
		if len(d.initPlains) > 0 {
			storedMD5 = d.initPlains[len(d.initPlains)-1]["fileMd5"]
		}
		if d.echoWrongMD5 {
			storedMD5 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
		}
		d.mu.Unlock()
		// The id arrives quoted here and bare in the capture; both have to
		// decode to the same digits.
		// The id comes back as a JSON string on the live endpoint and as a bare
		// number in the capture. Both have to decode to the same digits, so the
		// form is a knob and one test turns it.
		idJSON := json.RawMessage(`"` + id + `"`)
		if d.commitIDBare {
			idJSON = json.RawMessage(id)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"code": "SUCCESS",
			"file": map[string]any{"userFileId": idJSON, "fileName": name, "fileMd5": storedMD5},
		})

	case "/renameFile.action", "/renameFolder.action",
		"/family/file/renameFile.action", "/family/file/renameFolder.action":
		q := r.URL.Query()
		d.mu.Lock()
		id := q.Get("fileId")
		if id == "" {
			id = q.Get("folderId")
		}
		// a folder rename spells its new name destFolderName
		name := q.Get("destFileName")
		if strings.HasSuffix(r.URL.Path, "renameFolder.action") {
			name = q.Get("destFolderName")
		}
		d.renames = append(d.renames, renameCall{id: id, name: name})
		installNumber := len(d.renames)
		d.mu.Unlock()
		if d.failInstall && installNumber == 2 {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"res_code":    "FileAlreadyExists",
				"res_message": "renameFile() - operId=1, parentId = -11, newFileName = movie.bin, exist.",
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"res_code": 0, "res_message": "成功", "id": q.Get("fileId")})

	case "/createFolder.action", "/family/file/createFolder.action":
		q := r.URL.Query()
		name := q.Get("folderName")
		d.mu.Lock()
		d.mkdirs = append(d.mkdirs, name)
		id := d.folders[name]
		d.mu.Unlock()
		if id == "" {
			t.Errorf("the test scripted no directory named %q", name)
			writeJSON(w, http.StatusBadRequest, map[string]any{"res_code": "InvalidArgument"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"res_code": 0, "res_message": "成功", "id": id, "name": name, "parentId": q.Get("parentFolderId"), "fileCata": 1,
		})

	case "/listFiles.action", "/family/file/listFiles.action":
		if d.listingFn != nil {
			if raw := d.listingFn(r); raw != "" {
				w.Header().Set("content-type", "application/json")
				_, _ = io.WriteString(w, raw)
				return
			}
		}
		id := r.URL.Query().Get("folderId")
		if raw, ok := d.listings[id]; ok && raw != "" {
			w.Header().Set("content-type", "application/json")
			_, _ = io.WriteString(w, raw)
			return
		}
		if d.listing != "" {
			w.Header().Set("content-type", "application/json")
			_, _ = io.WriteString(w, d.listing)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"res_code": 0, "fileListAO": map[string]any{
			"count": 0, "fileList": []any{}, "folderList": []any{},
		}})

	case "/batch/createBatchTask.action":
		q, err := url.ParseQuery(string(bodyBytes))
		if err != nil {
			t.Fatalf("the task body is not a form: %v", err)
		}
		var infos []taskInfo
		if err := json.Unmarshal([]byte(q.Get("taskInfos")), &infos); err != nil {
			t.Fatalf("taskInfos is not the JSON array the drive expects: %v (%q)", err, q.Get("taskInfos"))
		}
		d.mu.Lock()
		d.tasks = append(d.tasks, taskCall{taskType: q.Get("type"), infos: infos})
		d.createBodies = append(d.createBodies, string(bodyBytes))
		d.createKeys = append(d.createKeys, r.Header.Get("sessionkey"))
		// A task of several objects reports several done, the way the drive does.
		d.taskSubs = len(infos)
		d.mu.Unlock()
		if len(q.Get("taskInfos")) == 0 {
			t.Errorf("the task sent no taskInfos")
		}
		// The wire shapes are load-bearing: fileId travels quoted so no digit
		// can be lost, and srcParentId travels bare because that is what the
		// client writes. This reads the decoded field, which is what the drive
		// sees.
		encodedInfos := q.Get("taskInfos")
		if !strings.Contains(encodedInfos, `"fileId":"`) {
			t.Errorf("fileId was not written as a quoted string: %s", encodedInfos)
		}
		if strings.Contains(encodedInfos, `"srcParentId":"`) {
			t.Errorf("srcParentId was quoted where the client writes a number: %s", encodedInfos)
		}
		switch q.Get("type") {
		case taskDelete, taskClearRecycle, taskCopy, taskMove:
		default:
			t.Errorf("unexpected task type %q", q.Get("type"))
		}
		writeJSON(w, http.StatusOK, map[string]any{"res_code": 0, "taskId": "900000000000000005"})

	case "/batch/checkBatchTask.action":
		q, err := url.ParseQuery(string(bodyBytes))
		if err != nil {
			t.Fatalf("the poll body is not a form: %v", err)
		}
		if q.Get("taskId") == "" {
			t.Errorf("the poll sent no taskId")
		}
		if q.Get("type") == "" {
			t.Errorf("the poll sent no type, which the server needs to find the task")
		}
		// A test can script the states a poll walks through; the captured one is
		// three, then four.
		d.mu.Lock()
		state := int64(taskStateDone)
		if len(d.taskStates) > 0 {
			state = d.taskStates[0]
			d.taskStates = d.taskStates[1:]
		}
		subs := d.taskSubs
		d.mu.Unlock()
		st := map[string]any{
			"res_code": 0, "res_message": "成功", "taskStatus": state,
			"failedCount": 0, "skipCount": 0, "subTaskCount": subs,
		}
		if state == taskStateDone {
			st["successedCount"] = subs
		}
		writeJSON(w, http.StatusOK, st)

	case "/getSessionForPC.action":
		// NewFs goes through the real entry point, so it exchanges the token
		// itself. The answer mints the same key the pre-seeded remotes use.
		writeJSON(w, http.StatusOK, map[string]any{
			"res_code": 0, "sessionKey": fakeKey, "sessionSecret": fakeSecret,
			"loginName": "fixture-account",
		})

	case "/getUserPrivilegesV2.action":
		d.mu.Lock()
		d.privReads++
		used := d.usedDayFlow
		if d.privReads > 1 && d.lateUsedSet {
			used = d.lateUsedFlow
		}
		d.mu.Unlock()
		if r.URL.Query().Get("flag") != "1" {
			t.Errorf("the privileges call has to ask with flag=1, the way the client does")
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"res_code": resCode(d.privResCode), "res_message": "成功",
			"data": []any{
				map[string]any{"privilegeName": "transDayFlow", "privilegeValue": d.transDayFlow, "privilegeValueUnit": "B"},
				map[string]any{"privilegeName": "transFileSize", "privilegeValue": d.transFileSize, "privilegeValueUnit": "B"},
				map[string]any{"privilegeName": "recycleDay", "privilegeValue": 10, "privilegeValueUnit": "D"},
			},
			"extData": map[string]any{"usedDayFlow": used, "msgTaskStatus": -1},
		})

	case "/getUserInfo.action":
		d.mu.Lock()
		d.capReads++
		d.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{
			"res_code": resCode(d.capResCode), "res_message": "成功",
			"available": d.available, "capacity": d.capacity, "maxFilesize": 4294967296,
			"loginName": "fixture@189.cn",
		})

	case "/family/file/getUserInfo.action":
		d.mu.Lock()
		d.capReads++
		d.mu.Unlock()
		// The family endpoint answers with no res_code at all (captured
		// 2026-10-22), which is why the space numbers cannot be trusted to be
		// there.
		writeJSON(w, http.StatusOK, map[string]any{
			"available": d.available, "capacity": d.capacity, "maxFilesize": 4294967296,
		})

	case "/getFileDownloadUrl.action", "/family/file/getFileDownloadUrl.action":
		fileID := r.URL.Query().Get("fileId")
		if fileID == "" {
			t.Errorf("%s: the download link call sent no fileId", r.URL.Path)
		}
		d.mu.Lock()
		d.downloadHits++
		d.mu.Unlock()
		link := d.downloadLink
		if link == "" {
			// Escaped exactly as the family endpoint writes it (captured
			// 2026-10-22): the ampersands inside the query arrive as &amp;.
			link = "http://" + r.Host + "/store/file?fileId=" + fileID + "&a=1&amp;sig=LIVE-SIGNATURE-VALUE"
		}
		writeJSON(w, http.StatusOK, map[string]any{"fileDownloadUrl": link})

	default:
		if strings.HasPrefix(r.URL.Path, "/store/") {
			if r.Method == http.MethodGet {
				d.storeRead(w, r)
				return
			}
			d.storePart(w, r, bodyBytes)
			return
		}
		t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		writeJSON(w, http.StatusNotFound, map[string]any{"code": "NoSuchEndpoint"})
	}
}

// storeRead is the storage layer on the way down: it serves the bytes a download
// link points at, and records the query it was actually asked with. That record
// is the point - the drive hands back a link whose `&` is written `&amp;`, and a
// client that forwarded it untouched would be asking for a different object.
func (d *fakeDrive) storeRead(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	d.readQueries = append(d.readQueries, r.URL.RawQuery)
	d.readRanges = append(d.readRanges, r.Header.Get("Range"))
	n := len(d.readQueries)
	status := http.StatusOK
	if n <= len(d.readStatus) {
		status = d.readStatus[n-1]
	}
	body := d.readBody
	cut := int64(-1)
	if n <= len(d.readCut) {
		cut = d.readCut[n-1]
	}
	d.mu.Unlock()
	if status != http.StatusOK {
		// Real storage layers quote the request they refused, URL and all, which
		// is why the error path has to strip the signature back out.
		writeJSON(w, status, map[string]any{"Error": "AccessDenied", "RequestPath": r.URL.String()})
		return
	}
	if body == nil {
		body = []byte("downloaded-bytes")
	}
	// A Range header is honoured the way the storage host honours it: 206 with
	// the rest of the object.
	payload := body
	status = http.StatusOK
	if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") && len(body) > 0 {
		spec := strings.TrimSuffix(strings.TrimPrefix(rng, "bytes="), "-")
		if i := strings.IndexByte(spec, '-'); i >= 0 {
			spec = spec[:i]
		}
		if start, err := strconv.ParseInt(spec, 10, 64); err == nil && start >= 0 && start <= int64(len(body)) {
			payload = body[start:]
			status = http.StatusPartialContent
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(body)-1, len(body)))
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	w.WriteHeader(status)
	if cut >= 0 && cut < int64(len(payload)) {
		// Announce the whole answer and send part of it, the way a host that drops
		// a transfer looks to the client.
		_, _ = w.Write(payload[:cut])
		return
	}
	_, _ = w.Write(payload)
}

// storePart is the storage layer. It compares the arriving bytes with the
// Content-MD5 it signed into the URL, which is the earliest and hardest check in
// this protocol - earlier than anything the drive itself does.
func (d *fakeDrive) storePart(w http.ResponseWriter, r *http.Request, body []byte) {
	d.mu.Lock()
	refuse := (d.refuseFirstPut && len(d.puts) == 0) || d.refuseAllPuts
	d.puts = append(d.puts, putCall{part: path.Base(r.URL.Path), headers: flattenHeaders(r.Header), body: body, contentLength: r.ContentLength})
	d.mu.Unlock()
	if refuse {
		// The storage layer quotes back the request it refused, query and all.
		writeJSON(w, http.StatusBadRequest, map[string]any{"Error": "SignatureDoesNotMatch", "RequestPath": r.URL.String()})
		return
	}
	sum := md5.Sum(body)
	if base64.StdEncoding.EncodeToString(sum[:]) != r.Header.Get("Content-MD5") {
		d.t.Errorf("%s: the bytes sent do not match the Content-MD5 the server issued", r.URL.Path)
		writeJSON(w, http.StatusBadRequest, map[string]any{"Error": "DigestMismatch"})
		return
	}
	w.WriteHeader(http.StatusOK)
}

// headerFor writes the requestHeader string for one part the way the server
// writes it, with the two values that contain "=" themselves.
func (d *fakeDrive) headerFor(partInfo string, index int) string {
	digests := digestsFromPartInfo(d.t, partInfo)
	return "Content-Type=application/octet-stream" +
		"&Content-MD5=" + base64.StdEncoding.EncodeToString(digests[index-1]) +
		"&x-amz-limit=rate=12800" +
		"&Authorization=AWS fakeaccesskeyid00:fakesignaturevalue=="
}

func digestsFromPartInfo(t *testing.T, partInfo string) [][]byte {
	t.Helper()
	var out [][]byte
	for _, item := range strings.Split(partInfo, ",") {
		_, b64, ok := strings.Cut(item, "-")
		if !ok {
			t.Fatalf("partInfo item %q is not number-digest", item)
		}
		raw, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			t.Fatalf("partInfo digest %q is not base64: %v", b64, err)
		}
		out = append(out, raw)
	}
	return out
}

func partsFromInfo(partInfo string) int {
	if partInfo == "" {
		return 0
	}
	return len(strings.Split(partInfo, ","))
}

func flattenHeaders(h http.Header) map[string]string {
	out := map[string]string{}
	for k, v := range h {
		if len(v) > 0 {
			out[strings.ToLower(k)] = v[0]
		}
	}
	return out
}

// noWaiting removes the delay between part attempts, for the tests that exercise
// the retry loop.
func noWaiting(t *testing.T) {
	t.Helper()
	old := partRetryDelay
	partRetryDelay = func(attempt, partIndex int) time.Duration { return 0 }
	t.Cleanup(func() { partRetryDelay = old })
}

// newFakeFs builds an Fs pointed at the stand-in. It does not go through NewFs,
// so no credential is exchanged and the only network is the test server.
func newFakeFs(t *testing.T, drive *fakeDrive) *Fs {
	t.Helper()
	srv := httptest.NewServer(drive)
	t.Cleanup(srv.Close)

	f := &Fs{
		name:       "test",
		apiBase:    srv.URL,
		uploadBase: srv.URL,
		httpClient: srv.Client(),
	}
	f.opt.AccessToken = "fixture-token-" + t.Name()
	f.opt.Space = spacePersonal
	f.opt.ChunkSize = fs.SizeSuffix(fakePartBase)
	f.opt.UploadConcurrency = 2
	f.opt.Enc = cloud189DefaultEncoding
	f.partClient = noRedirectClient(srv.Client())
	f.deleted = &deletedRows{ids: map[string]time.Time{}}
	f.pacer = fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Microsecond), pacer.MaxSleep(time.Millisecond), pacer.DecayConstant(1)))
	f.features = cloud189Features(context.Background(), f)
	f.dirCache = dircache.New("", personalRoot, f)
	f.fsRootID = personalRoot

	ts := f.session()
	ts.mu.Lock()
	ts.sessionKey = fakeKey
	ts.sessionSecret = fakeSecret
	ts.familySessionKey = fakeKey + "_family"
	ts.familySessionSecret = fakeSecret
	ts.mu.Unlock()
	return f
}

// pattern makes deterministic bytes, so every digest can be checked against a
// computation the test does for itself.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*31 + i/251)
	}
	return b
}

func upperHex(p []byte) string {
	sum := md5.Sum(p)
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}

func md5Of(p []byte) []byte {
	sum := md5.Sum(p)
	return sum[:]
}

// expectedSliceMD5 recomputes the aggregate the way the capture defined it, from
// the bytes rather than from the code under test.
func expectedSliceMD5(content []byte, partSize int64) string {
	var hexes []string
	for start := int64(0); start < int64(len(content)); start += partSize {
		end := start + partSize
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		hexes = append(hexes, upperHex(content[start:end]))
	}
	return upperHex([]byte(strings.Join(hexes, "\n")))
}

func staticObject(remote string, size int64) fs.ObjectInfo {
	return fsobject.NewStaticObjectInfo(remote, time.Now(), size, true, nil, nil)
}

func sequenceOf(d *fakeDrive) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, len(d.sequence))
	copy(out, d.sequence)
	return out
}

// TestPutLandsUnderItsName covers a fresh upload that needs three parts and no
// swap at all.
func TestPutLandsUnderItsName(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase * 3))

	obj, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content))))
	require.NoError(t, err)
	require.Equal(t, "9000000000000001", obj.(*Object).id)
	require.EqualValues(t, len(content), obj.Size())
	md5sum, err := obj.Hash(context.Background(), hash.MD5)
	require.NoError(t, err)
	require.Equal(t, strings.ToLower(upperHex(content)), md5sum)

	// The session was opened with the whole file hashed, before a byte moved.
	require.Len(t, drive.initPlains, 1)
	init := drive.initPlains[0]
	require.Equal(t, "-11", init["parentFolderId"])
	require.Equal(t, "movie.bin", init["fileName"])
	require.Equal(t, upperHex(content), init["fileMd5"])
	require.Equal(t, fmt.Sprint(len(content)), init["fileSize"])
	require.Equal(t, fmt.Sprint(fakePartBase), init["sliceSize"])
	require.Equal(t, expectedSliceMD5(content, fakePartBase), init["sliceMd5"])

	// Every part went up as the storage layer asked for it. Parts run
	// concurrently, so they are looked up by the URL they were sent to rather
	// than by the order they arrived in.
	require.Len(t, drive.puts, 3)
	for i := range 3 {
		name := fmt.Sprintf("part%d", i+1)
		put := putFor(t, drive, name)
		start := int64(i) * fakePartBase
		want := content[start : start+fakePartBase]
		require.Equal(t, want, put.body, "%s bytes", name)
		require.Equal(t, base64.StdEncoding.EncodeToString(md5Of(want)), put.headers["content-md5"], "%s digest", name)
		require.Equal(t, "rate=12800", put.headers["x-amz-limit"], "a header value holding a second = must survive intact")
		require.Equal(t, dartUserAgent, put.headers["user-agent"])
		require.Equal(t, "application/octet-stream", put.headers["content-type"])
		require.EqualValues(t, len(want), put.contentLength, "the length the storage layer signed for")
	}
	// The declared digests are what the URLs were issued against.
	require.Equal(t, api.PartInfo(md5sOfParts(content, fakePartBase)), drive.urlPlains[0]["partInfo"])

	// The commit named the session, and nothing else was needed.
	require.Equal(t, "900000000000002_3_1792658503979", drive.commitPlain["uploadFileId"])
	require.Empty(t, drive.renames, "an upload that lands where it was asked is not renamed")
	require.Empty(t, drive.tasks, "and nothing is deleted")

	require.Equal(t, []string{
		"POST /person/initMultiUpload",
		"GET /person/getMultiUploadUrls",
		"PUT /store/part",
		"PUT /store/part",
		"PUT /store/part",
		"POST /person/commitMultiUploadFile",
	}, sequenceOf(drive))
}

func md5sOfParts(content []byte, partSize int64) [][]byte {
	var out [][]byte
	for start := int64(0); start < int64(len(content)); start += partSize {
		end := start + partSize
		if end > int64(len(content)) {
			end = int64(len(content))
		}
		out = append(out, md5Of(content[start:end]))
	}
	return out
}

// TestUpdateParksTheNameBeforeTheNewBytesGoUp is the ordering the drive's own
// behaviour forces: the old object leaves the name, the new bytes land into the
// name that is now free, and only then is the old one dropped.
func TestUpdateParksTheNameBeforeTheNewBytesGoUp(t *testing.T) {
	drive := &fakeDrive{
		t: t,
		// what the proof reads back: this run's upload, standing at the name
		listing: `{"res_code":0,"fileListAO":{"count":1,"folderList":[],"fileList":[` +
			`{"id":"9000000000000001","name":"movie.bin","size":10,"type":"1"}]}}`,
	}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	old := &Object{fs: f, remote: "movie.bin", id: "5000000000000000001", size: 1, urlMu: new(sync.Mutex)}
	require.NoError(t, old.Update(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content)))))

	require.Equal(t, "9000000000000001", old.id, "the object now points at the copy the drive stored")
	require.EqualValues(t, len(content), old.size)
	// One listing, and it is the proof that runs before the delete: the swap
	// addresses the object by the id the commit reported, so the only thing it
	// needs from the drive is confirmation that this run's object stands at the
	// name the old one is about to be removed for.
	require.Equal(t, 1, sequencesCount(sequenceOf(drive), "GET /listFiles.action"), "the proof reads the destination once")

	// The park is the ONLY rename: the name was free when the upload went in, so
	// the fresh object arrived wearing it and nothing had to be renamed afterwards.
	require.Len(t, drive.renames, 1)
	park := drive.renames[0]
	require.Equal(t, "5000000000000000001", park.id, "the old object is moved out of the name")
	require.True(t, strings.HasPrefix(park.name, "rclone-park-"), "to a unique name, got %q", park.name)

	require.Len(t, drive.tasks, 1, "the parked object is deleted last")
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Equal(t, "5000000000000000001", drive.tasks[0].infos[0].FileID)
	require.Equal(t, park.name, drive.tasks[0].infos[0].FileName, "the delete names the object where it now is")
	require.EqualValues(t, 0, drive.tasks[0].infos[0].IsFolder, "a file is not flagged as a directory")
	require.NotNil(t, drive.tasks[0].infos[0].ParentID)
	require.EqualValues(t, -11, *drive.tasks[0].infos[0].ParentID)

	// The order is the whole point: park, then upload into the freed name, then
	// drop. Uploading first would make the drive store the new bytes beside the old
	// under a name it invents, and the swap would hit this name three times inside
	// a second - which is measured to leave the drive's listing seconds behind.
	require.Equal(t, []string{
		"POST /renameFile.action",
		"POST /person/initMultiUpload",
		"GET /person/getMultiUploadUrls",
		"PUT /store/part",
		"POST /person/commitMultiUploadFile",
		"GET /listFiles.action",
		"POST /batch/createBatchTask.action",
		"POST /batch/checkBatchTask.action",
	}, sequenceOf(drive))
}

// TestUpdateRestoresTheParkedObjectWhenTheUploadFails: the bytes that were on the
// drive before this run were only moved aside, never replaced, so a failed upload
// has to give the name back before it reports anything.
func TestUpdateRestoresTheParkedObjectWhenTheUploadFails(t *testing.T) {
	drive := &fakeDrive{t: t, refuseAllPuts: true}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	old := &Object{fs: f, remote: "movie.bin", id: "5000000000000000001", size: 1, urlMu: new(sync.Mutex)}
	require.Error(t, old.Update(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content)))))

	require.Len(t, drive.renames, 2, "parked, then given its name back")
	require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))
	require.Equal(t, "5000000000000000001", drive.renames[1].id)
	require.Equal(t, "movie.bin", drive.renames[1].name)
	require.Empty(t, drive.tasks, "and the original is not deleted - it is still the file on the drive")
}

// TestUpdateKeepsTheOriginalWhenTheDriveReusesItsId: the drive deduplicates by
// digest, so content identical to the object just parked can come back as THAT
// object's own id. It is the original, not a duplicate: it is put back under the
// wanted name and nothing is deleted. The dangerous part is that the commit echo
// spells the WANTED name while the file is still sitting under the park name - the
// fake drive's default echo does exactly that - so the swap has to ask the drive
// where the object is instead of believing what the upload reported.
func TestUpdateKeepsTheOriginalWhenTheDriveReusesItsId(t *testing.T) {
	drive := &fakeDrive{t: t, commitID: "5000000000000000001"}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	old := &Object{fs: f, remote: "movie.bin", id: "5000000000000000001", size: 1, urlMu: new(sync.Mutex)}
	require.NoError(t, old.Update(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content)))))
	require.Equal(t, "5000000000000000001", old.id, "the object is the one that was already there")

	require.Len(t, drive.renames, 2, "parked aside, then brought back under the wanted name")
	require.Equal(t, "5000000000000000001", drive.renames[1].id)
	require.Equal(t, "movie.bin", drive.renames[1].name)
	require.Empty(t, drive.tasks, "nothing is deleted: the fresh bytes are the object this run replaced")
}

// TestUpdateRestoresWithARenameEvenWhenTheListingStillShowsTheObject: a listing that
// still answers with the parked original under the wanted name is exactly what a
// listing one rename behind looks like, and taking it as confirmation would report
// the object as being in place while it sits under a park name and the name is
// empty. The rename is the confirmation, so it goes out whatever the listing says.
func TestUpdateRestoresWithARenameEvenWhenTheListingStillShowsTheObject(t *testing.T) {
	drive := &fakeDrive{
		t:        t,
		commitID: "5000000000000000001",
		listing: `{"res_code":0,"fileListAO":{"count":1,"folderList":[],"fileList":[` +
			`{"id":"5000000000000000001","name":"movie.bin","size":10,"type":"1"}]}}`,
	}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	old := &Object{fs: f, remote: "movie.bin", id: "5000000000000000001", size: 1, urlMu: new(sync.Mutex)}
	require.NoError(t, old.Update(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content)))))

	require.Len(t, drive.renames, 2, "parked aside, then brought back under the wanted name")
	require.Equal(t, "5000000000000000001", drive.renames[1].id)
	require.Equal(t, "movie.bin", drive.renames[1].name)
	require.Equal(t, 0, sequencesCount(sequenceOf(drive), "GET /listFiles.action"),
		"the object's place is confirmed by the rename, not by a listing that can be one rename behind")
}

// TestUpdatePutsTheOriginalBackWhenTheConfirmFails: the dedupe branch has no fresh
// object of its own, so a failed confirmation must not be the end of it - the
// original would be left under a park name with the wanted name empty.
func TestUpdatePutsTheOriginalBackWhenTheConfirmFails(t *testing.T) {
	drive := &fakeDrive{
		t:           t,
		commitID:    "5000000000000000001",
		failInstall: true, // the restore, this run's second rename, is refused
		listing: `{"res_code":0,"fileListAO":{"count":1,"folderList":[],"fileList":[` +
			`{"id":"6000000000000000099","name":"movie.bin","size":10,"type":"1"}]}}`,
	}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	old := &Object{fs: f, remote: "movie.bin", id: "5000000000000000001", size: 1, urlMu: new(sync.Mutex)}
	require.Error(t, old.Update(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content)))))

	require.Len(t, drive.renames, 3, "parked, a refused restore, and the put-back the failure path owes")
	require.Equal(t, "5000000000000000001", drive.renames[1].id, "the restore the drive refused")
	require.Equal(t, "5000000000000000001", drive.renames[2].id, "and the original asked for its name again")
	require.Equal(t, "movie.bin", drive.renames[2].name)
}

// TestUpdateTreatsARefusedRestoreAsDoneWhenTheOriginalIsAlreadyThere: the drive can
// refuse a rename onto the name an object already holds. Read back, the name holds
// the parked object itself - which means it is where it belongs, so the update is
// finished rather than failed.
func TestUpdateTreatsARefusedRestoreAsDoneWhenTheOriginalIsAlreadyThere(t *testing.T) {
	drive := &fakeDrive{
		t:           t,
		commitID:    "5000000000000000001",
		failInstall: true, // the restore, which is this run's second rename, is refused
		listing: `{"res_code":0,"fileListAO":{"count":1,"folderList":[],"fileList":[` +
			`{"id":"5000000000000000001","name":"movie.bin","size":10,"type":"1"}]}}`,
	}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	old := &Object{fs: f, remote: "movie.bin", id: "5000000000000000001", size: 1, urlMu: new(sync.Mutex)}
	require.NoError(t, old.Update(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content)))))

	require.Len(t, drive.renames, 2, "parked aside, then asked to come back and refused")
	require.Equal(t, "movie.bin", drive.renames[1].name)
	require.Empty(t, drive.tasks, "nothing is deleted")
}

func putFor(t *testing.T, d *fakeDrive, part string) putCall {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, p := range d.puts {
		if p.part == part {
			return p
		}
	}
	t.Fatalf("no part was sent to %s; the drive saw %+v", part, d.puts)
	return putCall{}
}

func sequencesCount(seq []string, want string) int {
	n := 0
	for _, s := range seq {
		if s == want {
			n++
		}
	}
	return n
}

func sequenceContains(seq []string, want string) string {
	for _, s := range seq {
		if s == want {
			return s
		}
	}
	return ""
}

// TestHardDeleteClearsTheBin covers the option's second step: the drive has no
// delete that skips the recycle bin, so permanent means deleting and then
// clearing the entry.
func TestHardDeleteClearsTheBin(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	f.opt.HardDelete = true

	o := &Object{fs: f, remote: "gone.bin", id: "6000000000000000009", size: 1, urlMu: new(sync.Mutex)}
	require.NoError(t, o.Remove(context.Background()))

	require.Len(t, drive.tasks, 2)
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Equal(t, taskClearRecycle, drive.tasks[1].taskType)
	require.Equal(t, drive.tasks[0].infos, drive.tasks[1].infos, "the same entry is what gets cleared")
}

// TestInstallFailureRestoresTheOriginal is the half of the swap that protects
// data: when the new object cannot take the name, the old one goes back where it
// was and only rclone's own copy is dropped.
func TestInstallFailureRestoresTheOriginal(t *testing.T) {
	drive := &fakeDrive{t: t, commitName: "movie(20261022191707).bin", failInstall: true}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	old := &Object{fs: f, remote: "movie.bin", id: "5000000000000000001", size: 1, urlMu: new(sync.Mutex)}
	err := old.Update(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content))))
	require.Error(t, err)
	require.Contains(t, err.Error(), "FileAlreadyExists")

	require.Len(t, drive.renames, 3, "park, failed install, restore")
	require.Equal(t, "5000000000000000001", drive.renames[2].id, "the third rename addresses the original object")
	require.Equal(t, "movie.bin", drive.renames[2].name, "and gives it back the name it came with")
	require.Equal(t, "5000000000000000001", old.id, "the object rclone reports is still the original, not the copy")

	require.Len(t, drive.tasks, 1)
	require.Equal(t, "9000000000000001", drive.tasks[0].infos[0].FileID, "only rclone's own copy is deleted")
}

// TestPutDetectsARacingOccupant covers the case the listing missed: the name was
// free when rclone looked, and the server stored the upload beside something that
// appeared in between.
func TestPutDetectsARacingOccupant(t *testing.T) {
	drive := &fakeDrive{
		t:          t,
		commitName: "movie(20261022191708).bin",
		listing: `{"res_code":0,"fileListAO":{"count":1,"folderList":[],"fileList":[` +
			`{"id":"7000000000000000002","name":"movie.bin","parentId":"-11","size":10,"type":"1"}]}}`,
	}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content))))
	require.NoError(t, err)
	require.Len(t, drive.renames, 2)
	require.Equal(t, "7000000000000000002", drive.renames[0].id, "the object that appeared is the one parked")
	require.Len(t, drive.tasks, 1)
	require.Equal(t, "7000000000000000002", drive.tasks[0].infos[0].FileID)
}

// TestPutRestoreGivesTheRacingHolderItsNameBack is the swap that fails in the
// middle: the upload arrived under a name of the server's own, the object that had
// raced into the wanted name was parked for it, and the rename that would have
// finished the swap was refused. The holder must be given its name back - it was on
// the drive before this run - and only rclone's own upload is taken back.
func TestPutRestoreGivesTheRacingHolderItsNameBack(t *testing.T) {
	drive := &fakeDrive{
		t:           t,
		commitName:  "movie(20261022191711).bin",
		failInstall: true,
		listing: `{"res_code":0,"fileListAO":{"count":1,"folderList":[],"fileList":[` +
			`{"id":"7000000000000000003","name":"movie.bin","size":10,"type":"1"}]}}`,
	}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content))))
	require.Error(t, err)
	require.Contains(t, err.Error(), "FileAlreadyExists")

	require.Len(t, drive.renames, 3, "park the holder, fail to install, give the holder its name back")
	require.Equal(t, "7000000000000000003", drive.renames[0].id, "the holder was the object moved aside")
	require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))
	require.Equal(t, "7000000000000000003", drive.renames[2].id, "the restore addresses the holder")
	require.Equal(t, "movie.bin", drive.renames[2].name, "with the name it came with")

	require.Len(t, drive.tasks, 1, "and only rclone's own upload is taken back")
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Equal(t, "9000000000000001", drive.tasks[0].infos[0].FileID)
}

// TestPutSwapsOutAFileTheDriveListedWithoutAParent is the personal endpoint's
// row shape, captured 2026-10-23: a file row carries no parentId at all, only
// the folderList rows do. The parked occupier still has to be deleted by a task
// that names its parent as a number, and the listing that found the row is the
// only place that number exists. Measured live the same day: without this the
// swap cleans up nothing and prints "the drive answers with the directory id
// \"\"" while the parked file keeps costing quota.
func TestPutSwapsOutAFileTheDriveListedWithoutAParent(t *testing.T) {
	drive := &fakeDrive{
		t:          t,
		commitName: "movie(20261022191710).bin",
		listing: `{"res_code":0,"fileListAO":{"count":1,"folderList":[],"fileList":[` +
			`{"id":"7000000000000000004","name":"movie.bin","size":10,"type":"1"}]}}`,
	}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content))))
	require.NoError(t, err)
	require.Len(t, drive.renames, 2, "park, install")
	require.Len(t, drive.tasks, 1, "and the parked object is taken back out")
	require.Equal(t, "7000000000000000004", drive.tasks[0].infos[0].FileID)
	require.NotNil(t, drive.tasks[0].infos[0].ParentID, "the parent comes from the listing, not from the row")
	require.EqualValues(t, -11, *drive.tasks[0].infos[0].ParentID, "this listing was of the space root, which the drive spells -11")
}

// TestPutLooksUpTheWantedNameByTheStoredSpelling is the upload half of the
// character table. The drive stores an upload onto a name that is in use beside
// the first object, under a name of its own, so the new bytes arrive under that
// invented name and the swap has to find whoever holds the name rclone wanted -
// by the bytes the drive stores, which for "a:b" are "a：b". Encoding that
// spelling a second time asks for a name that is not on the drive, and the object
// standing in the way stops being visible.
func TestPutLooksUpTheWantedNameByTheStoredSpelling(t *testing.T) {
	drive := &fakeDrive{
		t:          t,
		commitName: "a(20261024091500):b",
		// the personal endpoint's row shape: a file row carries no parentId, so
		// the parent the delete task has to name comes out of the listing walked
		listing: `{"res_code":0,"fileListAO":{"count":1,"folderList":[],"fileList":[` +
			`{"id":"7000000000000000002","name":"a：b","size":10,"type":"1"}]}}`,
	}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("a:b", int64(len(content))))
	require.NoError(t, err)

	require.Len(t, drive.renames, 2, "the holder parked, the upload installed")
	require.Equal(t, "7000000000000000002", drive.renames[0].id, "found by the spelling the drive stores")
	require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))
	require.Equal(t, "9000000000000001", drive.renames[1].id)
	require.Equal(t, "a：b", drive.renames[1].name, "the upload given the stored spelling of the name")
	require.Len(t, drive.tasks, 1, "and the parked object is taken off the drive")
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Equal(t, "7000000000000000002", drive.tasks[0].infos[0].FileID)
	require.NotNil(t, drive.tasks[0].infos[0].ParentID, "its parent comes from the listing that found it")
	require.EqualValues(t, -11, *drive.tasks[0].infos[0].ParentID)
}

// TestInstallLeavesAnObjectThatAlreadyHoldsTheName: the swap looks the wanted name
// up to find whoever holds it, and parks that before moving the new object in. If
// the listing answers with the object being installed - the drive already carries
// it under the wanted name, whatever the commit echo spelled - then the thing in
// the way is itself: parking it would move the new bytes aside and the delete that
// finishes the swap would take them off the drive.
func TestInstallLeavesAnObjectThatAlreadyHoldsTheName(t *testing.T) {
	drive := &fakeDrive{
		t:          t,
		commitName: "movie(20261024100000).bin",
		listing: `{"res_code":0,"fileListAO":{"count":1,"folderList":[],"fileList":[` +
			`{"id":"9000000000000001","name":"movie.bin","size":10,"type":"1"}]}}`,
	}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	obj, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content))))
	require.NoError(t, err)
	require.Equal(t, "9000000000000001", obj.(*Object).id)

	require.Empty(t, drive.renames, "nothing is parked when the holder of the name is the object itself")
	require.Empty(t, drive.tasks, "and nothing is deleted")
}

// TestPutRefusesToOverwriteADirectory: a name can be held by a directory, and a
// file must not displace it.
func TestPutRefusesToOverwriteADirectory(t *testing.T) {
	drive := &fakeDrive{
		t:          t,
		commitName: "movie(20261022191709).bin",
		listing: `{"res_code":0,"fileListAO":{"count":1,"fileList":[],"folderList":[` +
			`{"id":"7000000000000000003","name":"movie.bin","parentId":"-11"}]}}`,
	}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("movie.bin", int64(len(content))))
	require.Error(t, err)
	require.Contains(t, err.Error(), "is a directory on the drive")
	require.Empty(t, drive.renames, "the directory is never renamed")
	require.Len(t, drive.tasks, 1, "and what rclone uploaded is taken back out")
	require.Equal(t, "9000000000000001", drive.tasks[0].infos[0].FileID)
}

// TestRapidTransferSendsNoBytes is the init's second answer: the content is
// already stored, so no URL is asked for and no byte moves.
func TestRapidTransferSendsNoBytes(t *testing.T) {
	drive := &fakeDrive{t: t, fileDataExists: "1"}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase * 2))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("known.bin", int64(len(content))))
	require.NoError(t, err)
	require.Empty(t, drive.puts, "a hit must not transfer anything")
	require.Empty(t, drive.urlPlains, "nor ask for part URLs")
	require.Equal(t, []string{
		"POST /person/initMultiUpload",
		"POST /person/commitMultiUploadFile",
	}, sequenceOf(drive))
}

// TestRefusedPartIsSentAgainOnTheSameURL is the measured retry radius: one part
// refused does not doom the file, and the URL it came back on still works.
func TestRefusedPartIsSentAgainOnTheSameURL(t *testing.T) {
	noWaiting(t)
	drive := &fakeDrive{t: t, refuseFirstPut: true}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("again.bin", int64(len(content))))
	require.NoError(t, err, "a part refused on its first attempt must still complete the upload")
	require.Len(t, drive.puts, 2, "the same part was sent twice")
	require.Equal(t, drive.puts[0].body, drive.puts[1].body)
	require.Len(t, drive.urlPlains, 1, "the part URLs were fetched once, not re-fetched per attempt")
}

// TestPartRetriesStopAtLowLevelRetries: a part that never succeeds must fail the
// file without retrying forever, and must be marked so that the copy engine does
// not re-upload the whole file.
func TestPartRetriesStopAtLowLevelRetries(t *testing.T) {
	noWaiting(t)
	drive := &fakeDrive{t: t, refuseAllPuts: true}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("doomed.bin", int64(len(content))))
	require.Error(t, err)
	require.Contains(t, err.Error(), "attempts")
	require.True(t, fserrors.IsNoLowLevelRetryError(err), "the exhausted part must stop the low level ladder, not restart it")
	require.Equal(t, max(fs.GetConfig(context.Background()).LowLevelRetries, 1), len(drive.puts))
	// The upload never got to the commit, so nothing was left half written.
	require.Empty(t, sequenceContains(sequenceOf(drive), "POST /person/commitMultiUploadFile"))
}

// TestSessionExchangeIsNotTouchedByTheWritePath keeps the shared-account rule in
// the tests: writing files must not log anybody out.
func TestSessionExchangeIsNotTouchedByTheWritePath(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))
	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("quiet.bin", int64(len(content))))
	require.NoError(t, err)
	require.Empty(t, sequenceContains(sequenceOf(drive), "POST /getSessionForPC.action"), "a write must not re-exchange the session")
}

// TestStageSourceWritesTheWholeFileDown forces the temp path and checks the
// staged bytes and the digests computed from them against independent values.
func TestStageSourceWritesTheWholeFileDown(t *testing.T) {
	f := &Fs{}
	f.opt.ChunkSize = fs.SizeSuffix(fakePartBase)
	content := pattern(int(fakePartBase*3 + 7))

	st, err := f.stageSource(bytes.NewReader(content), int64(len(content)))
	require.NoError(t, err)
	require.NotEmpty(t, st.tmpPath, "the bytes went to a temp file, not only to memory")
	onDisk, err := os.ReadFile(st.tmpPath)
	require.NoError(t, err)
	require.Equal(t, content, onDisk, "every byte is staged, in order")

	require.Len(t, st.plans, 4)
	require.EqualValues(t, 7, st.plans[3].Size, "the last part swallows the remainder")
	require.Equal(t, upperHex(content), st.fileMD5)
	for i, p := range st.plans {
		start := int64(i) * fakePartBase
		require.Equal(t, md5Of(content[start:start+p.Size]), st.partMD5s[i], "part %d", i+1)
	}
	require.Equal(t, expectedSliceMD5(content, fakePartBase), st.sliceMD5)

	tmpPath := st.tmpPath
	st.release()
	_, err = os.Stat(tmpPath)
	require.True(t, os.IsNotExist(err), "the temp file is gone after release")
}

// TestStageSourceInMemoryForOnePart is the branch that keeps a small file in
// memory, where the server demands the same digest twice.
func TestStageSourceInMemoryForOnePart(t *testing.T) {
	f := &Fs{}
	f.opt.ChunkSize = fs.SizeSuffix(fakePartBase)
	content := pattern(int(fakePartBase))

	st, err := f.stageSource(bytes.NewReader(content), int64(len(content)))
	require.NoError(t, err)
	defer st.release()
	require.Empty(t, st.tmpPath, "a file that fits in one part is not staged on disk")
	require.Len(t, st.plans, 1)
	require.Equal(t, st.fileMD5, st.sliceMD5)
	require.Equal(t, upperHex(content), st.fileMD5)
}

// TestStageSourceRejectsASourceThatDisagreesAboutItsSize covers both directions:
// too short would store a truncated file, too long would silently lose the tail.
func TestStageSourceRejectsASourceThatDisagreesAboutItsSize(t *testing.T) {
	f := &Fs{}
	f.opt.ChunkSize = fs.SizeSuffix(fakePartBase)

	_, err := f.stageSource(bytes.NewReader(pattern(10)), 1<<20)
	require.Error(t, err, "a source shorter than its declared size cannot be uploaded")
	require.Contains(t, err.Error(), "reading the source")

	_, err = f.stageSource(bytes.NewReader(pattern(2*int(fakePartBase))), fakePartBase)
	require.Error(t, err, "a source longer than its declared size cannot be uploaded either")
	require.Contains(t, err.Error(), "more bytes than it declared")

	// The same rule on the temp path, where the parts are read one at a time.
	_, err = f.stageSource(bytes.NewReader(pattern(int(3*fakePartBase))), 4*fakePartBase)
	require.Error(t, err)
	require.Contains(t, err.Error(), "shorter than its own size")

	// And the other direction there: a declared size with bytes still to come
	// after the last part, which only the end-of-source check can see.
	_, err = f.stageSource(bytes.NewReader(pattern(int(3*fakePartBase))), 2*fakePartBase)
	require.Error(t, err)
	require.Contains(t, err.Error(), "more bytes than it declared")
}

// TestStageSourceRemovesItsTempFileWhenStagingFails is the leak test. The staged
// file is as large as the file being uploaded, and on a failure the caller never
// receives the stagedSource that carries its cleaner, so the disk path has to
// take the temp file away on the way out by itself.
func TestStageSourceRemovesItsTempFileWhenStagingFails(t *testing.T) {
	dir := t.TempDir()
	// os.CreateTemp("") asks the OS for its temp directory, which is what these
	// three variables decide, one per platform.
	t.Setenv("TMPDIR", dir)
	t.Setenv("TMP", dir)
	t.Setenv("TEMP", dir)

	count := func() int {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		return len(entries)
	}

	f := &Fs{}
	f.opt.ChunkSize = fs.SizeSuffix(fakePartBase)

	// The control goes first, so the count below is known to be able to be
	// nonzero: a staging that succeeds leaves its temp file on disk until release.
	st, err := f.stageSource(bytes.NewReader(pattern(int(3*fakePartBase))), 3*fakePartBase)
	require.NoError(t, err)
	require.NotEmpty(t, st.tmpPath, "a file of three parts is staged on disk")
	require.Equal(t, 1, count(), "the staged file is there while the upload would be running")
	st.release()
	require.Equal(t, 0, count(), "and it is gone once released")

	for _, tc := range []struct {
		what string
		size int64
		want string
	}{
		{"source shorter than declared", 4 * fakePartBase, "shorter than its own size"},
		{"source longer than declared", 2 * fakePartBase, "more bytes than it declared"},
	} {
		_, err := f.stageSource(bytes.NewReader(pattern(int(3*fakePartBase))), tc.size)
		require.Error(t, err, tc.what)
		require.Contains(t, err.Error(), tc.want)
		require.Equalf(t, 0, count(), "a staging that failed because the %s left a temp file behind", tc.what)
	}

	// The third way out: the source itself breaks part way through.
	_, err = f.stageSource(&sourceThatBreaks{data: pattern(int(2 * fakePartBase))}, 3*fakePartBase)
	require.Error(t, err, "a source that fails half way cannot be staged")
	require.Contains(t, err.Error(), "staging")
	require.Equal(t, 0, count(), "a staging the source broke off left a temp file behind")
}

// sourceThatBreaks hands out the bytes it holds and then fails, which is what a
// dying pipe or a read error on the source looks like to the staging loop.
type sourceThatBreaks struct {
	data []byte
	got  int
}

func (r *sourceThatBreaks) Read(p []byte) (int, error) {
	if r.got >= len(r.data) {
		return 0, errors.New("fixture: the source broke half way")
	}
	n := copy(p, r.data[r.got:])
	r.got += n
	return n, nil
}

// TestStageSourceRefusesUnknownSize: the protocol cannot start without a size.
func TestStageSourceRefusesUnknownSize(t *testing.T) {
	f := &Fs{}
	f.opt.ChunkSize = fs.SizeSuffix(fakePartBase)
	_, err := f.stageSource(bytes.NewReader(pattern(10)), -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown")
	require.True(t, fserrors.IsNoRetryError(err), "no retry can make an unknown size known")
}

// TestDigestsFromMatchesTheStagingPath checks the chunked writer's hashing
// against the same bytes hashed the other way.
func TestDigestsFromMatchesTheStagingPath(t *testing.T) {
	content := pattern(int(fakePartBase*2 + 5))
	plans := api.PlanParts(int64(len(content)), fakePartBase)
	fileMD5, partMD5s, err := digestsFrom(bytes.NewReader(content), plans, fakePartBase)
	require.NoError(t, err)
	require.Equal(t, upperHex(content), fileMD5)
	require.Len(t, partMD5s, 3)
	require.Equal(t, md5Of(content[:fakePartBase]), partMD5s[0])
	require.Equal(t, md5Of(content[2*fakePartBase:]), partMD5s[2])
}

// TestChunkWriterRefusesAHole: a chunk that never arrived would upload a file of
// the right size with the wrong bytes in it.
func TestChunkWriterRefusesAHole(t *testing.T) {
	f := &Fs{}
	f.opt.ChunkSize = fs.SizeSuffix(fakePartBase)
	content := pattern(int(fakePartBase * 3))
	tmp, err := os.CreateTemp(t.TempDir(), "hole-")
	require.NoError(t, err)
	w := &chunkWriter{
		f: f, tmp: tmp, tmpPath: tmp.Name(),
		remote: "hole.bin", dirID: personalRoot, leaf: "hole.bin",
		size: int64(len(content)), partSize: fakePartBase,
		plans: api.PlanParts(int64(len(content)), fakePartBase),
		done:  []bool{true, true, false},
	}
	// The staged file is full, so only the bookkeeping knows about the hole.
	_, err = tmp.Write(content)
	require.NoError(t, err)

	err = w.Close(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "never arrived")

	_, err = os.Stat(tmp.Name())
	require.True(t, os.IsNotExist(err), "Close drops the staged file even when it refuses to upload")
}

// TestChunkWriterWriteChunkRejectsAnImpossibleChunk guards the offset arithmetic
// that decides where a part lands in the staged file.
func TestChunkWriterWriteChunkRejectsAnImpossibleChunk(t *testing.T) {
	f := &Fs{}
	f.opt.ChunkSize = fs.SizeSuffix(fakePartBase)
	tmp, err := os.CreateTemp(t.TempDir(), "range-")
	require.NoError(t, err)
	size := fakePartBase * 2
	w := &chunkWriter{
		f: f, tmp: tmp, tmpPath: tmp.Name(), leaf: "range.bin",
		size: size, partSize: fakePartBase, plans: api.PlanParts(size, fakePartBase),
		done: make([]bool, 2),
	}
	_, err = w.WriteChunk(context.Background(), 7, bytes.NewReader(pattern(10)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "past the 2 parts")

	_, err = w.WriteChunk(context.Background(), 0, bytes.NewReader(pattern(int(fakePartBase)+1)))
	require.NoError(t, err, "a chunk longer than the part is cut to the part")
	require.True(t, w.done[0])

	w.done[0] = false
	_, err = w.WriteChunk(context.Background(), 0, bytes.NewReader(pattern(10)))
	require.Error(t, err, "a chunk shorter than the part is a hole, not a short write")
	require.Contains(t, err.Error(), "where 1048576 were due")
	require.False(t, w.done[0], "and the part is not marked as arrived")
	require.NoError(t, w.Abort(context.Background()))
}

// TestChunkWriterEndToEnd uploads through the writer interface, which is what a
// multi-threaded copy does.
func TestChunkWriterEndToEnd(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase*2 + 11))

	info, w, err := f.OpenChunkWriter(context.Background(), "cut.bin", staticObject("cut.bin", int64(len(content))))
	require.NoError(t, err)
	require.EqualValues(t, fakePartBase, info.ChunkSize)
	require.Equal(t, f.opt.UploadConcurrency, info.Concurrency)

	plans := api.PlanParts(int64(len(content)), fakePartBase)
	require.Len(t, plans, 3)
	for i, p := range plans {
		start := int64(i) * fakePartBase
		_, err := w.WriteChunk(context.Background(), i, bytes.NewReader(content[start:start+p.Size]))
		require.NoErrorf(t, err, "chunk %d", i)
	}
	require.NoError(t, w.Close(context.Background()))

	require.Len(t, drive.puts, 3)
	require.Equal(t, "cut.bin", drive.initPlains[0]["fileName"])
	require.Equal(t, upperHex(content), drive.initPlains[0]["fileMd5"])
	require.Equal(t, expectedSliceMD5(content, fakePartBase), drive.initPlains[0]["sliceMd5"])
	for i := range plans {
		put := putFor(t, drive, fmt.Sprintf("part%d", i+1))
		start := int64(i) * fakePartBase
		end := start + plans[i].Size
		require.Equal(t, content[start:end], put.body, "part %d", i+1)
	}
}

// TestOpenChunkWriterRefusesTheSizesItCannotServe covers the two sizes the
// protocol has no answer for.
func TestOpenChunkWriterRefusesTheSizesItCannotServe(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)

	_, _, err := f.OpenChunkWriter(context.Background(), "empty.bin", staticObject("empty.bin", 0))
	require.ErrorIs(t, err, fs.ErrorCantUploadEmptyFiles)

	_, _, err = f.OpenChunkWriter(context.Background(), "odd.bin", staticObject("odd.bin", -1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "known size")

	require.Empty(t, sequenceOf(drive), "and nothing was asked of the drive")
}

// TestCommitIDArrivingBareIsTheSameObject pins the other wire form the capture
// showed for the same field: an 18 digit id with no quotes at all. Through a
// float64 it would land two digits short, and rclone would then be addressing a
// different object.
func TestCommitIDArrivingBareIsTheSameObject(t *testing.T) {
	drive := &fakeDrive{t: t, commitIDBare: true, commitID: "900000000000000010"}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	obj, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("big.bin", int64(len(content))))
	require.NoError(t, err)
	require.Equal(t, "900000000000000010", obj.(*Object).id, "every digit of the id survives")
}

// TestCommitReportingADifferentDigestFails: the drive has already checked the
// bytes against the hash rclone declared, so a commit that names another content
// means the object on the drive is not the file that was sent.
func TestCommitReportingADifferentDigestFails(t *testing.T) {
	drive := &fakeDrive{t: t, echoWrongMD5: true}
	f := newFakeFs(t, drive)
	content := pattern(int(fakePartBase))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("other.bin", int64(len(content))))
	require.Error(t, err)
	require.Contains(t, err.Error(), "not this file")
	require.True(t, fserrors.IsNoRetryError(err), "retrying cannot make the drive store different bytes")
	require.Contains(t, err.Error(), upperHex(content), "the message has to name both digests")
}

// TestPutRefusesAnEmptyFile states the boundary that has not been measured.
func TestPutRefusesAnEmptyFile(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)

	_, err := f.Put(context.Background(), bytes.NewReader([]byte{}), staticObject("empty.bin", 0))
	require.ErrorIs(t, err, fs.ErrorCantUploadEmptyFiles)
	require.Empty(t, sequenceOf(drive))
}

// TestPartURLForZone covers the proxy rewrite: only the origin may move.
func TestPartURLForZone(t *testing.T) {
	f := &Fs{}
	f.uploadBase = uploadHost
	raw := "https://media-qh6oss.example.com/BUCKET/path/object?x-amz-signature=abc%3D%3D&partNumber=2"
	got, err := f.partURLFor(raw)
	require.NoError(t, err)
	require.Equal(t, raw, got, "without a zone the URL is the server's, untouched")

	f.uploadBase = "https://proxy.example.com:8443"
	got, err = f.partURLFor(raw)
	require.NoError(t, err)
	require.Equal(t, "https://proxy.example.com:8443/BUCKET/path/object?x-amz-signature=abc%3D%3D&partNumber=2", got,
		"path, query and signature stay exactly as issued")

	_, err = f.partURLFor("::not a url")
	require.Error(t, err, "a URL that cannot be parsed must not be sent on quietly")
}

// TestUploadAnswerCode: the upload endpoints put their refusal in a body that can
// arrive with HTTP 200, so the status alone is not a verdict.
func TestUploadAnswerCode(t *testing.T) {
	require.NoError(t, uploadAnswerCode("SUCCESS"))
	require.NoError(t, uploadAnswerCode("success"))
	require.NoError(t, uploadAnswerCode(""))
	err := uploadAnswerCode("IncompleteUpload")
	require.Error(t, err)
	require.Contains(t, err.Error(), "IncompleteUpload")
}

// TestTaskInfoWireShapes checks the two shapes the drive was captured using,
// because a wrong one of these deletes the wrong object.
func TestTaskInfoWireShapes(t *testing.T) {
	parent, err := taskParentID("-11")
	require.NoError(t, err)
	require.NotNil(t, parent)
	require.EqualValues(t, -11, *parent)

	parent, err = taskParentID("1234567890123456789")
	require.NoError(t, err)
	require.EqualValues(t, 1234567890123456789, *parent, "19 digits fit, and must not be rounded")

	_, err = taskParentID("not-a-number")
	require.Error(t, err, "an id that is not the number the task wants must stop the delete")
	require.True(t, fserrors.IsNoRetryError(err))

	encoded, err := json.Marshal([]taskInfo{{FileID: "1234567890123456789", FileName: "a.bin", IsFolder: b2i(true), ParentID: parent}})
	require.NoError(t, err)
	require.Equal(t, `[{"fileId":"1234567890123456789","fileName":"a.bin","isFolder":1,"srcParentId":1234567890123456789}]`, string(encoded))

	// The one captured case that sends no parent at all: a personal file copied
	// into a family.
	encoded, err = json.Marshal([]taskInfo{{FileID: "900000000000000006", FileName: "clip-01.mp4"}})
	require.NoError(t, err)
	require.Equal(t, `[{"fileId":"900000000000000006","fileName":"clip-01.mp4","isFolder":0}]`, string(encoded))
}

// TestTaskVerdictJudgesByCountsOnly: res_code and res_message say 成功 on a task
// that failed every object, so only the counts are read.
func TestTaskVerdictJudgesByCountsOnly(t *testing.T) {
	f := &Fs{}
	done := func(failed, succeeded, skipped int64, code string) *taskStatusResponse {
		return &taskStatusResponse{TaskStatus: taskStateDone, FailedCount: failed, SuccessedCount: succeeded, SkipCount: skipped, ErrorCode: api.FlexID(code)}
	}
	require.NoError(t, f.taskVerdict(taskDelete, 1, done(0, 1, 0, "")))
	require.Error(t, f.taskVerdict(taskDelete, 1, done(1, 0, 0, "")), "the measured shape: failed, with no error code at all")
	require.Error(t, f.taskVerdict(taskDelete, 1, done(0, 0, 0, "")), "nothing done is not success")
	require.Error(t, f.taskVerdict(taskDelete, 2, done(0, 1, 0, "")), "one of two done is not success either")
	require.Error(t, f.taskVerdict(taskCopy, 1, done(0, 0, 1, "")), "a skipped object is a conflict, not a copy")

	// The two refusals that cannot clear during a run honour fatal_if_no_space.
	for _, code := range []string{codeNoSpace, codeDayFlowOver} {
		err := f.taskVerdict(taskDelete, 1, done(1, 0, 0, code))
		require.True(t, fserrors.IsNoRetryError(err), "code %s", code)
		require.False(t, fserrors.IsFatalError(err), "off by default")
		f.opt.FatalIfNoSpace = true
		require.True(t, fserrors.IsFatalError(f.taskVerdict(taskDelete, 1, done(1, 0, 0, code))), "code %s with the option set", code)
		f.opt.FatalIfNoSpace = false
	}
}

// TestTaskVerdictOnEmptyWork: deleting nothing is a success, not a divide by
// zero, and is what Purge of an empty root does.
func TestTaskVerdictOnEmptyWork(t *testing.T) {
	f := &Fs{}
	require.NoError(t, f.taskVerdict(taskDelete, 0, &taskStatusResponse{TaskStatus: taskStateDone}))
}

// TestRenameCollidesAreNoRetry closes the loop on the shape that the measured
// rename refusal has: a string res_code at HTTP 400.
func TestRenameCollisionsAreClassified(t *testing.T) {
	err := refuse(http.StatusBadRequest, []byte(`{"res_code":"FileAlreadyExists","res_message":"renameFile() - newFileName = x, exist."}`))
	require.Error(t, err)
	var ae *apiError
	require.True(t, asAPIError(err, &ae))
	require.Equal(t, codeFileAlreadyExists, ae.Code)
	require.Equal(t, "res_code", ae.Field)
}

// ------------------------------------------------------------ scrubbing ----

// dialTimeout is an error shaped like the ones the transport returns, including
// the Timeout method, so a test can show that scrubbing does not lose it.
type dialTimeout struct{ text string }

func (e *dialTimeout) Error() string   { return e.text }
func (e *dialTimeout) Timeout() bool   { return true }
func (e *dialTimeout) Temporary() bool { return true }

// TestScrubErrHidesTheSecretAndKeepsTheChain is the reason scrubErr exists rather
// than a plain strings.ReplaceAll: the text has to stop quoting the URL while the
// error underneath still answers errors.Is and still satisfies the interfaces
// rclone's retry rules look for.
func TestScrubErrHidesTheSecretAndKeepsTheChain(t *testing.T) {
	const signedURL = `https://store.example/part2?sig=LIVE-SIGNATURE&token=LONG-LIVED-TOKEN`
	inner := &dialTimeout{text: fmt.Sprintf("Put %q: connection reset by peer", signedURL)}
	wrapped := fmt.Errorf("cloud189: part 2: %w", inner)

	out := scrubErr(wrapped, signedURL)
	require.NotContains(t, out.Error(), "LIVE-SIGNATURE")
	require.NotContains(t, out.Error(), "LONG-LIVED-TOKEN")
	require.Contains(t, out.Error(), "connection reset by peer", "what actually went wrong survives")
	require.Contains(t, out.Error(), "cloud189: part 2", "and so does where it happened")

	require.True(t, errors.Is(out, inner), "the chain is intact")
	var target *dialTimeout
	require.True(t, errors.As(out, &target), "a typed error is still findable")
	var netErr interface{ Timeout() bool }
	require.True(t, errors.As(out, &netErr) && netErr.Timeout(), "and its methods still answer")

	// An error with nothing secret in it comes back as itself, not as a wrapper:
	// the classification tests elsewhere depend on that identity.
	plain := errors.New("nothing to hide")
	require.Same(t, plain, scrubErr(plain, signedURL))
	require.Nil(t, scrubErr(nil, signedURL))
}

// TestExchangeFailureDoesNotPrintTheToken drives the one call that puts the
// credential in a URL. The access token is in the query there, so a transport
// error - which quotes the URL it failed on - is a credential leak on the most
// ordinary failure there is.
func TestExchangeFailureDoesNotPrintTheToken(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	token := f.opt.AccessToken
	// An address that refuses at once, so the error comes from the transport and
	// not from the server.
	f.apiBase = "http://127.0.0.1:1"

	err := f.exchange(context.Background(), "")
	require.Error(t, err)
	require.NotContains(t, err.Error(), token, "the token is never in the message")
	require.NotContains(t, err.Error(), url.QueryEscape(token))
	require.Contains(t, err.Error(), "<redacted>", "and the URL that held it is said to be redacted")
}
