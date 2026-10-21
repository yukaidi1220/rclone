package yun139

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/yun139/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	fsobject "github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The shared-group server-side operations (native move task, catalog rename,
// rapid-upload copy) talk to group.yun.139.com, a constant host, so these tests
// install a RoundTripper that answers the group endpoints and mutates a small
// in-memory tree the way the server does.
//
// Three properties the first draft of this harness did not have, and must:
// every request is recorded (including a part PUT to the storage edge, so
// "nothing was uploaded" can actually fail), each response is written exactly
// once (a second JSON in the same body would surface as a decode error and turn
// a business refusal into a transport error), and the group root is accepted as
// a destination under the numeric group id, which is how the implementation
// addresses it.

const (
	gtestGroupID = "1311970000000000001"
	gtestRootCat = "ROOTCATALOGID"
	gtestUserDOM = "1000000000000000001"
)

type gdir struct {
	catalogs []map[string]any
	contents []map[string]any
}

type groupTransport struct {
	mu    sync.Mutex
	calls []string
	// bodies records the decoded JSON body of every call, keyed by path.
	bodies map[string][]map[string]any
	dirs   map[string]*gdir

	// taskContents/taskCatalogs are what the last createBatchOprTask was given;
	// the detail endpoint echoes them back so the caller can check the ids it
	// asked about were the ids reported.
	taskContents []string
	taskCatalogs []string

	// pollResultCode nil means success (1). pollReason is the per-entry reason
	// ("0" when unset). pollReport decides whether the detail names the entries
	// at all. pollStatus unset means 2 (finished).
	pollResultCode *int
	pollStatus     int
	pollReason     string
	pollReport     bool

	// pollRunningOnce answers the FIRST detail call as a still-running task
	// whose entry list carries no reason at all, which is what a server that has
	// not finished indexing the entries sends.
	pollRunningOnce bool
	pollCount       int

	// createMode: "rapid" (default), "parts", "exist", "refused", "unexpected"
	createMode  string
	existsFile  string
	createdFile string

	// renameIsNoOp models the server answering success while leaving the entry
	// under its old name - the case only a read-back of the parent listing can
	// see.
	renameIsNoOp bool

	renameFails bool

	// taskRefusal answers the move task with a business error instead of a
	// task id, and moves nothing - which is the point: only a transport that
	// leaves the entries where they were can prove a decline is a decline.
	taskRefusal string

	// sensitiveOnCreate answers createGroupCatalog the way the server answers a
	// name its filter rejects (1909011520), creating nothing.
	sensitiveOnCreate bool
}

func (tr *groupTransport) record(path string, body map[string]any) {
	tr.calls = append(tr.calls, path)
	if tr.bodies == nil {
		tr.bodies = map[string][]map[string]any{}
	}
	tr.bodies[path] = append(tr.bodies[path], body)
}

func (tr *groupTransport) lastBody(t *testing.T, path string) map[string]any {
	t.Helper()
	got := tr.bodies[path]
	require.NotEmpty(t, got, "no call recorded to %s (calls: %v)", path, tr.calls)
	return got[len(got)-1]
}

func gcatalog(id, name, p string) map[string]any {
	return map[string]any{"catalogID": id, "catalogName": name, "path": p}
}

func gcontent(id, name string, size int64) map[string]any {
	return map[string]any{"contentID": id, "contentName": name, "contentSize": size}
}

func okResult(obj map[string]any) map[string]any {
	if obj == nil {
		obj = map[string]any{}
	}
	obj["result"] = map[string]any{"resultCode": "0", "resultDesc": "SUCCESS"}
	return obj
}

// normalize maps the numeric group id onto the root catalog id: the
// implementation addresses the group root by group id (that is what
// createGroupCatalog and a root listing use), while the tree here is keyed by
// catalog id.
func (tr *groupTransport) normalize(id string) string {
	if id == gtestGroupID {
		return gtestRootCat
	}
	return id
}

func (tr *groupTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	var buf bytes.Buffer
	w := &jsonWriter{buf: &buf}
	reqPath := strings.TrimPrefix(r.URL.Path, "/")

	body := map[string]any{}
	raw, _ := io.ReadAll(r.Body)
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &body)
	}
	// Every request is recorded, including a part PUT to the storage edge: an
	// assertion that nothing was uploaded has to be able to fail.
	tr.record(r.Method+" "+r.Host+reqPath, body)

	switch {
	case strings.HasSuffix(reqPath, "queryGroupContentListV2"):
		catalogID := tr.normalize(asString(body["catalogID"]))
		d := tr.dirs[catalogID]
		if d == nil {
			d = &gdir{}
		}
		return w.done(okResult(map[string]any{
			"getGroupContentResult": map[string]any{
				"parentCatalogID": catalogID,
				"catalogList":     d.catalogs,
				"contentList":     d.contents,
			},
		})), nil

	case strings.HasSuffix(reqPath, "createBatchOprTask"):
		if tr.taskRefusal != "" {
			return w.done(map[string]any{"result": map[string]any{
				"resultCode": tr.taskRefusal, "resultDesc": "目录不存在",
			}}), nil
		}
		catalogs := stringList(body["catalogList"])
		contents := stringList(body["contentList"])
		tr.taskCatalogs, tr.taskContents = catalogs, contents
		dest := tr.normalize(asString(body["destDirectoryId"]))
		dd := tr.dirs[dest]
		if dd == nil {
			dd = &gdir{}
			tr.dirs[dest] = dd
		}
		// Apply the move, so a later listing of the destination shows what the
		// server holds: this is the only way to tell a move that happened from
		// one that was merely requested.
		for _, id := range contents {
			if d, i, e, ok := tr.locate(id); ok {
				d.contents = append(d.contents[:i], d.contents[i+1:]...)
				dd.contents = append(dd.contents, e)
			}
		}
		for _, id := range catalogs {
			if d, i, e, ok := tr.locate(id); ok {
				d.catalogs = append(d.catalogs[:i], d.catalogs[i+1:]...)
				// The server reports a directory's path as its new parent's path
				// plus its own id, so a later listing of the destination hands
				// out the moved path, not the one from before the move.
				if p := asString(e["path"]); p != "" {
					e["path"] = asString(body["destPath"]) + "/" + id
				}
				dd.catalogs = append(dd.catalogs, e)
			}
		}
		return w.done(okResult(map[string]any{"taskID": "TASK-1"})), nil

	case strings.HasSuffix(reqPath, "queryBatchOprTaskDetailV3"):
		code := 1
		if tr.pollResultCode != nil {
			code = *tr.pollResultCode
		}
		entries := func(ids []string, reason string) []map[string]any {
			var out []map[string]any
			for _, id := range ids {
				out = append(out, map[string]any{"srcId": id, "reason": reason})
			}
			return out
		}
		tr.pollCount++
		running := tr.pollRunningOnce && tr.pollCount == 1
		status := tr.pollStatus
		if status == 0 {
			status = 2
		}
		if running {
			status = 1
		}
		reason := tr.pollReason
		if reason == "" {
			reason = "0"
		}
		out := okResult(map[string]any{
			"batchOprTask": map[string]any{"taskStatus": status, "taskResultCode": code, "progress": 100},
		})
		if tr.pollReport {
			// A running task's entries carry no reason yet: the field is absent,
			// which a naive reader sees as "".
			if running {
				var ids []map[string]any
				for _, id := range tr.taskContents {
					ids = append(ids, map[string]any{"srcId": id})
				}
				for _, id := range tr.taskCatalogs {
					ids = append(ids, map[string]any{"srcId": id})
				}
				out["contentList"] = ids
			} else {
				out["contentList"] = entries(tr.taskContents, reason)
				out["catalogList"] = entries(tr.taskCatalogs, reason)
			}
		}
		return w.done(out), nil

	case strings.HasSuffix(reqPath, "modifyGroupContent"):
		if tr.renameFails {
			return w.done(map[string]any{"result": map[string]any{
				"resultCode": "1809111402", "resultDesc": "目录或文件不存在",
			}}), nil
		}
		id := asString(body["contentID"])
		name := asString(body["contentName"])
		if tr.renameIsNoOp {
			return w.done(okResult(nil)), nil
		}
		if _, _, e, ok := tr.locate(id); ok {
			if _, isCatalog := e["catalogID"]; isCatalog {
				e["catalogName"] = name
			} else {
				e["contentName"] = name
			}
		}
		return w.done(okResult(nil)), nil

	case strings.HasSuffix(reqPath, "createGroupCatalog"):
		if tr.sensitiveOnCreate {
			return w.done(map[string]any{"result": map[string]any{
				"resultCode": groupSensitiveWordErrorCode, "resultDesc": "敏感词校验失败",
			}}), nil
		}
		parent := tr.normalize(asString(body["parentCatalogID"]))
		name := asString(body["catalogName"])
		id := "NEWCAT-" + name
		d := tr.dirs[parent]
		if d == nil {
			d = &gdir{}
			tr.dirs[parent] = d
		}
		d.catalogs = append(d.catalogs, gcatalog(id, name, "root:/"+parent+"/"+id))
		return w.done(okResult(map[string]any{"catalogID": id})), nil

	case strings.HasSuffix(reqPath, "hcy/group/dynamic/file/create"):
		parent := tr.normalize(asString(body["parentFileId"]))
		name := asString(body["name"])
		switch tr.createMode {
		case "refused":
			// The shape the create endpoint actually answers with, measured
			// 2026-10-20: success=false with the business code beside it.
			return w.done(map[string]any{"success": false, "code": "02010501", "message": "请求不合法"}), nil
		case "quota":
			// The account is full: this is a reason to stop, not to re-upload
			// the file the slow way.
			return w.done(map[string]any{"success": false, "code": "04000012", "message": "资源配额不足"}), nil
		case "sensitive":
			// The file-name filter, measured 2026-10-21.
			return w.done(map[string]any{"success": false, "code": sensitiveFileNameErrorCode, "message": "文件名包含敏感词"}), nil
		case "unexpected":
			return w.done(nil), nil
		case "exist":
			return w.done(map[string]any{"success": true, "code": "0000", "data": map[string]any{
				"fileId": tr.existsFile, "fileName": name, "rapidUpload": true, "exist": true,
			}}), nil
		case "parts":
			return w.done(map[string]any{"success": true, "code": "0000", "data": map[string]any{
				"fileId": "PENDING-1", "fileName": name, "uploadId": "UP-1", "rapidUpload": false,
				"partInfos": []map[string]any{{
					"partNumber": 1, "partSize": 1024,
					"uploadUrl": "https://eos-part.test/part1",
				}},
			}}), nil
		default:
			newID := "NEWFILE-" + name
			d := tr.dirs[parent]
			if d == nil {
				d = &gdir{}
				tr.dirs[parent] = d
			}
			d.contents = append(d.contents, gcontent(newID, name, asInt64(body["size"])))
			tr.createdFile = newID
			return w.done(map[string]any{"success": true, "code": "0000", "data": map[string]any{
				"fileId": newID, "fileName": name, "rapidUpload": true,
			}}), nil
		}

	default:
		// Anything else is a call the implementation must not make; it is
		// recorded above and answered as a business failure.
		return w.done(map[string]any{"result": map[string]any{
			"resultCode": "UNEXPECTED", "resultDesc": "unexpected call " + reqPath,
		}}), nil
	}
}

// jsonWriter is a minimal http.ResponseWriter stand-in: the handlers below are
// written like http.Handlers but run inside a RoundTripper, so the body is
// collected and turned into a response exactly once by done().
type jsonWriter struct {
	buf   *bytes.Buffer
	wrote bool
}

func (w *jsonWriter) Header() http.Header { return http.Header{} }

func (w *jsonWriter) Write(b []byte) (int, error) {
	w.wrote = true
	return w.buf.Write(b)
}

func (w *jsonWriter) WriteHeader(int) {}

func (w *jsonWriter) done(obj map[string]any) *http.Response {
	// Two responses in one body decode as trailing garbage, which turns a
	// business refusal into a transport error and makes the branch under test
	// untestable. That happened once already, so the second write fails loudly
	// instead of quietly.
	if w.wrote {
		panic("groupTransport handler answered the same request twice")
	}
	if obj != nil {
		b, err := json.Marshal(obj)
		if err != nil {
			panic(err)
		}
		_, _ = w.Write(b)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(w.buf.Bytes())),
	}
}

func stringList(v any) []string {
	l, _ := v.([]any)
	var out []string
	for _, x := range l {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

func asInt64(v any) int64 {
	if f, ok := v.(float64); ok {
		return int64(f)
	}
	return 0
}

// locate finds the entry with id and returns the directory holding it, its index
// there and the entry itself, so a handler can move or rename it in place the
// way the server would.
func (tr *groupTransport) locate(id string) (*gdir, int, map[string]any, bool) {
	for _, d := range tr.dirs {
		for i, e := range d.contents {
			if e["contentID"] == id {
				return d, i, e, true
			}
		}
		for i, e := range d.catalogs {
			if e["catalogID"] == id {
				return d, i, e, true
			}
		}
	}
	return nil, 0, nil, false
}

func newGroupTestFs(tr *groupTransport) *Fs {
	f := &Fs{
		name:         "test",
		opt:          Options{Enc: encoder.Standard | encoder.EncodeInvalidUtf8, GroupID: gtestGroupID},
		space:        spaceGroup,
		account:      "13900000000",
		userDomainID: gtestUserDOM,
		auth:         "dG9rZW4=",
		ts:           &tokenState{auth: "dG9rZW4="},
		tokMu:        &sync.Mutex{},
		groupRootMu:  &sync.Mutex{},
		httpClient:   &http.Client{Transport: tr},
		pacer:        fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Millisecond))),
		groupRootID:  gtestRootCat,
	}
	f.dirCache = dircache.New("", gtestRootCat, f)
	f.srvPathOf = newSrvPathCache()
	f.srvPathOf.put(gtestRootCat, "root:/"+gtestRootCat)
	return f
}

// gtestDirs is the fixture every test starts from: root holds a.txt and the two
// directories sub (which contains nested, with deep.txt) and dst.
func gtestDirs() map[string]*gdir {
	return map[string]*gdir{
		gtestRootCat: {
			catalogs: []map[string]any{
				gcatalog("D1", "sub", "root:/"+gtestRootCat+"/D1"),
				gcatalog("D2", "dst", "root:/"+gtestRootCat+"/D2"),
			},
			contents: []map[string]any{gcontent("F1", "a.txt", 4)},
		},
		"D1": {
			catalogs: []map[string]any{gcatalog("D3", "nested", "root:/"+gtestRootCat+"/D1/D3")},
		},
		"D3": {contents: []map[string]any{gcontent("F3", "deep.txt", 8)}},
		"D2": {},
	}
}

const (
	createBatchPath = "POST group.yun.139.comhcy/mutual/adapter/isbo/openApi/createBatchOprTask"
	pollPath        = "POST group.yun.139.comhcy/mutual/adapter/isbo/openApi/queryBatchOprTaskDetailV3"
	renamePath      = "POST group.yun.139.comhcy/mutual/adapter/isbo/openApi/modifyGroupContent"
	listPath        = "POST group.yun.139.comhcy/mutual/adapter/isbo/openApi/queryGroupContentListV2"
	createPath      = "POST group.yun.139.comhcy/group/dynamic/file/create"
)

// TestUnitGroupMoveFileSendsTheCapturedTaskShape pins the move request to the
// capture: file ids travel in contentList, the destination is addressed by its
// own server path plus its id, and the account block is the userDomainId one -
// not the accountName block the listing takes.
func TestUnitGroupMoveFileSendsTheCapturedTaskShape(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)

	o := newTestObject(f, "a.txt", "F1", 4)
	got, err := f.Move(context.Background(), o, "sub/a.txt")
	require.NoError(t, err)
	assert.Equal(t, "sub/a.txt", got.Remote())

	body := tr.lastBody(t, createBatchPath)
	assert.Equal(t, "move", body["eventType"])
	assert.Equal(t, float64(3), body["taskType"], "taskType 3 is what a move is")
	assert.Equal(t, []any{"F1"}, body["contentList"], "the file id travels in contentList")
	assert.Empty(t, body["catalogList"], "a file must not be listed as a catalog")
	assert.Equal(t, "root:/"+gtestRootCat+"/D1", body["destPath"], "destPath is the destination's own server path")
	assert.Equal(t, "D1", body["destDirectoryId"])
	assert.Equal(t, gtestGroupID, body["srcGroupID"])
	assert.Equal(t, float64(2), body["srcType"])
	assert.Equal(t, "2", body["destType"])
	assert.Equal(t, float64(1), body["businessType"])
	acct, _ := body["accountInfo"].(map[string]any)
	assert.Equal(t, gtestUserDOM, acct["userDomainId"], "the task endpoints key on userDomainId")
	assert.Equal(t, "1", acct["accountType"])
	assert.NotContains(t, acct, "accountName", "the listing's account block must not be reused here")

	// No upload, and no personal-space batch API anywhere.
	for _, c := range tr.calls {
		assert.NotContains(t, c, "PUT", "a server-side move must not upload: %s", c)
		assert.NotContains(t, c, "eos-part.test", "a server-side move must not touch the storage edge")
		assert.NotContains(t, c, "hcy/file/batchMove", "a group id must never reach the personal batch API")
		assert.NotContains(t, c, "hcy/file/create", "a server-side move must not create an entry")
	}
}

// TestUnitGroupMoveToGroupRootAddressesRootByGroupID covers the one move the
// capture does not show: a file moved out of a subdirectory into the group root.
// Measured against a live group on 2026-10-20, the root is named by the mount
// prefix plus the numeric group id in destPath and by the group id in
// destDirectoryId - the same way createGroupCatalog addresses it.
func TestUnitGroupMoveToGroupRootAddressesRootByGroupID(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	// Put the source where a root move can be seen to happen.
	tr.dirs["D1"].contents = append(tr.dirs["D1"].contents, gcontent("F2", "in-sub.txt", 5))

	o := newTestObject(f, "sub/in-sub.txt", "F2", 5)
	_, err = f.Move(context.Background(), o, "in-root.txt")
	require.NoError(t, err)

	body := tr.lastBody(t, createBatchPath)
	assert.Equal(t, api.GroupRootPathPrefix+"/"+gtestGroupID, body["destPath"],
		"the group root is not a root:/ path; it is the mount prefix plus the group id")
	assert.Equal(t, gtestGroupID, body["destDirectoryId"])
}

// TestUnitGroupMoveSameDirectoryIsRenameOnly: the group refuses a same-parent
// move, so a rename must not be dressed up as a task.
func TestUnitGroupMoveSameDirectoryIsRenameOnly(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs()}
	f := newGroupTestFs(tr)
	o := newTestObject(f, "a.txt", "F1", 4)
	_, err := f.Move(context.Background(), o, "b.txt")
	require.NoError(t, err)

	for _, c := range tr.calls {
		assert.NotContains(t, c, "createBatchOprTask", "a rename in place must not issue a move task")
	}
	body := tr.lastBody(t, renamePath)
	assert.Equal(t, "b.txt", body["contentName"])
	assert.Equal(t, "F1", body["contentID"], "the rename must act on the entry that was moved")
	assert.Equal(t, "file", body["fileType"])
	assert.Equal(t, gtestRootCat, body["parentCatalogID"])
}

// TestUnitGroupMoveSameLeafIsANoOp: asking for the same path must send nothing.
func TestUnitGroupMoveSameLeafIsANoOp(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs()}
	f := newGroupTestFs(tr)
	o := newTestObject(f, "a.txt", "F1", 4)
	got, err := f.Move(context.Background(), o, "a.txt")
	require.NoError(t, err)
	assert.Same(t, o, got)
	for _, c := range tr.calls {
		assert.NotContains(t, c, "createBatchOprTask")
		assert.NotContains(t, c, "modifyGroupContent")
	}
}

// TestUnitGroupMoveAfterTaskRenamesLeaf: a move that changes both the parent and
// the name has to move first and rename afterwards - the other way round would
// rename the entry while it is still in the old directory.
func TestUnitGroupMoveAfterTaskRenamesLeaf(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)

	o := newTestObject(f, "a.txt", "F1", 4)
	_, err = f.Move(context.Background(), o, "sub/renamed.txt")
	require.NoError(t, err)

	require.Len(t, tr.bodies[createBatchPath], 1)
	require.Len(t, tr.bodies[renamePath], 1)
	// Order: the task before the rename.
	var taskAt, renameAt = -1, -1
	for i, c := range tr.calls {
		if strings.Contains(c, "createBatchOprTask") && taskAt < 0 {
			taskAt = i
		}
		if strings.Contains(c, "modifyGroupContent") && renameAt < 0 {
			renameAt = i
		}
	}
	assert.NotEqual(t, -1, taskAt)
	assert.NotEqual(t, -1, renameAt)
	assert.Less(t, taskAt, renameAt, "the entry must be moved before it is renamed")
	assert.Equal(t, "renamed.txt", tr.lastBody(t, renamePath)["contentName"])
	assert.Equal(t, "F1", tr.lastBody(t, renamePath)["contentID"])
}

// TestUnitGroupRenameAfterMoveUsesTheNewParentPath: the rename that follows a
// move must describe the entry where it NOW lives. Sending the path from before
// the move is the failure mode the path cache is there to prevent, and a file's
// path is rebuilt from the destination directory because files are not cached.
func TestUnitGroupRenameAfterMoveUsesTheNewParentPath(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)

	o := newTestObject(f, "a.txt", "F1", 4)
	_, err = f.Move(context.Background(), o, "sub/renamed.txt")
	require.NoError(t, err)

	body := tr.lastBody(t, renamePath)
	assert.Equal(t, "root:/"+gtestRootCat+"/D1/F1", body["path"],
		"the renamed entry must be addressed under its new parent")
	assert.Equal(t, "D1", body["parentCatalogID"])
	assert.Equal(t, "renamed.txt", body["contentName"])
}

// TestUnitGroupDirMoveDropsTheWholeSubtreePaths: moving a directory invalidates
// the path of every entry under it, not just its own. The ids stay the same, so
// a cached child path still resolves - to the directory the child no longer
// lives in.
func TestUnitGroupDirMoveDropsTheWholeSubtreePaths(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	_, err = f.List(context.Background(), "sub/nested")
	require.NoError(t, err)
	_, err = f.List(context.Background(), "dst")
	require.NoError(t, err)
	// Only directories are cached: a file's listing entry carries its parent's
	// path, and a rename rebuilds the file's own path from that parent.
	f.srvPathOf.put("D4", "root:/"+gtestRootCat+"/D1/D3/D4")
	// A sibling whose id merely STARTS WITH the moved directory's id: the
	// subtree match must be on a path segment, not a raw prefix.
	f.srvPathOf.put("D1X", "root:/"+gtestRootCat+"/D1X")

	require.NoError(t, f.DirMove(context.Background(), f, "sub", "dst/moved"))

	// The moved directory itself is cached again - the rename that gives it the
	// destination name reads the parent listing back, and that listing reports
	// its NEW path. Everything still below it must be gone, because nothing has
	// re-listed it.
	got, ok := f.srvPathOf.get("D1")
	require.True(t, ok, "the moved directory is re-learned from its new parent")
	assert.Equal(t, "root:/"+gtestRootCat+"/D2/D1", got, "the cached path must be the post-move one")
	for _, id := range []string{"D3", "D4"} {
		_, ok := f.srvPathOf.get(id)
		assert.False(t, ok, "%s is under the moved directory, so its cached path points at the old parent", id)
	}
	for _, id := range []string{"D2", "D1X"} {
		_, ok := f.srvPathOf.get(id)
		assert.True(t, ok, "%s did not move, so its cached path must survive", id)
	}
}

// TestUnitGroupDirMoveSendsCatalogListAndRenames pins what the directory capture
// shows and the CLI cannot: a directory id travels in catalogList, and a move to
// a different leaf has to rename afterwards because the task keeps the name.
func TestUnitGroupDirMoveSendsCatalogListAndRenames(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	_, err = f.List(context.Background(), "dst")
	require.NoError(t, err)

	require.NoError(t, f.DirMove(context.Background(), f, "sub", "dst/moved"))

	body := tr.lastBody(t, createBatchPath)
	assert.Equal(t, []any{"D1"}, body["catalogList"], "a directory id must travel in catalogList")
	assert.Empty(t, body["contentList"], "a directory is not a content entry")
	assert.Equal(t, "root:/"+gtestRootCat+"/D2", body["destPath"])

	rn := tr.lastBody(t, renamePath)
	assert.Equal(t, "moved", rn["contentName"], "the task keeps the name, so the move has to rename")
	assert.Equal(t, "catalog", rn["fileType"], "a directory is renamed with the catalog type")
	assert.Equal(t, "D1", rn["contentID"], "the rename must act on the directory that moved")
	assert.True(t, strings.Contains(strings.Join(tr.calls, ","), "createBatchOprTask"),
		"calls: %v", tr.calls)
}

// TestUnitGroupDirMoveSameParentIsRenameOnly: the task refuses a same-parent
// move (measured 2026-10-20: result code 3), so DirMove must rename instead.
func TestUnitGroupDirMoveSameParentIsRenameOnly(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs()}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)

	require.NoError(t, f.DirMove(context.Background(), f, "sub", "sub2"))
	for _, c := range tr.calls {
		assert.NotContains(t, c, "createBatchOprTask", "a same-parent directory move must not issue a task")
	}
	body := tr.lastBody(t, renamePath)
	assert.Equal(t, "sub2", body["contentName"])
	assert.Equal(t, "catalog", body["fileType"])
	assert.Equal(t, "D1", body["contentID"])
}

// TestUnitGroupMoveOntoTakenNameRefuses: the server keeps the entry already at
// the destination and parks the moved one under another name. Reporting the
// object at remote as the moved file would hand the caller the wrong entry.
func TestUnitGroupMoveOntoTakenNameRefuses(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	// The destination directory already holds a DIFFERENT a.txt.
	tr.dirs["D1"].contents = append(tr.dirs["D1"].contents, gcontent("F9", "a.txt", 77))

	_, err = f.Move(context.Background(), newTestObject(f, "a.txt", "F1", 4), "sub/a.txt")
	require.Error(t, err, "a move that did not land must not be reported as success")
	assert.Contains(t, err.Error(), "kept the existing entry")
	for _, c := range tr.calls {
		assert.NotContains(t, c, "modifyGroupContent", "the pre-existing destination entry must not be renamed")
	}
}

// TestUnitGroupTaskBadResultCodeIsNotSuccess: a finished task whose result code
// is not 1 is a failure, and the message carries the number, not the address of
// the pointer that holds it.
func TestUnitGroupTaskBadResultCodeIsNotSuccess(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollResultCode: ptrInt(3)}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)

	_, err = f.Move(context.Background(), newTestObject(f, "a.txt", "F1", 4), "sub/a.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "result code 3")
	assert.NotContains(t, err.Error(), "0x", "the message must print the value, not the pointer")
}

// TestUnitGroupTaskUnreportedEntryIsNotSuccess: a task that finishes without
// saying anything about the id it was given is not evidence that it moved.
func TestUnitGroupTaskUnreportedEntryIsNotSuccess(t *testing.T) {
	// The detail's entry list is indexed asynchronously, so an unfinished report
	// is waited for; the wait is shortened here rather than sat through.
	restore := groupTaskDeadline
	grace := groupTaskReportGrace
	groupTaskDeadline = 2 * time.Second
	groupTaskReportGrace = time.Second
	t.Cleanup(func() { groupTaskDeadline, groupTaskReportGrace = restore, grace })

	tr := &groupTransport{dirs: gtestDirs(), pollReport: false}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	_, err = f.Move(context.Background(), newTestObject(f, "a.txt", "F1", 4), "sub/a.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "without reporting F1")
	assert.GreaterOrEqual(t, len(tr.bodies[pollPath]), 2, "a missing report must be waited for, not assumed")
}

// TestUnitGroupTaskEntryReasonIsChecked: the task record can say success while
// an individual entry was refused.
func TestUnitGroupTaskEntryReasonIsChecked(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true, pollReason: "1"}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	_, err = f.Move(context.Background(), newTestObject(f, "a.txt", "F1", 4), "sub/a.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refused F1")
}

// TestUnitGroupTaskIsPolledUntilItFinishes: a task that is still running on the
// first answer must be polled again rather than read as success.
func TestUnitGroupTaskIsPolledUntilItFinishes(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true, pollStatus: 0}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	_, err = f.Move(context.Background(), newTestObject(f, "a.txt", "F1", 4), "sub/a.txt")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(tr.bodies[pollPath]), 1, "the task must be polled")
}

// TestUnitGroupRapidCopySendsOnePartAndTakesTheEcho: the copy is one create call
// with the source hash declared as a single part, and the entry the answer names
// is what gets handed back.
func TestUnitGroupRapidCopySendsOnePartAndTakesTheEcho(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), createMode: "rapid"}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)

	src := newTestObject(f, "a.txt", "F1", 4)
	src.sha256 = strings.Repeat("ab", 32)
	got, err := f.Copy(context.Background(), src, "sub/a.txt")
	require.NoError(t, err)
	assert.Equal(t, "sub/a.txt", got.Remote())

	body := tr.lastBody(t, createPath)
	assert.Equal(t, src.sha256, body["contentHash"])
	assert.Equal(t, "SHA256", body["contentHashAlgorithm"])
	assert.Equal(t, "D1", body["parentFileId"])
	assert.Equal(t, float64(4), body["size"])
	parts, _ := body["partInfos"].([]any)
	require.Len(t, parts, 1, "a copy declares one part whatever the size: the store matches on hash+size")
	p0, _ := parts[0].(map[string]any)
	assert.Equal(t, float64(1), p0["partNumber"])
	assert.Equal(t, float64(4), p0["partSize"])
	_, hasCtx := p0["parallelHashCtx"]
	assert.False(t, hasCtx, "the midstates are a function of bytes a copy never reads")
	// Nothing was uploaded, and the copy cost exactly one create.
	assert.Len(t, tr.bodies[createPath], 1)
	for _, c := range tr.calls {
		assert.NotContains(t, c, "PUT", "a server-side copy must not upload: %s", c)
		assert.NotContains(t, c, "eos-part.test", "a server-side copy must not touch the storage edge")
	}
}

// TestUnitGroupRapidCopyMissFallsBackToBandwidthCopy: when the store has no
// content for the hash the answer carries part URLs. Those parts must not be
// uploaded here - the copy is declined so the engine can do it properly.
func TestUnitGroupRapidCopyMissFallsBackToBandwidthCopy(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), createMode: "parts"}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	src := newTestObject(f, "a.txt", "F1", 4)
	src.sha256 = strings.Repeat("cd", 32)

	_, err = f.Copy(context.Background(), src, "sub/a.txt")
	assert.ErrorIs(t, err, fs.ErrorCantCopy, "a miss must decline, not fail the transfer")
	assert.Contains(t, tr.calls, createPath, "the test is only meaningful if the create was offered")
	for _, c := range tr.calls {
		assert.NotContains(t, c, "PUT", "the parts a miss hands back must not be uploaded: %s", c)
		assert.NotContains(t, c, "eos-part.test", "the parts a miss hands back must not be uploaded")
	}
}

// TestUnitGroupRapidCopyRefusalIsNotFatal: the 02010501 refusal measured on a
// multi-part declaration is a reason to fall back, not a reason to abort a copy
// rclone can still do by moving bytes.
func TestUnitGroupRapidCopyRefusalIsNotFatal(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), createMode: "refused"}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	src := newTestObject(f, "a.txt", "F1", 4)
	src.sha256 = strings.Repeat("ef", 32)

	_, err = f.Copy(context.Background(), src, "sub/a.txt")
	require.ErrorIs(t, err, fs.ErrorCantCopy)
	assert.Contains(t, tr.calls, createPath, "the refusal must come from the create, not an earlier guard")
}

// TestUnitGroupRapidCopyDoesNotTouchTheHeldName: with exist=true the id in the
// answer belongs to the entry that already occupies the destination. Renaming or
// removing it would act on a file this copy did not make.
func TestUnitGroupRapidCopyDoesNotTouchTheHeldName(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), createMode: "exist", existsFile: "F9"}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	// F9 sits at the root, not in the destination: it must not be disturbed.
	tr.dirs[gtestRootCat].contents = append(tr.dirs[gtestRootCat].contents, gcontent("F9", "other.txt", 9))
	src := newTestObject(f, "a.txt", "F1", 4)
	src.sha256 = strings.Repeat("01", 32)

	_, err = f.Copy(context.Background(), src, "sub/b.txt")
	assert.ErrorIs(t, err, fs.ErrorCantCopy)
	assert.Contains(t, tr.calls, createPath, "exist=true is only modelled if the create ran")
	for _, c := range tr.calls {
		assert.NotContains(t, c, "modifyGroupContent", "the pre-existing entry must not be renamed")
		assert.NotContains(t, c, "batchTrash", "the pre-existing entry must not be removed")
	}
}

// TestUnitGroupRapidCopySkippedWhenNameTaken: the destination name being held is
// decided before any create call, so a taken name costs nothing.
func TestUnitGroupRapidCopySkippedWhenNameTaken(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), createMode: "rapid"}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	tr.dirs["D1"].contents = append(tr.dirs["D1"].contents, gcontent("F9", "a.txt", 77))
	src := newTestObject(f, "a.txt", "F1", 4)
	src.sha256 = strings.Repeat("22", 32)

	_, err = f.Copy(context.Background(), src, "sub/a.txt")
	assert.ErrorIs(t, err, fs.ErrorCantCopy)
	for _, c := range tr.calls {
		assert.NotContains(t, c, "file/create", "a taken destination name must not even be offered")
	}
}

// TestUnitGroupHashMissingDeclinesCopy: without a hash there is nothing to
// offer, so the copy is declined rather than attempted blind or failed hard.
func TestUnitGroupHashMissingDeclinesCopy(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs()}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	src := newTestObject(f, "a.txt", "F1", 4)

	_, err = f.Copy(context.Background(), src, "sub/a.txt")
	assert.ErrorIs(t, err, fs.ErrorCantCopy, "a source whose hash cannot be read must decline rather than abort")
	for _, c := range tr.calls {
		assert.NotContains(t, c, "file/create", "nothing may be offered without a hash")
	}
}

// TestUnitSharedCreateBodyFieldTable guards the extraction of the shared-space
// create body out of the upload path: the family space uploads through the same
// map, so a dropped or renamed field there is an upload failure on a remote the
// group tests never touch.
func TestUnitSharedCreateBodyFieldTable(t *testing.T) {
	parts := []api.PartInfo{{PartNumber: 1, PartSize: 10}}
	for _, space := range []string{spaceFamily, spaceGroup} {
		tr := &groupTransport{dirs: gtestDirs()}
		f := newGroupTestFs(tr)
		f.space = space
		f.opt.FamilyID = "FAM1"
		f.opt.RegionCode = "531:543"
		body := buildCreateBody("DIR", "n.txt", 10, "AA", parts, f.opt.RegionCode)
		m := f.sharedCreateBody(body, parts)

		if space == spaceGroup {
			assert.Equal(t, gtestGroupID, m["groupId"])
			assert.Equal(t, 2, m["groupType"], "the group sends groupType 2")
		} else {
			assert.Equal(t, "FAM1", m["groupId"], "the family create takes the family id in groupId")
			assert.Equal(t, 1, m["groupType"], "the family sends groupType 1")
		}
		assert.Equal(t, "AA", m["contentHash"])
		assert.Equal(t, "SHA256", m["contentHashAlgorithm"])
		assert.Equal(t, 86400, m["expireSec"])
		assert.Equal(t, "auto_rename", m["fileRenameMode"])
		assert.Equal(t, "n.txt", m["name"])
		assert.Equal(t, "DIR", m["parentFileId"])
		assert.Equal(t, int64(10), m["size"])
		assert.Equal(t, "file", m["type"])
		assert.Equal(t, true, m["parallelUpload"])
		assert.NotEmpty(t, m["seqNo"], "the shared create takes a fresh per-request seqNo")
		assert.NotEmpty(t, m["localCreatedAt"])
		assert.NotEmpty(t, m["localUpdatedAt"])
		assert.NotContains(t, m, "contentType", "the shared create takes no contentType; only the personal one does")

		// The part list is passed through untouched - an upload that loses it
		// gets no URLs and a rapid copy that invents one is refused 02010501.
		sent, ok := m["partInfos"].([]api.PartInfo)
		require.True(t, ok, "partInfos must be the []api.PartInfo the caller built, got %T", m["partInfos"])
		assert.Equal(t, parts, sent)

		region, ok := m["userRegion"].(map[string]any)
		require.True(t, ok, "userRegion must be present when region_code is set")
		assert.Equal(t, "543", region["cityCode"])
		assert.Equal(t, "531", region["provinceCode"])
	}
}

// TestUnitGroupRapidCopyQuotaRefusalStops: a full account must abort, not fall
// back. Declining is right for "I have not got this content"; for "there is no
// room" a bandwidth copy would read the whole file, land on the same answer and
// waste the transfer.
func TestUnitGroupRapidCopyQuotaRefusalStops(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), createMode: "quota"}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	src := newTestObject(f, "a.txt", "F1", 4)
	src.sha256 = strings.Repeat("33", 32)

	_, err = f.Copy(context.Background(), src, "sub/a.txt")
	require.Error(t, err)
	assert.NotErrorIs(t, err, fs.ErrorCantCopy, "a quota refusal must not be turned into a fallback")
	assert.True(t, fserrors.IsNoRetryError(err), "the sync must not retry into a full account: %v", err)
}

// TestUnitGroupMoveRenameFailureSaysWhereTheEntryIs: after the task moved the
// entry, a failed rename leaves the content in the new directory under its old
// name. Reporting only the rename error would invite a retry of a move that has
// already happened.
func TestUnitGroupMoveRenameFailureSaysWhereTheEntryIs(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true, renameFails: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)

	_, err = f.Move(context.Background(), newTestObject(f, "a.txt", "F1", 4), "sub/renamed.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `stores it as "a.txt"`, "the error has to say where the content actually is: %v", err)
}

// TestUnitGroupDirMoveIntoOwnSubtreeRefused: lib/dircache only refuses a move of
// or onto a root, so the backend has to notice that the destination sits inside
// the directory being moved.
func TestUnitGroupDirMoveIntoOwnSubtreeRefused(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)

	err = f.DirMove(context.Background(), f, "sub", "sub/nested/inside")
	require.Error(t, err, "a directory must not be moved into its own subtree")
	assert.Contains(t, err.Error(), "into itself or a subdirectory")
	for _, c := range tr.calls {
		assert.NotContains(t, c, "createBatchOprTask", "the refusal happens before the task is issued")
	}
}

// TestUnitGroupDirMoveInvalidatesWithoutTheOldPath: the moved directory's own
// path is not always known to this Fs - it can be resolved from the dircache
// without a listing - so the subtree has to be found by the moved id as a path
// segment instead of by the old path.
func TestUnitGroupDirMoveInvalidatesWithoutTheOldPath(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	_, err = f.List(context.Background(), "sub/nested")
	require.NoError(t, err)
	_, err = f.List(context.Background(), "dst")
	require.NoError(t, err)
	f.srvPathOf.put("D4", "root:/"+gtestRootCat+"/D1/D3/D4")
	// Forget the moved directory's own path, the way an Fs that resolved it from
	// its dircache rather than from a listing would have: the subtree then has
	// to be found by the moved id as a path segment.
	f.srvPathOf.mu.Lock()
	delete(f.srvPathOf.m, "D1")
	f.srvPathOf.mu.Unlock()

	require.NoError(t, f.DirMove(context.Background(), f, "sub", "dst/moved"))
	_, ok := f.srvPathOf.get("D4")
	assert.False(t, ok, "D4 sits under the moved directory and must not keep its old path")
}

// TestUnitGroupDirMoveRenameIsChecked: the rename that gives the moved directory
// its destination name answers a bare success envelope. If it did not take
// effect - the server parks it under a suffix, or stores nothing at all - then
// reporting success would leave the caller with a subtree at a path that does
// not exist, and the next sync would upload a second copy of the whole tree.
func TestUnitGroupDirMoveRenameIsChecked(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true, renameIsNoOp: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	_, err = f.List(context.Background(), "dst")
	require.NoError(t, err)

	err = f.DirMove(context.Background(), f, "sub", "dst/moved")
	require.Error(t, err, "a rename that did not take effect must not be reported as success")
	// The subtree was moved, so the failure is about the name only - say so
	// rather than implying the content went missing.
	assert.Contains(t, err.Error(), "rename after directory move")
}

// TestUnitGroupTaskRunningEntryWithoutReason: a task that is still running can
// already list its entries, and until it finishes those entries carry no reason.
// Reading that as a refusal turns a move that worked into a reported failure.
func TestUnitGroupTaskRunningEntryWithoutReason(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true, pollRunningOnce: true}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)

	o := newTestObject(f, "a.txt", "F1", 4)
	_, err = f.Move(context.Background(), o, "sub/a.txt")
	require.NoError(t, err, "a missing reason on a running task is not a refusal")
	assert.GreaterOrEqual(t, tr.pollCount, 2, "the task must be polled again after a running answer")
}

func ptrInt(v int) *int { return &v }

// TestUnitGroupMoveStaleDestPathDeclines covers the one case the caches cannot
// cover: a remote whose cached destination path predates a move issued
// elsewhere. The task answers 1909011551 目录不存在 (measured 2026-10-20), which
// is a reason to let the engine do a bandwidth move, not a failed transfer.
func TestUnitGroupMoveStaleDestPathDeclines(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true, taskRefusal: groupStalePathErrorCode}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "dst")
	require.NoError(t, err)
	_, ok := f.srvPathOf.get("D2")
	require.True(t, ok, "the listing must have cached the destination path, or the test proves nothing")
	// A child of the destination, cached exactly the way listGroup caches it: a
	// path chain running through D2.
	f.srvPathOf.put("D9", "root:/"+gtestRootCat+"/D2/D9")

	o := newTestObject(f, "a.txt", "F1", 4)
	_, err = f.Move(context.Background(), o, "dst/a.txt")
	assert.True(t, errors.Is(err, fs.ErrorCantMove), "a stale destination path must decline, not fail: %v", err)

	_, ok = f.srvPathOf.get("D2")
	assert.False(t, ok, "the path the server just refused must not be sent again")
	// Every path below the destination chains through the rejected string, so a
	// child of it has to go stale in the same shot - not one refused move each.
	_, ok = f.srvPathOf.get("D9")
	assert.False(t, ok, "the destination's child chained through the rejected path must be dropped too")
	assert.Len(t, tr.dirs["D2"].contents, 0, "a refused task moves nothing")
	assert.Len(t, tr.dirs[gtestRootCat].contents, 1, "the entry must still be where it was")
	for _, c := range tr.calls {
		assert.NotContains(t, c, "modifyGroupContent", "a move that declined must not rename: %s", c)
		assert.NotContains(t, c, "queryBatchOprTaskDetailV3", "a task that was refused was never started")
	}
}

// TestUnitGroupDirMoveStaleDestPathDeclines is the same refusal on the
// directory path, which has to decline as ErrorCantDirMove so the engine copies
// the subtree instead of reporting a failed move.
func TestUnitGroupDirMoveStaleDestPathDeclines(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), pollReport: true, taskRefusal: groupStalePathErrorCode}
	f := newGroupTestFs(tr)
	_, err := f.List(context.Background(), "sub")
	require.NoError(t, err)
	_, err = f.List(context.Background(), "dst")
	require.NoError(t, err)

	err = f.DirMove(context.Background(), f, "sub", "dst/moved")
	assert.True(t, errors.Is(err, fs.ErrorCantDirMove), "must decline so the subtree is copied: %v", err)

	_, ok := f.srvPathOf.get("D2")
	assert.False(t, ok, "the refused destination path must be dropped")
	assert.Len(t, tr.dirs["D2"].catalogs, 0, "a refused task moves nothing")
	for _, c := range tr.calls {
		assert.NotContains(t, c, "modifyGroupContent", "a declined directory move must not rename: %s", c)
	}
}

// TestUnitGroupSensitiveWordMkdirDeclinesOnce covers the refusal the first
// full sync produced: a directory name the group's filter rejects. The same
// name is refused every time, so retrying the files below it only repeats the
// answer - and the migration's verify step needs the subtree named in a SKIP
// line or it reports those files as missing from the destination.
func TestUnitGroupSensitiveWordMkdirDeclinesOnce(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), sensitiveOnCreate: true}
	f := newGroupTestFs(tr)

	name := "blocked-name"
	err := f.Mkdir(context.Background(), name)
	require.Error(t, err)
	assert.True(t, fserrors.IsNoRetryError(err), "a refused name must stop the retry ladder: %v", err)
	assert.Contains(t, err.Error(), groupSensitiveWordErrorCode)

	// The executor turns the SKIP line into an --exclude pattern with
	//   grep 'SKIP ' copy.log | sed -E 's/.*SKIP ([^:]+):.*/\1/'
	// so the line this backend emits has to survive that extractor verbatim.
	line := "NOTICE: " + groupSensitiveSkipMessage(name)
	m := regexp.MustCompile(`.*SKIP ([^:]+):.*`).FindStringSubmatch(line)
	require.Len(t, m, 2, "the SKIP line must match the executor's extractor: %q", line)
	assert.Equal(t, "blocked-name/", m[1])
}

// TestUnitEmptyFileNamesItselfInASkipLine covers what the bucket runs showed:
// a zero-byte file cannot be stored, the sentinel is already a NoRetryError,
// and verify reports it missing unless the copy log carries a SKIP line the
// executor can turn into an --exclude pattern.
func TestUnitEmptyFileNamesItselfInASkipLine(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs()}
	f := newGroupTestFs(tr)

	src := fsobject.NewStaticObjectInfo("empty.txt", time.Now(), 0, true, nil, nil)
	_, err := f.Put(context.Background(), strings.NewReader(""), src)
	require.Error(t, err)
	assert.Equal(t, fs.ErrorCantUploadEmptyFiles, err, "the engine's sentinel must survive unchanged")
	assert.True(t, fserrors.IsNoRetryError(err))

	// Replay the executor's extractor on the line the backend emits.
	line := "NOTICE: " + fmt.Sprintf("SKIP %s: this remote cannot store empty files; skipping", "empty.txt")
	m := regexp.MustCompile(`.*SKIP ([^:]+):.*`).FindStringSubmatch(line)
	require.Len(t, m, 2)
	assert.Equal(t, "empty.txt", m[1])
}

// TestUnitGroupSensitiveFileNameDeclinesOnce covers the file-name variant of
// the filter: the create endpoint answers 020101057 文件名包含敏感词, which is
// just as deterministic as the directory one and must stop the retry ladder
// and name the file for verify.
func TestUnitGroupSensitiveFileNameDeclinesOnce(t *testing.T) {
	tr := &groupTransport{dirs: gtestDirs(), createMode: "sensitive"}
	f := newGroupTestFs(tr)

	src := fsobject.NewStaticObjectInfo("ixQpUqSq_51cg.apk", time.Now(), 4, true, nil, nil)
	_, err := f.Put(context.Background(), strings.NewReader("data"), src)
	require.Error(t, err)
	assert.True(t, fserrors.IsNoRetryError(err), "a refused file name must stop the retry ladder: %v", err)
	assert.Contains(t, err.Error(), sensitiveFileNameErrorCode)

	line := "NOTICE: " + fmt.Sprintf("SKIP %s: the server rejects this file name as a sensitive word (%s); skipping", src.Remote(), sensitiveFileNameErrorCode)
	m := regexp.MustCompile(`.*SKIP ([^:]+):.*`).FindStringSubmatch(line)
	require.Len(t, m, 2)
	assert.Equal(t, "ixQpUqSq_51cg.apk", m[1])
}
