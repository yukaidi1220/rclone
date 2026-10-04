package yun139

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// moveTransport answers the personal-cloud endpoints Fs.Move touches, and
// records the call sequence so a test can tell a rename from a batchMove.
type moveTransport struct {
	mu    sync.Mutex
	calls []string

	// batchMoveTo records the toParentFileId of the last batchMove, so a
	// test can tell the destination parent from the source parent.
	batchMoveTo string

	// creates records every /hcy/file/create (folder creation) request,
	// so a test can assert a move never creates directories.
	creates []string

	// entries maps a parent dir id to the listing it returns.
	entries map[string][]map[string]any

	// afterRename replaces the listing entry the destination dir returns
	// once a rename has been issued, so a test can model the server
	// keeping the pre-existing entry instead of the moved file.
	afterRename map[string][]map[string]any

	// copied flips once a batchCopy has been issued; copyID is the id the
	// copy task echoes, and noCopyEcho models a task detail that omits it.
	copied      bool
	copyID      string
	copyLandsAs string
	noCopyEcho  bool
	batchCopyTo string

	// hideAfterRename makes the next N listings of the destination omit
	// the entry once a rename has been issued, modelling a rename the
	// server has accepted but not yet made visible.
	hideAfterRename int

	// autoRename models /hcy/file/create finding the requested leaf held:
	// the create reply reports the suffixed name the server chose, and the
	// listing keeps the held entry until a rename has been issued.
	autoRename bool

	// renameFailID makes the rename of that id answer a business error, so
	// a test can exercise the restore path.
	renameFailID string

	// renameParksAs maps a requested rename name to the name the server
	// actually stored. A rename onto a name the directory holds keeps the
	// existing entry and parks the renamed one under a suffixed name, which
	// the reply reports.
	renameParksAs map[string]string

	// renameAppliesEntry makes a successful rename update the listing, so a
	// later lookup by id sees the entry under the name it was given.
	renameAppliesEntry bool

	// renameFailsButApplies makes a rename of renameFailID answer a business
	// error while still storing the entry under the requested name: the server
	// reports failure but the rename took effect.
	renameFailsButApplies bool

	// createOmitsFileName makes /hcy/file/create leave the fileName field out
	// of its reply, which the caller must handle by locating the entry by id
	// rather than assuming the requested name.
	createOmitsFileName bool

	// omitsRenameName makes the rename reply leave the name field out: the
	// server stored the entry somewhere, but the reply does not say where.
	omitsRenameName bool

	// createReturnsParts makes /hcy/file/create answer with part upload URLs,
	// so the upload proceeds to the part PUTs instead of taking the
	// rapidUpload path.
	createReturnsParts bool

	// partUploadFails makes every part PUT answer a server error, so an
	// upload fails after the create call has already registered an entry.
	partUploadFails bool

	// createReportsID overrides the id the create reply reports, so a test
	// can model a rapidUpload that deduplicated against an existing entry.
	createReportsID string

	// createdID/createdName/createdParent record the entry a create call
	// registered, so listings can show what the server stored.
	createdID     string
	createdName   string
	createdParent string

	renamed bool
}

// failingReader fails every Read, so an upload built on it cannot complete.
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("simulated source failure")
}

// renameEntry applies a rename to the mock's listing: the entry with id is
// re-named, so a lookup by id afterwards reports where the server put it.
func (t *moveTransport) renameEntry(id, name string) {
	for parent, items := range t.entries {
		for i, it := range items {
			if it["fileId"] == id {
				items[i]["name"] = name
				t.entries[parent] = items
			}
		}
	}
	if t.createdID == id {
		t.createdName = name
	}
}

// entryNames returns the names the mock currently holds under parent, so a
// test can assert what the server shows after a flow.
func (t *moveTransport) entryNames(parent string) []string {
	var names []string
	for _, it := range t.entries[parent] {
		if n, ok := it["name"].(string); ok {
			names = append(names, n)
		}
	}
	return names
}

func (t *moveTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	t.mu.Lock()
	defer t.mu.Unlock()

	reply := func(v any) (*http.Response, error) {
		b, _ := json.Marshal(v)
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(string(b))),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
		}, nil
	}

	switch {
	case r.URL.Host == "upload-test.yun.139.com":
		t.calls = append(t.calls, "partPUT:"+r.URL.Path)
		if t.partUploadFails {
			b, _ := json.Marshal(map[string]any{"success": false, "code": "9999", "message": "系统异常"})
			return &http.Response{
				StatusCode: http.StatusInternalServerError,
				Status:     "500 Internal Server Error",
				Body:       io.NopCloser(strings.NewReader(string(b))),
				Header:     http.Header{"Content-Type": []string{"application/json"}},
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader("")),
			Header:     http.Header{},
		}, nil

	case strings.HasSuffix(r.URL.Path, "/hcy/file/list"):
		var req struct {
			ParentFileID string `json:"parentFileId"`
		}
		_ = json.Unmarshal(body, &req)
		t.calls = append(t.calls, "list:"+req.ParentFileID)
		items := t.entries[req.ParentFileID]
		if t.copied && t.copyID != "" {
			items = append(append([]map[string]any{}, items...), fileEntry(t.copyID, t.copyLandsAs, 4))
		}
		if t.createdID != "" && t.createdParent == req.ParentFileID {
			// The entry the create call registered is visible under the name
			// the server gave it, whatever that turned out to be.
			items = append(append([]map[string]any{}, items...), fileEntry(t.createdID, t.createdName, 4))
		}
		if t.renamed && t.hideAfterRename > 0 {
			// The rename landed but the server has not made it visible
			// yet: the entry is missing from this listing.
			t.hideAfterRename--
			items = nil
		} else if t.renamed && t.afterRename != nil {
			if alt, ok := t.afterRename[req.ParentFileID]; ok {
				items = alt
			}
		}
		return reply(map[string]any{
			"success": true,
			"data":    map[string]any{"items": items},
		})

	case strings.HasSuffix(r.URL.Path, "/hcy/file/update"):
		var req struct {
			FileID string `json:"fileId"`
			Name   string `json:"name"`
		}
		_ = json.Unmarshal(body, &req)
		if t.renameFailID != "" && req.FileID == t.renameFailID {
			t.calls = append(t.calls, "rename-rejected:"+req.FileID)
			if t.renameFailsButApplies {
				// The rename took effect even though the reply reports a
				// business error. Only this first rename fails: the retry
				// that puts the entry back must be able to land.
				t.renamed = true
				t.renameEntry(req.FileID, req.Name)
				t.renameFailID = ""
				return reply(map[string]any{"success": false, "code": "02010501", "message": "请求不合法"})
			}
			return reply(map[string]any{"success": false, "code": "02010501", "message": "请求不合法"})
		}
		t.calls = append(t.calls, "rename:"+req.FileID+"->"+req.Name)
		t.renamed = true
		// A rename onto a held name is answered with success, but the reply
		// reports the suffixed name the server stored the entry under.
		stored := req.Name
		if parked, ok := t.renameParksAs[req.Name]; ok {
			stored = parked
		}
		if t.renameAppliesEntry {
			t.renameEntry(req.FileID, stored)
		}
		data := map[string]any{"name": stored}
		if t.omitsRenameName {
			delete(data, "name")
		}
		return reply(map[string]any{"success": true, "data": data})

	case strings.HasSuffix(r.URL.Path, "/hcy/file/batchMove"):
		var req struct {
			ToParentFileID string `json:"toParentFileId"`
		}
		_ = json.Unmarshal(body, &req)
		t.calls = append(t.calls, "batchMove")
		t.batchMoveTo = req.ToParentFileID
		return reply(map[string]any{"success": true, "data": map[string]any{"taskId": "task-1"}})

	case strings.HasSuffix(r.URL.Path, "/hcy/file/create"):
		var req struct {
			Name         string `json:"name"`
			ParentFileID string `json:"parentFileId"`
		}
		_ = json.Unmarshal(body, &req)
		t.calls = append(t.calls, "create:"+req.Name)
		t.creates = append(t.creates, req.Name)
		name := req.Name
		if t.autoRename {
			// The server never overwrites: it stores the content under a
			// suffixed name and reports that name back.
			name = req.Name + "_20260925_000000"
		}
		t.createdID = "created-" + name
		if t.createReportsID != "" {
			t.createdID = t.createReportsID
		}
		t.createdName = name
		t.createdParent = req.ParentFileID
		data := map[string]any{"fileId": t.createdID, "fileName": name}
		if t.createOmitsFileName {
			// Older server builds leave the field out; the entry is only
			// findable through a listing.
			delete(data, "fileName")
		}
		if t.createReturnsParts {
			// The upload proceeds through the part PUTs; give it one URL.
			data["uploadId"] = "upload-1"
			data["partInfos"] = []map[string]any{
				{"partNumber": 1, "uploadUrl": "https://upload-test.yun.139.com/part1"},
			}
		}
		return reply(map[string]any{"success": true, "data": data})

	case strings.HasSuffix(r.URL.Path, "/hcy/file/batchCopy"):
		var req struct {
			FileIDs        []string `json:"fileIds"`
			ToParentFileID string   `json:"toParentFileId"`
		}
		_ = json.Unmarshal(body, &req)
		t.calls = append(t.calls, "batchCopy")
		t.batchCopyTo = req.ToParentFileID
		t.copied = true
		return reply(map[string]any{"success": true, "data": map[string]any{"taskId": "task-copy"}})

	case strings.HasSuffix(r.URL.Path, "/hcy/task/get"):
		t.calls = append(t.calls, "taskGet")
		// The copy's id is echoed in data.batchFileResults[].rstFile.fileId.
		rst := t.copyID
		if t.noCopyEcho {
			rst = ""
		}
		return reply(map[string]any{
			"success": true,
			"data": map[string]any{
				"taskInfo": map[string]any{"taskId": "task-1", "status": "Succeed"},
				"batchFileResults": []map[string]any{
					{
						"errCode": "0000",
						"fileId":  "id-a",
						"rstFile": map[string]any{"fileId": rst},
					},
				},
			},
		})

	case strings.HasSuffix(r.URL.Path, "/hcy/recyclebin/batchTrash"), strings.HasSuffix(r.URL.Path, "/hcy/file/batchDelete"):
		var req struct {
			FileIDs []string `json:"fileIds"`
		}
		_ = json.Unmarshal(body, &req)
		for _, id := range req.FileIDs {
			t.calls = append(t.calls, "delete:"+id)
		}
		// No taskId: the delete completes synchronously, so there is
		// nothing to poll.
		return reply(map[string]any{"success": true})
	}
	return nil, fmt.Errorf("moveTransport: unexpected path %s", r.URL.Path)
}

// newMoveTestFs builds an Fs whose dircache root is already resolved, so
// Move stays offline. personalHost is preset to keep the route-policy
// lookup out of the call sequence.
func newMoveTestFs(t http.RoundTripper) *Fs {
	return newMoveTestFsEnc(t, encoder.Standard|encoder.EncodeInvalidUtf8)
}

// newMoveTestFsEnc builds the same test Fs with a chosen encoder. Use an
// encoder that REWRITES a rune (EncodeQuestion) when the assertion depends on
// which domain a name is built in - Standard alone is a no-op on such leaves
// and hides an encode-twice bug.
func newMoveTestFsEnc(t http.RoundTripper, enc encoder.MultiEncoder) *Fs {
	f := &Fs{
		name:         "test",
		opt:          Options{Enc: enc, UploadConcurrency: 4},
		space:        spacePersonal,
		svcType:      svcTypePersonal,
		account:      "13800138000",
		auth:         "dG9rZW4=",
		ts:           &tokenState{auth: "dG9rZW4="},
		tokMu:        &sync.Mutex{},
		hostMu:       &sync.Mutex{},
		personalHost: "https://personal-test.yun.139.com",
		httpClient:   &http.Client{Transport: t},
		pacer:        fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Millisecond))),
	}
	f.dirCache = dircache.New("", "root-id", f)
	f.srvPathOf = newSrvPathCache()
	return f
}

func fileEntry(id, name string, size int64) map[string]any {
	return map[string]any{
		"fileId": id,
		"name":   name,
		"size":   size,
		"type":   "file",
	}
}

func dirEntry(id, name string) map[string]any {
	return map[string]any{
		"fileId": id,
		"name":   name,
		"type":   "folder",
	}
}

func newTestObject(f *Fs, remote, id string, size int64) *Object {
	return &Object{
		fs:     f,
		remote: remote,
		id:     id,
		size:   size,
		hashMu: &sync.Mutex{},
		urlMu:  &sync.Mutex{},
	}
}

// TestUnitMoveSameDirectoryRenames checks that a move inside one directory
// is issued as a rename: the server rejects a batchMove to the directory
// the file already lives in with '04010317'.
func TestUnitMoveSameDirectoryRenames(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-a", "one.txt", 4)},
		},
		// After the rename the same id holds the new name.
		afterRename: map[string][]map[string]any{
			"root-id": {fileEntry("id-a", "one-renamed.txt", 4)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "one.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "one-renamed.txt")
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Contains(t, tr.calls, "rename:id-a->one-renamed.txt", "same-directory move must rename")
	for _, c := range tr.calls {
		assert.NotEqual(t, "batchMove", c, "same-directory move must not issue a batchMove")
	}
}

// TestUnitMoveSameLeafIsNoOp checks that a move onto the identical path
// issues no server call at all.
func TestUnitMoveSameLeafIsNoOp(t *testing.T) {
	tr := &moveTransport{}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "one.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "one.txt")
	require.NoError(t, err)
	assert.Same(t, src, got, "a no-op move returns the source object")
	assert.Empty(t, tr.calls, "a no-op move must not touch the server")
}

// TestUnitMoveSameDirectoryRenameRetriesUntilVisible covers the visibility
// race on the personal same-directory rename: the rename is accepted but the
// entry is missing from the listing for a moment, so a single read would
// report a successful rename as a failed move. The read must be retried.
func TestUnitMoveSameDirectoryRenameRetriesUntilVisible(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-a", "four.txt", 4)},
		},
		// Once the rename is issued the entry carries the new name; the
		// first two listings still omit it.
		afterRename: map[string][]map[string]any{
			"root-id": {fileEntry("id-a", "four-new.txt", 4)},
		},
		hideAfterRename: 2,
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "four.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "four-new.txt")
	require.NoError(t, err, "a rename that has not become visible must be retried, not failed")
	require.NotNil(t, got)
	assert.Equal(t, "four-new.txt", got.Remote())
	assert.Contains(t, tr.calls, "rename:id-a->four-new.txt")
}

// TestUnitMoveSameDirectoryRenameVisibilityExhaustedRefuses checks that the
// retry is bounded: an entry that never becomes visible is reported rather
// than waited on forever.
func TestUnitMoveSameDirectoryRenameVisibilityExhaustedRefuses(t *testing.T) {
	old := copyVisibilityTimeout
	copyVisibilityTimeout = 50 * time.Millisecond
	defer func() { copyVisibilityTimeout = old }()
	tr := &moveTransport{
		entries:         map[string][]map[string]any{"root-id": {}},
		hideAfterRename: 1000,
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "four.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "four-new.txt")
	require.Error(t, err, "an entry that never becomes visible must be reported")
	assert.Nil(t, got)
}

func TestUnitMoveAcrossDirectoriesUsesTask(t *testing.T) {
	tr := &moveTransport{entries: map[string][]map[string]any{
		"dir-id": {fileEntry("id-a", "two.txt", 4)},
	}}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("sub", "dir-id")

	src := newTestObject(f, "two.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "sub/two.txt")
	require.NoError(t, err)
	require.NotNil(t, got)

	assert.Contains(t, tr.calls, "batchMove", "cross-directory move uses the task")
	assert.NotContains(t, tr.calls, "rename:id-a->two.txt", "the leaf is unchanged, so no rename")
}

// TestUnitMoveOntoExistingNameFails checks the non-overwrite rule: the
// server keeps the pre-existing entry and parks the moved file under a
// suffixed name, so the object at the destination is not the source. Move
// must report that instead of handing back the wrong object.
func TestUnitMoveOntoExistingNameFails(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			// Same size, different id: only the id distinguishes the moved
			// file from the pre-existing entry, so a size comparison would
			// not catch the non-overwrite case.
			"root-id": {fileEntry("id-a", "four.txt", 4), fileEntry("id-b", "four-target.txt", 4)},
		},
		// After the rename the destination still holds the original entry.
		afterRename: map[string][]map[string]any{
			"root-id": {fileEntry("id-a-renamed", "four_20260924.txt", 4), fileEntry("id-b", "four-target.txt", 4)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "four.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "four-target.txt")
	require.Error(t, err, "a move that did not land must not report success")
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "four-target.txt")
}

// TestUnitMoveAcrossDirsOntoExistingNameFails checks the same non-overwrite
// rule on the cross-directory branch. The source parent resolves to a
// directory of this Fs, so the batchMove path is taken.
func TestUnitMoveAcrossDirsOntoExistingNameFails(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"src-dir": {},
			"dir-id":  {fileEntry("id-b", "six.txt", 10)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("from", "src-dir")
	f.dirCache.Put("sub", "dir-id")

	src := newTestObject(f, "from/six-src.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "sub/six.txt")
	require.Error(t, err, "the destination still holds the old entry")
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "six.txt")
	assert.Contains(t, tr.calls, "batchMove", "the cross-directory branch must be the one under test")
}

// TestUnitCopyOntoTakenNameFallsBack checks that a copy whose destination
// name is already taken refuses the server-side path: the server never
// overwrites, so a batch copy would be auto-renamed and could not be given
// the requested leaf. ErrorCantCopy makes the engine fall back to a
// bandwidth copy, whose Update path replaces the entry losslessly.
func TestUnitCopyOntoTakenNameFallsBack(t *testing.T) {
	tr := &moveTransport{
		copyID:      "id-copy",
		copyLandsAs: "two.txt",
		entries: map[string][]map[string]any{
			// The destination already holds an entry under the target name.
			"dir-id": {fileEntry("id-old", "two.txt", 4)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("sub", "dir-id")

	src := newTestObject(f, "two.txt", "id-a", 4)
	got, err := f.Copy(context.Background(), src, "sub/two.txt")
	require.ErrorIs(t, err, fs.ErrorCantCopy, "a taken destination name must fall back to a bandwidth copy")
	assert.Nil(t, got)
	assert.NotContains(t, tr.calls, "batchCopy", "nothing may be created before the fallback")
}

// TestUnitCopyToFreeNameUsesServerSideCopy checks the ordinary copy: the
// destination name is free, so the batch copy runs and the copy is resolved
// by the id the task echoes.
func TestUnitCopyToFreeNameUsesServerSideCopy(t *testing.T) {
	tr := &moveTransport{
		copyID:      "id-copy",
		copyLandsAs: "two.txt",
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("sub", "dir-id")

	src := newTestObject(f, "src.txt", "id-a", 4)
	got, err := f.Copy(context.Background(), src, "sub/two.txt")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "id-copy", got.(*Object).id, "the copy is the entry the task created")
	assert.Equal(t, "sub/two.txt", got.Remote(), "the copy carries the destination directory")
	assert.Contains(t, tr.calls, "batchCopy")
}

// TestUnitCopyNoEchoedIDFails checks that a copy task which does not report
// the id it created fails instead of reporting success: with no id there is
// nothing that identifies the entry the server made.
func TestUnitCopyNoEchoedIDFails(t *testing.T) {
	tr := &moveTransport{noCopyEcho: true}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("sub", "dir-id")

	src := newTestObject(f, "two.txt", "id-a", 4)
	got, err := f.Copy(context.Background(), src, "sub/three.txt")
	require.Error(t, err, "a copy task with no id echo must not report success")
	assert.Nil(t, got)
	assert.Contains(t, tr.calls, "batchCopy", "the no-echo path must be the one under test")
	assert.NotContains(t, tr.calls, "rename:", "an entry under another name is not the copy")
}

// TestUnitResolveCopyLeafNoIDDoesNotGuess covers resolveCopyLeaf's contract
// when the task detail carries no id: the destination holds an unrelated entry
// under the requested leaf, so resolving by name would hand that entry back as
// if the copy had landed. A missing id must be reported instead.
func TestUnitResolveCopyLeafNoIDDoesNotGuess(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"dir-id": {fileEntry("id-other", "three.txt", 4)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	got, err := f.resolveCopyLeaf(context.Background(), "dir-id", "sub/three.txt", "id-src", "")
	require.Error(t, err, "a copy with no id must not be resolved by name")
	assert.Nil(t, got)
	assert.NotContains(t, tr.calls, "rename:", "the unrelated entry must not be renamed into place")
}

// TestUnitCopyRemovesCopyWhenResolveFails checks that a copy which cannot be
// given the requested name is removed again, so a failed copy does not leave
// the entry the server created behind under a name the caller never asked
// for.
func TestUnitCopyRemovesCopyWhenResolveFails(t *testing.T) {
	tr := &moveTransport{
		copyID: "id-copy",
		// The server parked the copy under a suffixed name, so it can
		// never carry the requested leaf.
		copyLandsAs: "two_20260925.txt",
		entries:     map[string][]map[string]any{"dir-id": {}},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("sub", "dir-id")

	src := newTestObject(f, "src.txt", "id-a", 4)
	got, err := f.Copy(context.Background(), src, "sub/two.txt")
	require.Error(t, err, "a copy that did not land must not report success")
	assert.Nil(t, got)
	assert.Contains(t, tr.calls, "delete:id-copy", "the unwanted copy must be removed")
}

// TestUnitMoveCrossRootIsNotARename covers a move between two roots of the
// same remote name (`rclone moveto remote:a/x.txt remote:b/x.txt`): the
// source's remote path is relative to the SOURCE root, so resolving it with
// the destination Fs's dircache would map it onto the destination root and
// mistake the move for a rename in place. The batchMove path must be used.
func TestUnitMoveCrossRootIsNotARename(t *testing.T) {
	tr := &moveTransport{entries: map[string][]map[string]any{
		"dir-b": {fileEntry("id-x", "x.txt", 5)},
	}}

	dst := newMoveTestFs(tr)
	dst.root = "b"
	dst.dirCache = dircache.New("b", "dir-b", dst)
	dst.dirCache.Put("b", "dir-b")

	srcFs := newMoveTestFs(tr)
	srcFs.root = "a"
	srcFs.dirCache = dircache.New("a", "dir-a", srcFs)
	srcFs.dirCache.Put("a", "dir-a")

	src := newTestObject(srcFs, "x.txt", "id-x", 5)

	got, err := dst.Move(context.Background(), src, "x.txt")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Contains(t, tr.calls, "batchMove", "a cross-root move is not a rename in place")
	assert.Contains(t, tr.calls, "list:dir-b", "the destination must be re-read")
	assert.Equal(t, "id-x", got.(*Object).id)
}

// TestUnitMoveCrossRootNewLeafMovesParent covers the cross-root move with a
// changed leaf: the parent directory must change, not just the name.
func TestUnitMoveCrossRootNewLeafMovesParent(t *testing.T) {
	tr := &moveTransport{entries: map[string][]map[string]any{
		"dir-b": {fileEntry("id-x", "y.txt", 5)},
	}}

	dst := newMoveTestFs(tr)
	dst.root = "b"
	dst.dirCache = dircache.New("b", "dir-b", dst)
	dst.dirCache.Put("b", "dir-b")

	srcFs := newMoveTestFs(tr)
	srcFs.root = "a"
	srcFs.dirCache = dircache.New("a", "dir-a", srcFs)
	srcFs.dirCache.Put("a", "dir-a")

	src := newTestObject(srcFs, "x.txt", "id-x", 5)

	got, err := dst.Move(context.Background(), src, "y.txt")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Contains(t, tr.calls, "batchMove", "the file must change parent, not just name")
	assert.Contains(t, tr.calls, "rename:id-x->y.txt", "the leaf change is applied on top of the move")
	assert.Equal(t, "y.txt", got.Remote())
}

// TestUnitMoveUnresolvableSourceDirFallsThrough checks that a source whose
// parent directory this dircache never listed (a move driven from another
// Fs of the same account) does not fail the move: the same-directory case
// is simply undecidable, so the batchMove path is used.
func TestUnitMoveUnresolvableSourceDirFallsThrough(t *testing.T) {
	tr := &moveTransport{entries: map[string][]map[string]any{
		"dir-id": {fileEntry("id-a", "two.txt", 4)},
	}}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("sub", "dir-id")

	// "ghost" is not in the dircache and the root listing does not have it,
	// so FindDir("ghost", false) fails with ErrorDirNotFound.
	src := newTestObject(f, "ghost/two.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "sub/two.txt")
	require.NoError(t, err, "an unresolvable source dir must not fail the move")
	require.NotNil(t, got)
	assert.Contains(t, tr.calls, "batchMove")
}

// TestUnitMoveRejectsForeignFs checks that a source object from an unrelated
// account is refused so the engine falls back to copy+delete. Both Fs are
// otherwise identical - same root, same resolved dircache - so only the
// account check can reject the move.
func TestUnitMoveRejectsForeignFs(t *testing.T) {
	tr := &moveTransport{}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	other := newMoveTestFs(tr)
	other.account = "13900139000"
	require.NoError(t, other.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(other, "one.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "one.txt")
	assert.ErrorIs(t, err, fs.ErrorCantMove)
	assert.Nil(t, got)
	assert.Empty(t, tr.calls, "a foreign object must be rejected before any server call")
}

// TestUnitMoveRejectsDifferentSpace checks the other half of sameCloud: the
// same account but a different space (personal vs family) must not share
// ids, so the move is refused even though the dircache resolves and the
// leaf matches.
func TestUnitMoveRejectsDifferentSpace(t *testing.T) {
	tr := &moveTransport{}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	other := newMoveTestFs(tr)
	other.space = spaceFamily
	require.NoError(t, other.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(other, "one.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "one.txt")
	assert.ErrorIs(t, err, fs.ErrorCantMove)
	assert.Nil(t, got)
	assert.Empty(t, tr.calls, "a different space must be rejected before any server call")
}

// TestUnitMoveDoesNotCreateSourceParent checks that resolving the source
// parent for the same-directory test is a read-only lookup: a move must
// never create the source's directory as a side effect.
func TestUnitMoveDoesNotCreateSourceParent(t *testing.T) {
	tr := &moveTransport{entries: map[string][]map[string]any{
		// The moved file lands at the root under the source's id.
		"root-id": {fileEntry("id-a", "two.txt", 4)},
	}}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	// "ghost" is absent from the root listing, so a create=true lookup
	// would issue /hcy/file/create for it.
	src := newTestObject(f, "ghost/two.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "two.txt")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Empty(t, tr.creates, "resolving the source parent must not create it")
}

// TestUnitMoveMovesToDestinationParent checks that the batch-move targets
// the destination directory, not the source's: with two sibling
// directories, moving between them must send the destination id.
func TestUnitMoveMovesToDestinationParent(t *testing.T) {
	tr := &moveTransport{entries: map[string][]map[string]any{
		"dir-a": {fileEntry("id-a", "two.txt", 4)},
		// After the move the destination holds the file under its id.
		"dir-b": {fileEntry("id-a", "two.txt", 4)},
	}}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("a", "dir-a")
	f.dirCache.Put("b", "dir-b")

	src := newTestObject(f, "a/two.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "b/two.txt")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Contains(t, tr.calls, "batchMove")
	assert.Equal(t, "dir-b", tr.batchMoveTo, "the move must target the destination directory")
	assert.NotEqual(t, "dir-a", tr.batchMoveTo, "the source directory must not be the target")
}

// TestUnitMoveCrossDirsComparesIDNotSize checks that the non-overwrite
// detection on the cross-directory branch keys on the object id: a
// same-size different-id entry at the destination must still be reported
// as a move that did not land.
func TestUnitMoveCrossDirsComparesIDNotSize(t *testing.T) {
	tr := &moveTransport{entries: map[string][]map[string]any{
		"src-dir": {},
		// Same size as the source, different id.
		"dir-id": {fileEntry("id-b", "six.txt", 4)},
	}}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("from", "src-dir")
	f.dirCache.Put("sub", "dir-id")

	src := newTestObject(f, "from/six-src.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "sub/six.txt")
	require.Error(t, err, "a same-size different-id destination is still a different file")
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "six.txt")
	require.Contains(t, tr.calls, "batchMove")
}

// TestUnitMoveSameDirComparesIDNotSize checks the same rule on the rename
// branch: a same-size different-id entry must be reported as not landing.
func TestUnitMoveSameDirComparesIDNotSize(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-a", "one.txt", 4), fileEntry("id-b", "one-target.txt", 4)},
		},
		afterRename: map[string][]map[string]any{
			"root-id": {fileEntry("id-a-renamed", "one_20260924.txt", 4), fileEntry("id-b", "one-target.txt", 4)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "one.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "one-target.txt")
	require.Error(t, err, "a same-size different-id destination is still a different file")
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "one-target.txt")
}

// TestUnitMoveSameLeafIsNoOpCaseSensitive checks that the no-op shortcut
// compares the leaf exactly: a case-only rename must reach the server,
// because a case-insensitive space would otherwise drop the rename.
func TestUnitMoveSameLeafIsNoOpCaseSensitive(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-a", "one.txt", 4)},
		},
		afterRename: map[string][]map[string]any{
			"root-id": {fileEntry("id-a", "ONE.txt", 4)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "one.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "ONE.txt")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Contains(t, tr.calls, "rename:id-a->ONE.txt", "a case-only rename must reach the server")
}

// TestUnitMoveSameDirectoryRenameFailurePropagates checks that a failed
// rename is surfaced rather than swallowed.
func TestUnitMoveSameDirectoryRenameFailurePropagates(t *testing.T) {
	tr := &failingRenameTransport{moveTransport: moveTransport{}}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "one.txt", "id-a", 4)
	got, err := f.Move(context.Background(), src, "one-renamed.txt")
	require.Error(t, err)
	assert.Nil(t, got)
	assert.False(t, fserrors.IsNoRetryError(err), "a server rejection is not a retryable transport error")
}

// TestUnitSettleAutoRenameParksHeldEntry covers the contract of
// settleAutoRename: when the server stored the upload under a suffixed name
// because the requested leaf was held, the helper must park the held entry,
// rename the upload into place, and delete the parked entry - in that order.
// Nothing may be left under a name the caller never asked for.
func TestUnitSettleAutoRenameParksHeldEntry(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	require.NoError(t, f.settleAutoRename(context.Background(), "root-id", "x.txt", "created-x.txt"))

	require.True(t, hasPrefixIn(tr.calls, "rename:id-held->x-rclone-old-"), "the held entry must be parked")
	var park, install string
	for _, c := range tr.calls {
		switch {
		case strings.HasPrefix(c, "rename:id-held->"):
			park = c
		case strings.HasPrefix(c, "rename:created-x.txt->"):
			install = c
		}
	}
	require.NotEmpty(t, park, "the held entry must be renamed away")
	assert.Contains(t, park, "x-rclone-old-", "the held entry goes under a backup name")
	assert.Equal(t, "rename:created-x.txt->x.txt", install, "the upload takes the requested leaf")
	assert.Contains(t, tr.calls, "delete:id-held", "the parked entry must be removed once the upload is in place")
	assert.Less(t, indexOf(tr.calls, park), indexOf(tr.calls, install),
		"the held entry must be parked before the upload is renamed into place")
}

// TestUnitSettleAutoRenameRestoresHeldEntryWhenInstallFails covers the failure
// path: if the upload cannot be renamed into place, the held entry must be put
// back under its own name and the upload removed, so the leaf keeps its
// previous content and no stray entry is left behind.
func TestUnitSettleAutoRenameRestoresHeldEntryWhenInstallFails(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
		renameFailID: "created-x.txt",
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	err := f.settleAutoRename(context.Background(), "root-id", "x.txt", "created-x.txt")
	require.Error(t, err, "an upload that cannot take its name must fail the Put")

	assert.True(t, hasPrefixIn(tr.calls, "rename:id-held->x-rclone-old-"), "the held entry is parked first")
	assert.Contains(t, tr.calls, "rename:id-held->x.txt",
		"the held entry must be restored under its own name")
	assert.Contains(t, tr.calls, "delete:created-x.txt",
		"the upload that could not take its name must be removed")
	assert.NotContains(t, tr.calls, "delete:id-held",
		"the held entry is the live file and must never be deleted")
}

// TestUnitSettleAutoRenameFailsWhenParkingFails covers the first failure path:
// if the held entry cannot be parked, nothing has changed yet, so the upload
// must be removed and the error reported - the leaf keeps its content.
func TestUnitSettleAutoRenameFailsWhenParkingFails(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
		renameFailID: "id-held",
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	err := f.settleAutoRename(context.Background(), "root-id", "x.txt", "created-x.txt")
	require.Error(t, err, "a held entry that cannot be parked must fail the Put")
	assert.Contains(t, tr.calls, "delete:created-x.txt", "the upload must be removed")
	assert.NotContains(t, tr.calls, "delete:id-held", "the held entry must survive")
}

// TestUnitPutLandsAutoRenamedUpload covers Put's guard against the server's
// never-overwrite rule: when /hcy/file/create reports the upload went to a
// suffixed name because the requested leaf was held, Put must move it to the
// requested path. Without that, the call reports success while the path keeps
// the previous content and the new content sits under a name nobody asked for.
func TestUnitPutLandsAutoRenamedUpload(t *testing.T) {
	tr := &moveTransport{
		autoRename: true,
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "x.txt", "id-src", 11)
	o, err := f.Put(context.Background(), strings.NewReader("NEWCONTENT!"), src)
	require.NoError(t, err, "Put must land the content at the requested path")
	require.NotNil(t, o)
	assert.Equal(t, "x.txt", o.Remote())

	// The server's reply named the upload "x.txt_20260925_000000"; it must
	// end up at "x.txt", with the entry that held the leaf removed.
	assert.Contains(t, tr.calls, "create:x.txt", "the upload is issued under the requested name")
	assert.True(t, hasPrefixIn(tr.calls, "rename:id-held->x-rclone-old-"),
		"the entry holding the leaf must be parked")
	assert.Contains(t, tr.calls, "rename:created-x.txt_20260925_000000->x.txt",
		"the auto-renamed upload must be moved to the requested leaf")
	assert.Contains(t, tr.calls, "delete:id-held", "the parked entry must be removed")
	assert.NotContains(t, tr.calls, "delete:created-x.txt_20260925_000000",
		"the upload is the live content and must not be deleted")
}

// TestUnitPutReportsFailureWhenSettleFails covers Put's error path: when the
// auto-renamed upload cannot be moved to the requested leaf, Put must report
// the failure rather than claim a success that did not happen.
func TestUnitPutReportsFailureWhenSettleFails(t *testing.T) {
	tr := &moveTransport{
		autoRename:   true,
		renameFailID: "id-held",
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "x.txt", "id-src", 11)
	o, err := f.Put(context.Background(), strings.NewReader("NEWCONTENT!"), src)
	require.Error(t, err, "an upload that cannot reach the requested path must fail")
	assert.Nil(t, o)
	assert.Contains(t, tr.calls, "delete:created-x.txt_20260925_000000",
		"the upload that could not take its name must be removed")
}

// TestUnitUpdateLandsRenamedUpload covers Update's handling of an upload the
// server stored under a suffixed name (something still held the leaf). Update
// must land it at the target name rather than report success with the old
// content in place - the same operation Put performs, and lossless because
// settleAutoRename only drops the entry it parked.
func TestUnitUpdateLandsRenamedUpload(t *testing.T) {
	tr := &moveTransport{
		autoRename: true,
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	o := newTestObject(f, "x.txt", "id-held", 11)
	require.NoError(t, o.Update(context.Background(), strings.NewReader("NEWCONTENT!"), newTestObject(f, "x.txt", "id-held", 11)),
		"an upload the server renamed must still be landed")

	assert.Contains(t, tr.calls, "rename:created-x.txt_20260925_000000->x.txt",
		"the renamed upload must be moved onto the target leaf")
	assert.NotContains(t, tr.calls, "delete:created-x.txt_20260925_000000",
		"the upload is the live content and must not be deleted")
	assert.Equal(t, "created-x.txt_20260925_000000", o.id, "the receiver must point at the uploaded object")
	assert.Equal(t, 1, countCalls(tr.calls, "delete:id-held"),
		"the parked entry must be deleted exactly once: settleAutoRename already removed it")
}

// TestUnitSettleAutoRenameInstallsWhenLeafFree covers settleAutoRename's
// no-held-entry branch: the server stored the upload under a suffixed name
// but nothing carries the requested leaf any more (the holder was removed,
// or the listing has not caught up). The upload must still be renamed onto
// the requested leaf; leaving it under the server's name would put content
// at a path the caller never asked for.
func TestUnitSettleAutoRenameInstallsWhenLeafFree(t *testing.T) {
	tr := &moveTransport{}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	require.NoError(t, f.settleAutoRename(context.Background(), "root-id", "x.txt", "created-x.txt"))

	assert.Contains(t, tr.calls, "rename:created-x.txt->x.txt",
		"an upload whose requested leaf is free must be renamed into place")
	assert.NotContains(t, tr.calls, "delete:created-x.txt",
		"the upload is the live content and must not be removed")
	assert.False(t, hasPrefixIn(tr.calls, "rename:id-held->"), "nothing holds the leaf to park")
}

// TestUnitSettleAutoRenameInstallsWhenListingShowsUpload covers the other
// arm of the same branch: the listing already shows the upload itself, so
// there is nothing to park and the rename still runs.
func TestUnitSettleAutoRenameInstallsWhenListingShowsUpload(t *testing.T) {
	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("created-x.txt", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	require.NoError(t, f.settleAutoRename(context.Background(), "root-id", "x.txt", "created-x.txt"))

	assert.Contains(t, tr.calls, "rename:created-x.txt->x.txt",
		"the upload must be renamed onto the requested leaf")
	assert.NotContains(t, tr.calls, "delete:created-x.txt",
		"the upload is the live content and must not be removed")
}

// TestUnitChunkWriterLandsAutoRenamedUpload covers the multi-thread copy
// path: OpenChunkWriter's Close runs the same upload pipeline as Put, so an
// upload the server stored under a suffixed name (the requested leaf was
// held) must still be landed at the requested path. The engine looks the
// object up by path once Close returns, so leaving it under the server's
// name would hand the engine the old entry, fail verification, and delete
// the destination the user already had.
func TestUnitChunkWriterLandsAutoRenamedUpload(t *testing.T) {
	tr := &moveTransport{
		autoRename: true,
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "big.bin", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "big.bin", "id-src", 16)
	_, w, err := f.OpenChunkWriter(context.Background(), "big.bin", src)
	require.NoError(t, err)
	_, err = w.WriteChunk(context.Background(), 0, strings.NewReader("NEWCONTENT!12345"))
	require.NoError(t, err)
	require.NoError(t, w.Close(context.Background()), "Close must land the upload at the requested path")

	assert.Contains(t, tr.calls, "rename:created-big.bin_20260925_000000->big.bin",
		"the auto-renamed upload must be moved onto the requested leaf")
	assert.True(t, hasPrefixIn(tr.calls, "rename:id-held->big-rclone-old-"),
		"the entry holding the leaf must be parked")
	assert.Contains(t, tr.calls, "delete:id-held", "the parked entry must be removed")
	assert.NotContains(t, tr.calls, "delete:created-big.bin_20260925_000000",
		"the upload is the live content and must not be deleted")
}

// TestUnitRenameOntoHeldNameFails covers the non-overwrite rule at the
// rename layer: a rename onto a name the directory already holds is answered
// with success, but the server stores the entry under a suffixed name. The
// caller must see an error rather than assume the requested name was taken.
func TestUnitRenameOntoHeldNameFails(t *testing.T) {
	tr := &moveTransport{
		renameParksAs: map[string]string{"x.txt": "x_20260925_000000.txt"},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	err := f.renameObject(context.Background(), "id-a", "x.txt", "root-id", false)
	require.Error(t, err, "a rename the server parked under another name must not report success")
	assert.Contains(t, err.Error(), "server stored it as")
}

// TestUnitRenameToOwnNameSucceeds guards the other side of the same rule: the
// reply echoes the requested name, so the check must not fire.
func TestUnitRenameToOwnNameSucceeds(t *testing.T) {
	tr := &moveTransport{}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	require.NoError(t, f.renameObject(context.Background(), "id-a", "x.txt", "root-id", false))
}

// TestUnitChunkWriterFailsWhenSettleCannotLand covers Close's error arm: when
// settleAutoRename cannot put the upload on the requested leaf, Close must
// return the error. Swallowing it lets the engine verify the object by path,
// find the pre-existing entry and delete the destination.
func TestUnitChunkWriterFailsWhenSettleCannotLand(t *testing.T) {
	tr := &moveTransport{
		autoRename:    true,
		renameParksAs: map[string]string{"big.bin": "big_20260925_000000.bin"},
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "big.bin", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "big.bin", "id-src", 16)
	_, w, err := f.OpenChunkWriter(context.Background(), "big.bin", src)
	require.NoError(t, err)
	_, err = w.WriteChunk(context.Background(), 0, strings.NewReader("NEWCONTENT!12345"))
	require.NoError(t, err)

	err = w.Close(context.Background())
	require.Error(t, err, "Close must report an upload it could not land")
	assert.Contains(t, err.Error(), "server stored it as")
	assert.NotContains(t, tr.calls, "delete:id-held",
		"a failed settle must not delete the entry holding the leaf")
}

// TestUnitBackupNameKeepsLeafExtension pins the shape of the name an entry is
// parked under while an update replaces it. The family space appends the
// renamed entry's extension to a requested name that does not already end with
// it, so a backup has to end with the leaf's extension (and a dotless leaf must
// be parked under a dotless name) - otherwise parking and restoring both get
// moved to a different name and the restore never lands.
func TestUnitBackupNameKeepsLeafExtension(t *testing.T) {
	for _, tc := range []struct {
		leaf    string
		wantExt string // suffix the backup must carry; "" means it must be dotless
	}{
		{"plain", ""},
		{"plain.txt", ".txt"},
		{"movie.mkv", ".mkv"},
		{"archive.tar.gz", ".gz"},
		{"a.bin", ".bin"},
		{".gitignore", ".gitignore"},
	} {
		t.Run(tc.leaf, func(t *testing.T) {
			backup := backupLeafName(tc.leaf)
			prefix := backupPrefix(tc.leaf)

			assert.NotEqual(t, tc.leaf, backup, "the backup must not be the leaf itself")
			assert.True(t, strings.HasPrefix(backup, prefix),
				"backup %q must start with the prefix %q cleanup matches on", backup, prefix)
			assert.Contains(t, backup, "rclone-old",
				"backup %q must be recognisable as ours", backup)

			if tc.wantExt == "" {
				assert.NotContains(t, backup, ".",
					"a dotless leaf must be parked under a dotless name, else the server moves it: %q", backup)
			} else {
				assert.True(t, strings.HasSuffix(backup, tc.wantExt),
					"backup %q must keep the leaf's extension %q, else the server appends it again",
					backup, tc.wantExt)
			}
		})
	}
}

// TestUnitBackupOfMatchesOneLeafOnly pins that a backup name is recognised for
// its own leaf and no other: a sibling whose name starts with the same text
// ("plain" vs "plain.txt") has backups of the same shape, so a prefix-only
// match would let the cleanup of one file delete the other's parked content.
func TestUnitBackupOfMatchesOneLeafOnly(t *testing.T) {
	for _, tc := range []struct {
		name, leaf string
		want       bool
	}{
		{"plain-rclone-old-xobiqoc5", "plain", true},
		{"plain-rclone-old-xobiqoc5.txt", "plain.txt", true},
		{"movie-rclone-old-xobiqoc5.mkv", "movie.mkv", true},
		{"archive.tar-rclone-old-xobiqoc5.gz", "archive.tar.gz", true},
		{"rclone-old-xobiqoc5.gitignore", ".gitignore", true},
		// A dotless leaf must not claim the backup of the sibling that
		// carries an extension, and vice versa.
		{"plain-rclone-old-xobiqoc5.txt", "plain", false},
		{"plain-rclone-old-xobiqoc5", "plain.txt", false},
		// Wrong marker, other leaf, or the leaf itself: never a backup.
		{"plain-rclone-old-xobiqoc5.gz", "plain.txt", false},
		{"plain-rclone-old-toolong01", "plain", false},
		{"plain-rclone-old-xobiqoc", "plain", false},
		{"other-rclone-old-xobiqoc5", "plain", false},
		{"plain", "plain", false},
		{"plain-rclone-old-xobiqoc5x", "plain", false},
		// The generator's pattern is consonant/vowel/.../digit, so a name
		// shaped like a marker but outside that pattern is a user file, not
		// a leftover of ours.
		{"plain-rclone-old-abcdefg1", "plain", false},
		{"plain-rclone-old-20240925", "plain", false},
		// The server cannot hold two names that differ only in case, so a
		// backup spelled differently from the requested leaf is still that
		// leaf's backup and has to be cleaned up. The fold is the one the
		// live probe measured: ASCII, and the Ü/ü pair too.
		{"Plain-rclone-old-xobiqoc5", "plain", true},
		{"plain-rclone-old-xobiqoc5", "Plain", true},
		{"Movie-rclone-old-xobiqoc5.MKV", "movie.mkv", true},
		{"Ünïcode-rclone-old-xobiqoc5.txt", "ünïcode.txt", true},
		// Folding must not merge two leaves that really are different.
		{"plain-rclone-old-xobiqoc5.txt", "plain", false},
		{"plain-rclone-old-xobiqoc5", "plain.txt", false},
		{"movie-rclone-old-xobiqoc5.mkv", "other.mkv", false},
	} {
		t.Run(tc.name+"/"+tc.leaf, func(t *testing.T) {
			assert.Equal(t, tc.want, isBackupOf(tc.name, tc.leaf))
		})
	}
}

// TestUnitSettleRefusesDirHeldLeaf covers the leaf a DIRECTORY holds: the
// settle scan must treat it as a taken name, so a rename under it is reported
// as a failure instead of landing at a path that is still a directory.
func TestUnitSettleRefusesDirHeldLeaf(t *testing.T) {
	tr := &moveTransport{
		autoRename: true,
		entries: map[string][]map[string]any{
			"root-id": {dirEntry("id-dir", "plain")},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	err := f.settleAutoRename(context.Background(), "root-id", "plain", "id-new")
	require.Error(t, err, "a leaf held by a directory cannot be landed")
	assert.Contains(t, err.Error(), "a directory holds that name")
	assert.Contains(t, tr.calls, "delete:id-new",
		"the upload that could not take the name must be dropped: %v", tr.calls)
	assert.NotContains(t, tr.calls, "rename:id-new->plain",
		"the upload must not be renamed onto a name a directory holds: %v", tr.calls)
}

// TestUnitCheckRenameLanded covers the reply-name rule directly. The name the
// reply reports is the only confirmation a rename landed, and the two failure
// directions are not symmetric: treating a park as landed leaves content under
// a path nobody asked for, while treating a landing as a park makes
// settleAutoRename delete an upload that is already the file's only copy. An
// absent name is therefore not evidence of a park.
//
// A name the server changed is not a landing in either space, including the
// family append: rclone records the requested path, so accepting a different
// stored name would leave every later lookup addressing a path the server
// does not hold.
func TestUnitCheckRenameLanded(t *testing.T) {
	cases := []struct {
		req, stored string
		wantErr     bool
	}{
		{"x.txt", "x.txt", false},
		{"plain", "plain", false},
		{"plain", "plain.mkv", true},             // the family space appends the source's extension
		{"other.mp4", "other.mp4.mkv", true},     // appended even though the name has a dot
		{"plain", "plain.gz", true},              // archive.tar.gz -> plain stores plain.gz
		{"x.txt", "x.txt.mkv", true},             // a held name the family space appended to
		{"x.txt", "x_20260925_143349.txt", true}, // parked under a timestamp
		{"plain", "plain_20260925_143039.bin", true},
		{"x.txt", "x.txtabc", true}, // a bare prefix is not a landing
		{"x.txt", "", false},        // an absent name confirms nothing, so it must not delete
	}
	for _, c := range cases {
		err := checkRenameLanded(c.req, c.stored)
		if c.wantErr {
			assert.Error(t, err, "checkRenameLanded(%q, %q) must report the rename did not land", c.req, c.stored)
		} else {
			assert.NoError(t, err, "checkRenameLanded(%q, %q) must accept", c.req, c.stored)
		}
	}
}

// TestUnitRenameOntoHeldNameNotRetried covers the retry class of a parked
// rename: the server's answer is definitive, so re-issuing the request cannot
// change it and must not be attempted.
func TestUnitRenameOntoHeldNameNotRetried(t *testing.T) {
	tr := &moveTransport{
		renameParksAs: map[string]string{"x.txt": "x_20260925_000000.txt"},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	err := f.renameObject(context.Background(), "id-a", "x.txt", "root-id", false)
	require.Error(t, err, "a rename the server parked must be reported")
	assert.Equal(t, 1, countCalls(tr.calls, "rename:id-a->x.txt"),
		"a parked rename is definitive and must be issued exactly once: %v", tr.calls)
}

// TestUnitChunkWriterEncodesLeafOnce covers the name domain in Close: the
// writer's leaf is already encoded, so it must be decoded back to the standard
// form before settleAutoRename encodes it again. An invalid UTF-8 byte is the
// trigger - it encodes to a multi-rune escape group, so passing the encoded
// leaf through the encoder again addresses a name that decodes to a different
// file, and the scan then fails to recognise the entry holding the leaf.
func TestUnitChunkWriterEncodesLeafOnce(t *testing.T) {
	const leaf = "a\xffb.txt"
	enc := encoder.Standard | encoder.EncodeInvalidUtf8
	wire := enc.FromStandardName(leaf)
	tr := &moveTransport{
		autoRename: true,
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", wire, 11)},
		},
	}
	f := newMoveTestFsEnc(tr, enc)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, leaf, "id-src", 16)
	_, w, err := f.OpenChunkWriter(context.Background(), leaf, src)
	require.NoError(t, err)
	_, err = w.WriteChunk(context.Background(), 0, strings.NewReader("NEWCONTENT!12345"))
	require.NoError(t, err)
	require.NoError(t, w.Close(context.Background()), "Close must land the upload: %v", tr.calls)

	assert.True(t, hasPrefixIn(tr.calls, "rename:id-held->"),
		"the entry holding the leaf must be recognised and parked: %v", tr.calls)
	assert.Contains(t, tr.calls, "delete:id-held", "the parked entry must be removed: %v", tr.calls)
}

// TestUnitSettleFreeLeafEncodesOnce covers the name domain on settleAutoRename's
// free-leaf branch: the install rename must carry the once-encoded name.
func TestUnitSettleFreeLeafEncodesOnce(t *testing.T) {
	const leaf = "a\xffb.txt"
	enc := encoder.Standard | encoder.EncodeInvalidUtf8
	tr := &moveTransport{}
	f := newMoveTestFsEnc(tr, enc)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	require.NoError(t, f.settleAutoRename(context.Background(), "root-id", leaf, "new-id"))
	assert.Contains(t, tr.calls, "rename:new-id->"+enc.FromStandardName(leaf),
		"the install rename must carry the once-encoded name: %v", tr.calls)
}

// TestUnitSettleFreeLeafFailureDiscardsUpload covers the free-leaf failure arm:
// an upload that cannot be given its requested name must be removed, so a
// failed Put leaves no entry the caller never asked for.
func TestUnitSettleFreeLeafFailureDiscardsUpload(t *testing.T) {
	tr := &moveTransport{renameFailID: "new-id"}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	err := f.settleAutoRename(context.Background(), "root-id", "x.txt", "new-id")
	require.Error(t, err, "an upload that cannot take its name must fail the Put")
	assert.Contains(t, tr.calls, "delete:new-id",
		"the upload that could not take its name must be removed: %v", tr.calls)
}

// TestUnitUpdateRestoresOldObjectToRealLeaf covers the restore arm after a
// failed landing: the pre-rename parked the old object under a backup name, so
// a restore that targets the backup name leaves the destination path empty and
// the file missing from where the user asked for it. The old object must go
// back under the real leaf.
func TestUnitUpdateRestoresOldObjectToRealLeaf(t *testing.T) {
	tr := &moveTransport{
		autoRename: true,
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
		// the install rename of the auto-renamed upload fails
		renameFailID: "created-x.txt_20260925_000000",
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	o := newTestObject(f, "x.txt", "id-held", 11)
	err := o.Update(context.Background(), strings.NewReader("NEWCONTENT!"), newTestObject(f, "x.txt", "id-held", 11))
	require.Error(t, err, "an update that cannot land its upload must fail")

	last := ""
	for _, c := range tr.calls {
		if strings.HasPrefix(c, "rename:id-held->") {
			last = c
		}
	}
	assert.Equal(t, "rename:id-held->x.txt", last,
		"the old object must end up back under its real leaf, not the backup name: %v", tr.calls)
}

// TestUnitUpdatePreRenameFailureKeepsOldContent covers Update's fallback when
// the pre-rename fails: the old object must be left in place, not deleted,
// because deleting it opens a window where a failed upload loses the previous
// content. The upload must then win the name by itself.
func TestUnitUpdatePreRenameFailureKeepsOldContent(t *testing.T) {
	tr := &moveTransport{
		renameFailID: "id-held",
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	o := newTestObject(f, "x.txt", "id-held", 11)
	_ = o.Update(context.Background(), strings.NewReader("NEWCONTENT!"), newTestObject(f, "x.txt", "id-held", 11))

	assert.NotContains(t, tr.calls, "delete:id-held",
		"the old content must never be deleted before the new upload is complete")
}

// TestUnitUpdateEncodesNameOnce covers the name domain in Update: the leaf
// comes back from FindPath in STANDARD form, and every server-facing name
// must be encoded exactly once. An invalid UTF-8 byte is the trigger - it
// encodes to a multi-rune escape group, so encoding the already-encoded leaf
// again yields a name that decodes to a DIFFERENT file and the park/restore
// would address the wrong entry.
func TestUnitUpdateEncodesNameOnce(t *testing.T) {
	const leaf = "a\xffb.txt"
	enc := encoder.Standard | encoder.EncodeInvalidUtf8
	// Guard the premise: the encoder must actually rewrite this leaf, or the
	// assertion below is vacuously true.
	require.NotEqual(t, leaf, enc.FromStandardName(leaf),
		"this leaf must be rewritten by the encoder for the test to bite")

	tr := &moveTransport{
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", leaf, 11)},
		},
	}
	f := newMoveTestFsEnc(tr, enc)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	o := newTestObject(f, leaf, "id-held", 11)
	require.NoError(t, o.Update(context.Background(), strings.NewReader("NEWCONTENT!"), newTestObject(f, leaf, "id-held", 11)))

	// Every name the backend sent must decode back to the requested leaf or a
	// backup of it: a doubly-encoded name decodes to something else, so the
	// park/restore would address the wrong entry.
	for _, c := range tr.calls {
		if !strings.HasPrefix(c, "rename:") {
			continue
		}
		parts := strings.Split(strings.TrimPrefix(c, "rename:"), "->")
		if len(parts) != 2 {
			continue
		}
		std := f.opt.Enc.ToStandardName(parts[1])
		assert.True(t, std == leaf || strings.HasPrefix(std, backupPrefix(leaf)),
			"rename %q must address a name that decodes to %q or a backup of it, got %q", c, leaf, std)
	}
}

// TestUnitUpdateEncodesNameOnceOnFailureArms extends the encode-once rule to
// the paths the happy path does not reach: the restore after a failed upload
// and the cleanup scan. Each of them builds a name the server has to match, so
// encoding an already-encoded leaf would address a different file.
func TestUnitUpdateEncodesNameOnceOnFailureArms(t *testing.T) {
	const leaf = "a\xffb.txt"
	enc := encoder.Standard | encoder.EncodeInvalidUtf8
	require.NotEqual(t, leaf, enc.FromStandardName(leaf),
		"this leaf must be rewritten by the encoder for the test to bite")

	// Every server-facing name must decode back to the leaf or a backup of it.
	checkNames := func(t *testing.T, f *Fs, calls []string) {
		t.Helper()
		seen := 0
		for _, c := range calls {
			var name string
			switch {
			case strings.HasPrefix(c, "create:"):
				name = strings.TrimPrefix(c, "create:")
			case strings.HasPrefix(c, "rename:"):
				parts := strings.SplitN(strings.TrimPrefix(c, "rename:"), "->", 2)
				if len(parts) != 2 {
					continue
				}
				name = parts[1]
			default:
				continue
			}
			seen++
			std := f.opt.Enc.ToStandardName(name)
			assert.True(t, std == leaf || strings.HasPrefix(std, backupPrefix(leaf)),
				"server-facing name %q decodes to %q, which is neither %q nor a backup of it", name, std, leaf)
		}
		assert.NotZero(t, seen, "the probe must observe at least one server-facing name")
	}

	// Upload failure: the restore must address the same wire name the park used.
	t.Run("upload failure restore", func(t *testing.T) {
		tr := &moveTransport{
			entries: map[string][]map[string]any{
				"root-id": {fileEntry("id-held", leaf, 11)},
			},
		}
		f := newMoveTestFsEnc(tr, enc)
		require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

		o := newTestObject(f, leaf, "id-held", 11)
		require.Error(t, o.Update(context.Background(), failingReader{}, newTestObject(f, leaf, "id-held", 11)))
		checkNames(t, f, tr.calls)
	})

	// Landing failure: the restore of the parked entry must decode to the leaf.
	t.Run("landing failure restore", func(t *testing.T) {
		wire := enc.FromStandardName(leaf)
		tr := &moveTransport{
			autoRename:    true,
			renameParksAs: map[string]string{wire: wire + "_20260925_000000"},
			entries: map[string][]map[string]any{
				"root-id": {fileEntry("id-held", wire, 11)},
			},
		}
		f := newMoveTestFsEnc(tr, enc)
		require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

		o := newTestObject(f, leaf, "id-held", 11)
		require.Error(t, o.Update(context.Background(), strings.NewReader("NEWCONTENT!"), newTestObject(f, leaf, "id-held", 11)))
		checkNames(t, f, tr.calls)
	})

	// Cleanup: an orphaned backup of this leaf must be matched and removed, so
	// the scan prefix has to be built in the standard domain the listing uses.
	t.Run("cleanup matches orphan", func(t *testing.T) {
		wire := enc.FromStandardName(leaf)
		tr := &moveTransport{
			entries: map[string][]map[string]any{
				"root-id": {
					fileEntry("id-held", wire, 11),
					fileEntry("id-orphan", enc.FromStandardName(backupLeafName(leaf)), 4),
				},
			},
		}
		f := newMoveTestFsEnc(tr, enc)
		require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

		o := newTestObject(f, leaf, "id-held", 11)
		require.NoError(t, o.Update(context.Background(), strings.NewReader("NEWCONTENT!"), newTestObject(f, leaf, "id-held", 11)))
		assert.Contains(t, tr.calls, "delete:id-orphan",
			"the orphaned backup must be removed; its prefix must be the standard leaf")
	})
}

// indexOf returns the position of want in calls, or -1.
func indexOf(calls []string, want string) int {
	for i, c := range calls {
		if c == want {
			return i
		}
	}
	return -1
}

// hasPrefixIn reports whether any call starts with prefix.
func hasPrefixIn(calls []string, prefix string) bool {
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

// ============================================================================
// Regression tests: an unconfirmed server outcome must not be read as a fact
// ============================================================================

// TestUnitPutLandsUploadWhoseReplyOmitsTheName covers the create reply that
// leaves the fileName field out. The entry is then only findable by id, and a
// caller that assumed the requested name would skip landing an upload the
// server parked under a suffixed name - reporting success while the requested
// path still held the previous entry.
func TestUnitPutLandsUploadWhoseReplyOmitsTheName(t *testing.T) {
	tr := &moveTransport{
		autoRename:          true,
		createOmitsFileName: true,
		renameAppliesEntry:  true,
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "x.txt", "id-src", 11)
	o, err := f.Put(context.Background(), strings.NewReader("NEWCONTENT!"), src)
	require.NoError(t, err, "the upload must be landed at the requested path")

	// The server stored the upload under x.txt_20260925_000000; with the name
	// left out of the reply, only the listing reveals that, and Put must still
	// move it onto x.txt.
	assert.Contains(t, tr.calls, "rename:created-x.txt_20260925_000000->x.txt",
		"the parked upload must be moved onto the requested leaf: %v", tr.calls)
	assert.Equal(t, "x.txt", o.Remote())
}

// TestUnitRenameReplyWithoutNameIsResolvedByID covers a rename answered
// without the name it stored. The reply alone cannot tell a landing from a
// park, so the entry is located by id: a rename the server stored under a
// suffixed name must be reported as a failure, not as the requested name.
func TestUnitRenameReplyWithoutNameIsResolvedByID(t *testing.T) {
	tr := &moveTransport{
		renameAppliesEntry: true,
		renameParksAs:      map[string]string{"x.txt": "x_20260925_000000.txt"},
		omitsRenameName:    true,
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-a", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	err := f.renameObject(context.Background(), "id-a", "x.txt", "root-id", false)
	require.Error(t, err, "a rename stored under another name must not report success")
	assert.Contains(t, err.Error(), "server stored it as",
		"the error must name the place the entry actually went: %v", err)
}

// TestUnitRenameReplyWithoutNameConfirmsALanding covers the other direction of
// the same rule: silence is not evidence of a park either, so a rename that did
// land must not be failed. Failing it would make settleAutoRename delete an
// upload that is already the file's only copy.
func TestUnitRenameReplyWithoutNameConfirmsALanding(t *testing.T) {
	tr := &moveTransport{
		renameAppliesEntry: true,
		omitsRenameName:    true,
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-a", "old.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	require.NoError(t, f.renameObject(context.Background(), "id-a", "x.txt", "root-id", false),
		"a rename the listing confirms at the requested name is a landing")
}

// TestUnitUpdateRestoresWhenPreRenameFailsButMoved covers a pre-rename the
// reply reports as a failure while the entry was in fact moved away. Reading
// that reply as "the old entry is still in place" skips the restore, so after
// the upload fails the requested path is empty and the old content sits under
// a name nothing cleans up.
func TestUnitUpdateRestoresWhenPreRenameFailsButMoved(t *testing.T) {
	tr := &moveTransport{
		renameFailID:          "id-held",
		renameFailsButApplies: true,
		renameAppliesEntry:    true,
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	o := newTestObject(f, "x.txt", "id-held", 11)
	err := o.Update(context.Background(), failingReader{}, newTestObject(f, "x.txt", "id-held", 11))
	require.Error(t, err, "an update whose upload fails must report the failure")

	last := ""
	for _, c := range tr.calls {
		if strings.HasPrefix(c, "rename:id-held->") {
			last = c
		}
	}
	assert.Equal(t, "rename:id-held->x.txt", last,
		"the old content must be restored to the requested leaf after the pre-rename moved it: %v", tr.calls)
}

// TestUnitSettleRunsAfterParkReplyOmitsName covers settleAutoRename's parking
// rename answered without a name: the held entry was parked, so the leaf is
// free and the upload must still be installed. Reading the silence as "the
// park failed" would abandon the leaf with the upload discarded.
func TestUnitSettleRunsAfterParkReplyOmitsName(t *testing.T) {
	tr := &moveTransport{
		renameAppliesEntry: true,
		omitsRenameName:    true,
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
		// The upload settleAutoRename is given already exists as an entry;
		// the rename reply does not name it, so it is found by id.
		createdID:     "created-x.txt",
		createdName:   "created-x.txt",
		createdParent: "root-id",
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	require.NoError(t, f.settleAutoRename(context.Background(), "root-id", "x.txt", "created-x.txt"))

	assert.True(t, hasPrefixIn(tr.calls, "rename:id-held->x-rclone-old-"),
		"the held entry is parked: %v", tr.calls)
	assert.Contains(t, tr.calls, "rename:created-x.txt->x.txt",
		"the upload must take the requested leaf once the park frees it: %v", tr.calls)
}

// TestUnitPutDropsPartialEntryWhenUploadFails covers an upload that dies after
// the create call registered an entry. The entry sits at the target path with
// no content; leaving it there means a failed Put leaves something the caller
// never asked for, and the next listing shows a half-uploaded file.
func TestUnitPutDropsPartialEntryWhenUploadFails(t *testing.T) {
	tr := &moveTransport{
		createReturnsParts: true,
		partUploadFails:    true,
		entries:            map[string][]map[string]any{"root-id": {}},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "x.txt", "id-src", 11)
	_, err := f.Put(context.Background(), strings.NewReader("NEWCONTENT!"), src)
	require.Error(t, err, "an upload whose parts fail must report the failure")

	assert.Contains(t, tr.calls, "delete:created-x.txt",
		"the entry the failed upload registered must be removed: %v", tr.calls)
}

// TestUnitUpdateKeepsContentWhenDedupReturnsTheOldEntry covers a rapidUpload
// that answers with the id of the entry it deduplicated against. When that
// entry is the one the update parked, deleting it removes the content the
// receiver has just been pointed at, leaving the requested path empty.
func TestUnitUpdateKeepsContentWhenDedupReturnsTheOldEntry(t *testing.T) {
	tr := &moveTransport{
		renameAppliesEntry: true,
		// The create call deduplicates against the parked old entry.
		createReportsID: "id-held",
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	o := newTestObject(f, "x.txt", "id-held", 11)
	require.NoError(t, o.Update(context.Background(), strings.NewReader("NEWCONTENT!"), newTestObject(f, "x.txt", "id-held", 11)))

	assert.NotContains(t, tr.calls, "delete:id-held",
		"the entry the receiver now points at must not be deleted: %v", tr.calls)
	assert.Equal(t, "id-held", o.id)
}

// TestUnitUpdateKeepsOldContentWhenFailedUploadDedupsAgainstIt covers the
// failure arm of the same dedup reply: the create call answers with the id of
// the entry the update parked, then the part upload fails. Discarding that id
// destroys the previous content before the restore arm can rename it back, so
// the update ends with the requested path empty and the old content gone.
func TestUnitUpdateKeepsOldContentWhenFailedUploadDedupsAgainstIt(t *testing.T) {
	tr := &moveTransport{
		renameAppliesEntry: true,
		createReturnsParts: true,
		partUploadFails:    true,
		// The create call deduplicates against the parked old entry.
		createReportsID: "id-held",
		entries: map[string][]map[string]any{
			"root-id": {fileEntry("id-held", "x.txt", 11)},
		},
	}
	f := newMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	o := newTestObject(f, "x.txt", "id-held", 11)
	err := o.Update(context.Background(), strings.NewReader("NEWCONTENT!"), newTestObject(f, "x.txt", "id-held", 11))
	require.Error(t, err, "an upload whose parts fail must report the failure")

	assert.NotContains(t, tr.calls, "delete:id-held",
		"the previous content must survive a failed upload that deduplicated against it: %v", tr.calls)
	names := tr.entryNames("root-id")
	assert.Contains(t, names, "x.txt",
		"the previous content must be back under the requested name: %v", names)
}

// failingRenameTransport rejects the rename endpoint with a business error.
type failingRenameTransport struct {
	moveTransport
}

func (t *failingRenameTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasSuffix(r.URL.Path, "/hcy/file/update") {
		t.mu.Lock()
		t.calls = append(t.calls, "rename-rejected")
		t.mu.Unlock()
		b, _ := json.Marshal(map[string]any{"success": false, "code": "04010319", "message": "权益不足"})
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(string(b))),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
		}, nil
	}
	return t.moveTransport.RoundTrip(r)
}
