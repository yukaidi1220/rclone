package yun139

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// familyMoveTransport answers the family-cloud endpoints Fs.Move and Fs.Copy
// touch, and models the two rules the real server enforces: a copy keeps the
// source's name, and a rename onto a name the directory already holds does
// not overwrite it - the entry keeps its name and the renamed file is parked
// under a suffixed one. The copy's id is echoed by the task detail, which is
// what the backend resolves the copy with.
type familyMoveTransport struct {
	mu    sync.Mutex
	calls []string

	// dstHeld is the destination listing before the copy.
	dstHeld []map[string]any

	// copyLandsAs is the name the copy task lands the new content under.
	copyLandsAs string

	// copied flips once the copy task has been issued, so the listing
	// models the server before and after the copy.
	copied bool

	// renamedTo records the name the copy carries after the rename.
	renamedTo string

	// newID is the id the copy task reports. renameCalls counts renames.
	newID       string
	renameCalls int

	// noEcho makes the task detail omit the created entry's id, modelling
	// a server that does not report what the copy produced.
	noEcho bool

	// echoSrcID makes the task detail report the SOURCE's id, modelling a
	// server whose copy task does not name the entry it created. Acting on
	// that id would act on the original file.
	echoSrcID bool

	// renameNotFound makes the next N rename calls answer the family
	// not-found code, modelling a rename that races the server's
	// indexing of the entry the copy task just created.
	renameNotFound int

	// delTask is set once a delete task has been issued, so the task
	// detail answers with a delete's shape: no rstID, and "0" rather
	// than "0000" as the per-item reason.
	delTask bool

	// paths maps a catalogID to the server-side path a listing of it
	// reports. A directory absent from the map answers with the root
	// path, modelling a listing that does not name the directory.
	paths map[string]string

	// deletePaths records the Path field of every delete task, so a
	// test can check the cleanup addressed the entry by a real path
	// rather than an empty one the server would reject.
	deletePaths []string

	// cleanupID, when set, names the entry a delete task is expected to
	// target, so a test driving removeCopy directly can label the call.
	cleanupID string

	// hideCopyFor makes the next N listings omit the entry the copy
	// created, modelling a copy task that reports success before the
	// server has made the new entry visible.
	hideCopyFor int
}

func (t *familyMoveTransport) RoundTrip(r *http.Request) (*http.Response, error) {
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

	// entries is the current listing of the destination directory.
	entries := func() []map[string]any {
		items := append([]map[string]any{}, t.dstHeld...)
		if t.copied && t.hideCopyFor <= 0 {
			name := t.copyLandsAs
			if t.renamedTo != "" {
				name = t.renamedTo
			}
			if name != "" {
				items = append(items, familyFileEntry(t.newID, name, 4))
			}
		}
		return items
	}

	switch {
	case strings.HasSuffix(r.URL.Path, "queryContentListV3"):
		var req struct {
			CatalogID string `json:"catalogID"`
		}
		_ = json.Unmarshal(body, &req)
		t.calls = append(t.calls, "list:"+req.CatalogID)
		items := entries()
		if t.hideCopyFor > 0 {
			// The copy exists but the server has not indexed it yet.
			t.hideCopyFor--
		}
		p := "root:/dir-b"
		if t.paths != nil {
			if v, ok := t.paths[req.CatalogID]; ok {
				p = v
			}
		}
		return reply(map[string]any{
			"result":           map[string]any{"resultCode": "0", "resultDesc": "ok"},
			"path":             p,
			"totalCount":       len(items),
			"cloudContentList": items,
		})

	case strings.HasSuffix(r.URL.Path, "createBatchOprTaskV2"):
		// taskType 1 = copy, 2 = delete.
		var req struct {
			TaskType    int      `json:"taskType"`
			ContentList []string `json:"contentList"`
			Path        string   `json:"path"`
		}
		_ = json.Unmarshal(body, &req)
		if req.TaskType == 2 {
			t.deletePaths = append(t.deletePaths, req.Path)
			who := "other"
			for _, id := range req.ContentList {
				if id == "id-src" {
					who = "source"
				} else if id != "" && (id == t.newID || id == t.cleanupID) {
					who = "copy"
				}
			}
			t.calls = append(t.calls, "delete:"+who)
			t.delTask = true
		} else {
			t.calls = append(t.calls, "batchOpr")
			t.copied = true
			if t.newID == "" {
				t.newID = "id-copy"
			}
		}
		return reply(map[string]any{
			"result":       map[string]any{"resultCode": "0", "resultDesc": "ok"},
			"batchOprTask": map[string]any{"taskID": "fam-task-1"},
			"taskID":       "fam-task-1",
		})

	case strings.HasSuffix(r.URL.Path, "queryBatchOprTaskDetailV3"):
		t.calls = append(t.calls, "taskPoll")
		// taskStatus 2 = finished; success requires taskResultCode 1.
		// The copy's id is echoed in contentList[].rstID.
		rc := 1
		rst := t.newID
		if t.noEcho {
			rst = ""
		}
		if t.echoSrcID {
			rst = "id-src"
		}
		reason := "0000"
		if t.delTask {
			// A delete task names nothing it created and spells its
			// per-item success "0".
			rst = ""
			reason = "0"
		}
		return reply(map[string]any{
			"result": map[string]any{"resultCode": "0", "resultDesc": "ok"},
			"batchOprTask": map[string]any{
				"taskStatus":     2,
				"taskResultCode": rc,
			},
			"contentList": []map[string]any{
				{"srcID": "id-src", "rstID": rst, "reason": reason},
			},
		})

	case strings.HasSuffix(r.URL.Path, "modifyContentInfo"):
		var req struct {
			ContentID   string `json:"contentID"`
			ContentName string `json:"contentName"`
		}
		_ = json.Unmarshal(body, &req)
		t.renameCalls++
		t.calls = append(t.calls, "rename:"+req.ContentName)
		// A rename that arrives before the server has indexed the entry
		// the copy task just created is answered with the family
		// not-found code; the caller must retry it, not fail.
		if t.renameNotFound > 0 {
			t.renameNotFound--
			return reply(map[string]any{
				"result": map[string]any{"resultCode": "1809111402", "resultDesc": "目录或文件不存在"},
			})
		}
		// The non-overwrite rule: a name the directory already holds is
		// kept and the renamed file is parked under a suffixed name, which
		// the reply reports.
		lands := req.ContentName
		for _, e := range t.dstHeld {
			if e["contentName"] == req.ContentName {
				lands = strings.TrimSuffix(req.ContentName, ".txt") + "_20260924_221607.txt"
			}
		}
		t.renamedTo = lands
		return reply(map[string]any{
			"result":               map[string]any{"resultCode": "0"},
			"updateContentInfoRes": map[string]any{"contentName": lands},
		})

	case strings.HasSuffix(r.URL.Path, "deleteContentV2"):
		t.calls = append(t.calls, "delete:source")
		return reply(map[string]any{
			"result":       map[string]any{"resultCode": "0", "resultDesc": "ok"},
			"batchOprTask": map[string]any{"taskID": "fam-del-1"},
			"taskID":       "fam-del-1",
		})
	}
	return nil, fmt.Errorf("familyMoveTransport: unexpected path %s", r.URL.Path)
}

func familyFileEntry(id, name string, size int64) map[string]any {
	return map[string]any{
		"contentID":      id,
		"contentName":    name,
		"contentSize":    size,
		"lastUpdateTime": time.Now().Format("20060102150405"),
	}
}

// newFamilyMoveTestFs builds a family-space Fs whose dircache is resolved.
// The family root id is preset so Move does not need to discover it, and
// the dircache root path is empty (a non-empty one is not in the cache and
// would trigger a root listing during FindRoot).
func newFamilyMoveTestFs(t http.RoundTripper) *Fs {
	f := &Fs{
		name:         "test",
		root:         "b",
		opt:          Options{Enc: encoder.Standard | encoder.EncodeInvalidUtf8, FamilyID: "1303251918616070593"},
		space:        spaceFamily,
		svcType:      svcTypeFamily,
		account:      "13800138000",
		auth:         "dG9rZW4=",
		ts:           &tokenState{auth: "dG9rZW4="},
		tokMu:        &sync.Mutex{},
		hostMu:       &sync.Mutex{},
		familyRootMu: &sync.Mutex{},
		familyRootID: "dir-b",
		userDomainID: "1301956522699563527",
		httpClient:   &http.Client{Transport: t},
		pacer:        fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Millisecond))),
	}
	f.dirCache = dircache.New("", "dir-b", f)
	f.srvPathOf = newSrvPathCache()
	f.srvPathOf.put("dir-b", "root:/dir-b")
	return f
}

// TestUnitFamilyMoveOntoExistingNameDoesNotDeleteSource checks that a family
// move onto a name the destination already holds refuses instead of deleting
// the source: the rename cannot take the name, so the copy is removed again
// and the move reported - the content is never reachable only under a name
// the caller was not told about, with the source gone.
func TestUnitFamilyMoveOntoExistingNameDoesNotDeleteSource(t *testing.T) {
	tr := &familyMoveTransport{
		dstHeld:     []map[string]any{familyFileEntry("id-old", "dst.txt", 10)},
		copyLandsAs: "src.txt",
	}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "dst.txt")
	require.Error(t, err, "a move that did not land must not report success")
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "dst.txt")
	assert.NotContains(t, tr.calls, "delete:source",
		"the source must survive when the copy did not land under the requested name")
	assert.Contains(t, tr.calls, "delete:copy",
		"the copy that could not take the name must be removed again")
}

// TestUnitFamilyRenameOntoHeldNameFails covers the non-overwrite rule on the
// family rename endpoint: the reply reports the name the server actually
// stored, so a rename it parked under a suffixed name must be reported as a
// failure rather than as success.
func TestUnitFamilyRenameOntoHeldNameFails(t *testing.T) {
	tr := &familyMoveTransport{
		dstHeld: []map[string]any{familyFileEntry("id-old", "dst.txt", 10)},
	}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	err := f.renameObject(context.Background(), "id-src", "dst.txt", "dir-b", true)
	require.Error(t, err, "a rename the server parked under another name must not report success")
	assert.Contains(t, err.Error(), "server stored it as")
}

// TestUnitFamilyRenameToOwnNameSucceeds guards the other side: the reply
// echoes the requested name, so the check must not fire.
func TestUnitFamilyRenameToOwnNameSucceeds(t *testing.T) {
	tr := &familyMoveTransport{}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	require.NoError(t, f.renameObject(context.Background(), "id-src", "src.txt", "dir-b", true))
}

// TestUnitFamilyMoveSameSizePreExistingEntryDoesNotDeleteSource is the
// coverage for the id-based resolution: the destination already holds a
// same-size entry that the pre-copy lookup may not even see (139 listings
// are eventually consistent), so only resolving the copy by the id the task
// echoes can tell the copy from the entry that was already there.
func TestUnitFamilyMoveSameSizePreExistingEntryDoesNotDeleteSource(t *testing.T) {
	tr := &familyMoveTransport{
		dstHeld:     []map[string]any{familyFileEntry("id-old", "dst.txt", 4)},
		copyLandsAs: "src.txt",
	}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "dst.txt")
	require.Error(t, err, "a same-size entry at the destination is still a different file")
	assert.Nil(t, got)
	assert.NotContains(t, tr.calls, "delete:source")
	assert.Contains(t, tr.calls, "delete:copy")
}

// TestUnitFamilyMoveCleanLandsAndDeletesSource checks the ordinary family
// move: no collision, so the copy lands under the requested name and the
// source is deleted.
func TestUnitFamilyMoveCleanLandsAndDeletesSource(t *testing.T) {
	tr := &familyMoveTransport{copyLandsAs: "src.txt"}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "dst.txt")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Contains(t, tr.calls, "batchOpr", "the move goes through the family copy task")
	assert.Contains(t, tr.calls, "delete:source", "the source is deleted once the copy landed")
	assert.Equal(t, "id-copy", got.(*Object).id, "the returned object is the copy, not the source")
	assert.Equal(t, "dst.txt", got.Remote(), "the copy is renamed to the requested leaf")
}

// TestUnitFamilyMoveKeepsDestinationDirectory checks that the returned
// object carries the destination directory, not just the leaf: the object's
// remote is built from the requested destination path, so a move into a
// subdirectory must report the subdirectory.
func TestUnitFamilyMoveKeepsDestinationDirectory(t *testing.T) {
	tr := &familyMoveTransport{copyLandsAs: "src.txt"}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))
	f.dirCache.Put("sub", "dir-sub")
	f.srvPathOf.put("dir-sub", "root:/dir-b/dir-sub")

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "sub/dst.txt")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "sub/dst.txt", got.Remote(), "the destination directory must be part of the remote")
}

// TestUnitFamilyMoveSameDirectoryRenames checks a family move that changes
// only the name: the copy is renamed to the new leaf, the source is deleted,
// and no duplicate is left behind.
func TestUnitFamilyMoveSameDirectoryRenames(t *testing.T) {
	tr := &familyMoveTransport{copyLandsAs: "src.txt"}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "renamed.txt")
	require.NoError(t, err, "a same-directory family rename must succeed")
	require.NotNil(t, got)
	assert.Contains(t, tr.calls, "rename:renamed.txt")
	assert.Contains(t, tr.calls, "delete:source")
	assert.Equal(t, "renamed.txt", got.Remote())
}

// TestUnitFamilyMoveNoOpTouchesNothing checks that a move onto the identical
// path issues no server call at all: copying there would duplicate the file.
func TestUnitFamilyMoveNoOpTouchesNothing(t *testing.T) {
	tr := &familyMoveTransport{}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "src.txt")
	require.NoError(t, err)
	assert.Same(t, src, got, "a no-op move returns the source object")
	assert.Empty(t, tr.calls, "a no-op move must not touch the server")
}

// TestUnitFamilyMoveNoEchoedIDRefuses checks that a copy task which does not
// report the id it created fails the move: without the id there is no way to
// tell the copy from an entry that already occupied the destination, so
// deleting the source would risk losing the content.
func TestUnitFamilyMoveNoEchoedIDRefuses(t *testing.T) {
	tr := &familyMoveTransport{copyLandsAs: "src.txt", noEcho: true}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "dst.txt")
	require.Error(t, err, "a copy task with no id echo must not be reported as a move")
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "no new id",
		"the failure must say the copy could not be identified")
	assert.NotContains(t, tr.calls, "delete:source",
		"the source must survive when the copy cannot be identified")
}

// TestUnitFamilyMoveEchoedSourceIDRefuses checks that a task detail reporting
// the SOURCE's id instead of the copy's fails the move. That id names the
// original file, so acting on it would rename or delete the source itself
// rather than a copy, losing the content if the copy never landed.
func TestUnitFamilyMoveEchoedSourceIDRefuses(t *testing.T) {
	tr := &familyMoveTransport{copyLandsAs: "src.txt", echoSrcID: true}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "dst.txt")
	require.Error(t, err, "a task detail naming the source must not be reported as a move")
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), "no new id",
		"the failure must say the copy could not be identified")
	assert.NotContains(t, tr.calls, "delete:source",
		"the source must survive when the task names the source rather than a copy")
	assert.NotContains(t, tr.calls, "rename:id-src",
		"a task naming the source must not rename the source")
}

// TestUnitFamilyMoveCrossRootIsNotANoOp checks that a family move whose source
// comes from another Fs of the same account resolves the source's parent
// through the SOURCE's dircache. Resolving it through the receiver's would map
// the source onto the receiver's root, and when the destination is that same
// root the move would look like a same-directory no-op: reported as success
// while the file never moved and the copy was never made.
func TestUnitFamilyMoveCrossRootIsNotANoOp(t *testing.T) {
	tr := &familyMoveTransport{copyLandsAs: "x.txt"}
	dst := newFamilyMoveTestFs(tr)

	srcFs := newFamilyMoveTestFs(tr)
	srcFs.root = "a"
	srcFs.dirCache = dircache.New("a", "dir-a", srcFs)
	srcFs.dirCache.Put("a", "dir-a")
	srcFs.familyRootID = "dir-a"
	srcFs.srvPathOf = newSrvPathCache()
	srcFs.srvPathOf.put("dir-a", "root:/dir-a")

	src := newTestObject(srcFs, "x.txt", "id-src", 4)

	got, err := dst.Move(context.Background(), src, "x.txt")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Contains(t, tr.calls, "batchOpr",
		"a source from another root must be copied, not mistaken for a no-op")
	assert.Contains(t, tr.calls, "delete:source",
		"the move must delete the source once the copy landed")
}

// TestUnitFamilyMoveRenameRejectedRefuses checks that a server rejection of
// the rename is surfaced rather than swallowed, and that the copy is removed
// again so no orphan is left.
func TestUnitFamilyMoveRenameRejectedRefuses(t *testing.T) {
	tr := &failingFamilyRenameTransport{familyMoveTransport: familyMoveTransport{copyLandsAs: "src.txt"}}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "dst.txt")
	require.Error(t, err)
	assert.Nil(t, got)
	assert.NotContains(t, tr.calls, "delete:source")
	assert.Contains(t, tr.calls, "delete:copy")
	assert.Equal(t, 1, countCalls(tr.calls, "rename-rejected"),
		"a business rejection that is not the visibility code must not be retried")
}

// TestUnitFamilyMoveCleanupResolvesColdDestinationPath covers removeCopy's own
// contract: the delete task addresses the copy by its parent's server-side
// path, so the helper resolves and primes that path itself rather than
// trusting a caller to have done it. The cache can be cold for a directory
// the Fs never listed, and the server rejects a delete task with an empty
// path ('02010501'), which would leave the copy behind as an orphan.
//
// It drives removeCopy directly: a copy that reaches the cleanup through
// Move has already been located by resolveCopyLeaf, which lists the
// destination and so warms the cache, leaving this branch untaken.
func TestUnitFamilyMoveCleanupResolvesColdDestinationPath(t *testing.T) {
	tr := &familyMoveTransport{
		paths:     map[string]string{"dir-sub": "root:/dir-b/dir-sub"},
		cleanupID: "id-copy",
	}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	// srvPathOf is deliberately cold for dir-sub: the Fs never listed it.
	_, cached := f.srvPathOf.get("dir-sub")
	require.False(t, cached, "the fixture must start with a cold path cache")

	f.removeCopy(context.Background(), "id-copy", "dir-sub", "id-src")

	require.Contains(t, tr.calls, "delete:copy", "the copy must be removed")
	require.Len(t, tr.deletePaths, 1, "exactly one delete task must be issued")
	assert.Equal(t, "root:/dir-b/dir-sub", tr.deletePaths[0],
		"the cleanup must resolve the parent's server path, not issue an empty one")
}

// countPrefix returns how many calls start with prefix.
func countPrefix(calls []string, prefix string) int {
	n := 0
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			n++
		}
	}
	return n
}

// countCalls returns how many times want appears in calls.
func countCalls(calls []string, want string) int {
	n := 0
	for _, c := range calls {
		if c == want {
			n++
		}
	}
	return n
}

// TestUnitFamilyCopyOntoTakenNameFallsBack checks that a family copy whose
// destination name is taken refuses the server-side path for the same reason
// the personal one does: the server never overwrites, so the copy would be
// auto-renamed and could not carry the requested leaf. The engine then
// replaces the entry with a bandwidth copy.
func TestUnitFamilyCopyOntoTakenNameFallsBack(t *testing.T) {
	tr := &familyMoveTransport{
		dstHeld:     []map[string]any{familyFileEntry("id-old", "dst.txt", 4)},
		copyLandsAs: "src.txt",
	}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Copy(context.Background(), src, "dst.txt")
	require.ErrorIs(t, err, fs.ErrorCantCopy, "a taken destination name must fall back to a bandwidth copy")
	assert.Nil(t, got)
	assert.NotContains(t, tr.calls, "batchOpr", "nothing may be created before the fallback")
}

// TestUnitFamilyCopyToFreeNameUsesServerSideCopy checks the ordinary family
// copy: the name is free, so the copy task runs and the copy is resolved by
// the id the task echoes.
func TestUnitFamilyCopyToFreeNameUsesServerSideCopy(t *testing.T) {
	tr := &familyMoveTransport{copyLandsAs: "src.txt"}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Copy(context.Background(), src, "dst.txt")
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Contains(t, tr.calls, "batchOpr")
	assert.Equal(t, "id-copy", got.(*Object).id, "the copy is the entry the task created")
	assert.Equal(t, "dst.txt", got.Remote())
}

// TestUnitFamilyMoveRetriesRenameUntilIndexed covers the rename race: the
// copy task reports success before the server has indexed the entry it
// created, so the rename that follows is answered '1809111402: 目录或文件
// 不存在'. That answer must be retried, not surfaced as a failed move, and
// the move must still land under the requested name.
func TestUnitFamilyMoveRetriesRenameUntilIndexed(t *testing.T) {
	tr := &familyMoveTransport{
		copyLandsAs:    "src.txt",
		renameNotFound: 2,
	}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "dst.txt")
	require.NoError(t, err, "a rename that races indexing must be retried, not failed")
	require.NotNil(t, got)
	assert.Equal(t, "dst.txt", got.Remote())
	assert.GreaterOrEqual(t, tr.renameCalls, 3, "the rename must be retried until it takes")
	assert.Contains(t, tr.calls, "delete:source", "the move completes once the rename takes")
	assert.NotContains(t, tr.calls, "delete:copy", "a retried rename is not an orphan")
}

// TestUnitFamilyMoveRenameNotFoundExhaustedRefuses checks that the retry is
// bounded: a rename the server keeps rejecting as not-found must eventually
// be reported, with the source intact and the copy removed.
func TestUnitFamilyMoveRenameNotFoundExhaustedRefuses(t *testing.T) {
	old := copyVisibilityTimeout
	copyVisibilityTimeout = 50 * time.Millisecond
	defer func() { copyVisibilityTimeout = old }()
	tr := &familyMoveTransport{
		copyLandsAs:    "src.txt",
		renameNotFound: 1000,
	}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Move(context.Background(), src, "dst.txt")
	require.Error(t, err, "a rename that never takes must not be reported as a move")
	assert.Nil(t, got)
	assert.NotContains(t, tr.calls, "delete:source", "the source must survive")
	assert.Contains(t, tr.calls, "delete:copy", "the copy that could not be renamed is removed")
}

// TestUnitFamilyCopyRemovesCopyWhenResolveFails covers the family Copy
// branch's cleanup: the copy task lands content under the source's name, and
// when it cannot be given the requested leaf the copy must be removed. A
// failed copy that leaves the entry behind puts content in the directory
// under a name the caller never asked for.
func TestUnitFamilyCopyRemovesCopyWhenResolveFails(t *testing.T) {
	tr := &failingFamilyRenameTransport{}
	tr.copyLandsAs = "src.txt"
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Copy(context.Background(), src, "dst.txt")
	require.Error(t, err, "a copy that cannot take the requested leaf must fail")
	assert.Nil(t, got)

	require.Contains(t, tr.calls, "delete:copy", "the unwanted copy must be removed")
	require.Len(t, tr.deletePaths, 1, "exactly one delete task must be issued")
	assert.NotEmpty(t, tr.deletePaths[0], "the delete must address the copy by a real path")
}

// TestUnitFamilyResolveWaitsForCopyVisibility covers newObjectByID's retry:
// the copy task reports success before the server has indexed the new entry,
// so the listing that resolves the copy's id can miss it for a moment. A
// single read would report a completed copy as missing.
func TestUnitFamilyResolveWaitsForCopyVisibility(t *testing.T) {
	tr := &familyMoveTransport{copyLandsAs: "src.txt", hideCopyFor: 2}
	f := newFamilyMoveTestFs(tr)
	require.NoError(t, f.dirCache.FindRoot(context.Background(), false))

	src := newTestObject(f, "src.txt", "id-src", 4)
	got, err := f.Copy(context.Background(), src, "dst.txt")
	require.NoError(t, err, "a copy that is not yet listed must be waited for, not failed")
	require.NotNil(t, got)
	assert.Equal(t, "id-copy", got.(*Object).id)
	assert.GreaterOrEqual(t, countPrefix(tr.calls, "list:"), 3,
		"the listing must be polled until the copy appears: %v", tr.calls)
}

// failingFamilyRenameTransport rejects the rename endpoint with a business
// error.
type failingFamilyRenameTransport struct {
	familyMoveTransport
}

func (t *failingFamilyRenameTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasSuffix(r.URL.Path, "modifyContentInfo") {
		t.mu.Lock()
		t.calls = append(t.calls, "rename-rejected")
		t.mu.Unlock()
		b, _ := json.Marshal(map[string]any{"result": map[string]any{"resultCode": "04010319", "resultDesc": "权益不足"}})
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(string(b))),
			Header:     http.Header{"Content-Type": []string{"application/json"}},
		}, nil
	}
	return t.familyMoveTransport.RoundTrip(r)
}
