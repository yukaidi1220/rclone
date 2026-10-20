package yun139

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProbeGroupDirMove measures what the group move capture leaves open: that
// a cross-parent directory move takes the subtree with it, that the move task
// refuses a directory whose new home is the parent it already lives in, and
// that DirMove then renames the catalog instead. Run with YUN139_PROBE=1
// against a space=group remote.
func TestProbeGroupDirMove(t *testing.T) {
	probeSkip(t)
	f := probeFs(t, probeRemote())
	require.Equal(t, spaceGroup, f.space, "this probe needs a space=group remote")
	ctx := context.Background()
	base := probeBase()

	baseID, err := f.dirCache.FindDir(ctx, base, true)
	require.NoError(t, err, "create the probe base directory")
	t.Cleanup(func() {
		_ = f.Purge(ctx, base)
	})

	_, err = f.dirCache.FindDir(ctx, base+"/dstDir", true)
	require.NoError(t, err)
	_, err = f.dirCache.FindDir(ctx, base+"/srcDir", true)
	require.NoError(t, err)
	probePut(ctx, t, f, base, "srcDir/file.txt", "subtree-content")
	probeSettle(ctx, t, f, base+"/srcDir/file.txt")
	_, err = f.dirCache.FindDir(ctx, base+"/same1", true)
	require.NoError(t, err)
	probePut(ctx, t, f, base, "same1/keep.txt", "keep-content")
	probeSettle(ctx, t, f, base+"/same1/keep.txt")

	t.Run("TaskRefusesSameParent", func(t *testing.T) {
		// The evidence for DirMove's rename branch: a catalog moved inside its
		// own parent is refused by the task.
		sameID, err := f.dirCache.FindDir(ctx, base+"/same1", false)
		require.NoError(t, err)
		err = f.groupMoveTask(ctx, []string{sameID}, baseID, true)
		require.Error(t, err, "the task should refuse a same-parent directory move")
		assert.Contains(t, err.Error(), "result code",
			"an auth or transport failure is not the refusal this probe measures: %v", err)
		t.Logf("same-parent task: %v", err)
	})

	t.Run("CrossParentDirMove", func(t *testing.T) {
		require.NoError(t, f.DirMove(ctx, f, base+"/srcDir", base+"/dstDir/moved"))
		probeSettle(ctx, t, f, base+"/dstDir/moved/file.txt")
		o, err := f.NewObject(ctx, base+"/dstDir/moved/file.txt")
		require.NoError(t, err, "the file must have come with its directory")
		assert.Equal(t, int64(len("subtree-content")), o.Size())
		// The old location disappears on the server's own schedule.
		tries := 0
		var gone error
		for ; tries < 20; tries++ {
			_, gone = f.NewObject(ctx, base+"/srcDir/file.txt")
			if gone != nil {
				break
			}
			time.Sleep(2 * time.Second)
		}
		t.Logf("old location cleared after %d tries, err=%v", tries, gone)
		assert.Error(t, gone, "the old location must go away eventually")
	})

	t.Run("SameParentDirMoveIsARename", func(t *testing.T) {
		require.NoError(t, f.DirMove(ctx, f, base+"/same1", base+"/same2"))
		o, err := f.NewObject(ctx, base+"/same2/keep.txt")
		require.NoError(t, err, "the renamed directory must answer to the new name")
		assert.Equal(t, int64(len("keep-content")), o.Size())
	})

	t.Run("MovedSubtreeStaysAddressable", func(t *testing.T) {
		// Move the subtree again, one level up: every cached server path under
		// the moved directory went stale, so the next operation inside it has to
		// relearn it instead of sending the old one.
		require.NoError(t, f.DirMove(ctx, f, base+"/dstDir/moved", base+"/up"))
		probeSettle(ctx, t, f, base+"/up/file.txt")
		o, err := f.NewObject(ctx, base+"/up/file.txt")
		require.NoError(t, err)
		assert.Equal(t, int64(len("subtree-content")), o.Size())
		require.NoError(t, f.DirMove(ctx, f, base+"/up", base+"/up2"))
		probeSettle(ctx, t, f, base+"/up2/file.txt")
		_, err = f.NewObject(ctx, base+"/up2/file.txt")
		assert.NoError(t, err)
	})
}

// TestProbeGroupDownloadLinkIgnoresPath answers the question the DirMove FIXME
// leaves open: the download-link request sends the containing directory's
// server path alongside the content id, so does a path that no longer matches
// where the entry lives make the link fail? Run with YUN139_PROBE=1 against a
// space=group remote.
func TestProbeGroupDownloadLinkIgnoresPath(t *testing.T) {
	probeSkip(t)
	f := probeFs(t, probeRemote())
	require.Equal(t, spaceGroup, f.space, "this probe needs a space=group remote")
	ctx := context.Background()
	base := probeBase()
	_, err := f.dirCache.FindDir(ctx, base, true)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = f.Purge(ctx, base)
	})

	sub := base + "/holder"
	_, err = f.dirCache.FindDir(ctx, sub, true)
	require.NoError(t, err)
	probePut(ctx, t, f, sub, "leaf.txt", "link-path-probe")
	probeSettle(ctx, t, f, sub+"/leaf.txt")

	o, err := f.NewObject(ctx, sub+"/leaf.txt")
	require.NoError(t, err)
	so := o.(*Object)
	goodPath := so.serverPath
	require.NotEmpty(t, goodPath, "a listed group object carries the path it was listed under")
	t.Logf("listed path length=%d", len(goodPath))

	// Move the entry out from under that path, then ask for a link with the
	// path it no longer lives at.
	dstID, err := f.dirCache.FindDir(ctx, base+"/other", true)
	require.NoError(t, err)
	require.NoError(t, f.groupMoveTask(ctx, []string{so.id}, dstID, false))
	tries := 0
	for ; tries < 20; tries++ {
		if _, gone := f.NewObject(ctx, base+"/other/leaf.txt"); gone == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	require.NotEqual(t, 20, tries, "the moved entry never appeared at the destination")
	so.invalidateURL()
	movedLink, err := f.fetchDownloadURL(ctx, so)
	t.Logf("link after the move, sent with the pre-move path: err=%v", err)
	if err == nil {
		body, rerr := readProbeLink(ctx, f, movedLink)
		t.Logf("bytes from that link: n=%d err=%v", len(body), rerr)
		assert.Equal(t, "link-path-probe", string(body), "the link must still carry the entry's content")
	}

	// A path that never existed separates "the server checks the path" from
	// "the server only looks at contentID".
	so.serverPath = "root:/00000000000000000000/00000000000000000000/nonexistent-id"
	so.invalidateURL()
	bogusLink, err := f.fetchDownloadURL(ctx, so)
	t.Logf("link with a path that never existed: err=%v", err)
	if err == nil {
		body, rerr := readProbeLink(ctx, f, bogusLink)
		t.Logf("bytes from that link: n=%d err=%v", len(body), rerr)
		assert.Equal(t, "link-path-probe", string(body), "the id alone must be enough")
	}

	// And the hash path, which is what `rclone check` uses.
	so.serverPath = goodPath
	so.invalidateURL()
	h, herr := f.groupContentHash(ctx, so)
	t.Logf("hash with the pre-move path: len=%d err=%v", len(h), herr)
	assert.NoError(t, herr)
	assert.NotEmpty(t, h, "a moved entry must still answer for its hash")
}

// readProbeLink fetches a download link the probe just asked for.
func readProbeLink(ctx context.Context, f *Fs, link string) ([]byte, error) {
	res, err := f.getURL(ctx, link, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("http %d", res.StatusCode)
	}
	return io.ReadAll(res.Body)
}

// TestProbeGroupRenameToleratesStalePath measures the other half of the same
// question: modifyGroupContent sends the entry's server path, and a remote that
// did not issue the move still holds the path from before it. If the server
// checks that path, such a rename fails; if it does not, the stale cache is
// only a stale string. Run with YUN139_PROBE=1 against a space=group remote.
func TestProbeGroupRenameToleratesStalePath(t *testing.T) {
	probeSkip(t)
	f := probeFs(t, probeRemote())
	require.Equal(t, spaceGroup, f.space, "this probe needs a space=group remote")
	ctx := context.Background()
	base := probeBase()
	dirID, err := f.dirCache.FindDir(ctx, base, true)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = f.Purge(ctx, base)
	})

	o := probePut(ctx, t, f, base, "before.txt", "rename-path-probe")
	probeSettle(ctx, t, f, base+"/before.txt")

	// The listing has now filled the cache for this directory; replace what it
	// learned with a path that never existed, which is what a stale remote
	// would send.
	f.srvPathOf.put(dirID, "root:/00000000000000000000/deadbeef000000000000")
	stale, ok := f.srvPathOf.get(dirID)
	require.True(t, ok, "the poisoned path must be in the cache for the probe to mean anything")
	t.Logf("renaming with a poisoned parent path (length=%d)", len(stale))

	err = f.groupRenameEntry(ctx, o.id, "after.txt", dirID, "file")
	t.Logf("rename with the stale path: err=%v", err)
	if err != nil {
		// A refusal is only a finding if the entry really did not move; check
		// rather than assume.
		_, gone := f.NewObject(ctx, base+"/after.txt")
		t.Logf("after the refusal, the new name answers: err=%v", gone)
		assert.Error(t, gone, "if the rename failed, the old name must still be the one the server holds")
		return
	}
	// The server accepted it: confirm from the listing, not from the envelope.
	probeSettle(ctx, t, f, base+"/after.txt")
	landed, err := f.NewObject(ctx, base+"/after.txt")
	require.NoError(t, err, "a rename the server accepted must be readable under the new name")
	body, rerr := readProbeObject(ctx, landed)
	require.NoError(t, rerr)
	assert.Equal(t, "rename-path-probe", string(body), "the renamed entry must be the same file")
}

// readProbeObject reads an object through the backend, not a local file.
func readProbeObject(ctx context.Context, o fs.Object) ([]byte, error) {
	rc, err := o.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	return io.ReadAll(rc)
}

// TestProbeGroupMoveTaskChecksDestPath finishes the survey of the path field.
// Measured 2026-10-20, and the answer differs by endpoint: the download link
// and the rename resolve by contentID and ignore the path, but the move task
// answers 1909011551 目录不存在 for a destination path that does not exist - so
// the task is the endpoint where a stale cache can bite, and the reason Move
// and DirMove decline to a bandwidth transfer on that code.
// Run with YUN139_PROBE=1 against a space=group remote.
func TestProbeGroupMoveTaskChecksDestPath(t *testing.T) {
	probeSkip(t)
	f := probeFs(t, probeRemote())
	require.Equal(t, spaceGroup, f.space, "this probe needs a space=group remote")
	ctx := context.Background()
	base := probeBase()
	_, err := f.dirCache.FindDir(ctx, base, true)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = f.Purge(ctx, base)
	})

	dstID, err := f.dirCache.FindDir(ctx, base+"/dst", true)
	require.NoError(t, err)
	o := probePut(ctx, t, f, base, "parcel.txt", "dest-path-probe")
	probeSettle(ctx, t, f, base+"/parcel.txt")

	f.srvPathOf.put(dstID, "root:/00000000000000000000/deadbeef000000000000")
	err = f.groupMoveTask(ctx, []string{o.id}, dstID, false)
	t.Logf("move task with a poisoned destPath: err=%v", err)
	require.Error(t, err, "the task is supposed to check the destination path")
	assert.Contains(t, err.Error(), groupStalePathErrorCode,
		"the refusal must be the code the decline keys on, not some other failure: %v", err)

	// The refusal leaves the entry where it was.
	_, still := f.NewObject(ctx, base+"/parcel.txt")
	assert.NoError(t, still, "a refused task must not have moved the entry")

	// And the poisoned cache is not sticky: listing heals it, which is why a
	// move issued afterwards goes through. The decline branch in Move/DirMove
	// therefore covers only a remote that never re-lists, and is unit-tested
	// rather than reached here.
	_, err = f.List(ctx, base)
	require.NoError(t, err)
	t.Logf("dest path after re-listing the parent=%q", f.groupSrvPath(dstID))

	dst, err := f.Move(ctx, o, base+"/dst/parcel.txt")
	require.NoError(t, err, "with the path relearned, the same move goes through")
	assert.Equal(t, base+"/dst/parcel.txt", dst.Remote())
}
