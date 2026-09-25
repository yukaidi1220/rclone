package yun139

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	fsobject "github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Live protocol probes behind YUN139_PROBE=1. They exercise the move /
// rename shapes that fstests cannot express - in particular a move that
// stays inside one directory, which the server rejects as a batchMove
// ('04010317: 移动失败，无法移动到自身、自身所在目录、自身子目录下') and
// which the backend therefore has to issue as a plain rename.
//
// Without YUN139_PROBE set they skip instantly, so CI and `go test ./...`
// stay hermetic.

func probeSkip(t *testing.T) {
	t.Helper()
	if os.Getenv("YUN139_PROBE") == "" {
		t.Skip("set YUN139_PROBE=1 (and RCLONE_CONFIG with [TestYun139]) for live probes")
	}
}

// probeFs builds an Fs from RCLONE_CONFIG under the given remote name.
// Probes bypass fstest.Initialise, so the config loader is installed by
// hand - outside cmd.Main and fstest nothing points rclone at RCLONE_CONFIG.
func probeFs(t *testing.T, remote string) *Fs {
	t.Helper()
	if p := os.Getenv("RCLONE_CONFIG"); p != "" {
		require.NoError(t, config.SetConfigPath(p))
	}
	configfile.Install()
	fsInfo, err := fs.NewFs(context.Background(), remote)
	require.NoError(t, err, "creating %s", remote)
	f, ok := fsInfo.(*Fs)
	require.True(t, ok, "%s is not a yun139 Fs", remote)
	return f
}

// probeFamilyFs builds a family-space Fs. The family cloud cannot be
// discovered from an empty family_id the way personal is, so the remote is
// named explicitly; the default matches the documented probe config.
func probeFamilyFs(t *testing.T) *Fs {
	t.Helper()
	remote := os.Getenv("YUN139_FAMILY_REMOTE")
	if remote == "" {
		remote = "TestYun139Fam:"
	}
	return probeFs(t, remote)
}

// probeRemote returns the personal remote the probes drive, overridable for
// a config that names it differently.
func probeRemote() string {
	if r := os.Getenv("YUN139_PERSONAL_REMOTE"); r != "" {
		return r
	}
	return "TestYun139:"
}

// probeBase is unique per run so a failed probe never poisons the next one.
func probeBase() string {
	return fmt.Sprintf("rclone-probe-%d", time.Now().UnixNano())
}

// probePut uploads a small file under the given base and returns the object.
func probePut(ctx context.Context, t *testing.T, f *Fs, base, remote, content string) *Object {
	t.Helper()
	src := fsobject.NewStaticObjectInfo(base+"/"+remote, time.Now(), int64(len(content)), true, nil, nil)
	obj, err := f.Put(ctx, strings.NewReader(content), src)
	require.NoError(t, err, "put %s", remote)
	return obj.(*Object)
}

// probeSettle waits for a just-written object to become visible in listings;
// 139 is eventually consistent for a few seconds after an upload.
func probeSettle(ctx context.Context, t *testing.T, f *Fs, remote string) {
	t.Helper()
	for i := 0; i < 15; i++ {
		if _, err := f.NewObject(ctx, remote); err == nil {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("probeSettle: %s never became visible", remote)
}

// probeWaitSize polls until the object at remote reports the expected size,
// failing the test if it never does. 139 listings are eventually consistent:
// a rename or overwrite can take a few seconds to show through, so a probe
// that asserts immediately would race the server.
func probeWaitSize(ctx context.Context, t *testing.T, f *Fs, remote string, want int64) int64 {
	t.Helper()
	got := int64(-1)
	for i := 0; i < 15; i++ {
		o, err := f.NewObject(ctx, remote)
		if err == nil {
			got = o.Size()
			if got == want {
				return got
			}
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("%s: size never became %d within the poll window (last observed %d)", remote, want, got)
	return got
}

// TestProbeChunkWriterLandsAutoRenamedUpload drives the multi-thread copy
// path (OpenChunkWriter -> WriteChunk -> Close) onto a name that is already
// taken. The server stores the upload under a suffixed name; Close must land
// it at the requested leaf, otherwise the engine would verify the wrong
// object and delete the destination the user already had.
func TestProbeChunkWriterLandsAutoRenamedUpload(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFs(t, "TestYun139:")
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	probePut(ctx, t, f, base, "chunk.bin", "OLD-CONTENT")
	probeSettle(ctx, t, f, base+"/chunk.bin")

	const payload = "NEW-CONTENT-LONGER-THAN-OLD"
	_ = probePut(ctx, t, f, base, "chunk-src.bin", payload)
	probeSettle(ctx, t, f, base+"/chunk-src.bin")
	src, err := f.NewObject(ctx, base+"/chunk-src.bin")
	require.NoError(t, err)

	// Drive the chunk writer at the occupied name.
	_, w, err := f.OpenChunkWriter(ctx, base+"/chunk.bin", src)
	require.NoError(t, err)
	n, err := w.WriteChunk(ctx, 0, strings.NewReader(payload))
	require.NoError(t, err)
	assert.EqualValues(t, len(payload), n)
	require.NoError(t, w.Close(ctx), "Close must land the upload at the requested path")

	got := probeWaitSize(ctx, t, f, base+"/chunk.bin", int64(len(payload)))
	assert.EqualValues(t, len(payload), got, "the requested path must hold the uploaded content")

	// Exactly one entry may remain at that leaf - no orphan under the server's
	// auto-renamed name.
	entries, err := f.List(ctx, base)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, path.Base(e.Remote()))
	}
	assert.Contains(t, names, "chunk.bin")
	for _, n := range names {
		assert.False(t, strings.HasPrefix(n, "chunk.bin_"),
			"no orphan may be left under the server's renamed form: %v", names)
	}
}

// TestProbeLosslessUpdateKeepsContentOnRenamedUpload covers Update's lossless
// contract on the real server: the old file is parked first, the new content
// is uploaded, and the target ends up holding the new content with no backup
// left behind. It also exercises the path where the server stores the upload
// under a suffixed name and Update has to land it.
func TestProbeLosslessUpdateKeepsContentOnRenamedUpload(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFs(t, "TestYun139:")
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	probePut(ctx, t, f, base, "upd.txt", "OLD-CONTENT")
	probeSettle(ctx, t, f, base+"/upd.txt")
	old, err := f.NewObject(ctx, base+"/upd.txt")
	require.NoError(t, err)

	const payload = "NEW-CONTENT-LONGER-THAN-BEFORE"
	src := fsobject.NewStaticObjectInfo(base+"/upd.txt", time.Now(), int64(len(payload)), true, nil, nil)
	require.NoError(t, old.Update(ctx, strings.NewReader(payload), src), "update must succeed")

	got := probeWaitSize(ctx, t, f, base+"/upd.txt", int64(len(payload)))
	assert.EqualValues(t, len(payload), got, "the target must hold the new content")

	// No backup may linger next to it.
	entries, err := f.List(ctx, base)
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, path.Base(e.Remote()))
	}
	for _, n := range names {
		assert.NotContains(t, n, "rclone-old-",
			"a completed update must leave no backup behind: %v", names)
	}
}

// TestProbeRenameOntoHeldNameReportsFailure covers the non-overwrite rule on
// the real server: renaming a file onto a name the directory already holds is
// answered with success, but the server keeps the existing entry and stores the
// renamed one under a suffixed name. renameObject must report that as a
// failure, so callers do not believe the requested name was taken.
func TestProbeRenameOntoHeldNameReportsFailure(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFs(t, "TestYun139:")
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	holder := probePut(ctx, t, f, base, "holder.bin", "HOLDER-CONTENT")
	probeSettle(ctx, t, f, base+"/holder.bin")
	victim := probePut(ctx, t, f, base, "victim.bin", "VICTIM-CONTENT-LONGER-THAN-HOLDER")
	probeSettle(ctx, t, f, base+"/victim.bin")

	_, dirID, err := f.dirCache.FindPath(ctx, base+"/victim.bin", false)
	require.NoError(t, err)

	err = f.renameObject(ctx, victim.id, "holder.bin", dirID, false)
	require.Error(t, err, "a rename the server parked under another name must report failure")
	assert.Contains(t, err.Error(), "server stored it as")

	// The holder must be untouched and still carry its own id.
	got, gerr := f.NewObject(ctx, base+"/holder.bin")
	require.NoError(t, gerr)
	assert.Equal(t, holder.id, got.(*Object).id, "the held entry must not be replaced")
}

// TestProbeRenameToOwnNameSucceeds covers the other side of the same rule on
// the real server: renaming an entry to the name it already has is echoed back
// unchanged, so the check must not fire.
func TestProbeRenameToOwnNameSucceeds(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFs(t, "TestYun139:")
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	obj := probePut(ctx, t, f, base, "self.bin", "SELF-CONTENT")
	probeSettle(ctx, t, f, base+"/self.bin")

	_, dirID, err := f.dirCache.FindPath(ctx, base+"/self.bin", false)
	require.NoError(t, err)
	require.NoError(t, f.renameObject(ctx, obj.id, "self.bin", dirID, false),
		"renaming to the name the entry already has must succeed")
}

// TestProbeExtensionAppendIsFamilyOnly pins the rename-name rule that decides
// whether a stored name counts as landed. A family rename appends the source's
// extension whenever the requested name does not already end with it, so a
// request for the free dotless name noext stores noext.bin and the reply
// reports that name: the rename is refused, because rclone records the path it
// was asked for and a lookup of noext would find nothing. The personal space
// stores the requested name verbatim, so the same rename lands.
func TestProbeExtensionAppendIsFamilyOnly(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	base := probeBase()

	t.Run("family refuses the appended name", func(t *testing.T) {
		f := probeFamilyFs(t)
		dir := base + "-fam"
		defer func() { _ = operations.Purge(ctx, f, dir) }()
		require.NoError(t, f.Mkdir(ctx, dir))

		src := probePut(ctx, t, f, dir, "victim.bin", "FAMILY-CONTENT")
		_, dirID, err := f.dirCache.FindPath(ctx, dir+"/victim.bin", false)
		require.NoError(t, err)

		err = f.renameObject(ctx, src.id, "noext", dirID, true)
		require.Error(t, err,
			"a family rename stored under the appended name must be reported, not accepted")
		assert.Contains(t, err.Error(), "server stored it as")

		ents := probeListNames(ctx, t, f, dir)
		assert.True(t, hasEntry(ents, dir+"/noext.bin"),
			"the server stores the appended name: %v", ents)
	})

	t.Run("personal stores the requested name verbatim", func(t *testing.T) {
		f := probeFs(t, "TestYun139:")
		dir := base + "-per"
		defer func() { _ = operations.Purge(ctx, f, dir) }()
		require.NoError(t, f.Mkdir(ctx, dir))

		src := probePut(ctx, t, f, dir, "src.bin", "PERSONAL-CONTENT")
		probeSettle(ctx, t, f, dir+"/src.bin")
		_, dirID, err := f.dirCache.FindPath(ctx, dir+"/src.bin", false)
		require.NoError(t, err)

		require.NoError(t, f.renameObject(ctx, src.id, "personal-noext", dirID, false),
			"the personal space stores the requested name, which is a landing")
		ents := probeListNames(ctx, t, f, dir)
		assert.True(t, hasEntry(ents, dir+"/personal-noext"),
			"the personal space does not append an extension: %v", ents)
	})
}

// TestProbeMoveWithinDirectory covers a move whose source and destination
// share a parent directory. The server rejects the batchMove form outright,
// so the backend must rename in place.
func TestProbeMoveWithinDirectory(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFs(t, "TestYun139:")
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	src := probePut(ctx, t, f, base, "one.txt", "content-one")
	probeSettle(ctx, t, f, base+"/one.txt")

	newObj, err := f.Move(ctx, src, base+"/one-renamed.txt")
	require.NoError(t, err, "same-directory move must not fail")
	require.NotNil(t, newObj)
	assert.Equal(t, base+"/one-renamed.txt", newObj.Remote())

	_, err = f.NewObject(ctx, base+"/one.txt")
	assert.Error(t, err, "old name should be gone")

	got, err := f.NewObject(ctx, base+"/one-renamed.txt")
	require.NoError(t, err, "renamed object should exist")
	assert.EqualValues(t, len("content-one"), got.Size())
}

// TestProbeMoveAcrossDirectories covers the ordinary cross-directory move,
// which goes through the batchMove task.
func TestProbeMoveAcrossDirectories(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFs(t, "TestYun139:")
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	require.NoError(t, f.Mkdir(ctx, base+"/sub"))
	probePut(ctx, t, f, base, "two.txt", "content-two")
	probeSettle(ctx, t, f, base+"/two.txt")

	o, err := f.NewObject(ctx, base+"/two.txt")
	require.NoError(t, err)

	newObj, err := f.Move(ctx, o, base+"/sub/two.txt")
	require.NoError(t, err, "cross-directory move must not fail")
	require.NotNil(t, newObj)

	_, err = f.NewObject(ctx, base+"/two.txt")
	assert.Error(t, err, "source name should be gone after the move")
	got, err := f.NewObject(ctx, base+"/sub/two.txt")
	require.NoError(t, err, "moved object should exist at the destination")
	assert.EqualValues(t, len("content-two"), got.Size())
}

// TestProbeMoveNoOp covers a move onto the identical path: the server
// rejects it as a batchMove too, so the backend must short-circuit.
func TestProbeMoveNoOp(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFs(t, "TestYun139:")
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	probePut(ctx, t, f, base, "three.txt", "content-three")
	probeSettle(ctx, t, f, base+"/three.txt")

	o, err := f.NewObject(ctx, base+"/three.txt")
	require.NoError(t, err)

	_, err = f.Move(ctx, o, base+"/three.txt")
	require.NoError(t, err, "no-op move must not fail")

	got, err := f.NewObject(ctx, base+"/three.txt")
	require.NoError(t, err, "file must survive a no-op move")
	assert.EqualValues(t, len("content-three"), got.Size())
}

// TestProbeMoveOntoExistingName pins the server's non-overwrite rule: a
// rename onto a name the directory already holds does NOT replace it - the
// server keeps the existing entry and parks the moved file under a
// timestamped name. Move must therefore fail loudly rather than return the
// stale object at the destination.
//
// The ordinary overwrite path is driven by operations.move(), which deletes
// the destination before calling Move; see TestProbeMoveOverwriteViaEngine.
func TestProbeMoveOntoExistingName(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFs(t, "TestYun139:")
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	probePut(ctx, t, f, base, "four.txt", "AAAA")
	probePut(ctx, t, f, base, "four-target.txt", "BBBBBBBBBB")
	probeSettle(ctx, t, f, base+"/four.txt")
	probeSettle(ctx, t, f, base+"/four-target.txt")

	src, err := f.NewObject(ctx, base+"/four.txt")
	require.NoError(t, err)
	srcID := src.(*Object).id

	_, err = f.Move(ctx, src, base+"/four-target.txt")
	require.Error(t, err, "a rename onto an existing name must be reported, not silently accepted")

	// the pre-existing entry is untouched
	dst, err := f.NewObject(ctx, base+"/four-target.txt")
	require.NoError(t, err, "the original destination must survive")
	assert.EqualValues(t, 10, dst.(*Object).Size(), "destination must keep its own content")
	assert.NotEqual(t, srcID, dst.(*Object).id, "destination must not be the moved file")
}

// TestProbeMoveOverwriteViaEngine covers the shape rclone actually drives:
// operations.move() deletes the destination first, so the backend rename
// finds the name free and the move succeeds.
func TestProbeMoveOverwriteViaEngine(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFs(t, "TestYun139:")
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	probePut(ctx, t, f, base, "five.txt", "AAAA")
	probePut(ctx, t, f, base, "five-target.txt", "BBBBBBBBBB")
	probeSettle(ctx, t, f, base+"/five.txt")
	probeSettle(ctx, t, f, base+"/five-target.txt")

	require.NoError(t, operations.MoveFile(ctx, f, f, base+"/five-target.txt", base+"/five.txt"))

	got := probeWaitSize(ctx, t, f, base+"/five-target.txt", 4)
	assert.EqualValues(t, 4, got, "destination should hold the source content")
	_, err := f.NewObject(ctx, base+"/five.txt")
	assert.Error(t, err, "source name should be gone after the move")
}
