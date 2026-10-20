package yun139

import (
	"context"
	"testing"
	"time"

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
