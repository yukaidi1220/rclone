package yun139

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	fsobject "github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/require"
)

// TestProbeUpdateFailureRestoresLeaf covers the restore arm of Update on a
// family file: the pre-rename parks the old content, the upload fails, and the
// old content must come back under the name the user asked for instead of
// staying parked. The family space appends the renamed entry's own extension to
// a requested name that does not end with it, so the backup name and the
// restore both have to keep the leaf's extension - a dotless leaf and a leaf
// with an extension take different shapes.
func TestProbeUpdateFailureRestoresLeaf(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFamilyFs(t)

	for _, leaf := range []string{"plain", "movie.mkv"} {
		dir := fmt.Sprintf("%s-fail-%s", probeBase(), leaf)
		require.NoError(t, f.Mkdir(ctx, dir))
		func() {
			defer func() { _ = operations.Purge(ctx, f, dir) }()

			obj := probePut(ctx, t, f, dir, leaf, "OLD-CONTENT")
			t.Logf("[%s] initial => %v", leaf, probeListNames(ctx, t, f, dir))

			const newSize = 20
			src := fsobject.NewStaticObjectInfo(dir+"/"+leaf, time.Now(), newSize, true, nil, nil)
			uerr := obj.Update(ctx, io.Reader(failingReader{}), src)
			t.Logf("[%s] Update err=%v", leaf, uerr)
			require.Error(t, uerr, "a failing upload must be reported")

			// The restore is issued before Update returns; give the server a
			// moment to apply it, then look for the old content.
			var lastErr error
			var ents []string
			for i := 0; i < 3; i++ {
				time.Sleep(2 * time.Second)
				ents = probeListNames(ctx, t, f, dir)
				if _, lastErr = f.NewObject(ctx, dir+"/"+leaf); lastErr == nil {
					break
				}
			}
			t.Logf("[%s] after failed update => %v", leaf, ents)
			require.NoError(t, lastErr,
				"[%s] the old content must be back at the requested name, server holds %v", leaf, ents)

			got, gerr := f.NewObject(ctx, dir+"/"+leaf)
			require.NoError(t, gerr)
			require.EqualValues(t, int64(len("OLD-CONTENT")), got.Size(),
				"[%s] the restored entry must be the old content", leaf)
			assertNoBackupLeftovers(t, ents)
		}()
	}
}

// assertNoBackupLeftovers fails when a completed/aborted update left a parked
// entry behind under a name the caller never asked for.
func assertNoBackupLeftovers(t *testing.T, ents []string) {
	t.Helper()
	for _, n := range ents {
		if isBackupOf(n, "plain") || isBackupOf(n, "movie.mkv") {
			t.Errorf("a backup entry was left behind: %v", ents)
			return
		}
	}
}
