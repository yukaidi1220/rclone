package yun139

import (
	"context"

	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Live protocol probes for the family space and for server-side copy. The
// family space has no native move, so its Move is a copy task plus a delete
// task, and the copy task is what produces the entry the move must then
// rename. These shapes cannot be expressed by fstests.
//
// Without YUN139_PROBE set they skip instantly, so CI and `go test ./...`
// stay hermetic.

// probeListNames lists dir (relative to base) as "name(size)" strings, so a
// probe can assert on the entries a failed operation left behind.
func probeListNames(ctx context.Context, t *testing.T, f *Fs, dir string) []string {
	t.Helper()
	entries, err := f.List(ctx, dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		out = append(out, fmt.Sprintf("%s(%d)", e.Remote(), e.Size()))
	}
	return out
}

// hasEntry reports whether a probeListNames result holds the given remote,
// ignoring the trailing size annotation the helper appends.
func hasEntry(ents []string, remote string) bool {
	for _, e := range ents {
		if strings.HasPrefix(e, remote+"(") {
			return true
		}
	}
	return false
}

// probeReadAll reads an object's whole content, so a probe can assert the
// bytes survived a move rather than just its size.
func probeReadAll(ctx context.Context, t *testing.T, o fs.Object) string {
	t.Helper()
	rc, err := operations.Open(ctx, o)
	require.NoError(t, err)
	defer func() { _ = rc.Close() }()
	buf := new(strings.Builder)
	_, err = io.Copy(buf, rc)
	require.NoError(t, err)
	return buf.String()
}

// TestProbeFamilyMoveShapes covers the family-space move end to end: a move
// into a subdirectory (the returned object must carry the directory, not
// just the leaf), a rename to a free name, a no-op, and a move onto a name
// the destination already holds. The last one must be refused with the
// source intact and no orphan copy left behind - the family copy task parks
// its result under a suffixed name when the target name is taken, and
// deleting the source then would lose the content.
func TestProbeFamilyMoveShapes(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFamilyFs(t)
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	require.NoError(t, f.Mkdir(ctx, base+"/sub"))

	put := func(remote, content string) *Object {
		o := probePut(ctx, t, f, base, remote, content)
		probeSettle(ctx, t, f, base+"/"+remote)
		return o
	}

	// A move into a subdirectory: the returned object must name the
	// subdirectory, and the file must really be there.
	t.Run("into subdirectory", func(t *testing.T) {
		src := put("a.txt", "AAAA")
		got, err := f.Move(ctx, src, base+"/sub/a.txt")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, base+"/sub/a.txt", got.Remote(),
			"the returned object must carry the destination directory")
		obj, err := f.NewObject(ctx, base+"/sub/a.txt")
		require.NoError(t, err, "the file must exist at the destination")
		assert.Equal(t, "AAAA", probeReadAll(ctx, t, obj))
		_, err = f.NewObject(ctx, base+"/a.txt")
		assert.Error(t, err, "the source name must be gone")
	})

	// A same-directory rename to a free name.
	t.Run("rename to a free name", func(t *testing.T) {
		src := put("b.txt", "BBBB")
		got, err := f.Move(ctx, src, base+"/b-renamed.txt")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, base+"/b-renamed.txt", got.Remote())
		obj, err := f.NewObject(ctx, base+"/b-renamed.txt")
		require.NoError(t, err)
		assert.Equal(t, "BBBB", probeReadAll(ctx, t, obj))
		_, err = f.NewObject(ctx, base+"/b.txt")
		assert.Error(t, err, "the old name must be gone")
	})

	// A no-op move onto the identical path must not duplicate the file.
	t.Run("no-op", func(t *testing.T) {
		src := put("c.txt", "CCCC")
		got, err := f.Move(ctx, src, base+"/c.txt")
		require.NoError(t, err)
		assert.Same(t, src, got)
		obj, err := f.NewObject(ctx, base+"/c.txt")
		require.NoError(t, err)
		assert.Equal(t, "CCCC", probeReadAll(ctx, t, obj))
	})

	// A move onto a taken name must be refused, the source and the
	// pre-existing entry must both survive, and no orphan may remain.
	t.Run("onto a taken name", func(t *testing.T) {
		src := put("d.txt", "DDDD")
		put("d-taken.txt", "TAKEN-ENTRY")
		before := probeListNames(ctx, t, f, base)

		got, err := f.Move(ctx, src, base+"/d-taken.txt")
		require.Error(t, err, "a move that cannot land must be reported")
		assert.Nil(t, got)

		after := probeListNames(ctx, t, f, base)
		assert.Equal(t, len(before), len(after),
			"no orphan entry may be left behind (before=%v after=%v)", before, after)
		obj, err := f.NewObject(ctx, base+"/d.txt")
		require.NoError(t, err, "the source must survive")
		assert.Equal(t, "DDDD", probeReadAll(ctx, t, obj))
		taken, err := f.NewObject(ctx, base+"/d-taken.txt")
		require.NoError(t, err, "the pre-existing entry must survive")
		assert.Equal(t, "TAKEN-ENTRY", probeReadAll(ctx, t, taken))
	})
}

// TestProbeCopyShapes covers server-side copy in the space the remote names:
// a copy into a subdirectory under a new leaf must land under that leaf and
// carry the destination directory, and it must leave the source in place.
func TestProbeCopyShapes(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFs(t, probeRemote())
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	require.NoError(t, f.Mkdir(ctx, base+"/sub"))

	src := probePut(ctx, t, f, base, "src.txt", "COPY")
	probeSettle(ctx, t, f, base+"/src.txt")

	got, err := f.Copy(ctx, src, base+"/sub/copied.txt")
	require.NoError(t, err, "a copy to a free name must succeed")
	require.NotNil(t, got)
	assert.Equal(t, base+"/sub/copied.txt", got.Remote(),
		"the copy must carry the destination directory")
	assert.NotEqual(t, src.id, got.(*Object).id, "the copy is a new entry")

	var found fs.Object
	for i := 0; i < 15; i++ {
		if found, err = f.NewObject(ctx, base+"/sub/copied.txt"); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	require.NoError(t, err, "the copy must exist at the destination")
	assert.EqualValues(t, 4, found.Size())
	_, err = f.NewObject(ctx, base+"/src.txt")
	assert.NoError(t, err, "a copy must leave the source in place")
}

// TestProbeCopyOntoTakenName covers the destination name collision on a
// server-side copy. The server never overwrites: a batch copy whose
// destination name is taken is auto-renamed, so the backend refuses the
// server-side path and the engine replaces the entry with a bandwidth copy.
// The copy must land under the requested name with the source's content, the
// pre-existing entry must be replaced rather than duplicated, and nothing
// may be left under an auto-renamed name.
//
// The engine is driven rather than Fs.Copy directly, because the fallback is
// what makes the overwrite work; the point is the user-visible result.
func TestProbeCopyOntoTakenName(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFamilyFs(t)
	base := probeBase()
	defer func() { _ = operations.Purge(ctx, f, base) }()

	require.NoError(t, f.Mkdir(ctx, base))
	require.NoError(t, f.Mkdir(ctx, base+"/sub"))

	probePut(ctx, t, f, base, "src.txt", "SOURCECONTENT")
	probeSettle(ctx, t, f, base+"/src.txt")
	probePut(ctx, t, f, base, "sub/taken.txt", "OLDCONTENT")
	probeSettle(ctx, t, f, base+"/sub/taken.txt")

	// Re-resolve both objects from a listing, which is how the engine
	// obtains them: the object a Put returns carries no server-side path,
	// and the bandwidth fallback needs the source's.
	srcObj, err := f.NewObject(ctx, base+"/src.txt")
	require.NoError(t, err)
	src := srcObj.(*Object)
	dst, err := f.NewObject(ctx, base+"/sub/taken.txt")
	require.NoError(t, err, "the destination must exist before the copy")

	got, err := operations.Copy(ctx, f, dst, base+"/sub/taken.txt", src)
	require.NoError(t, err, "copying onto a taken name must succeed")
	require.NotNil(t, got)

	// The destination must carry the source's content under the requested
	// name and nothing else may appear.
	var names []string
	for i := 0; i < 15; i++ {
		names = probeListNames(ctx, t, f, base+"/sub")
		if len(names) == 1 {
			break
		}
		time.Sleep(2 * time.Second)
	}
	require.Len(t, names, 1, "the destination must hold exactly one entry, with no auto-renamed orphan: %v", names)
	assert.Contains(t, names[0], "taken.txt", "the entry must carry the requested name")
	obj, err := f.NewObject(ctx, base+"/sub/taken.txt")
	require.NoError(t, err)
	assert.Equal(t, "SOURCECONTENT", probeReadAll(ctx, t, obj),
		"the destination must carry the source's content")
	_, err = f.NewObject(ctx, base+"/src.txt")
	assert.NoError(t, err, "a copy must leave the source in place")
}

// TestProbeFamilyDirDeleteShapes covers removing family directories, which
// the purge path drives: a nested tree is removed child-first, so each
// directory delete must be addressed correctly. A delete the server refuses
// would leave the tree behind while reporting success.
func TestProbeFamilyDirDeleteShapes(t *testing.T) {
	probeSkip(t)
	ctx := context.Background()
	f := probeFamilyFs(t)
	base := probeBase()

	require.NoError(t, f.Mkdir(ctx, base))
	require.NoError(t, f.Mkdir(ctx, base+"/outer"))
	require.NoError(t, f.Mkdir(ctx, base+"/outer/inner"))
	probePut(ctx, t, f, base, "outer/a.txt", "AAAA")
	probeSettle(ctx, t, f, base+"/outer/a.txt")
	probePut(ctx, t, f, base, "outer/inner/b.txt", "BBBB")
	probeSettle(ctx, t, f, base+"/outer/inner/b.txt")

	// Purge removes the whole tree: directories are deleted after their
	// children, and the tree must really be gone.
	require.NoError(t, operations.Purge(ctx, f, base), "purge must remove the tree")

	var err error
	for i := 0; i < 15; i++ {
		if _, err = f.NewObject(ctx, base); err != nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	assert.Error(t, err, "the purged tree must be gone")
}
