package wopan

import (
	"context"
	"testing"

	"github.com/rclone/rclone/fs/fserrors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnitPrepareDirectoryRefused locks the hook fs/operations/copy now calls
// before opening a source object: a directory the server refuses has to surface
// here with exactly the message the migration judges grep, stay marked no-retry,
// and cost one CreateDirectory for the whole subtree instead of one per file.
func TestUnitPrepareDirectoryRefused(t *testing.T) {
	ctx := context.Background()
	tr := newDirTransport()
	tr.create = func(*dirRejectTransport, string, string) (string, string) {
		return codeSensitiveWord, ""
	}
	f := newUnitTestFs(tr)

	for _, name := range []string{"blocked-name/a.txt", "blocked-name/b.txt", "blocked-name/c.txt"} {
		err := f.PrepareDirectory(ctx, name)
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "failed to make directory", name)
		assert.True(t, fserrors.IsNoRetryError(err), "refusal must stay no-retry: %v", err)
	}
	assert.Equal(t, 1, tr.creates, "3 files under one refused directory must send one CreateDirectory")
}

// TestUnitPrepareDirectoryAccepted locks the two shapes that must not break:
// a file sitting in the root (its parent is "." on the way in, "" for dircache)
// and a directory that repeats, which must be served from the cache.
func TestUnitPrepareDirectoryAccepted(t *testing.T) {
	ctx := context.Background()
	tr := newDirTransport()
	// The model only creates when the test hands it a creator; empty code means
	// "accepted" and the second return value is the new directory id.
	tr.create = func(_ *dirRejectTransport, _, name string) (string, string) {
		return "", "D-" + name
	}
	f := newUnitTestFs(tr)

	require.NoError(t, f.PrepareDirectory(ctx, "toplevel.txt"))
	assert.Equal(t, 0, tr.creates, "root needs no CreateDirectory")

	require.NoError(t, f.PrepareDirectory(ctx, "ok-dir/a.txt"))
	after := tr.creates
	require.NoError(t, f.PrepareDirectory(ctx, "ok-dir/b.txt"))
	assert.Equal(t, after, tr.creates, "a known directory must not be created twice")
}

// TestUnitPrepareDirectoryNonAbsorbedCode answers the one judge risk raised in
// review: a mkdir refusal that is NOT RSP_CODE=4444/1009 (for example no-space)
// must still come back through the same "failed to make directory" wrapper, so
// moving it earlier in the copy path cannot silently change how the migration
// judges classify it.
func TestUnitPrepareDirectoryNonAbsorbedCode(t *testing.T) {
	ctx := context.Background()
	tr := newDirTransport()
	tr.create = func(*dirRejectTransport, string, string) (string, string) {
		return "2001", ""
	}
	f := newUnitTestFs(tr)

	err := f.PrepareDirectory(ctx, "no-space-dir/a.txt")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to make directory")
	assert.Contains(t, err.Error(), "RSP_CODE=2001")
	assert.True(t, fserrors.IsNoRetryError(err), "no-space must stay no-retry: %v", err)
}
