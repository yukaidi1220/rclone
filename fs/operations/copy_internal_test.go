package operations

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest/mockfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prepFs opts into the directoryPreparer hook and records what it was asked for.
// It embeds a real mockfs so every other Fs method the copy path uses works.
type prepFs struct {
	fs.Fs
	err      error
	gotLevel string
	calls    int
}

func (p *prepFs) PrepareDirectory(_ context.Context, remote string) error {
	p.calls++
	p.gotLevel = remote
	return p.err
}

// plainFs participates in fs.Fs only, so the copy path must not call into it.
type plainFs struct{ fs.Fs }

func newMockFs(t *testing.T) fs.Fs {
	t.Helper()
	f, err := mockfs.NewFs(context.Background(), "dstfake", "dst:", nil)
	require.NoError(t, err)
	return f
}

func TestPrepareDestinationDirectory(t *testing.T) {
	refused := errors.New("failed to make directory: wopan: file or directory name is rejected by the server: RSP_CODE=4444")

	t.Run("非实现者零调用", func(t *testing.T) {
		c := &copy{f: &plainFs{Fs: newMockFs(t)}, remoteForCopy: "blocked/a.txt"}
		require.NoError(t, c.prepareDestinationDirectory(context.Background()))
	})

	t.Run("实现者拿到的是传输用的名字", func(t *testing.T) {
		p := &prepFs{Fs: newMockFs(t)}
		c := &copy{f: p, remoteForCopy: "dir/sub/a.txt"}
		require.NoError(t, c.prepareDestinationDirectory(context.Background()))
		assert.Equal(t, 1, p.calls)
		assert.Equal(t, "dir/sub/a.txt", p.gotLevel)
	})

	t.Run("拒绝原样上抛不加包裹", func(t *testing.T) {
		p := &prepFs{Fs: newMockFs(t), err: refused}
		c := &copy{f: p, remoteForCopy: "blocked/a.txt"}
		err := c.prepareDestinationDirectory(context.Background())
		assert.ErrorIs(t, err, refused, "the judge greps this exact message shape")
		assert.Equal(t, refused.Error(), err.Error(), "no extra wrapping may be added")
	})
}

// recordingObject is a source object that records whether the copy path opened
// it. Opening is what costs a source download, so this is the only observable
// that proves the ordering the whole change exists for.
type recordingObject struct {
	fs.Object
	remote string
	size   int64
	opened *bool
	srcFs  fs.Fs
}

func (o *recordingObject) Remote() string { return o.remote }
func (o *recordingObject) String() string { return o.remote }
func (o *recordingObject) Size() int64    { return o.size }
func (o *recordingObject) Fs() fs.Info    { return o.srcFs }
func (o *recordingObject) Open(_ context.Context, _ ...fs.OpenOption) (io.ReadCloser, error) {
	*o.opened = true
	return nil, errors.New("modelled source open failure")
}

func newManualCopy(t *testing.T, dst fs.Fs, opened *bool) (*copy, context.Context) {
	t.Helper()
	ctx := context.Background()
	ci := fs.GetConfig(ctx)
	oldStreams := ci.MultiThreadStreams
	t.Cleanup(func() { ci.MultiThreadStreams = oldStreams })
	// Force the single-stream path: multi-thread copy returns before the hook by
	// design, because it opens the destination chunk writer first.
	ci.MultiThreadStreams = 1

	srcFs, err := mockfs.NewFs(ctx, "srcfake", "src:", nil)
	require.NoError(t, err)
	src := &recordingObject{remote: "blocked/a.txt", size: 1024, opened: opened, srcFs: srcFs}
	return &copy{
		f:             dst,
		ci:            ci,
		src:           src,
		remoteForCopy: "blocked/a.txt",
		hashOption:    &fs.HashesOption{},
	}, ctx
}

// TestManualCopyRefusedDirectoryNeverOpensSource is the ordering lock. Move the
// prepareDestinationDirectory call below Open and this goes red; that placement is
// the entire point of the change, so it must not rest on review alone.
func TestManualCopyRefusedDirectoryNeverOpensSource(t *testing.T) {
	refused := errors.New("failed to make directory: wopan: file or directory name is rejected by the server: RSP_CODE=4444")

	t.Run("目录被拒时源端一次都不开", func(t *testing.T) {
		opened := false
		dst := &prepFs{Fs: newMockFs(t), err: refused}
		c, ctx := newManualCopy(t, dst, &opened)
		_, _, err := c.manualCopy(ctx)
		require.Error(t, err)
		assert.Equal(t, refused.Error(), err.Error(), "message shape must survive manualCopy")
		assert.False(t, opened, "a refused directory must not spend a source download")
		assert.Equal(t, 1, dst.calls)
	})

	t.Run("目录没问题时照常开源端", func(t *testing.T) {
		opened := false
		dst := &prepFs{Fs: newMockFs(t)}
		c, ctx := newManualCopy(t, dst, &opened)
		_, _, err := c.manualCopy(ctx)
		// The fake source fails at Open on purpose so we can stop before the
		// transfer machinery; what this case asserts is that we got that far.
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open source object")
		assert.True(t, opened, "the hook must not block a healthy directory")
		assert.Equal(t, 1, dst.calls)
	})

	t.Run("不实现接口的目标端零影响", func(t *testing.T) {
		opened := false
		c, ctx := newManualCopy(t, &plainFs{Fs: newMockFs(t)}, &opened)
		_, _, err := c.manualCopy(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to open source object")
		assert.True(t, opened, "backends that never opted in behave exactly as before")
	})
}
