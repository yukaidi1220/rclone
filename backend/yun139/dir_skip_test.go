package yun139

import (
	"context"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	fserrors "github.com/rclone/rclone/fs/fserrors"
	fsobject "github.com/rclone/rclone/fs/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureLogs routes rclone's logger into a buffer for the duration of the
// test. The extractor under test reads the migration's copy log, so the line
// has to be observed as it is actually emitted, not rebuilt from the format
// string.
func captureLogs(t *testing.T) *strings.Builder {
	t.Helper()
	var buf strings.Builder
	fs.SetLogger(slog.NewTextHandler(&buf, nil))
	t.Cleanup(func() { fs.SetLogger(slog.NewTextHandler(io.Discard, nil)) })
	return &buf
}

// executorExtractor is the pipeline the migration executor runs over copy.log:
//
//	grep 'SKIP ' copy.log | sed -E 's/.*SKIP ([^:]+):.*/\1/'
//
// Replaying it here is what makes a passed test mean "the exclude file gets
// this path", not "the backend printed something".
func executorExtractor(t *testing.T, logged string) string {
	t.Helper()
	m := regexp.MustCompile(`.*SKIP ([^:]+):.*`).FindStringSubmatch(logged)
	require.Len(t, m, 2, "no SKIP line survived the executor's extractor in %q", logged)
	return m[1]
}

// TestUnitGroupSensitiveWordPutNamesParentInSkipLine is the shape production
// produced: --size-only leaves the refused subtree's files to be re-attempted
// every round, each file's parent directories are created through dirCache,
// which never enters Mkdir, so nothing named the subtree and the verify step
// reported every file below it as missing from the destination.
func TestUnitGroupSensitiveWordPutNamesParentInSkipLine(t *testing.T) {
	logs := captureLogs(t)

	tr := &groupTransport{dirs: gtestDirs(), sensitiveOnCreate: true}
	f := newGroupTestFs(tr)

	// Positive control: the capture and the extractor see the one SKIP line
	// this backend has always emitted. Without this, a silent logger would let
	// every assertion below pass on an empty buffer.
	srcEmpty := fsobject.NewStaticObjectInfo("empty.txt", time.Now(), 0, true, nil, nil)
	_, err := f.Put(context.Background(), strings.NewReader(""), srcEmpty)
	require.Error(t, err)
	require.Contains(t, logs.String(), "SKIP empty.txt: this remote cannot store empty files")
	require.Equal(t, "empty.txt", executorExtractor(t, logs.String()))
	logs.Reset()

	remote := "blocked-name/sub/x.txt"
	src := fsobject.NewStaticObjectInfo(remote, time.Now(), 4, true, nil, nil)
	_, err = f.Put(context.Background(), strings.NewReader("data"), src)
	require.Error(t, err, "a refused parent directory must fail the upload")

	got := executorExtractor(t, logs.String())
	assert.Equal(t, "blocked-name/sub/", got, "the exclude must name the object's parent directory")

	assert.True(t, fserrors.IsNoRetryError(err),
		"the refusal must not be re-run by the retry ladder: %v", err)
	assert.Contains(t, err.Error(), groupSensitiveWordErrorCode)

	// One file, one line: the executor excludes the parent of every file that
	// fails, so the union is exact and no wider pattern is printed.
	assert.Equal(t, 1, strings.Count(logs.String(), "SKIP "), "each failing file prints exactly one SKIP line")
}

// TestUnitGroupSensitiveWordUpdateNamesParentInSkipLine covers the re-upload
// path. An object whose size changed is replaced through Update, and Update
// creates the parent directories on its own, so it needs the same recognition.
func TestUnitGroupSensitiveWordUpdateNamesParentInSkipLine(t *testing.T) {
	logs := captureLogs(t)

	tr := &groupTransport{dirs: gtestDirs(), sensitiveOnCreate: true}
	f := newGroupTestFs(tr)

	o := &Object{fs: f, remote: "blocked-name/sub/z.txt", id: "ZZZ", hashMu: &sync.Mutex{}, urlMu: &sync.Mutex{}}
	src := fsobject.NewStaticObjectInfo("blocked-name/sub/z.txt", time.Now(), 4, true, nil, nil)
	err := o.Update(context.Background(), strings.NewReader("data"), src)
	require.Error(t, err, "a refused parent directory must fail the re-upload")

	assert.Equal(t, "blocked-name/sub/", executorExtractor(t, logs.String()))
	assert.True(t, fserrors.IsNoRetryError(err), "the refusal must not be re-run: %v", err)
	assert.Equal(t, 1, strings.Count(logs.String(), "SKIP "))
}

// TestUnitGroupSensitiveWordChunkWriterNamesParentInSkipLine covers the
// multi-thread copy engine. Above --multi-thread-cutoff a file never reaches
// Put, so the chunk path needs the same recognition or those files reappear as
// missing from the destination.
func TestUnitGroupSensitiveWordChunkWriterNamesParentInSkipLine(t *testing.T) {
	logs := captureLogs(t)

	tr := &groupTransport{dirs: gtestDirs(), sensitiveOnCreate: true}
	f := newGroupTestFs(tr)

	remote := "blocked-name/sub/big.bin"
	src := fsobject.NewStaticObjectInfo(remote, time.Now(), 1<<10, true, nil, nil)
	_, _, err := f.OpenChunkWriter(context.Background(), remote, src)
	require.Error(t, err, "a refused parent directory must fail the chunk writer")

	assert.Equal(t, "blocked-name/sub/", executorExtractor(t, logs.String()))
	assert.True(t, fserrors.IsNoRetryError(err), "the refusal must not be re-run: %v", err)
}

// TestUnitGroupSensitiveWordPersonalSpaceStaysQuiet keeps the recognition bound
// to the space that produces this refusal. A personal-cloud remote has no group
// filter, so an unrelated error must reach the caller unchanged and silently.
func TestUnitGroupSensitiveWordPersonalSpaceStaysQuiet(t *testing.T) {
	logs := captureLogs(t)

	f := newGroupTestFs(&groupTransport{dirs: gtestDirs()})
	f.space = spacePersonal

	in := newAPIErrorForTest(groupSensitiveWordErrorCode)
	err := f.groupDirSensitiveSkip(in, "blocked-name/sub")
	assert.Equal(t, in, err, "outside the group space the error must pass through untouched")
	assert.NotContains(t, logs.String(), "SKIP", "the group's answer must not be claimed for another space")
}

// TestUnitGroupSensitiveWordOtherCodeStaysQuiet is the other half of the
// boundary: inside the group space, a different refusal is not this one.
func TestUnitGroupSensitiveWordOtherCodeStaysQuiet(t *testing.T) {
	logs := captureLogs(t)

	f := newGroupTestFs(&groupTransport{dirs: gtestDirs()})
	require.Equal(t, spaceGroup, f.space)

	in := newAPIErrorForTest(sensitiveFileNameErrorCode)
	err := f.groupDirSensitiveSkip(in, "blocked-name/sub")
	assert.Equal(t, in, err, "only the directory refusal is recognised here")
	assert.NotContains(t, logs.String(), "SKIP")

	other := newAPIErrorForTest("9999999999")
	assert.Equal(t, other, f.groupDirSensitiveSkip(other, "blocked-name/sub"))
}

// TestUnitGroupSensitiveWordAtRemoteRootNamesNoPattern guards the one dir value
// that would turn a helpful exclude into a blanket one: a file sitting at the
// root has "." as its parent, and "SKIP ./..." would exclude every object in
// the check. The refusal must still stop retrying, and must not print.
func TestUnitGroupSensitiveWordAtRemoteRootNamesNoPattern(t *testing.T) {
	logs := captureLogs(t)

	f := newGroupTestFs(&groupTransport{dirs: gtestDirs()})
	in := newAPIErrorForTest(groupSensitiveWordErrorCode)

	for _, dir := range []string{".", ""} {
		logs.Reset()
		err := f.groupDirSensitiveSkip(in, dir)
		assert.True(t, fserrors.IsNoRetryError(err), "still no retry for dir %q", dir)
		assert.NotContains(t, logs.String(), "SKIP", "dir %q would exclude the whole remote", dir)
	}
}

// newAPIErrorForTest builds the typed refusal the recognition path unwraps.
func newAPIErrorForTest(code string) error {
	return &apiError{Code: code, Message: "敏感词校验失败"}
}
