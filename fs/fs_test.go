package fs

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/rclone/rclone/fs/fserrors"
	"github.com/stretchr/testify/assert"
)

func TestFeaturesDisable(t *testing.T) {
	ft := new(Features)
	ft.Copy = func(ctx context.Context, src Object, remote string) (Object, error) {
		return nil, nil
	}
	ft.CaseInsensitive = true

	assert.NotNil(t, ft.Copy)
	assert.Nil(t, ft.Purge)
	ft.Disable("copy")
	assert.Nil(t, ft.Copy)
	assert.Nil(t, ft.Purge)

	assert.True(t, ft.CaseInsensitive)
	assert.False(t, ft.DuplicateFiles)
	ft.Disable("caseinsensitive")
	assert.False(t, ft.CaseInsensitive)
	assert.False(t, ft.DuplicateFiles)
}

func TestFeaturesList(t *testing.T) {
	ft := new(Features)
	names := strings.Join(ft.List(), ",")
	assert.True(t, strings.Contains(names, ",Copy,"))
}

func TestFeaturesEnabled(t *testing.T) {
	ft := new(Features)
	ft.CaseInsensitive = true
	ft.Purge = func(ctx context.Context, dir string) error { return nil }
	enabled := ft.Enabled()

	flag, ok := enabled["CaseInsensitive"]
	assert.Equal(t, true, ok)
	assert.Equal(t, true, flag, enabled)

	flag, ok = enabled["Purge"]
	assert.Equal(t, true, ok)
	assert.Equal(t, true, flag, enabled)

	flag, ok = enabled["DuplicateFiles"]
	assert.Equal(t, true, ok)
	assert.Equal(t, false, flag, enabled)

	flag, ok = enabled["Copy"]
	assert.Equal(t, true, ok)
	assert.Equal(t, false, flag, enabled)

	assert.Equal(t, len(ft.List()), len(enabled))
}

func TestFeaturesDisableList(t *testing.T) {
	ft := new(Features)
	ft.Copy = func(ctx context.Context, src Object, remote string) (Object, error) {
		return nil, nil
	}
	ft.CaseInsensitive = true

	assert.NotNil(t, ft.Copy)
	assert.Nil(t, ft.Purge)
	assert.True(t, ft.CaseInsensitive)
	assert.False(t, ft.DuplicateFiles)

	ft.DisableList([]string{"copy", "caseinsensitive"})

	assert.Nil(t, ft.Copy)
	assert.Nil(t, ft.Purge)
	assert.False(t, ft.CaseInsensitive)
	assert.False(t, ft.DuplicateFiles)
}

// TestErrorCantUploadEmptyFilesIsNoRetry pins the two properties the empty file
// sentinel must keep: it carries NoRetry, so sync does not spend a full
// --retries ladder and an extra round per zero byte file, and it stays directly
// comparable with ==, which is how fstests matches it.
func TestErrorCantUploadEmptyFilesIsNoRetry(t *testing.T) {
	assert.True(t, fserrors.IsNoRetryError(ErrorCantUploadEmptyFiles))

	err := ErrorCantUploadEmptyFiles
	assert.True(t, err == ErrorCantUploadEmptyFiles, "== must keep matching the bare sentinel")

	wrapped := fmt.Errorf("put: %w", ErrorCantUploadEmptyFiles)
	assert.True(t, fserrors.IsNoRetryError(wrapped), "a wrapped copy must still classify as no-retry")
	assert.ErrorIs(t, wrapped, ErrorCantUploadEmptyFiles)
}
