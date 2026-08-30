package wopan

// Live protocol probes behind WOPAN_PROBE=1. They exercise server behaviours
// that fstests cannot express - collision renames, rename visibility, recycle
// timing, parameter redundancy - and log the observed behaviour for the
// acceptance review. Without WOPAN_PROBE set they skip instantly, so CI and
// `go test ./...` stay hermetic.

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/wopan/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	fsobject "github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/random"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// probeRoot is unique per test run: a directory that received a collision
// CopyFile can keep rejecting list APIs with 9999 for a long time (recorded
// by TestProbeCase58), so reusing a fixed name would poison subsequent runs.
var probeRoot = "rclone-wopan-probe-" + random.String(8)

func probeOpenFs(t *testing.T) *Fs {
	if os.Getenv("WOPAN_PROBE") == "" {
		t.Skip("set WOPAN_PROBE=1 (and RCLONE_CONFIG with [TestWopan]) for live probes")
	}
	// Probes bypass fstest.Initialise, so install the config loader manually -
	// outside cmd.Main and fstest nothing points rclone at RCLONE_CONFIG.
	if p := os.Getenv("RCLONE_CONFIG"); p != "" {
		require.NoError(t, config.SetConfigPath(p))
	}
	configfile.Install()
	ctx := context.Background()
	fsi, err := fs.NewFs(ctx, "TestWopan:")
	require.NoError(t, err)
	f, ok := fsi.(*Fs)
	require.True(t, ok, "remote is not a wopan Fs")
	require.NoError(t, f.Mkdir(ctx, probeRoot))
	t.Cleanup(func() {
		_ = f.Purge(ctx, probeRoot)
	})
	return f
}

// probePut uploads a small file under the probe root and returns the object.
func probePut(ctx context.Context, t *testing.T, f *Fs, remote, content string) *Object {
	return probePutAt(ctx, t, f, probeRoot, remote, content)
}

// probePutAt uploads a small file under the given base root.
func probePutAt(ctx context.Context, t *testing.T, f *Fs, base, remote, content string) *Object {
	src := fsobject.NewStaticObjectInfo(base+"/"+remote, time.Now(), int64(len(content)), true, nil, nil)
	obj, err := f.Put(ctx, strings.NewReader(content), src)
	require.NoError(t, err, "put %s", remote)
	return obj.(*Object)
}

// probeNames lists every file name directly under the probe path. Directories
// are skipped; the raw entry types are logged by the individual probes where
// they matter.
func probeNames(ctx context.Context, t *testing.T, f *Fs, dir string) []string {
	dirID, err := f.dirCache.FindDir(ctx, probeRoot+"/"+dir, false)
	require.NoError(t, err)
	entries, err := f.listDirEntries(ctx, dirID)
	require.NoError(t, err)
	var names []string
	for _, item := range entries {
		if item.Type != fileTypeDir {
			names = append(names, item.Name)
		} else {
			t.Logf("probeNames: %s holds directory %q (type=%d)", dir, item.Name, item.Type)
		}
	}
	return names
}

// probeReadBestEffort downloads the object's content, reporting failures as
// values instead of failing the test - freshly collision-copied objects can
// refuse downloads with 9999 while the server settles.
func probeReadBestEffort(ctx context.Context, o *Object) (string, error) {
	rc, err := o.Open(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = rc.Close() }()
	buf, err := io.ReadAll(rc)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}

// TestProbeCase58CopyFileOntoExistingName records what CopyFile does when the
// copy lands on a name the target directory already holds (overwrite, auto
// rename or error). The plan's B5 rules need this to pick the right
// findNewEntry strategy for cross-directory copies.
//
// The test uses its own root: a directory that received a collision copy can
// keep rejecting list APIs with 9999 for minutes (recorded finding), and that
// poison spreads to listing the parent directory, so it must not share the
// root the other probes use.
func TestProbeCase58CopyFileOntoExistingName(t *testing.T) {
	if os.Getenv("WOPAN_PROBE") == "" {
		t.Skip("set WOPAN_PROBE=1 (and RCLONE_CONFIG with [TestWopan]) for live probes")
	}
	if p := os.Getenv("RCLONE_CONFIG"); p != "" {
		require.NoError(t, config.SetConfigPath(p))
	}
	configfile.Install()
	ctx := context.Background()
	fsi, err := fs.NewFs(ctx, "TestWopan:")
	require.NoError(t, err)
	f, ok := fsi.(*Fs)
	require.True(t, ok, "remote is not a wopan Fs")

	root := "rclone-wopan-probe-c58-" + random.String(8)
	require.NoError(t, f.Mkdir(ctx, root))
	t.Cleanup(func() {
		// Best effort: the poisoned directory may not be listable, let alone
		// purgeable, for a while.
		_ = f.Purge(ctx, root)
	})

	src := probePutAt(ctx, t, f, root, "case58/src.txt", "source-content")
	_ = probePutAt(ctx, t, f, root, "case58/dst/src.txt", "existing-content")

	tryCopy := func(targetDir string) error {
		dirID, err := f.dirCache.FindDir(ctx, root+"/"+targetDir, true)
		require.NoError(t, err)
		p := f.moveCopyParams(dirID, []string{}, []string{src.id})
		return f.pacer.Call(func() (bool, error) {
			_, err := f.call(ctx, chanWoHome, "CopyFile", p, map[string]any{"secret": true})
			return shouldRetryCall(err)
		})
	}

	// Control: a clean target directory - the backend's normal cross-dir
	// server side copy path.
	err = tryCopy("case58/dst-empty")
	t.Logf("case58: CopyFile into an empty directory: err=%v", err)

	// The collision case: the target directory already holds src.txt.
	err = tryCopy("case58/dst")
	t.Logf("case58: CopyFile onto an existing name: err=%v", err)
	require.NoError(t, err, "record the server's collision behaviour")

	// Give the listing time to settle. Record but do not fail: the list API
	// has been observed to 9999 for a while after a collision copy lands,
	// which is itself a finding (the directory index is left in a state the
	// list API rejects), so the backend's findNewEntry would surface an error
	// rather than silently losing the copy.
	listDst := func() ([]*api.File, error) {
		dirID, err := f.dirCache.FindDir(ctx, root+"/case58/dst", false)
		if err != nil {
			return nil, err
		}
		return f.listDirEntries(ctx, dirID)
	}
	var entries []*api.File
	var listErr error
	for attempt := 1; attempt <= 5; attempt++ {
		time.Sleep(3 * time.Second)
		entries, listErr = listDst()
		if listErr == nil {
			break
		}
		t.Logf("case58: post-copy listing attempt %d: %v", attempt, listErr)
	}
	if listErr != nil {
		t.Logf("case58: target directory stays unlistable after the collision copy: %v", listErr)
	}
	for _, item := range entries {
		if !strings.HasPrefix(item.Name, "src") {
			continue
		}
		o := &Object{fs: f, remote: root + "/case58/dst/" + item.Name, id: item.ID, fid: item.ID}
		// The copied object can also refuse downloads with 9999 while the
		// server settles the collision, so record what works.
		content, err := probeReadBestEffort(ctx, o)
		t.Logf("case58: target holds %q (id %s type %d) content %q (read err: %v)", item.Name, item.ID, item.Type, content, err)
	}
}

// mustDirID resolves a probe path to that directory's own id (FindPath would
// return the PARENT's id plus the leaf, which is wrong for API calls that
// take a directory id).
func mustDirID(ctx context.Context, t *testing.T, f *Fs, dir string) string {
	dirID, err := f.dirCache.FindDir(ctx, strings.TrimSuffix(probeRoot+"/"+dir, "/"), true)
	require.NoError(t, err)
	return dirID
}

// TestProbeCase59RenameOldNameGone confirms a rename leaves no duplicate: the
// old name must disappear (plan use case 59, fifth-round R5).
func TestProbeCase59RenameOldNameGone(t *testing.T) {
	f := probeOpenFs(t)
	ctx := context.Background()

	src := probePut(ctx, t, f, "case59/ren.txt", "rename-me")
	require.NoError(t, f.renameWithBackoff(ctx, src.id, "ren2.txt"))

	time.Sleep(5 * time.Second)
	names := probeNames(ctx, t, f, "case59")
	t.Logf("case59: listing after rename: %v", names)
	assert.NotContains(t, names, "ren.txt", "the old name must be gone")
	assert.Contains(t, names, "ren2.txt")
}

// TestProbeCase61QuestionMarkAndCaseCopy probes two collision-adjacent
// behaviours: whether the server keeps fullwidth ？ and ASCII ? distinct, and
// how a case-only rename behaves (plan use cases 51/61).
func TestProbeCase61QuestionMarkAndCaseCopy(t *testing.T) {
	f := probeOpenFs(t)
	ctx := context.Background()

	// Same directory, ASCII ? vs fullwidth ？: same-dir CopyFile is guarded
	// (ErrorCantCopy), so upload both names directly and compare listings.
	_ = probePut(ctx, t, f, "case61/q?x.txt", "ascii")
	_ = probePut(ctx, t, f, "case61/q？x.txt", "fullwidth")
	time.Sleep(5 * time.Second)
	names := probeNames(ctx, t, f, "case61")
	t.Logf("case61: listing with ascii/fullwidth question marks: %v", names)

	// Case-only rename: does the server accept renaming Case.txt to case.txt?
	src := probePut(ctx, t, f, "case61/Case.txt", "case-only")
	err := f.renameWithBackoff(ctx, src.id, "case.txt")
	t.Logf("case61: case-only rename error: %v", err)
	time.Sleep(5 * time.Second)
	t.Logf("case61: listing after case-only rename: %v", probeNames(ctx, t, f, "case61"))
}

// TestProbeSpaceTypeRedundantFamilyId sends a personal-space CreateDirectory
// carrying the familyId key the code normally omits - the server must either
// tolerate it or return a stable, documented rejection. The probe directory
// lives under probeRoot so the cleanup purge removes it either way.
func TestProbeSpaceTypeRedundantFamilyId(t *testing.T) {
	f := probeOpenFs(t)
	ctx := context.Background()

	p := f.spaceParams()
	p["parentDirectoryId"] = mustDirID(ctx, t, f, "")
	p["directoryName"] = "redundant-family-id"
	p["familyId"] = f.opt.FamilyID // redundant on the personal space
	var resp api.CreateDirectoryResponse
	err := f.pacer.Call(func() (bool, error) {
		data, err := f.call(ctx, chanWoHome, "CreateDirectory", p, map[string]any{"secret": true})
		if err != nil {
			return shouldRetryCall(err)
		}
		return false, json.Unmarshal(data, &resp)
	})
	t.Logf("spaceType: CreateDirectory with redundant familyId: err=%v id=%q", err, resp.ID)

	if resp.ID != "" {
		// Best-effort removal through the same API purge uses for directories.
		p2 := f.spaceParams()
		p2["dirList"] = []string{resp.ID}
		p2["fileList"] = []string{}
		_, derr := f.call(ctx, chanWoHome, "DeleteFile", p2, map[string]any{"secret": true})
		t.Logf("spaceType: cleanup of %s: err=%v", resp.ID, derr)
	}
	// Either outcome is a valid, recorded finding: tolerated (err == nil) or
	// a stable business rejection (9999/1000) that justifies the backend
	// omitting the familyId key entirely on the personal space.
	if err != nil {
		var ae *apiError
		require.True(t, asAPIError(err, &ae), "rejection must be a business error, got: %v", err)
		t.Logf("spaceType: server rejects the redundant familyId key with RSP_CODE=%s", ae.Code)
		assert.Contains(t, []string{"9999", "1000"}, ae.Code)
	}
}

// TestProbeAboutEmptyPhoneNum records that QueryCloudUsageInfo works with the
// empty phoneNum the backend always sends (plan use case 16 smoke).
func TestProbeAboutEmptyPhoneNum(t *testing.T) {
	f := probeOpenFs(t)
	usage, err := f.About(context.Background())
	require.NoError(t, err)
	t.Logf("about: total=%d used=%d", *usage.Total, *usage.Used)
	assert.Greater(t, *usage.Total, int64(0))
}

// TestProbeCase60HardDeleteTiming times soft-delete + recycle purge cycles
// against the current recycle-bin size (plan use case 60 smoke; the full
// >=100-entry scenario needs a polluted recycle bin and stays manual).
func TestProbeCase60HardDeleteTiming(t *testing.T) {
	f := probeOpenFs(t)
	ctx := context.Background()

	// A shallow copy with hard_delete enabled shares the token state and
	// dircache, which is all deleteFile/purgeRecycle touch.
	fHD := *f
	fHD.opt = f.opt
	fHD.opt.HardDelete = true

	var ids []string
	for i := 0; i < 5; i++ {
		o := probePut(ctx, t, f, "case60/blob"+string(rune('a'+i))+".bin", strings.Repeat("x", 100))
		ids = append(ids, o.id)
	}
	for i, id := range ids {
		start := time.Now()
		require.NoError(t, fHD.deleteFile(ctx, id))
		t.Logf("case60: hard delete #%d took %s", i, time.Since(start).Round(time.Millisecond))
	}
}
