package cloud189

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloud189/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/stretchr/testify/require"
)

// The gates below exist because the server refuses these two conditions itself -
// at the call that opens the session, without taking a byte (measured 2026-10-22:
// 403 UserDayFlowOverLimited and 403 InsufficientStorageSpace). Refusing locally
// saves the source read, the temp file and the bandwidth; the numbers all come
// from the account, because the same field is 2GiB on one account and int64 max
// on another.

// newQuotaFs builds a remote that has read the account's limits, the way newFs
// does before it will upload anything.
func newQuotaFs(t *testing.T, drive *fakeDrive) *Fs {
	t.Helper()
	f := newFakeFs(t, drive)
	f.quota = &quotaState{}
	require.NoError(t, f.readQuota(context.Background()))
	return f
}

func miB(n int64) int64 { return n * 1024 * 1024 }

// TestGateRefusesOverTheDayAllowanceWithoutTouchingTheSource is the whole point
// of the pre-check: a file the day cannot carry must not be read or staged.
func TestGateRefusesOverTheDayAllowanceWithoutTouchingTheSource(t *testing.T) {
	drive := &fakeDrive{t: t, transDayFlow: miB(10), transFileSize: miB(10), usedDayFlow: miB(9)}
	f := newQuotaFs(t, drive)
	content := pattern(int(miB(2)))

	_, err := f.Put(context.Background(), bytes.NewReader(content), staticObject("big.bin", int64(len(content))))
	require.Error(t, err)
	require.Contains(t, err.Error(), "day's allowance")
	require.True(t, fserrors.IsNoRetryError(err), "the allowance only comes back with the next day")
	require.Empty(t, sequenceContains(sequenceOf(drive), "POST /person/initMultiUpload"), "a doomed upload must not open a session")
	require.Contains(t, err.Error(), "10Mi")
}

// TestGateRefreshesBeforeRefusing: the number the account gave when the remote
// was opened can be behind, so an overrun is checked against the account again
// before anything is refused.
func TestGateRefreshesBeforeRefusing(t *testing.T) {
	drive := &fakeDrive{t: t, transDayFlow: miB(10), transFileSize: miB(10), usedDayFlow: miB(9),
		lateUsedFlow: 0, lateUsedSet: true}
	f := newQuotaFs(t, drive)
	// The first file moves the process's own count, so the second one looks over
	// the line to the local model...
	_, err := f.Put(context.Background(), bytes.NewReader(pattern(int(miB(1)))), staticObject("one.bin", miB(1)))
	require.NoError(t, err)
	require.EqualValues(t, miB(1), f.quota.sent, "what went up is counted")

	// ... and the account, which the fixture never moves, still says there is
	// room. So the file goes up rather than being refused on a stale figure.
	_, err = f.Put(context.Background(), bytes.NewReader(pattern(int(miB(2)))), staticObject("two.bin", miB(2)))
	require.NoError(t, err)
	require.Greater(t, drive.privReads, 1, "the model was re-read before it was allowed to refuse")
}

// TestGateRefusesWhenTheAccountSaysItIsOut: the refresh is not a way to talk
// oneself into an upload.
func TestGateRefusesWhenTheAccountSaysItIsOut(t *testing.T) {
	drive := &fakeDrive{t: t, transDayFlow: miB(10), transFileSize: miB(10), usedDayFlow: miB(10)}
	f := newQuotaFs(t, drive)
	_, err := f.Put(context.Background(), bytes.NewReader(pattern(1024)), staticObject("late.bin", 1024))
	require.Error(t, err)
	require.Contains(t, err.Error(), "comes back with the next day")
	require.Equal(t, 2, drive.privReads, "once at the open, once to make sure before refusing")
}

// TestGateRefusesAFullDrive, including the negative number a drive past full
// reports (measured -49,582,543,299).
func TestGateRefusesAFullDrive(t *testing.T) {
	for _, available := range []int64{0, miB(1), -miB(100)} {
		drive := &fakeDrive{
			t: t, transDayFlow: miB(100), transFileSize: miB(100),
			capacity: miB(200), available: available,
		}
		f := newQuotaFs(t, drive)

		_, err := f.Put(context.Background(), bytes.NewReader(pattern(int(miB(2)))), staticObject("full.bin", miB(2)))
		if available >= miB(2) {
			require.NoErrorf(t, err, "available %d", available)
			continue
		}
		require.Errorf(t, err, "a drive with %d bytes free must refuse 2Mi", available)
		require.Contains(t, err.Error(), "free")
		require.Contains(t, err.Error(), "freed by hand")
		require.True(t, fserrors.IsNoRetryError(err))
	}
}

// TestGateUsesTheSpaceItWasGiven: a drive with room is not refused.
func TestGateLetsASpaceyDriveThrough(t *testing.T) {
	drive := &fakeDrive{
		t: t, transDayFlow: miB(100), transFileSize: miB(100),
		capacity: miB(200), available: miB(150),
	}
	f := newQuotaFs(t, drive)
	_, err := f.Put(context.Background(), bytes.NewReader(pattern(int(miB(2)))), staticObject("fine.bin", miB(2)))
	require.NoError(t, err)
}

// TestGateHonoursTheSingleFileCeiling is the permanent one: a file over the
// account's ceiling cannot go up today at all, so there is nothing to re-read.
func TestGateHonoursTheSingleFileCeiling(t *testing.T) {
	drive := &fakeDrive{t: t, transDayFlow: miB(100), transFileSize: miB(1), usedDayFlow: 0}
	f := newQuotaFs(t, drive)
	_, err := f.Put(context.Background(), bytes.NewReader(pattern(int(miB(2)))), staticObject("over.bin", miB(2)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "single-file ceiling")
	require.Equal(t, 1, drive.privReads, "a permanent limit needs no refresh to state")
}

// TestMaxFileSizeOverridesTheAccount lets a person keep a run inside a budget.
func TestMaxFileSizeOverridesTheAccount(t *testing.T) {
	drive := &fakeDrive{t: t, transDayFlow: miB(100), transFileSize: miB(100)}
	f := newQuotaFs(t, drive)
	f.opt.MaxFileSize = fs.SizeSuffix(miB(1))

	_, err := f.Put(context.Background(), bytes.NewReader(pattern(int(miB(2)))), staticObject("budget.bin", miB(2)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "--cloud189-max_file_size is 1Mi",
		"the refusal names the limit that is actually in force, not the account's")

	_, err = f.Put(context.Background(), bytes.NewReader(pattern(512*1024)), staticObject("small.bin", 512*1024))
	require.NoError(t, err)
}

// TestCeilingRefusalIsNotFatalButStopsRetryingTheFile keeps the promise the option
// help and the docs make: fatal_if_no_space is for the two conditions that cannot
// clear during a run. A file over a limit this run set is not one of them, so it
// must not end the rest of the list - but re-uploading it can never help either.
func TestCeilingRefusalIsNotFatalButStopsRetryingTheFile(t *testing.T) {
	drive := &fakeDrive{t: t, transDayFlow: miB(100), transFileSize: miB(100)}
	f := newQuotaFs(t, drive)
	f.opt.MaxFileSize = fs.SizeSuffix(miB(1))
	f.opt.FatalIfNoSpace = true

	_, err := f.Put(context.Background(), bytes.NewReader(pattern(int(miB(2)))), staticObject("budget.bin", miB(2)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "LocalSingleFileCeiling",
		"the refusal is not filed under a code that means the account has run out")
	require.True(t, fserrors.IsNoRetryError(err), "the file does not get smaller by trying again")
	require.False(t, fserrors.IsFatalError(err), "a limit this run chose does not cancel the whole run")

	// The documented fatal case still is one, on an account that has spent its day.
	driveOver := &fakeDrive{t: t, transDayFlow: miB(1), transFileSize: 0, usedDayFlow: miB(1)}
	fOver := newQuotaFs(t, driveOver)
	fOver.opt.FatalIfNoSpace = true
	_, err = fOver.Put(context.Background(), bytes.NewReader(pattern(512*1024)), staticObject("day.bin", 512*1024))
	require.Error(t, err)
	require.Contains(t, err.Error(), "day's allowance")
	require.True(t, fserrors.IsFatalError(err), "the day's allowance cannot come back during a run")
}

// TestGateReadsTheLimitsWhenTheyWereNeverFetched: the read done when the remote
// opened can fail. The day's allowance is the reason these gates exist, so a gate
// that stayed shut off for the rest of the run on a figure that was never fetched
// would be silently doing nothing.
func TestGateReadsTheLimitsWhenTheyWereNeverFetched(t *testing.T) {
	drive := &fakeDrive{t: t, transDayFlow: miB(1), transFileSize: miB(100), usedDayFlow: 0}
	f := newFakeFs(t, drive)
	f.quota = &quotaState{} // what an open that failed to read the account leaves behind
	require.False(t, f.quota.loaded)

	readsBefore := drive.privReads
	_, err := f.Put(context.Background(), bytes.NewReader(pattern(int(miB(2)))), staticObject("later.bin", miB(2)))
	require.Error(t, err, "the gate asks again and then uses the answer")
	require.Contains(t, err.Error(), "day's allowance")
	require.Greater(t, drive.privReads, readsBefore, "it asked the account instead of returning silently")
}

// TestGateAsksForLimitsOnceAMinuteNotOnceAFile is the cost that comes with asking
// again: an endpoint that keeps refusing must not charge two metadata requests to
// every object in a long run.
func TestGateAsksForLimitsOnceAMinuteNotOnceAFile(t *testing.T) {
	refused := json.RawMessage("1")
	drive := &fakeDrive{t: t, transDayFlow: miB(100), transFileSize: miB(100), privResCode: refused}
	f := newFakeFs(t, drive)
	f.quota = &quotaState{}

	_, _ = f.Put(context.Background(), bytes.NewReader(pattern(1024)), staticObject("a.bin", 1024))
	first := drive.privReads
	require.Greater(t, first, 0, "the gates go back for the limits they never got")

	_, _ = f.Put(context.Background(), bytes.NewReader(pattern(1024)), staticObject("b.bin", 1024))
	require.Equal(t, first, drive.privReads, "and then wait out the gap rather than ask for every file")

	// Out of the gap it asks again, so a failure that has healed is noticed.
	f.quota.mu.Lock()
	f.quota.limitsAsk = time.Now().Add(-2 * time.Minute)
	f.quota.mu.Unlock()
	_, _ = f.Put(context.Background(), bytes.NewReader(pattern(1024)), staticObject("c.bin", 1024))
	require.Greater(t, drive.privReads, first, "past the gap the limits are fetched again")
}

// TestGateReChecksTheDayOnceAMinuteNotOnceAFile is the same cost on the other
// re-read: an account that has genuinely used up its day refuses every file
// left in the run, and each refusal used to ask the account first. On a tree of
// a hundred thousand files that is two hundred thousand metadata requests spent
// to be told the same thing, and a 5xx from either endpoint would multiply it by
// the low-level retries. Between checks the process's own count keeps the model
// moving, so the wait costs accuracy nobody needed.
func TestGateReChecksTheDayOnceAMinuteNotOnceAFile(t *testing.T) {
	drive := &fakeDrive{t: t, transDayFlow: miB(10), transFileSize: miB(10), usedDayFlow: miB(10)}
	f := newQuotaFs(t, drive)
	reads, caps := drive.privReads, drive.capReads

	for _, name := range []string{"a.bin", "b.bin", "c.bin", "d.bin", "e.bin"} {
		_, err := f.Put(context.Background(), bytes.NewReader(pattern(1024)), staticObject(name, 1024))
		require.Errorf(t, err, "%s must still be refused", name)
		require.Contains(t, err.Error(), "comes back with the next day")
		require.True(t, fserrors.IsNoRetryError(err), "for %s", name)
	}
	require.Equal(t, reads+1, drive.privReads, "the first refusal asks the account and the rest wait out the gap")
	require.Equal(t, caps+1, drive.capReads, "the same is true of the endpoint that carries the used figure")

	// Past the gap the account is asked again, so a day that has turned over - or
	// space that has been cleared by hand - is noticed without restarting the run.
	f.quota.mu.Lock()
	f.quota.overRunAsk = time.Now().Add(-2 * time.Minute)
	f.quota.mu.Unlock()
	_, err := f.Put(context.Background(), bytes.NewReader(pattern(1024)), staticObject("f.bin", 1024))
	require.Error(t, err)
	require.Equal(t, reads+2, drive.privReads, "a refusal past the gap re-reads once more")

	// And when the account says the day has come back, that is what stops the
	// refusal: the file goes up rather than being refused on the old model.
	drive.usedDayFlow = 0
	f.quota.mu.Lock()
	f.quota.overRunAsk = time.Now().Add(-2 * time.Minute)
	f.quota.mu.Unlock()
	_, err = f.Put(context.Background(), bytes.NewReader(pattern(int(miB(1)))), staticObject("g.bin", miB(1)))
	require.NoError(t, err, "a day that has turned over must not be refused on a stale figure")
}

// TestNoQuotaCheckMeansNoGatingAtAll: with the option set, the limits are not
// read for the transfer and not consulted for it either, even when the account
// has plainly run out. The server's own refusal is then the only gate.
func TestNoQuotaCheckMeansNoGatingAtAll(t *testing.T) {
	drive := &fakeDrive{t: t, transDayFlow: miB(1), transFileSize: miB(1), usedDayFlow: miB(1)}
	f := newFakeFs(t, drive)
	f.quota = &quotaState{}
	require.NoError(t, f.readQuota(context.Background()), "the account is read while the check is on")
	require.EqualValues(t, miB(1), f.quota.transDayFlow)

	f.opt.NoQuotaCheck = true
	readsBefore, capBefore := drive.privReads, drive.capReads

	_, err := f.Put(context.Background(), bytes.NewReader(pattern(int(miB(2)))), staticObject("unchecked.bin", miB(2)))
	require.NoError(t, err, "with the check off the server is the only gate")
	require.Equal(t, readsBefore, drive.privReads, "and no limit is asked for along the way")
	require.Equal(t, capBefore, drive.capReads)
	require.NotEmpty(t, sequenceContains(sequenceOf(drive), "POST /person/initMultiUpload"), "the upload really went to the drive")
}

// TestQuotaReadRefusesToTreatAResCodeAsAnAnswer: an account that refuses this
// endpoint must not be read as an account with no limits.
func TestQuotaReadRefusesToTreatAResCodeAsAnAnswer(t *testing.T) {
	for _, tc := range []struct {
		name          string
		priv          json.RawMessage
		capCode       json.RawMessage
		wantAboutErr  bool
		aboutErrCause string
	}{
		// The limits are the optional half of About: the space was still read
		// properly, so a person asking for it gets it, with no gate model built.
		{name: "privileges refused", priv: json.RawMessage(`"InvalidArgument"`)},
		// The space is the half About promises, so a refusal there is a failure.
		{name: "capacity refused", capCode: json.RawMessage(`"InvalidArgument"`),
			wantAboutErr: true, aboutErrCause: "InvalidArgument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drive := &fakeDrive{t: t, transDayFlow: miB(1), transFileSize: miB(1), usedDayFlow: miB(1),
				capacity: miB(100), available: miB(50), privResCode: tc.priv, capResCode: tc.capCode}
			f := newFakeFs(t, drive)
			f.quota = &quotaState{}
			err := f.readQuota(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), "InvalidArgument")
			require.False(t, f.quota.loaded, "a refused read leaves no model to gate on")

			// Fail open rather than invent a limit: the server still refuses what
			// it refuses, and a made-up gate would lose files that could go up.
			_, err = f.Put(context.Background(), bytes.NewReader(pattern(int(miB(2)))), staticObject("open.bin", miB(2)))
			require.NoError(t, err)

			usage, err := f.About(context.Background())
			if tc.wantAboutErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.aboutErrCause)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, miB(100), *usage.Total, "the space that was read is still reported")
		})
	}
}

// TestGateIsSilentWhenTheLimitsAreUnknown: an account that answered nothing
// usable must not be read as an account with no room.
func TestGateIsSilentWhenTheLimitsAreUnknown(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newQuotaFs(t, drive)
	require.False(t, f.quota.spaceKnown, "a capacity of zero was not an answer")

	_, err := f.Put(context.Background(), bytes.NewReader(pattern(int(miB(2)))), staticObject("unknown.bin", miB(2)))
	require.NoError(t, err)
}

// TestFatalIfNoSpaceAppliesToTheGates too: the local refusal carries the same
// code, so the option that cancels a run on a server refusal cancels it here.
func TestFatalIfNoSpaceAppliesToTheGates(t *testing.T) {
	for _, fatal := range []bool{false, true} {
		drive := &fakeDrive{t: t, transDayFlow: miB(1), transFileSize: miB(1), usedDayFlow: miB(1)}
		f := newQuotaFs(t, drive)
		f.opt.FatalIfNoSpace = fatal
		_, err := f.Put(context.Background(), bytes.NewReader(pattern(1024)), staticObject("fatal.bin", 1024))
		require.Error(t, err)
		require.Equal(t, fatal, fserrors.IsFatalError(err), "fatal_if_no_space %v", fatal)
	}
}

// TestAboutReportsTheSpace pins the two readings that matter: available is free
// (not used), and a drive past full reports a negative number that rclone must
// not show as a negative amount of free space.
func TestAboutReportsTheSpace(t *testing.T) {
	drive := &fakeDrive{t: t, capacity: miB(200), available: -miB(100), transDayFlow: miB(10), usedDayFlow: miB(3)}
	f := newQuotaFs(t, drive)

	usage, err := f.About(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, miB(200), *usage.Total)
	require.EqualValues(t, 0, *usage.Free, "a negative free space is reported as none, not as a negative")
	require.EqualValues(t, miB(300), *usage.Used, "the drive is over full, and that is what it says it holds")

	// About also refreshes, because the same numbers decide the next upload.
	require.Equal(t, 3, drive.capReads, "once at the open, twice through About: it reads, then refreshes")
	require.EqualValues(t, -miB(100), f.quota.available, "the raw figure is kept for the gates to clamp")
}

// TestGateRefusesTheChunkedPathAsWell: OpenChunkWriter is a second way into the
// upload, and it must not be a way around the limits. The size is known the moment
// the writer is asked for, so the refusal comes before any temp file is written.
func TestGateRefusesTheChunkedPathAsWell(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	t.Setenv("TMP", dir)
	t.Setenv("TEMP", dir)
	count := func() int {
		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		return len(entries)
	}

	drive := &fakeDrive{t: t, transDayFlow: miB(10), transFileSize: miB(10), usedDayFlow: miB(10)}
	f := newQuotaFs(t, drive)
	content := pattern(int(miB(2)))

	info, w, err := f.OpenChunkWriter(context.Background(), "threaded.bin", staticObject("threaded.bin", int64(len(content))))
	require.Error(t, err, "the day's allowance is spent, so the writer is refused at once")
	require.Contains(t, err.Error(), "day's allowance")
	require.Nil(t, w, "and no writer, with no temp file inside it, is handed back")
	require.Equal(t, fs.ChunkWriterInfo{}, info, "and the writer information it would have used is not handed out either")
	require.Equal(t, 0, count(), "the refused chunked upload left no temp file behind")
	require.Empty(t, sequenceContains(sequenceOf(drive), "POST /person/initMultiUpload"),
		"and the drive was never asked for a session")
}

// TestGateRefusesTheChunkedPathAtCloseIsTheOtherHalf: what the day has used moves
// while the parts arrive, so Close asks again. The bytes are already staged by
// then, which is why the temp file has to go.
func TestGateRefusesTheChunkedPathWhenTheDayRunsOutMidway(t *testing.T) {
	drive := &fakeDrive{t: t, transDayFlow: miB(10), transFileSize: miB(10), usedDayFlow: 0}
	f := newQuotaFs(t, drive)
	content := pattern(int(miB(2)))

	info, w, err := f.OpenChunkWriter(context.Background(), "threaded.bin", staticObject("threaded.bin", int64(len(content))))
	require.NoError(t, err, "there is room for this file when the writer opens")
	plans := api.PlanParts(int64(len(content)), info.ChunkSize)
	require.Len(t, plans, 2)
	for i, p := range plans {
		start := int64(i) * info.ChunkSize
		_, err := w.WriteChunk(context.Background(), i, bytes.NewReader(content[start:start+p.Size]))
		require.NoError(t, err)
	}

	// Another program spends the rest of the day while the parts are staged.
	drive.usedDayFlow = miB(10)
	f.quota.mu.Lock()
	f.quota.usedDayFlow = miB(10)
	f.quota.mu.Unlock()

	err = w.Close(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "day's allowance")
	require.Empty(t, sequenceContains(sequenceOf(drive), "POST /person/initMultiUpload"), "Close must refuse before any session")
}
