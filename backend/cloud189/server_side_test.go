package cloud189

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/stretchr/testify/require"
)

// The server-side operations move objects by id inside the drive, so they
// transfer nothing and charge the day's allowance nothing. The captures of
// 2026-10-22 give the shape: one task, polled to taskStatus 4, and a COPY that
// reports no id for what it made - so the new object is read back from the
// destination listing.

const (
	rootID    = "-11"
	archiveID = "900000000000000009"
	famRoot   = "9000000000000003"
	famID     = "900000001"
)

func fileJSON(id, name, parentID string) string {
	return fmt.Sprintf(`{"id":%q,"name":%q,"parentId":%q,"size":10,"md5":"D41D8CD98F00B204E9800998ECF8427E","type":"1"}`, id, name, parentID)
}

func folderJSON(id, name, parentID string) string {
	return fmt.Sprintf(`{"id":%q,"name":%q,"parentId":%q,"type":"2"}`, id, name, parentID)
}

func listingJSON(files, folders []string) string {
	return `{"res_code":0,"res_message":"成功","fileListAO":{"count":` +
		fmt.Sprint(len(files)+len(folders)) + `,"fileList": [` + strings.Join(files, ",") +
		`],"folderList": [` + strings.Join(folders, ",") + `]}}`
}

func emptyListing() string { return listingJSON(nil, nil) }

func srcObject(f *Fs, remote, id string) *Object {
	return &Object{fs: f, remote: remote, id: id, size: 10, urlMu: new(sync.Mutex)}
}

// listingScript answers each directory's listings in a scripted order, so a test
// can show the drive as it looks before a task and after it.
//
// One queued body is one walk of one directory, not one HTTP request. listAll
// opens every walk with pageNum=1, so that request is what takes the next body
// from the queue; the pages after it are served empty, because every body here
// fits on one page. A walk that stops early - a name found, so listAll returns
// without asking for the next page - therefore consumes exactly one body, while a
// walk to the end of a directory consumes its body plus the empty page that
// proved it was the last one. Keying the queue on requests instead would hand the
// next caller the page a previous walk never asked for.
//
// A queue that runs out answers empty, which is the drive looking bare: the
// assertion that depends on the missing listing fails, rather than quietly
// reusing a stale body.
type listingScript struct {
	mu    sync.Mutex
	queue map[string][]string
	seen  map[string]int // directories walked over, so a test can name what was never listed
}

func newListingScript(queue map[string][]string) *listingScript {
	return &listingScript{queue: queue, seen: map[string]int{}}
}

// walksOf reports how many times the script was asked to list one directory.
func (s *listingScript) walksOf(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[id]
}

func (s *listingScript) fn(r *http.Request) string {
	q := r.URL.Query()
	id := q.Get("folderId")
	if id == "" {
		id = rootID // the family endpoint treats an omitted folderId as its root
	}
	if q.Get("pageNum") != "1" {
		return emptyListing()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[id]++
	if next := s.queue[id]; len(next) > 0 {
		out := next[0]
		s.queue[id] = next[1:]
		return out
	}
	return emptyListing()
}

// TestServerSideCopyOfAFileMovesNoBytes is the ordinary case: the destination
// directory is found, the task runs, and the new object is read back.
func TestServerSideCopyOfAFileMovesNoBytes(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()}, // nothing named archive yet
		// the created "archive" is verified by listing it first, so the read
		// back of the copied object is the third walk of it
		archiveID: {emptyListing(), emptyListing(), listingJSON([]string{fileJSON("900000000000000011", "clip-01.mp4", archiveID)}, nil)},
	}).fn

	obj, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/clip-01.mp4")
	require.NoError(t, err)
	require.Equal(t, "900000000000000011", obj.(*Object).id)
	require.Equal(t, "archive/clip-01.mp4", obj.Remote())

	require.Equal(t, []string{"archive"}, drive.mkdirs, "the destination directory was made as usual")
	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskCopy, drive.tasks[0].taskType)
	require.Equal(t, "900000000000000006", drive.tasks[0].infos[0].FileID, "the task addresses the source")
	require.Contains(t, drive.createBodies[0], "targetFolderId="+archiveID)
	require.Empty(t, drive.renames, "a copy into a free name is not renamed")
	require.Len(t, drive.tasks, 1, "and nothing is deleted")

	for _, s := range sequenceOf(drive) {
		require.False(t, strings.Contains(s, "/person/") || strings.Contains(s, "/store/"),
			"a server-side copy must not touch the upload path: %v", sequenceOf(drive))
	}
}

// TestServerSideCopyArrivesNamedAfterItsSource pins the measured task shape: the
// drive ignores the fileName a COPY carries and lands the copy in the target
// folder under its SOURCE's name (measured live 2026-10-24 - a copy of A.txt
// asked to arrive as sub/B.txt came back as sub/A.txt). So the name this run
// clears is the source's spelling, and the arrival is renamed by id into the name
// rclone asked for.
func TestServerSideCopyArrivesNamedAfterItsSource(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()}, // the destination's parent, which is not there yet
		archiveID: {
			emptyListing(), // the create's verification
			emptyListing(), // the name the copy is to arrive under is free
			listingJSON([]string{fileJSON("7000000000000000007", "clip-01.mp4", archiveID)}, nil),
		},
	}).fn

	obj, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/holiday.MP4")
	require.NoError(t, err)
	require.Equal(t, "7000000000000000007", obj.(*Object).id, "the copy, not the source")
	require.Equal(t, "archive/holiday.MP4", obj.Remote())

	require.Len(t, drive.tasks, 1, "the source is left alone by a copy")
	require.Equal(t, taskCopy, drive.tasks[0].taskType)
	require.Equal(t, "clip-01.mp4", drive.tasks[0].infos[0].FileName, "the task names what lands")
	require.Len(t, drive.renames, 1)
	require.Equal(t, "7000000000000000007", drive.renames[0].id)
	require.Equal(t, "holiday.MP4", drive.renames[0].name, "and the arrival is renamed into the asked-for name")
}

// TestCopyWithinItsOwnDirectoryIsRefused: a copy whose destination directory is
// the directory holding the source would have to arrive under a name its own
// source is holding, and the drive refuses that instead of duplicating the object
// (measured live 2026-10-24 as COPY failing with FileAlreadyExists, and on
// 2026-10-23 as a task stopped in the state that waits for a client to answer).
// rclone's answer for a server-side operation a backend cannot do is
// fs.ErrorCantCopy, which lets the caller transfer the bytes instead. The drive
// is not asked for anything.
func TestCopyWithinItsOwnDirectoryIsRefused(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)

	_, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "other.txt")
	require.ErrorIs(t, err, fs.ErrorCantCopy)
	require.Empty(t, sequenceOf(drive), "and the drive is not asked")
}

// TestServerSideCopyParksTheOccupierAndDeletesIt is the collision case: the name
// is held, the holder is moved aside, the copy lands, then the holder is deleted.
func TestServerSideCopyParksTheOccupierAndDeletesIt(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(), // the create's verification
			listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", archiveID)}, nil),
			listingJSON([]string{fileJSON("900000000000000011", "clip-01.mp4", archiveID)}, nil),
		},
	}).fn

	_, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/clip-01.mp4")
	require.NoError(t, err)

	require.Len(t, drive.renames, 1)
	require.Equal(t, "6000000000000000006", drive.renames[0].id, "the occupier is moved aside first")
	require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))

	require.Len(t, drive.tasks, 2)
	require.Equal(t, taskCopy, drive.tasks[0].taskType)
	require.Equal(t, taskDelete, drive.tasks[1].taskType)
	require.Equal(t, "6000000000000000006", drive.tasks[1].infos[0].FileID, "what gets deleted is the occupier")
	require.Equal(t, drive.renames[0].name, drive.tasks[1].infos[0].FileName, "by the name it was parked under")
}

// TestCopyRefusesToDisplaceADirectory: the delete that follows a successful task
// is recursive, so a directory is never moved out of a name's way.
func TestCopyRefusesToDisplaceADirectory(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID:    {emptyListing()},
		archiveID: {emptyListing(), listingJSON(nil, []string{folderJSON("5000000000000000005", "clip-01.mp4", archiveID)})},
	}).fn

	_, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/clip-01.mp4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is a directory on the drive")
	require.True(t, fserrors.IsNoRetryError(err))
	require.Empty(t, drive.renames, "nothing is moved aside")
	require.Empty(t, drive.tasks, "and no task is started")
}

// TestCrossSpaceCopyUsesThePersonalSessionAndMarksTheDirection is the captured
// 个人盘 to 家庭盘 shape: the source session signs it, the destination family is
// named in the body, copyType says which way it went, and no source parent is
// sent at all.
func TestCrossSpaceCopyUsesThePersonalSessionAndMarksTheDirection(t *testing.T) {
	drive := &fakeDrive{t: t}
	src := newFakeFs(t, drive)
	dst := newFakeFs(t, drive)
	dst.opt.AccessToken = src.opt.AccessToken
	dst.opt.Space = spaceFamily
	dst.familyID = famID
	dst.familyRootID = famRoot
	dst.dirCache = dircache.New("", famRoot, dst)

	drive.listingFn = newListingScript(map[string][]string{
		famRoot: {emptyListing(), listingJSON([]string{fileJSON("900000000000000011", "clip-01.mp4", famRoot)}, nil)},
	}).fn

	obj, err := dst.Copy(context.Background(), srcObject(src, "clip-01.mp4", "900000000000000006"), "clip-01.mp4")
	require.NoError(t, err)
	require.Equal(t, "900000000000000011", obj.(*Object).id)

	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskCopy, drive.tasks[0].taskType)
	require.Nil(t, drive.tasks[0].infos[0].ParentID, "the captured cross-space copy names no source parent")
	body := drive.createBodies[0]
	require.Contains(t, body, "copyType=1")
	require.Contains(t, body, "familyId="+famID)
	require.Contains(t, body, "targetFolderId="+famRoot)
	require.Equal(t, []string{fakeKey}, drive.createKeys, "signed by the source session, as captured")
	require.Contains(t, strings.Join(sequenceOf(drive), "\n"), "GET /family/file/listFiles.action",
		"the new object is looked for in the destination drive")
}

// TestPurgeOfTheRootEmptiesOnlyThisRemotesRoot: the Fs may be opened at a
// subdirectory, and a purge of "" then means that subdirectory, not the space's top
// level. Getting this wrong emptied a whole family drive into its recycle bin when
// the integration suite's teardown purged the directory it ran in.
func TestPurgeEmptiesOnlyTheRootTheRemoteWasOpenedAt(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	const openedAt = "9000000000000007"
	f.dirCache = dircache.New("rc-fstest", openedAt, f)
	f.fsRootID = openedAt
	drive.listingFn = newListingScript(map[string][]string{
		openedAt: {listingJSON([]string{
			fileJSON("900000000000000021", "keep.bin", openedAt),
			fileJSON("900000000000000022", "gone.bin", openedAt),
		}, nil)},
		rootID: {listingJSON([]string{fileJSON("900000000000000031", "root.bin", rootID)}, nil)},
	}).fn

	require.NoError(t, f.Purge(context.Background(), ""))

	require.Len(t, drive.tasks, 1, "one task")
	var ids []string
	for _, info := range drive.tasks[0].infos {
		ids = append(ids, info.FileID)
	}
	require.ElementsMatch(t, []string{"900000000000000021", "900000000000000022"}, ids,
		"the entries under the directory the remote was opened at, and nothing else")
	require.NotContains(t, ids, "900000000000000031", "the space root's own entries are not this remote's to empty")
}

// TestDestructiveOperationsRefuseAnUnknownFamilyRoot: the root id is read from a
// listing that can come back empty, and a purge aimed at the alias then deletes
// whatever that alias names. Measured 2026-10-23: one run emptied a family drive
// into its recycle bin. With no root id the destructive paths must refuse and send
// nothing at all.
func TestDestructiveOperationsRefuseAnUnknownFamilyRoot(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	f.opt.Space = spaceFamily
	f.familyID = famID
	f.familyRootID = ""
	f.opt.RootFolderID = ""
	f.dirCache = dircache.New("", famRoot, f)

	for _, tc := range []struct {
		what string
		call func() error
	}{
		{"Purge of the root", func() error { return f.Purge(context.Background(), "") }},
		{"Purge of a directory", func() error { return f.Purge(context.Background(), "sub") }},
		{"Rmdir of a directory", func() error { return f.Rmdir(context.Background(), "sub") }},
	} {
		err := tc.call()
		require.Error(t, err, tc.what)
		require.Contains(t, err.Error(), "family root id", tc.what)
		require.True(t, fserrors.IsNoRetryError(err), "%s cannot heal by trying again", tc.what)
	}
	require.Empty(t, sequenceOf(drive), "and not one request went out, let alone a delete task")
}

// TestDirMoveBetweenTwoRemotesOfOneSpaceRuns: two remote instances of one
// account in the same space share the session by token, so the move runs between
// them - which is what `rclone movedto` across two configured roots of one drive
// makes. The task ignores the name it carries, so the directory arrives under the
// name it moved with and is renamed here.
func TestDirMoveBetweenTwoRemotesOfOneSpaceRuns(t *testing.T) {
	const subID = "900000000000000046"
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	src := newFakeFs(t, drive)
	dst := newFakeFs(t, drive)
	dst.opt.AccessToken = src.opt.AccessToken
	dst.opt.Enc = encoder.Standard // different on purpose
	subFolder := listingJSON(nil, []string{folderJSON(subID, "sub", rootID)})
	script := newListingScript(map[string][]string{
		// The root answers the source's lookup of "sub"; the walks of "archive"
		// are the create's verification, the check that the destination does not
		// exist, the name the arrival will wear, and the readback that finds it.
		rootID: {subFolder, subFolder, subFolder, subFolder},
		archiveID: {emptyListing(), emptyListing(), emptyListing(), emptyListing(),
			listingJSON(nil, []string{folderJSON(subID, "sub", archiveID)})},
	})
	drive.listingFn = script.fn

	require.NoError(t, dst.DirMove(context.Background(), src, "sub", "archive/moved"))
	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskMove, drive.tasks[0].taskType)
	require.Equal(t, subID, drive.tasks[0].infos[0].FileID)
	require.Equal(t, "sub", drive.tasks[0].infos[0].FileName, "the task names what lands, which is the source")
	require.Len(t, drive.renames, 1, "and the arrival is renamed into the asked-for name")
	require.Equal(t, subID, drive.renames[0].id)
	require.Equal(t, "moved", drive.renames[0].name)
}

// TestDirMoveRenameInsideItsOwnParentIsNotATask is the case a task must never be
// used for: a directory moved to a new name in the directory that already holds
// it. The drive has been measured to answer a MOVE addressed at the object's own
// directory by destroying the object (2026-10-23, on a file), and a directory
// takes a whole subtree with it, so the rename goes through the synchronous
// rename endpoint instead - after the same park-and-swap the task cases clear
// their name with.
func TestDirMoveRenameInsideItsOwnParentIsNotATask(t *testing.T) {
	const subID = "900000000000000047"
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	subFolder := listingJSON(nil, []string{folderJSON(subID, "sub", rootID)})
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {subFolder, subFolder, subFolder, subFolder, subFolder},
	}).fn

	require.NoError(t, f.DirMove(context.Background(), f, "sub", "renamed"))

	require.Empty(t, drive.tasks, "no task is addressed at the directory's own parent")
	require.Len(t, drive.renames, 1)
	require.Equal(t, subID, drive.renames[0].id)
	require.Equal(t, "renamed", drive.renames[0].name)
	_, cached := f.dirCache.Get("sub")
	require.False(t, cached, "the old path leaves the cache")
}

// TestDirMoveKeepsTheOccupierUntilTheDirectoryArrives is why a directory move
// reads its destination back before it deletes anything: the name that has to be
// free is the one the arrival will wear, which is the source's, and whoever held
// it was moved aside on the strength of a task that has only been reported done.
// If the directory is not standing there, the parked object is handed its name
// back and nothing that was already on the drive is removed.
func TestDirMoveKeepsTheOccupierUntilTheDirectoryArrives(t *testing.T) {
	const subID = "900000000000000048"
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	// archive holds a file named "sub", which is parked so the directory can
	// arrive under that name; the destination is then read back and answers
	// nothing, because the directory did not arrive.
	drive.listingFn = newListingScript(map[string][]string{
		// the first root walk answers the destination parent lookup (no "archive"
		// yet), the rest carry the source's "sub"
		rootID: {emptyListing(), listingJSON(nil, []string{folderJSON(subID, "sub", rootID)}),
			listingJSON(nil, []string{folderJSON(subID, "sub", rootID)}),
			listingJSON(nil, []string{folderJSON(subID, "sub", rootID)})},
		archiveID: {
			emptyListing(), // the create's verification
			emptyListing(), // the check that the destination does not exist yet
			emptyListing(), // the wanted name holds no file
			listingJSON([]string{fileJSON("6000000000000000006", "sub", archiveID)}, nil), // the occupier
			emptyListing(), // the readback, and the two waited out
			emptyListing(),
			emptyListing(),
		},
	}).fn

	err := f.DirMove(context.Background(), f, "sub", "archive/moved")
	require.Error(t, err)
	require.ErrorIs(t, err, fs.ErrorObjectNotFound, "what the destination was asked for is what is missing")

	require.Len(t, drive.renames, 2, "parked, then given its name back")
	require.Equal(t, "6000000000000000006", drive.renames[1].id)
	require.Equal(t, "sub", drive.renames[1].name)
	require.Len(t, drive.tasks, 1, "the move, and nothing else")
	require.Equal(t, taskMove, drive.tasks[0].taskType)
}

// TestCopyReadbackSkipsTheRowThisRunParked is the row this run renamed aside
// showing up in the destination listing as if it were the copy: the drive keeps
// answering a name for a moment after it has been renamed away, and a parked row is
// not in the negative cache because a rename is not a delete. Picking it means
// installing an unrelated object under the wanted name and then deleting it.
func TestCopyReadbackSkipsTheRowThisRunParked(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	unrelated := fileJSON("7000000000000000020", "clip-01.mp4", archiveID)
	realCopy := fileJSON("7000000000000000007", "clip-01.mp4", archiveID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(),                                  // the create's verification
			listingJSON([]string{unrelated}, nil),           // the holder of the arrival name
			listingJSON([]string{unrelated, realCopy}, nil), // the readback, still one rename behind
			emptyListing(),                                  // the wanted name is free
		},
	}).fn

	obj, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/Holiday.MP4")
	require.NoError(t, err)
	require.Equal(t, "7000000000000000007", obj.(*Object).id, "the copy, not the object parked out of its way")

	require.Len(t, drive.renames, 3)
	require.Equal(t, "7000000000000000020", drive.renames[0].id, "parked")
	require.Equal(t, "7000000000000000007", drive.renames[1].id, "the copy installed")
	require.Equal(t, "Holiday.MP4", drive.renames[1].name)
	require.Equal(t, "7000000000000000020", drive.renames[2].id, "and the unrelated file given its name back")
	require.Equal(t, "clip-01.mp4", drive.renames[2].name)

	require.Len(t, drive.tasks, 1, "the copy, and nothing deleted")
	require.Equal(t, taskCopy, drive.tasks[0].taskType)
}

// TestMoveWithholdsTheDeleteWhenTheDestinationCannotBeConfirmed: the task reported
// itself done, the counters agreed, and the destination does not hold the object at
// all. The object that was parked out of the arrival's way must not be deleted on
// the strength of those counters - it was on the drive first - so it is given its
// name back and the move says so.
func TestMoveWithholdsTheDeleteWhenTheDestinationCannotBeConfirmed(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	unrelated := fileJSON("7000000000000000021", "clip-01.mp4", archiveID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(),                        // the create's verification
			listingJSON([]string{unrelated}, nil), // the holder of the arrival name
			// nothing else is scripted: every read of the proof finds the
			// destination empty, which is the case where the task did nothing.
		},
	}).fn

	_, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/moved.mp4")
	require.ErrorIs(t, err, fs.ErrorObjectNotFound, "the move is reported as what it was: nothing there")

	require.Len(t, drive.renames, 2, "the holder is parked and given its name back; nothing was installed")
	require.Equal(t, "7000000000000000021", drive.renames[0].id)
	require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))
	require.Equal(t, "7000000000000000021", drive.renames[1].id)
	require.Equal(t, "clip-01.mp4", drive.renames[1].name)

	require.Len(t, drive.tasks, 1, "the move, and nothing deleted for it")
	require.Equal(t, taskMove, drive.tasks[0].taskType)
}

// TestFailedMoveHandsTheArrivalNameBack: the task ran and landed this run's object
// on the name the destination already held, and the step after it failed - here the
// name rclone asked for is a directory. A plain restore cannot work any more: the
// name is held by the moved object and the drive does not overwrite, so the moved
// object is handed over first. Both stay on the drive, and the object that was
// there before this run keeps the name it had.
func TestFailedMoveHandsTheArrivalNameBack(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}, failInstall: true}
	f := newFakeFs(t, drive)
	unrelated := fileJSON("7000000000000000023", "clip-01.mp4", archiveID)
	fresh := fileJSON("900000000000000006", "clip-01.mp4", archiveID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(),                        // the create's verification
			listingJSON([]string{unrelated}, nil), // the holder of the arrival name
			listingJSON([]string{fresh}, nil),     // the proof: the task landed it
			// the name rclone asked for is a directory
			listingJSON(nil, []string{folderJSON("5000000000000000007", "sub", archiveID)}),
			listingJSON([]string{fresh}, nil), // undoArrival reads who holds the arrival name
		},
	}).fn

	_, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/sub")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is a directory on the drive")

	require.Len(t, drive.renames, 4, "park, a refused restore, the moved object handed over, the restore that worked")
	require.Equal(t, "7000000000000000023", drive.renames[0].id)
	require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))
	require.Equal(t, "7000000000000000023", drive.renames[1].id, "the restore the drive refused")
	require.Equal(t, "clip-01.mp4", drive.renames[1].name)
	require.Equal(t, "900000000000000006", drive.renames[2].id, "the moved object is set aside")
	require.True(t, strings.HasPrefix(drive.renames[2].name, "rclone-park-"))
	require.Equal(t, "7000000000000000023", drive.renames[3].id)
	require.Equal(t, "clip-01.mp4", drive.renames[3].name, "the object that was there first has its name back")

	require.Len(t, drive.tasks, 1, "the move, and nothing deleted for it")
	require.Equal(t, taskMove, drive.tasks[0].taskType)
}

// TestFailedMoveRetriesTheRestoreWhenTheNameReadsFree: the first restore is refused
// and the name then reads back empty - the refusal was about the rename itself, not
// a collision - so it is tried once more rather than leaving the object that was on
// the drive before the run parked for good.
func TestFailedMoveRetriesTheRestoreWhenTheNameReadsFree(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}, failInstall: true}
	f := newFakeFs(t, drive)
	unrelated := fileJSON("7000000000000000028", "clip-01.mp4", archiveID)
	fresh := fileJSON("900000000000000006", "clip-01.mp4", archiveID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(),                        // the create's verification
			listingJSON([]string{unrelated}, nil), // the holder of the arrival name
			listingJSON([]string{fresh}, nil),     // the proof: the task landed it
			// the name rclone asked for is a directory
			listingJSON(nil, []string{folderJSON("5000000000000000009", "sub", archiveID)}),
			// and the arrival name reads back free, so a restore is worth trying again
			emptyListing(),
		},
	}).fn

	_, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/sub")
	require.Error(t, err)

	require.Len(t, drive.renames, 3, "parked, one refused restore, and the restore that was tried again")
	require.Equal(t, "7000000000000000028", drive.renames[1].id)
	require.Equal(t, "clip-01.mp4", drive.renames[1].name, "the restore the drive refused")
	require.Equal(t, "7000000000000000028", drive.renames[2].id)
	require.Equal(t, "clip-01.mp4", drive.renames[2].name, "and the one that put it back")
}

// TestFailedMoveLeavesAForeignHolderAlone: the name the move failed on is held by an
// object this run did not put there, so it is not something this run may set aside.
// The parked object stays parked and the log says so; what must not happen is the
// foreign object being renamed out of the way to make room.
func TestFailedMoveLeavesAForeignHolderAlone(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}, failInstall: true}
	f := newFakeFs(t, drive)
	unrelated := fileJSON("7000000000000000026", "clip-01.mp4", archiveID)
	fresh := fileJSON("900000000000000006", "clip-01.mp4", archiveID)
	foreign := fileJSON("7000000000000000027", "clip-01.mp4", archiveID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(),                        // the create's verification
			listingJSON([]string{unrelated}, nil), // the holder of the arrival name
			listingJSON([]string{fresh}, nil),     // the proof: the task landed it
			// the name rclone asked for is a directory
			listingJSON(nil, []string{folderJSON("5000000000000000008", "sub", archiveID)}),
			// and the arrival name is now held by an object this run did not write
			listingJSON([]string{foreign}, nil),
		},
	}).fn

	_, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/sub")
	require.Error(t, err)

	require.Len(t, drive.renames, 2, "the holder parked and one refused restore; the foreign object is left alone")
	require.Equal(t, "7000000000000000026", drive.renames[0].id)
	require.Equal(t, "7000000000000000026", drive.renames[1].id)
	require.Equal(t, "clip-01.mp4", drive.renames[1].name)
}

// TestArrivalWaitOutlastsAStaleRow: the first read after the task answers with the
// row the park moved away, because the listing is one rename behind. That row is not
// this run's object, and treating it as an answer would report a move that worked as
// failed - and the restore that follows would then collide with the object on its
// way in. The wait exists to let the listing catch up.
func TestArrivalWaitOutlastsAStaleRow(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	parked := fileJSON("7000000000000000024", "clip-01.mp4", archiveID)
	fresh := fileJSON("900000000000000006", "clip-01.mp4", archiveID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(),                     // the create's verification
			listingJSON([]string{parked}, nil), // the holder of the arrival name
			listingJSON([]string{parked}, nil), // the stale read: the row the park moved
			listingJSON([]string{fresh}, nil),  // and then the drive catches up
			emptyListing(),                     // the install's lookup of the wanted name
		},
	}).fn

	obj, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/moved.mp4")
	require.NoError(t, err)
	require.Equal(t, "archive/moved.mp4", obj.Remote())

	require.Len(t, drive.renames, 3, "the holder parked, the arrival renamed, the holder given its name back")
	require.Equal(t, "7000000000000000024", drive.renames[0].id)
	require.Equal(t, "900000000000000006", drive.renames[1].id)
	require.Equal(t, "moved.mp4", drive.renames[1].name)
	require.Equal(t, "7000000000000000024", drive.renames[2].id)
	require.Equal(t, "clip-01.mp4", drive.renames[2].name)
}

// TestMoveRefusesAnArrivalThatIsSomeoneElse: every read of the arrival name answers
// with a row that is not this run's object. A name being occupied is not the same as
// the move having landed, so the move is an error - and nothing that was on the drive
// is removed for it.
func TestMoveRefusesAnArrivalThatIsSomeoneElse(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	other := fileJSON("7000000000000000025", "clip-01.mp4", archiveID)
	drive.listingFn = func(r *http.Request) string {
		if r.URL.Query().Get("folderId") == archiveID {
			return listingJSON([]string{other}, nil)
		}
		return emptyListing()
	}

	_, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/moved.mp4")
	require.Error(t, err)
	require.True(t, fserrors.IsNoRetryError(err), "that row is not going to turn into this run's object")

	require.Len(t, drive.tasks, 1, "the task was sent")
	require.Len(t, drive.renames, 2, "the holder was parked and given its name back; no arrival was renamed")
	require.Equal(t, "7000000000000000025", drive.renames[1].id)
	require.Equal(t, "clip-01.mp4", drive.renames[1].name)
}

// TestArrivedCopyWaitsForTheObjectToAppear: a COPY answers with no id, so the only
// way to find what it made is the destination listing - and the first read after the
// task can still answer with the state before it. Reporting failure on that read
// makes the caller copy the object a second time.
func TestArrivedCopyWaitsForTheObjectToAppear(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {
			emptyListing(), // the first read, one task behind
			listingJSON([]string{fileJSON("7000000000000000007", "clip-01.mp4", rootID)}, nil),
		},
	}).fn

	row, err := f.arrivedCopy(context.Background(), rootID, "clip-01.mp4", "900000000000000006", "")
	require.NoError(t, err)
	require.Equal(t, "7000000000000000007", row.ID.String())
}

// TestDirMoveRefusesAFileAtTheDestinationName: the directory cache refuses a
// destination that already holds a DIRECTORY and says nothing about a file standing
// there. Without this check the file is met by the rename at the end, after the task
// has run and something has been parked - which is too late to give that something
// its name back.
func TestDirMoveRefusesAFileAtTheDestinationName(t *testing.T) {
	const subID = "900000000000000048"
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	subFolder := listingJSON(nil, []string{folderJSON(subID, "sub", rootID)})
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {subFolder, subFolder, subFolder, subFolder},
		archiveID: {
			emptyListing(), // the create's verification of the parent chain
			// the existence check looks for a DIRECTORY and finds none
			listingJSON([]string{fileJSON("6000000000000000007", "renamed", archiveID)}, nil),
			// and the check that runs before anything is parked finds the file
			listingJSON([]string{fileJSON("6000000000000000007", "renamed", archiveID)}, nil),
		},
	}).fn

	err := f.DirMove(context.Background(), f, "sub", "archive/renamed")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is a file on the drive")
	require.False(t, fserrors.IsNoRetryError(err),
		"a row another remote deleted can still be in this one's listing, so the refusal is worth a retry")

	require.Empty(t, drive.tasks, "nothing is sent")
	require.Empty(t, drive.renames, "and nothing is parked")
}

// TestSwapWaitsForTheListingToCatchUp pins the wait itself: the first read of the
// destination is the stale one, and giving up on it would turn a completed
// operation into a reported failure whose restore then collides with the object
// that is on its way in.
func TestSwapWaitsForTheListingToCatchUp(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(), // the create's verification
			emptyListing(), // nothing holds the arrival name
			emptyListing(), // the first, stale read of the proof
			listingJSON([]string{fileJSON("900000000000000006", "clip-01.mp4", archiveID)}, nil),
			emptyListing(), // the install's lookup of the wanted name
		},
	}).fn

	obj, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/moved.mp4")
	require.NoError(t, err, "the second read is the one that agrees with the task")
	require.Equal(t, "archive/moved.mp4", obj.Remote())
	require.Len(t, drive.tasks, 1)
}

// TestUndoClearRunsAfterTheContextIsGone: the undo is the handler for a failed
// operation, and the commonest way an operation fails part-way through a park is
// the user stopping the run. Renaming with the dead context would strand the
// object under a park name - and the next sync reads that as a file to delete.
func TestUndoClearRunsAfterTheContextIsGone(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	parked := fileRow("6000000000000000006", "rclone-park-1234-5678", rootID)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f.undoClear(ctx, parked, "clip-01.mp4")

	require.Len(t, drive.renames, 1, "the restore still goes out on a detached context")
	require.Equal(t, "6000000000000000006", drive.renames[0].id)
	require.Equal(t, "clip-01.mp4", drive.renames[0].name)
}

// TestDirMoveBetweenTwoDifferentFamiliesIsRefused: a family to family move
// across two different families was never captured, and stays refused even
// though both remotes share the account.
func TestDirMoveBetweenTwoDifferentFamiliesIsRefused(t *testing.T) {
	drive := &fakeDrive{t: t}
	src := newFakeFs(t, drive)
	dst := newFakeFs(t, drive)
	dst.opt.AccessToken = src.opt.AccessToken
	dst.opt.Space = spaceFamily
	dst.familyID = famID + "9"
	dst.familyRootID = famRoot
	dst.dirCache = dircache.New("", famRoot, dst)

	err := dst.DirMove(context.Background(), src, "sub", "sub")
	require.ErrorIs(t, err, fs.ErrorCantDirMove)
	require.Empty(t, sequenceOf(drive), "and the drive is not asked")
}

// TestCopyNamesTheSourceAsTheSourceRemoteStoresIt is the case of two remotes of
// one account carrying different encoding options. The name a COPY task carries
// is the name the drive lands the copy under - its source's own bytes - so the
// name this run clears and the object it reads back are both spelled by the
// source remote, while the destination's spelling belongs only to the rename that
// follows the task.
func TestCopyNamesTheSourceAsTheSourceRemoteStoresIt(t *testing.T) {
	drive := &fakeDrive{t: t}
	src := newFakeFs(t, drive)
	dst := newFakeFs(t, drive)
	dst.opt.AccessToken = src.opt.AccessToken
	dst.opt.Space = spaceFamily
	dst.familyID = famID
	dst.familyRootID = famRoot
	dst.dirCache = dircache.New("", famRoot, dst)
	// A destination that leaves < and > alone, unlike the source's default.
	dst.opt.Enc = encoder.Standard

	drive.listingFn = newListingScript(map[string][]string{
		famRoot: {
			// the name the arrival will wear is free here
			emptyListing(),
			// and the readback finds the copy under the SOURCE's spelling
			listingJSON([]string{fileJSON("900000000000000011", "a＜b＞.mp4", famRoot)}, nil),
			// the destination's own spelling, which nothing holds
			emptyListing(),
		},
	}).fn

	obj, err := dst.Copy(context.Background(), srcObject(src, "a<b>.mp4", "900000000000000006"), "a<b>.mp4")
	require.NoError(t, err, "the copy is found in the destination under the name the drive gave it")
	require.Equal(t, "900000000000000011", obj.(*Object).id)

	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskCopy, drive.tasks[0].taskType)
	require.Equal(t, "a＜b＞.mp4", drive.tasks[0].infos[0].FileName,
		"the task names the source the way the source remote stores it, not the way the destination would")
	require.Len(t, drive.renames, 1)
	require.Equal(t, "a<b>.mp4", drive.renames[0].name,
		"and the requested name is spelled with the destination's own encoding")
}

// TestFamilyToFamilyCopyIsRefused: that direction was never captured, so nothing
// is guessed with someone else's data on the line.
func TestFamilyToFamilyCopyIsRefused(t *testing.T) {
	drive := &fakeDrive{t: t}
	src := newFakeFs(t, drive)
	dst := newFakeFs(t, drive)
	for _, pair := range []struct {
		f        *Fs
		familyID string
		root     string
	}{
		{src, famID, famRoot},
		{dst, "999999999", "8888888888888888"},
	} {
		pair.f.opt.Space = spaceFamily
		pair.f.familyID = pair.familyID
		pair.f.familyRootID = pair.root
		pair.f.dirCache = dircache.New("", pair.root, pair.f)
	}

	_, err := dst.Copy(context.Background(), srcObject(src, "a.bin", "1234567890123456789"), "a.bin")
	require.ErrorIs(t, err, fs.ErrorCantCopy)
	require.Empty(t, sequenceOf(drive), "and the drive is not asked")
}

// TestServerSideMoveKeepsTheId is the ordinary move: a task, and the same object
// at a new path.
func TestServerSideMoveKeepsTheId(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(), // the create's verification
			emptyListing(), // nothing holds the name the arrival lands under
			// the task left the object in the destination under the name it moved with
			listingJSON([]string{fileJSON("900000000000000006", "clip-01.mp4", archiveID)}, nil),
			emptyListing(), // and nothing holds the wanted name; the rename is all that follows
		},
	}).fn

	obj, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/moved.mp4")
	require.NoError(t, err)
	require.Equal(t, "900000000000000006", obj.(*Object).id, "a move preserves the id")
	require.Equal(t, "archive/moved.mp4", obj.Remote())

	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskMove, drive.tasks[0].taskType)
	require.Equal(t, "clip-01.mp4", drive.tasks[0].infos[0].FileName,
		"the task names what lands, which is the name the object already had")
	require.Contains(t, drive.createBodies[0], "targetFolderId="+archiveID)
	require.Len(t, drive.renames, 1, "and the arrival is renamed into the name rclone asked for")
	require.Equal(t, "moved.mp4", drive.renames[0].name)
	require.Equal(t, "900000000000000006", drive.renames[0].id, "the same object, moved and renamed")
	require.Len(t, drive.tasks, 1, "and nothing was deleted")
}

// TestMoveDoesNotDeleteTheUnrelatedFileAtTheArrivalName is the hole the live suite
// walked into: the name that has to be free for a MOVE to land is the object's OWN
// name, which in the destination is usually held by somebody else - and that
// somebody is not what rclone asked to replace. Only when the move keeps the name
// (srcLeaf == wireLeaf) does the object standing in the way become the replaced
// one. Anything else has to be given its name back, or a move that renames deletes
// an unrelated file the user had in the destination all along.
func TestMoveDoesNotDeleteTheUnrelatedFileAtTheArrivalName(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	unrelated := fileJSON("7000000000000000020", "z.txt", archiveID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(),                        // the create's verification
			listingJSON([]string{unrelated}, nil), // the holder of the arrival name
			// the proof: the task left the object in the destination, under its own name
			listingJSON([]string{fileJSON("900000000000000006", "z.txt", archiveID)}, nil),
			emptyListing(), // and the wanted name is free
		},
	}).fn

	obj, err := f.Move(context.Background(), srcObject(f, "z.txt", "900000000000000006"), "archive/other.txt")
	require.NoError(t, err)
	require.Equal(t, "archive/other.txt", obj.Remote())

	require.Len(t, drive.renames, 3, "park the holder, install ours, give the holder its name back")
	require.Equal(t, "7000000000000000020", drive.renames[0].id)
	require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))
	require.Equal(t, "900000000000000006", drive.renames[1].id, "the moved object takes the wanted name")
	require.Equal(t, "other.txt", drive.renames[1].name)
	require.Equal(t, "7000000000000000020", drive.renames[2].id)
	require.Equal(t, "z.txt", drive.renames[2].name, "the unrelated file goes back where it was")

	require.Len(t, drive.tasks, 1, "the move, and nothing is deleted")
	require.Equal(t, taskMove, drive.tasks[0].taskType)
}

// TestMoveParksTheOccupierThenRenamesIntoTheName covers a move that also renames
// onto a name someone else holds.
func TestMoveParksTheOccupierThenRenamesIntoTheName(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	source := fileJSON("900000000000000006", "clip-01.mp4", rootID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {
			listingJSON([]string{source}, nil),
			listingJSON([]string{
				source,
				fileJSON("6000000000000000006", "holiday.MP4", rootID),
			}, nil),
		},
	}).fn

	_, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "holiday.MP4")
	require.NoError(t, err)

	// A move within one directory is a rename and no task: the drive has been
	// measured to destroy an object a MOVE addresses at its own directory.
	// What is left is the parked occupier's delete.
	require.Len(t, drive.tasks, 1, "only the delete of the parked occupier")
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Equal(t, "6000000000000000006", drive.tasks[0].infos[0].FileID)

	require.Len(t, drive.renames, 2)
	require.Equal(t, "6000000000000000006", drive.renames[0].id, "the occupier was parked")
	require.Equal(t, "900000000000000006", drive.renames[1].id, "then the moved object took the name")
	require.Equal(t, "holiday.MP4", drive.renames[1].name)
}

// TestMoveRefusesCrossSpace: only copy between the two spaces was captured.
func TestMoveRefusesCrossSpace(t *testing.T) {
	drive := &fakeDrive{t: t}
	src := newFakeFs(t, drive)
	dst := newFakeFs(t, drive)
	dst.opt.AccessToken = src.opt.AccessToken
	dst.opt.Space = spaceFamily
	dst.familyID = famID
	dst.familyRootID = famRoot
	dst.dirCache = dircache.New("", famRoot, dst)

	_, err := dst.Move(context.Background(), srcObject(src, "a.bin", "1234567890123456789"), "a.bin")
	require.ErrorIs(t, err, fs.ErrorCantMove)
	_, err = src.Move(context.Background(), srcObject(dst, "a.bin", "1234567890123456789"), "a.bin")
	require.ErrorIs(t, err, fs.ErrorCantMove)
	require.Empty(t, sequenceOf(drive))
}

// TestMoveReportsThroughTheDestinationRemote: two remote instances of one account
// can carry different roots and different encoding options, so the object a
// server-side move reports has to belong to the remote it was moved INTO. The
// source's object would spell the destination's path relative to the remote it
// came from, and every later read of it - a hash, a size, a download - would go
// to the wrong place. Both branches are covered because they build their answer
// separately.
func TestMoveReportsThroughTheDestinationRemote(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	src := newFakeFs(t, drive)
	dst := newFakeFs(t, drive)
	dst.opt.AccessToken = src.opt.AccessToken
	// The same object answers under both names over time: first the one the task
	// moved it with, which is what the proof reads back, then the one rclone asked
	// for, which is what installObject looks up and the final read confirms. Both
	// cannot be in one listing: the walk dedupes by id, and a move keeps the id.
	// The root stays empty, so the rename branch has nothing in its way.
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(), // the create's verification
			emptyListing(), // nothing holds the arrival name
			listingJSON([]string{fileJSON("900000000000000006", "clip-01.mp4", archiveID)}, nil),
			emptyListing(), // nothing holds the wanted name, so the rename goes out
		},
	}).fn

	moved, err := dst.Move(context.Background(), srcObject(src, "clip-01.mp4", "900000000000000006"), "archive/moved.mp4")
	require.NoError(t, err)
	require.Equal(t, "archive/moved.mp4", moved.Remote())
	require.True(t, moved.(*Object).fs == dst, "the task branch reports through the destination")
	require.False(t, moved.(*Object).fs == src)

	renamed, err := dst.Move(context.Background(), srcObject(src, "clip-01.mp4", "900000000000000007"), "renamed.mp4")
	require.NoError(t, err)
	require.Equal(t, "renamed.mp4", renamed.Remote())
	require.True(t, renamed.(*Object).fs == dst, "and so does the rename branch")
}

// TestDirMoveIsOneRecursiveTask is the point of moving a directory on the
// server: the drive takes the subtree with it, in one task.
func TestDirMoveIsOneRecursiveTask(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	subFolder := listingJSON(nil, []string{folderJSON("900000000000000007", "sub", rootID)})
	script := newListingScript(map[string][]string{
		// The create of "archive" verifies itself by listing the created
		// directory (an archive walk), and the destination-side walks all land
		// on archiveID; the root queue answers the destination-parent lookup
		// first and the source lookup after it. The last archive walk is the
		// arrival, still under the name the directory moved with.
		rootID: {emptyListing(), subFolder, subFolder, subFolder},
		archiveID: {emptyListing(), emptyListing(), emptyListing(), emptyListing(),
			listingJSON(nil, []string{folderJSON("900000000000000007", "sub", archiveID)})},
	})
	drive.listingFn = script.fn

	require.NoError(t, f.DirMove(context.Background(), f, "sub", "archive/moved"))

	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskMove, drive.tasks[0].taskType)
	require.Equal(t, "900000000000000007", drive.tasks[0].infos[0].FileID)
	require.EqualValues(t, 1, drive.tasks[0].infos[0].IsFolder, "a directory moves as one object")
	require.Len(t, drive.renames, 1, "and the arrival is renamed into the asked-for name")
	require.Equal(t, "moved", drive.renames[0].name)
	require.Contains(t, drive.createBodies[0], "targetFolderId="+archiveID)
	require.Empty(t, drive.puts, "no bytes moved")
	_, cached := f.dirCache.Get("sub")
	require.False(t, cached, "the moved directory leaves the source cache, it is not there any more")
}

// TestDirMoveRefusesTheRootAndAnExistingDestination: both are refused before the
// drive is asked to do anything.
func TestDirMoveRefusesTheRootAndAnExistingDestination(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {listingJSON(nil, []string{folderJSON("900000000000000004", "there", rootID)})},
	}).fn

	err := f.DirMove(context.Background(), f, "", "elsewhere")
	require.Error(t, err)
	require.Contains(t, err.Error(), "root")

	err = f.DirMove(context.Background(), f, "sub", "there")
	require.ErrorIs(t, err, fs.ErrorDirExists)
	require.Empty(t, drive.tasks, "and the drive was not asked to move anything")
}

// TestCrossSpaceDirMoveIsRefused keeps DirMove inside the rules Move follows.
func TestCrossSpaceDirMoveIsRefused(t *testing.T) {
	drive := &fakeDrive{t: t}
	src := newFakeFs(t, drive)
	dst := newFakeFs(t, drive)
	dst.opt.AccessToken = src.opt.AccessToken
	dst.opt.Space = spaceFamily
	dst.familyID = famID
	dst.familyRootID = famRoot
	dst.dirCache = dircache.New("", famRoot, dst)

	err := dst.DirMove(context.Background(), src, "sub", "sub")
	require.ErrorIs(t, err, fs.ErrorCantDirMove)
	require.Empty(t, sequenceOf(drive))
}

// TestTaskStateConflictIsReportedNotAnswered: a task that stops for a decision
// is a failure said out loud, because answering it needs the conflict endpoints,
// which are captured but not implemented.
func TestTaskStateConflictIsReportedNotAnswered(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}, taskStates: []int64{taskStateConflict}}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID:    {emptyListing()},
		archiveID: {emptyListing(), emptyListing()},
	}).fn

	// A cross-directory copy is the shape that still runs a task here: the
	// same-directory Copy is refused before the drive is asked, and the
	// same-directory Move is a rename that never reaches the task engine.
	_, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/clip-01.mp4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflict")
	require.True(t, fserrors.IsNoRetryError(err))
	require.Empty(t, drive.renames, "nothing was renamed on the way in")
}

// TestArrivedCopySkipsTheSource pins the helper's rule directly: of the rows
// carrying the name a copy arrived under, the one that is the source is not the
// copy, and neither is a row this process has just deleted. Copy refuses a
// same-directory copy before the task, so the listing that would need this is one
// the drive should not produce - which is exactly why the answer must not be the
// original.
func TestArrivedCopySkipsTheSource(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	source := fileJSON("900000000000000006", "clip-01.mp4", rootID)
	// every walk sees the same two rows: the source and the copy, both under the
	// name the readback asks for. A scripted queue would run out under the wait,
	// and an empty listing would make the failure cases pass for the wrong reason.
	twoRows := listingJSON([]string{source, fileJSON("7000000000000000007", "clip-01.mp4", rootID)}, nil)
	drive.listingFn = func(*http.Request) string { return twoRows }

	row, err := f.arrivedCopy(context.Background(), rootID, "clip-01.mp4", "900000000000000006", "")
	require.NoError(t, err)
	require.Equal(t, "7000000000000000007", row.ID.String())

	_, err = f.arrivedCopy(context.Background(), rootID, "gone.MP4", "900000000000000006", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "other than its source")

	// The drive keeps answering a deleted row for about a second after the delete
	// task reports done. A readback that picked such a row would hand rclone an
	// object already on its way to the recycle bin, so the same distrust findEntry
	// applies is applied here too.
	f.deleted.remember("7000000000000000007")
	_, err = f.arrivedCopy(context.Background(), rootID, "clip-01.mp4", "900000000000000006", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "other than its source")

	// A directory and a file can carry the same name in the same directory, and a
	// directory is never what a copy of a file turned into.
	drive2 := &fakeDrive{t: t}
	f2 := newFakeFs(t, drive2)
	nameSharedByAFolder := listingJSON(
		[]string{source, fileJSON("7000000000000000009", "clip-01.mp4", rootID)},
		[]string{folderJSON("5000000000000000009", "clip-01.mp4", rootID)})
	drive2.listingFn = func(*http.Request) string { return nameSharedByAFolder }
	row, err = f2.arrivedCopy(context.Background(), rootID, "clip-01.mp4", "900000000000000006", "")
	require.NoError(t, err)
	require.Equal(t, "7000000000000000009", row.ID.String(), "the folder that shares the name is not the copy")
}

// ------------------------------------------------------------ deleting -----

// TestFailedCopyPutsTheParkedNameBack is the other half of clearName: a name
// moved aside for a task that then did not run has to be handed back, because
// the object standing under that name was on the drive before this run and is
// not ours to leave stranded.
func TestFailedCopyPutsTheParkedNameBack(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	drive.taskStates = []int64{taskStateConflict}
	drive.listingFn = newListingScript(map[string][]string{
		rootID:    {emptyListing()},
		archiveID: {emptyListing(), listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", archiveID)}, nil)},
	}).fn

	_, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/clip-01.mp4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflict")
	require.True(t, fserrors.IsNoRetryError(err))

	require.Len(t, drive.tasks, 1, "the task that never finished")
	require.Len(t, drive.renames, 2, "parked, then given its name back")
	require.Equal(t, "6000000000000000006", drive.renames[0].id)
	require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))
	require.Equal(t, "6000000000000000006", drive.renames[1].id)
	require.Equal(t, "clip-01.mp4", drive.renames[1].name)
	for _, task := range drive.tasks {
		require.NotEqual(t, taskDelete, task.taskType, "nothing parked is deleted when the copy did not happen")
	}
}

// TestFailedMovePutsTheParkedNameBack is the same undo on the move side, and the
// reason the destination's name has to be the one handed back: the occupier was
// moved out of the destination name, which is a different name from the one the
// moving object still wears in its own directory. Restoring it under the source's
// name would leave the destination name held by nothing and the parked object
// sitting under a name that was never its own.
func TestFailedMovePutsTheParkedNameBack(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}, taskStates: []int64{taskStateConflict}}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(), // the create's verification
			// the arrival would wear the source's name here, and that name is held
			listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", archiveID)}, nil),
		},
	}).fn

	_, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/renamed.mp4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflict")

	require.Len(t, drive.tasks, 1, "the task that never finished")
	require.Equal(t, taskMove, drive.tasks[0].taskType)
	require.Len(t, drive.renames, 2, "parked, then given its name back")
	require.Equal(t, "6000000000000000006", drive.renames[0].id)
	require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))
	require.Equal(t, "6000000000000000006", drive.renames[1].id)
	require.Equal(t, "clip-01.mp4", drive.renames[1].name,
		"the name it was cleared from, which is the name the arrival would have worn")
	for _, task := range drive.tasks {
		require.NotEqual(t, taskDelete, task.taskType, "nothing parked is deleted when the move did not happen")
	}
}

// TestCopyRefusesToTakeADirectoryNameAndRemovesWhatItMade: the copy did land, so
// the object it made is the one thing this run created, and the rule is that a
// failure handler takes back what it made and never touches what was already on
// the drive. The directory holding the wanted name is neither listed nor moved.
func TestCopyRefusesToTakeADirectoryNameAndRemovesWhatItMade(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	script := newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(), // the create's verification
			emptyListing(), // the name the arrival will wear is free
			// the arrival, under the source's name
			listingJSON([]string{fileJSON("900000000000000011", "clip-01.mp4", archiveID)}, nil),
			// the holder of the wanted name: a directory, which nothing is moved aside for
			listingJSON(nil, []string{folderJSON("5000000000000000005", "holiday.MP4", archiveID)}),
		},
	})
	drive.listingFn = script.fn

	_, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/holiday.MP4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "is a directory on the drive")
	require.True(t, fserrors.IsNoRetryError(err))

	require.Equal(t, []string{taskCopy, taskDelete}, []string{drive.tasks[0].taskType, drive.tasks[1].taskType},
		"the copy is taken back, the directory is left alone")
	require.Equal(t, "900000000000000011", drive.tasks[1].infos[0].FileID, "what is deleted is the fresh copy")
	require.Zero(t, script.walksOf("5000000000000000005"), "the directory standing in the way is never listed")
	require.Empty(t, drive.renames)
}

// TestPurgeOfADirectoryIsOneRecursiveTask is the whole point of the delete task:
// the drive takes the subtree, so rclone does not walk it.
func TestPurgeOfADirectoryIsOneRecursiveTask(t *testing.T) {
	const subID = "900000000000000007"
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	script := newListingScript(map[string][]string{
		rootID: {listingJSON(nil, []string{folderJSON(subID, "sub", rootID)}),
			listingJSON(nil, []string{folderJSON(subID, "sub", rootID)})},
	})
	drive.listingFn = script.fn

	_, err := f.dirCache.FindDir(context.Background(), "sub", false)
	require.NoError(t, err)
	_, cached := f.dirCache.Get("sub")
	require.True(t, cached, "resolving a directory is what puts its id in the cache")

	require.NoError(t, f.Purge(context.Background(), "sub"))

	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Len(t, drive.tasks[0].infos, 1, "the directory itself, not one entry per child")
	require.Equal(t, subID, drive.tasks[0].infos[0].FileID)
	require.EqualValues(t, 1, drive.tasks[0].infos[0].IsFolder)
	require.Zero(t, script.walksOf(subID), "nothing inside it is listed")
	require.Empty(t, drive.puts)
	_, cached = f.dirCache.Get("sub")
	require.False(t, cached, "a deleted directory id must not stay in the cache")
}

// TestPurgeOfTheRootEmptiesItInOneTask: the root itself cannot go, but the drive
// takes several objects in one task, so emptying it is one request whatever it
// holds.
func TestPurgeOfTheRootEmptiesItInOneTask(t *testing.T) {
	const subID = "5000000000000000005"
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	root := listingJSON(
		[]string{fileJSON("3000000000000000003", "a.bin", rootID), fileJSON("4000000000000000004", "b.bin", rootID)},
		[]string{folderJSON(subID, "sub", rootID)})
	drive.listingFn = newListingScript(map[string][]string{
		// The same drive state, twice: once while the test resolves a directory
		// of its own, once for the walk Purge does before it deletes.
		rootID: {root, root},
	}).fn

	_, err := f.dirCache.FindDir(context.Background(), "sub", false)
	require.NoError(t, err)
	_, cached := f.dirCache.Get("sub")
	require.True(t, cached, "the root purge has to know about the ids it cached")

	require.NoError(t, f.Purge(context.Background(), ""))

	require.Len(t, drive.tasks, 1, "one task for the whole root")
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Len(t, drive.tasks[0].infos, 3)
	got := map[string]int{}
	for _, info := range drive.tasks[0].infos {
		got[info.FileID] = info.IsFolder
	}
	require.Equal(t, map[string]int{
		"3000000000000000003": 0, "4000000000000000004": 0, subID: 1,
	}, got, "files and directories go in the same task, each flagged as it is stored")
	_, cached = f.dirCache.Get("sub")
	require.False(t, cached, "a directory deleted from the root leaves the cache too")
}

// TestPurgeOfTheRootWithHardDeleteClearsTheRecycleBin: the drive has no delete
// that skips the recycle bin, so a hard delete is the same task and then a clear.
func TestPurgeOfTheRootWithHardDeleteClearsTheRecycleBin(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	f.opt.HardDelete = true
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {listingJSON([]string{fileJSON("3000000000000000003", "a.bin", rootID)}, nil)},
	}).fn

	require.NoError(t, f.Purge(context.Background(), ""))

	require.Len(t, drive.tasks, 2)
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Equal(t, taskClearRecycle, drive.tasks[1].taskType)
	require.Equal(t, drive.tasks[0].infos, drive.tasks[1].infos, "the clear names what the delete took")
}

// TestRmdirRefusesTheRoot: emptying the top level is Purge's job, and a task
// addressed at the root id would take everything with it.
func TestRmdirRefusesTheRoot(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)

	err := f.Rmdir(context.Background(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "top level")
	require.True(t, fserrors.IsNoRetryError(err))
	require.Empty(t, sequenceOf(drive), "and the drive is not asked")
}

// TestRmdirOfTheRootTheRemoteWasOpenedAt is the other half: a remote opened
// below the space root owns that directory, so rmdir of "" removes it - the
// shape the suite's teardown and `rclone rmdir remote:sub` both mean. The row
// is found in the space root's listing, which is where its parent number lives
// (captured 2026-10-23 [15]: a top-level delete names srcParentId -11).
func TestRmdirOfTheRootTheRemoteWasOpenedAt(t *testing.T) {
	const subID = "900000000000000042"
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	f.root = "sub"
	f.fsRootID = subID
	f.dirCache = dircache.New("sub", subID, f)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {listingJSON(nil, []string{folderJSON(subID, "sub", rootID)})},
		subID:  {emptyListing()},
	}).fn

	require.NoError(t, f.Rmdir(context.Background(), ""))
	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Equal(t, subID, drive.tasks[0].infos[0].FileID, "the directory the remote was opened at is what goes")
	require.EqualValues(t, -11, *drive.tasks[0].infos[0].ParentID, "its parent is named the way the drive spells the space root")
}

// TestMkdirAfterRmdirOfTheRootGoesBackToTheDrive: the cache of a remote whose
// root was just deleted holds nothing usable, and a Flush that kept the root's
// own id made the next mkdir of "" come back from the cache holding the deleted
// id - every operation under it then answered FileNotFound. Measured live
// 2026-10-23, twice, by the suite's own teardown. The root reset sends the next
// lookup back to the drive, which creates the directory again.
func TestMkdirAfterRmdirOfTheRootGoesBackToTheDrive(t *testing.T) {
	const subID = "900000000000000042"
	drive := &fakeDrive{t: t, folders: map[string]string{"sub": subID}}
	f := newFakeFs(t, drive)
	f.root = "sub"
	f.fsRootID = subID
	// seeded the way NewFs seeds it: with the space root, before the walk
	f.dirCache = dircache.New("sub", rootID, f)
	drive.listingFn = newListingScript(map[string][]string{
		// the walk that resolves the root, the rmdir's own walk, and after the
		// reset the re-resolution that must find the name gone. subID: the
		// rmdir's emptiness check and the recreated directory's verification.
		rootID: {listingJSON(nil, []string{folderJSON(subID, "sub", rootID)}),
			listingJSON(nil, []string{folderJSON(subID, "sub", rootID)}), emptyListing()},
		subID: {emptyListing(), emptyListing()},
	}).fn

	require.NoError(t, f.resolveAnchor(context.Background()))
	require.Equal(t, subID, f.fsRootID)
	require.NoError(t, f.Rmdir(context.Background(), ""))
	require.NoError(t, f.Mkdir(context.Background(), ""))
	require.Equal(t, []string{"sub"}, drive.mkdirs, "the deleted root is created again on the drive, not resurrected from the cache")
}

// TestPurgeAnchorIsTheDirectoryTheWalkResolved is the live lesson of 2026-10-23:
// NewFs seeded the purge anchor from rootID(), which names the space's top level
// whatever the remote is opened at. A remote opened at a subdirectory then
// carried the space root as its anchor, and `rclone purge remote:sub` listed the
// drive's top level and sent one DELETE task naming every entry of it - the
// server refused that batch, and a remote opened at a family subdirectory
// emptied the family root the same way before any refusal existed to save it.
// The Fs here is built exactly as NewFs used to leave it: dircache seeded with
// the space root, then resolved.
func TestPurgeAnchorIsTheDirectoryTheWalkResolved(t *testing.T) {
	const subID = "900000000000000043"
	drive := &fakeDrive{t: t, listings: map[string]string{
		rootID: listingJSON([]string{fileJSON("300000000000000009", "top.bin", rootID)},
			[]string{folderJSON(subID, "sub", rootID)}),
		// The personal endpoint answers file rows without a parentId; what the
		// delete task names as the parent has to come from the listing itself.
		subID: `{"res_code":0,"res_message":"成功","fileListAO":{"count":1,"fileList":[` +
			`{"id":"300000000000000010","name":"inside.bin","size":10,"type":"1"}],"folderList":[]}}`,
	}}
	f := newFakeFs(t, drive)
	f.root = "sub"
	f.dirCache = dircache.New("sub", rootID, f)
	f.fsRootID = rootID

	require.NoError(t, f.resolveAnchor(context.Background()))
	require.Equal(t, subID, f.fsRootID, "the anchor is the directory the walk resolved, not the space root")

	require.NoError(t, f.Purge(context.Background(), ""))
	require.Len(t, drive.tasks, 2)
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Len(t, drive.tasks[0].infos, 1, "only what the opened directory holds is addressed")
	require.Equal(t, "300000000000000010", drive.tasks[0].infos[0].FileID, "the top-level entry is not in the task")
	require.EqualValues(t, 900000000000000043, *drive.tasks[0].infos[0].ParentID)
	require.Equal(t, taskDelete, drive.tasks[1].taskType)
	require.Equal(t, subID, drive.tasks[1].infos[0].FileID, "and the emptied directory itself goes with it")
}

// TestRmdirOfADirectoryWhoseLastFileWasJustDeleted: the listing keeps answering
// the file this process just deleted, and its id is one nothing will answer to.
// Counting it refuses a directory that is empty - measured live 2026-10-25, the
// standard suite removes a directory's last file and the directory in the same
// breath, and was told the directory was not empty.
func TestRmdirOfADirectoryWhoseLastFileWasJustDeleted(t *testing.T) {
	const subID = "900000000000000046"
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	gone := fileJSON("3000000000000000077", "a.bin", subID)
	folder := folderJSON(subID, "sub", rootID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {listingJSON(nil, []string{folder})}, // the lookup of "sub"
		subID:  {listingJSON([]string{gone}, nil)},   // its children, still listing the deleted file
	}).fn
	f.deleted.remember("3000000000000000077")

	require.NoError(t, f.Rmdir(context.Background(), "sub"))
	require.Len(t, drive.tasks, 1, "the directory itself goes, and nothing else")
	require.Equal(t, subID, drive.tasks[0].infos[0].FileID)
	require.EqualValues(t, 1, drive.tasks[0].infos[0].IsFolder)
}

// TestPurgeReadsItsAnchorAgainWhenTheOpenLeftItUnset: a listing that fails while
// the remote is being opened leaves the purge anchor empty, while every other path
// keeps working through the directory cache. Measured live 2026-10-25: the standard
// suite's teardown purge refused a root it had been using all run. The anchor is
// read again, and a root that really does not resolve is still refused.
func TestPurgeReadsItsAnchorAgainWhenTheOpenLeftItUnset(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	root := listingJSON([]string{fileJSON("3000000000000000003", "a.bin", rootID)}, nil)
	drive.listingFn = newListingScript(map[string][]string{rootID: {root}}).fn
	f.fsRootID = ""

	require.NoError(t, f.Purge(context.Background(), ""))
	require.Equal(t, personalRoot, f.fsRootID, "the anchor was read back from the cache before the purge")
	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Equal(t, "3000000000000000003", drive.tasks[0].infos[0].FileID)
}

// TestPurgeRefusesWhenNothingWasResolved: a remote whose root the walk could not
// resolve has no honest anchor, and the space root is not one - the anchor stays
// empty and the destructive paths refuse rather than guess.
func TestPurgeRefusesWhenNothingWasResolved(t *testing.T) {
	drive := &fakeDrive{t: t, listings: map[string]string{
		rootID: listingJSON([]string{fileJSON("300000000000000009", "top.bin", rootID)}, nil),
	}}
	f := newFakeFs(t, drive)
	f.root = "missing"
	f.dirCache = dircache.New("missing", rootID, f)
	f.fsRootID = ""
	require.Error(t, f.resolveAnchor(context.Background()), "nothing resolved, so there is no anchor to read back")

	err := f.Purge(context.Background(), "")
	require.Error(t, err)
	require.Empty(t, drive.tasks, "the space root is never dressed up as the target")
}

// TestFindEntryDistrustsAJustDeletedRow: the listing keeps answering a deleted
// object's row for about a second after the delete task reports done, and the
// id that row carries is one nothing will ever answer to again. A lookup that
// trusts it binds the dead id - the shape the suite's rmdir-then-mkdir of the
// same root measured live.
func TestFindEntryDistrustsAJustDeletedRow(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		// two walks: one for the remote that deleted the row, one for the fresh one
		rootID: {listingJSON([]string{fileJSON("300000000000000003", "a.bin", rootID)}, nil),
			listingJSON([]string{fileJSON("300000000000000003", "a.bin", rootID)}, nil)},
	}).fn
	f.deleted.remember("300000000000000003")

	_, err := f.findEntry(context.Background(), rootID, f.opt.Enc.FromStandardName("a.bin"))
	require.ErrorIs(t, err, fs.ErrorObjectNotFound, "the row the drive still carries is the one this process deleted")

	f2 := newFakeFs(t, drive)
	_, err = f2.findEntry(context.Background(), rootID, f2.opt.Enc.FromStandardName("a.bin"))
	require.NoError(t, err, "a fresh remote has deleted nothing, so the row is found as usual")
}

// TestNewFsOnAFilePathReturnsErrorIsFile: a remote whose root names an existing
// file returns the parent Fs and fs.ErrorIsFile, however deep the file sits -
// the suite's FsIsFile opens exactly such a path and checks Root() names the
// parent directory.
func TestNewFsOnAFilePathReturnsErrorIsFile(t *testing.T) {
	const deepID = "900000000000000045"
	drive := &fakeDrive{t: t, listings: map[string]string{
		rootID:               listingJSON(nil, []string{folderJSON("900000000000000044", "sub", rootID)}),
		"900000000000000044": listingJSON(nil, []string{folderJSON(deepID, "deep", "900000000000000044")}),
		deepID:               listingJSON([]string{fileJSON("300000000000000011", "z.txt", deepID)}, nil),
	}}
	f := newFakeFs(t, drive)
	// seeded the way NewFs seeds it, root naming the file
	f.root = "sub/deep/z.txt"
	f.dirCache = dircache.New(f.root, rootID, f)

	err := f.resolveRoot(context.Background())
	require.ErrorIs(t, err, fs.ErrorIsFile)
	require.Equal(t, "sub/deep", f.root, "the remote is re-rooted at the file's directory")
	require.Equal(t, deepID, f.fsRootID, "and the purge anchor is that directory")
}

// TestNewFsOnAMissingDirectoryKeepsItsOwnPath is the other half of the same
// walk: the parent resolved, the leaf is neither a file nor a directory, so the
// path names nothing. The remote keeps that path and the cache stays the one
// seeded with the space root. Had the parent-seeded cache been left in place -
// which is what this path used to do - the Fs would have claimed
// "sub/missing" while resolving every operation against "sub": a listing there
// would read the parent's contents, and a write would make its folders one level
// above where the user pointed.
func TestNewFsOnAMissingDirectoryKeepsItsOwnPath(t *testing.T) {
	const (
		subID2    = "900000000000000049"
		missingID = "900000000000000050"
		innerID   = "900000000000000051"
	)
	drive := &fakeDrive{
		t:       t,
		folders: map[string]string{"missing": missingID, "inner": innerID},
		listings: map[string]string{
			rootID: listingJSON(nil, []string{folderJSON(subID2, "sub", rootID)}),
			subID2: listingJSON([]string{fileJSON("300000000000000012", "sibling.txt", subID2)}, nil),
		},
	}
	f := newFakeFs(t, drive)
	f.root = "sub/missing"
	f.dirCache = dircache.New(f.root, rootID, f)

	require.NoError(t, f.resolveRoot(context.Background()), "a path naming nothing is not a failure: a write may still make it")
	require.Equal(t, "sub/missing", f.root, "the remote keeps the path it was given")
	require.Empty(t, f.fsRootID, "and the anchor stays empty, so whole-directory operations refuse")

	_, err := f.List(context.Background(), "")
	require.ErrorIs(t, err, fs.ErrorDirNotFound, "listing the missing directory says so, it does not read its parent")

	require.NoError(t, f.Mkdir(context.Background(), "inner"))
	require.Equal(t, []string{"missing", "inner"}, drive.mkdirs,
		"the missing chain is made inside the path the user named, not beside it")
	id, cerr := f.dirCache.FindDir(context.Background(), "inner", false)
	require.NoError(t, cerr)
	require.Equal(t, innerID, id, "and it resolves inside the directory that was missing")
}

// TestRmdirRefusesANonEmptyDirectory is the difference between Rmdir and Purge:
// the drive cannot remove a directory that has children without taking the
// children, so the listing is checked and the call is refused.
func TestRmdirRefusesANonEmptyDirectory(t *testing.T) {
	const subID = "900000000000000007"
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	script := newListingScript(map[string][]string{
		rootID: {listingJSON(nil, []string{folderJSON(subID, "sub", rootID)})},
		subID:  {listingJSON([]string{fileJSON("3000000000000000003", "a.bin", subID)}, nil)},
	})
	drive.listingFn = script.fn

	err := f.Rmdir(context.Background(), "sub")
	require.ErrorIs(t, err, fs.ErrorDirectoryNotEmpty)
	require.Empty(t, drive.tasks, "nothing is deleted")
}

// TestRmdirOfAnEmptyDirectoryIsOneTask covers the case it does handle.
func TestRmdirOfAnEmptyDirectoryIsOneTask(t *testing.T) {
	const subID = "900000000000000007"
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	sub := listingJSON(nil, []string{folderJSON(subID, "sub", rootID)})
	script := newListingScript(map[string][]string{
		// Twice, because the test resolves the directory first: that is what
		// puts its id in the cache, which is the thing Rmdir has to forget.
		rootID: {sub, sub},
		subID:  {emptyListing()},
	})
	drive.listingFn = script.fn

	_, err := f.dirCache.FindDir(context.Background(), "sub", false)
	require.NoError(t, err)
	_, cached := f.dirCache.Get("sub")
	require.True(t, cached)

	require.NoError(t, f.Rmdir(context.Background(), "sub"))

	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Equal(t, subID, drive.tasks[0].infos[0].FileID)
	require.Equal(t, 1, script.walksOf(subID), "it is listed once, to prove it is empty")
	require.Equal(t, 2, script.walksOf(rootID), "the test's own lookup, then Rmdir's")
	_, cached = f.dirCache.Get("sub")
	require.False(t, cached, "and its id leaves the cache")
}

// TestRmdirOfAFileSaysDirectoryNotFound: the name is there but is not a
// directory, and an object that happens to share the name must not be deleted.
func TestRmdirOfAFileSaysDirectoryNotFound(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	script := newListingScript(map[string][]string{
		rootID: {listingJSON([]string{fileJSON("3000000000000000003", "sub", rootID)}, nil)},
	})
	drive.listingFn = script.fn

	err := f.Rmdir(context.Background(), "sub")
	require.ErrorIs(t, err, fs.ErrorDirNotFound)
	require.Empty(t, drive.tasks, "the file is not deleted")
	require.Zero(t, script.walksOf("3000000000000000003"), "and it is not listed as if it were a directory")
}

// TestRmdirOfAMissingDirectorySaysSo: the drive is asked once and answers
// nothing, which is a missing directory rather than an error.
func TestRmdirOfAMissingDirectorySaysSo(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
	}).fn

	err := f.Rmdir(context.Background(), "sub")
	require.ErrorIs(t, err, fs.ErrorDirNotFound)
	require.Empty(t, drive.tasks)
}

// TestNamesReachTheDriveEncodedOnce is the character-table rule written into the
// code. A name rclone spells "a:b" is stored as "a：b", and the lookalike table
// runs both ways, so encoding that stored name a second time asks for "a:b" -
// which is not on the drive. Every lookup here goes by the stored spelling, so a
// second encoding anywhere on the path makes a real directory look missing.
func TestNamesReachTheDriveEncodedOnce(t *testing.T) {
	const dirID = "9000000000000000009"
	t.Run("an empty directory whose stored name is the encoded one", func(t *testing.T) {
		drive := &fakeDrive{t: t}
		f := newFakeFs(t, drive)
		drive.listingFn = newListingScript(map[string][]string{
			rootID: {listingJSON(nil, []string{folderJSON(dirID, "a：b", rootID)})},
			dirID:  {emptyListing()},
		}).fn

		require.NoError(t, f.Rmdir(context.Background(), "a:b"))
		require.Len(t, drive.tasks, 1)
		require.Equal(t, dirID, drive.tasks[0].infos[0].FileID, "addressed by id, found by the name the drive stores")
		require.Equal(t, "a：b", drive.tasks[0].infos[0].FileName)
	})

	t.Run("a name taken over by a copy", func(t *testing.T) {
		drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
		f := newFakeFs(t, drive)
		drive.listingFn = newListingScript(map[string][]string{
			rootID: {emptyListing()},
			archiveID: {
				emptyListing(), // the create's verification
				emptyListing(), // the name the arrival will wear is free
				// the arrival, under the source's own spelling
				listingJSON([]string{fileJSON("900000000000000011", "clip-01.mp4", archiveID)}, nil),
				// the holder of the wanted name, spelled the way the drive stores it
				listingJSON([]string{fileJSON("6000000000000000006", "a：b", archiveID)}, nil),
			},
		}).fn

		_, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/a:b")
		require.NoError(t, err)

		require.Len(t, drive.renames, 2)
		require.Equal(t, "6000000000000000006", drive.renames[0].id, "the holder of the encoded name was found")
		require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))
		require.Equal(t, "900000000000000011", drive.renames[1].id)
		require.Equal(t, "a：b", drive.renames[1].name, "and the copy was given the stored spelling of it")

		require.Len(t, drive.tasks, 2)
		require.Equal(t, taskDelete, drive.tasks[1].taskType)
		require.Equal(t, "6000000000000000006", drive.tasks[1].infos[0].FileID)
		require.Equal(t, drive.renames[0].name, drive.tasks[1].infos[0].FileName,
			"the delete describes the object as the drive now spells it, not as it did before the swap")
	})

	t.Run("an object read by its rclone name", func(t *testing.T) {
		drive := &fakeDrive{t: t}
		f := newFakeFs(t, drive)
		drive.listingFn = newListingScript(map[string][]string{
			rootID: {listingJSON([]string{fileJSON("7000000000000000007", "a：b", rootID)}, nil)},
		}).fn

		obj, err := f.NewObject(context.Background(), "a:b")
		require.NoError(t, err)
		require.Equal(t, "a:b", obj.Remote(), "reported back in the spelling rclone uses")
		require.Equal(t, int64(10), obj.Size())
	})
}

// TestMkdirCreatesTheDirectoryOnce covers the entry point rclone uses for
// `rclone mkdir` and for --mkdirs, and the second half is the point: once the
// directory cache knows the id, nothing goes to the drive at all.
func TestMkdirCreatesTheDirectoryOnce(t *testing.T) {
	const newID = "900000000000000020"
	drive := &fakeDrive{t: t, folders: map[string]string{"fresh": newID}}
	f := newFakeFs(t, drive)
	script := newListingScript(map[string][]string{
		// one walk for FindLeaf, one for the create's read-back
		rootID: {emptyListing(), emptyListing()},
	})
	drive.listingFn = script.fn

	require.NoError(t, f.Mkdir(context.Background(), "fresh"))
	require.Equal(t, []string{"fresh"}, drive.mkdirs)
	require.Equal(t, 1, script.walksOf(rootID), "the parent is listed once, to find the name missing")
	require.Equal(t, 1, script.walksOf(newID), "and the created directory is listed once, to prove the id answers")

	require.NoError(t, f.Mkdir(context.Background(), "fresh"))
	require.Equal(t, []string{"fresh"}, drive.mkdirs, "a directory the cache knows is not created twice")
	require.Equal(t, 1, script.walksOf(rootID), "nor is anything listed again")
}

// TestMkdirOfTheFirstFolderInAnEmptyFamilyDriveNamesNoParent: a family drive that
// has never held anything lists no entries, so there is no row to read the numeric
// root id from - and the family createFolder refuses the 0 alias. The one shape the
// server accepts there leaves parentId out, which is how the official client makes
// its own transfer folder (measured 2026-10-25 from a capture of the PC client).
func TestMkdirOfTheFirstFolderInAnEmptyFamilyDriveNamesNoParent(t *testing.T) {
	const newID = "900000000000000021"
	drive := &fakeDrive{t: t, folders: map[string]string{"s3-bak": newID}}
	f := newFakeFs(t, drive)
	f.opt.Space = spaceFamily
	f.familyID = famID
	f.familyRootID = ""
	f.dirCache = dircache.New("", familyRootAlias, f)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()}, // the parent walk: the drive is empty
		newID:  {emptyListing()}, // the create's read-back
	}).fn

	require.NoError(t, f.Mkdir(context.Background(), "s3-bak"))
	require.Equal(t, []string{"s3-bak"}, drive.mkdirs)
	require.Len(t, drive.mkdirQueries, 1)
	q := drive.mkdirQueries[0]
	require.Equal(t, famID, q.Get("familyId"))
	require.Equal(t, "s3-bak", q.Get("folderName"))
	require.Empty(t, q.Get("parentId"),
		"an empty family drive has no numeric root id, so the folder is created without naming a parent")
}

// TestNilObjectStringIsTheNilConvention: the suite hands every backend a typed
// nil and expects "<nil>" back, the same a plain interface would print.
func TestNilObjectStringIsTheNilConvention(t *testing.T) {
	var o *Object
	require.Equal(t, "<nil>", o.String())
}

// TestSetModTimeReportsItCannotBeDone pins both halves of that answer: the
// sentinel rclone's callers look for, and the reason a user would need.
func TestSetModTimeReportsItCannotBeDone(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip.mp4", "900000000000000004")

	err := obj.SetModTime(context.Background(), time.Now())
	require.Error(t, err)
	// the bare sentinel: the suite compares it with ==, and operations treats
	// it as unsupported rather than as a failed transfer
	require.Equal(t, fs.ErrorCantSetModTime, err)
	require.Empty(t, sequenceOf(drive), "and the drive is never asked")
}

// TestADeleteTaskThatFailsWithNoReasonIsSentAgain is the shape measured in
// production on 2026-10-27 and captured on 2026-10-22: the DELETE task ends with
// failedCount 1 and no errorCode at all while the object is still on the drive,
// and the same targets sent again finish the job.
func TestADeleteTaskThatFailsWithNoReasonIsSentAgain(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1}}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", rootID)}, nil)
	}

	require.NoError(t, srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background()))

	require.Len(t, drive.tasks, 2, "the delete was sent twice")
	require.Equal(t, taskDelete, drive.tasks[0].taskType)
	require.Equal(t, taskDelete, drive.tasks[1].taskType)
	require.Equal(t, drive.tasks[0].infos, drive.tasks[1].infos, "the same targets, unchanged")
}

// TestADeleteStillFailingWithNoReasonIsReportedWhenTheObjectRemains: the two
// resends are the whole of the tolerance. If every send fails the same way and the
// object is still listed, this stays a failure - a delete that did not happen is
// not something to report as done.
func TestADeleteStillFailingWithNoReasonIsReportedWhenTheObjectRemains(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", rootID)}, nil)
	}

	err := srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "the server gave no reason")
	require.True(t, fserrors.IsNoRetryError(err), "and it is still not worth a second pass over the file")
	require.Len(t, drive.tasks, 3, "both resends, and no more")
}

// TestADeleteStillRefusedAfterTheFirstResendIsSentAgainAndClears is what the
// second resend is for. What was measured (2026-10-28, six passes with a tolerance
// of one) is that 16 of 63 refused tasks came back refused from the first resend
// too, and that those 16 were cleared by a later pass of rclone's rather than by
// anything the backend sent inside the failed one - see the deleteTaskResends
// comment. A refused delete never works itself out while the client waits, so
// within one pass the answer is to send again; how many sends one pass needs is a
// number that has not been measured.
func TestADeleteStillRefusedAfterTheFirstResendIsSentAgainAndClears(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1}}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", rootID)}, nil)
	}

	require.NoError(t, srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background()))
	sent := drivesTasks(drive)
	require.Len(t, sent, 3, "the object is listed between the sends, so all three are spent")
	// Every send has to carry the object. Dropping it from the second resend -
	// which is what a resend that trims its own target list looks like - would
	// report the delete done having only ever asked once.
	for i := range sent {
		require.Len(t, sent[i].infos, 1, "send %d of the chain carries the one target it was created for", i)
		require.Equal(t, "6000000000000000006", sent[i].infos[0].FileID, "and it is the refused object, every time")
	}
}

// TestADeleteThatIsAlreadyGoneIsNotSentAgain is the point of asking the drive
// before sending anything again: the task said it failed, the listing says the
// object is not there, so it is done - and no second task is spent on it.
//
// The directory has to be shown holding something else. A page that claims nothing
// at all is not evidence of absence, because it is also exactly what an address the
// listing endpoint does not know answers with - see
// TestADeleteWithRootFolderIDNamingTheSpaceRootStillSeesTheObject.
func TestADeleteThatIsAlreadyGoneIsNotSentAgain(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("6000000000000000007", "other.mp4", rootID)}, nil)
	}

	require.NoError(t, srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background()))

	require.Len(t, drive.tasks, 1, "the listing already said it is gone")
}

// TestADeleteOneLookIsNotEnoughToCallItGone pins the agreement rule itself. The
// first look says the object is not on the drive; the second says it is. A single
// "gone" must not finalise a delete, because a walk that came back without the row
// is also what a listing window that moved under it looks like - and a delete
// reported done while the object is there cannot be taken back.
func TestADeleteOneLookIsNotEnoughToCallItGone(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	walks := 0
	drive.listingFn = func(r *http.Request) string {
		if r.URL.Query().Get("pageNum") != "1" {
			// the page that ends a walk: nothing new on it
			return ""
		}
		walks++
		if walks == 1 {
			return listingJSON([]string{fileJSON("6000000000000000007", "other.mp4", rootID)}, nil)
		}
		return listingJSON([]string{
			fileJSON("6000000000000000006", "clip-01.mp4", rootID),
			fileJSON("6000000000000000007", "other.mp4", rootID),
		}, nil)
	}

	err := srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background())
	require.Error(t, err, "the second look found it, so the first look's 'gone' decided nothing")
	require.GreaterOrEqual(t, walks, 2, "both looks were spent")
	require.Len(t, drivesTasks(drive), 3, "the resend was owed, and the resend of that")
}

// TestADeleteThatSkippedIsSettledByTheListing: a skip is another answer that says
// nothing about whether the object is on the drive. The first look says it is
// still there, so it goes back once; the repeat is skipped and the second look
// finds it gone.
func TestADeleteThatSkippedIsSettledByTheListing(t *testing.T) {
	drive := &fakeDrive{t: t,
		deleteTaskFailures: []int64{1, 0},
		deleteTaskSkips:    []int64{0, 1},
	}
	f := newFakeFs(t, drive)
	// The object is on the drive for as long as only the first task has been sent,
	// and off it once the repeat is in: the first look therefore owes a resend, and
	// the second one settles it as done.
	drive.listingFn = func(*http.Request) string {
		if len(drivesTasks(drive)) < 2 {
			return listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", rootID)}, nil)
		}
		// gone, and shown so: the directory still holds another object, so the
		// page is a real answer and not a nothing-claims page
		return listingJSON([]string{fileJSON("6000000000000000007", "other.mp4", rootID)}, nil)
	}

	require.NoError(t, srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background()))
	require.Len(t, drive.tasks, 2, "the first look said still there, so the resend was owed")
}

// TestADeleteTaskThatFailsOnOneOfTwoObjectsIsNotTakenAsDone: the listing check asks
// about every target, not just the first one that is missing. The drive is shown
// with two objects before the task and one after, and the survivor is second in
// the list, so a check that stopped at the first absent target would call this a
// success while an object is still on the drive.
func TestADeleteTaskThatFailsOnOneOfTwoObjectsIsNotTakenAsDone(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	// The first walk is the purge's own listing and holds both objects; every walk
	// after it is a check, and answers with the survivor only. The check runs more
	// than once, so a queued script would run dry - this stays honest however many
	// looks it takes.
	walks := 0
	drive.listingFn = func(*http.Request) string {
		walks++
		if walks == 1 {
			return listingJSON([]string{
				fileJSON("6000000000000000008", "gone.mp4", rootID),
				fileJSON("6000000000000000007", "keep-me.mp4", rootID),
			}, nil)
		}
		return listingJSON([]string{fileJSON("6000000000000000007", "keep-me.mp4", rootID)}, nil)
	}

	err := f.Purge(context.Background(), "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "the server gave no reason")
	require.Len(t, drive.tasks, 3, "one task and two resends, and no more")
}

// TestADeleteCheckMatchesTheIdNotTheName: the object deleted was the one the task
// was given. A listing that has the name back on a different id means ours is gone,
// and asking by name would report a leftover that is not the one aimed at.
func TestADeleteCheckMatchesTheIdNotTheName(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("6000000000000000009", "clip-01.mp4", rootID)}, nil)
	}

	require.NoError(t, srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background()))
	require.Len(t, drive.tasks, 1, "ours is gone; a different object holding the name does not owe a second task")
}

// TestADeleteAtTheFamilyRootIsNotTakenAsDoneWhileTheObjectIsThere is the measured
// trap of the family space: a listing addressed at the numeric root id answers an
// empty page with no error. A check that fed the task's parent id straight to the
// listing would read a full drive as empty and report a delete that never
// happened - so the check has to name the alias, the way every other listing does.
func TestADeleteAtTheFamilyRootIsNotTakenAsDoneWhileTheObjectIsThere(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	f.opt.Space = spaceFamily
	f.familyID = famID
	f.familyRootID = famRoot
	// discoverFamilyRoot writes both: the anchor bodies name, and the space root
	// whose listing has to be addressed by the alias.
	f.familySpaceRoot = famRoot
	f.dirCache = dircache.New("", famRoot, f)
	drive.listingFn = func(r *http.Request) string {
		if r.URL.Query().Get("folderId") == famRoot {
			// measured 2026-10-22: the numeric root answers nothing, and no error
			return emptyListing()
		}
		return listingJSON([]string{fileJSON("900000000000000021", "clip-01.mp4", famRoot)}, nil)
	}

	err := srcObject(f, "clip-01.mp4", "900000000000000021").Remove(context.Background())
	require.Error(t, err, "the drive still holds the object, so the delete did not happen")
	require.Contains(t, err.Error(), "the server gave no reason")
}

// TestADeleteOneEmptyPageIsNotEnough: an empty listing is also what a truncated
// walk looks like, so a single "gone" answer must not settle a delete. The drive
// answers the object, then one empty page, then the object again, and the delete
// is still a failure at the end of it.
func TestADeleteOneEmptyPageIsNotEnough(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	looks := 0
	drive.listingFn = func(*http.Request) string {
		looks++
		if looks == 2 {
			return emptyListing()
		}
		return listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", rootID)}, nil)
	}

	err := srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background())
	require.Error(t, err, "one blank page in the middle of a walk is not proof the object is gone")
	require.Len(t, drivesTasks(drive), 3, "both resends are owed, and no more")
}

// drivesTasks reads the tasks a drive was asked to run, under its lock.
func drivesTasks(d *fakeDrive) []taskCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]taskCall(nil), d.tasks...)
}

// TestADeleteUnderARootFolderIDChecksTheDirectoryItWasToldAbout is the trap of
// having one field serve as both the anchor and the space root: with
// root_folder_id naming a subdirectory, the anchor is not the drive's top level,
// so rewriting it to the `0` alias looks in the wrong directory, finds nothing,
// and reports a live object as deleted.
func TestADeleteUnderARootFolderIDChecksTheDirectoryItWasToldAbout(t *testing.T) {
	const (
		anchor    = "9000000000000055" // where this remote is opened: a subdirectory
		spaceRoot = "9000000000000066" // the drive's own top level, read off a listing
		liveID    = "9000000000000077"
	)
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	f.opt.Space = spaceFamily
	f.opt.RootFolderID = anchor
	f.familyID = famID
	f.familyRootID = anchor
	f.familySpaceRoot = spaceRoot
	f.dirCache = dircache.New("", anchor, f)
	drive.listingFn = func(r *http.Request) string {
		if r.URL.Query().Get("folderId") != anchor {
			// the space root, which holds other things and not this object
			return listingJSON([]string{fileJSON("9000000000000088", "elsewhere.mp4", spaceRoot)}, nil)
		}
		return listingJSON([]string{fileJSON(liveID, "clip-01.mp4", anchor)}, nil)
	}

	err := srcObject(f, "clip-01.mp4", liveID).Remove(context.Background())
	require.Error(t, err, "the object is in the directory this remote is opened at, and it is still there")
	folders := driveFolders(drive)
	require.NotEmpty(t, folders)
	for _, got := range folders {
		require.Equal(t, anchor, got, "the check has to name the directory the row came from, never the alias")
	}
}

// TestADeleteWithRootFolderIDNamingTheSpaceRootStillSeesTheObject is the shape the
// option's own help recommends - space=family with root_folder_id set to the drive's
// real root id - and the reason rootID() now asks for the space root even then.
// Without that, familySpaceRoot stays empty, the absence check addresses the number
// directly, and the family endpoint answers a number with an empty page and no error
// (measured 2026-10-22). An empty page that claims nothing is a complete walk, so
// the check would conclude "gone" from a directory it never read and report a
// surviving object as deleted.
func TestADeleteWithRootFolderIDNamingTheSpaceRootStillSeesTheObject(t *testing.T) {
	const liveID = "9000000000000099"
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	f.opt.Space = spaceFamily
	f.opt.RootFolderID = famRoot // the drive's own top level, named as a number
	f.familyID = famID
	f.familyRootID = famRoot
	// familySpaceRoot is deliberately left empty: that is what NewFs hands over
	// when root_folder_id is set, and rootID() has to fill it in.
	f.dirCache = dircache.New("", famRoot, f)
	drive.listingFn = func(r *http.Request) string {
		if r.URL.Query().Get("folderId") == famRoot {
			// measured: the numeric root answers nothing at all, and no error
			return emptyListing()
		}
		// the alias, or the omitted folderId that means the same thing
		return listingJSON([]string{fileJSON(liveID, "clip-01.mp4", famRoot)}, nil)
	}

	err := srcObject(f, "clip-01.mp4", liveID).Remove(context.Background())
	require.Error(t, err, "the object is on the drive; an address that answers nothing must not read as absent")
	require.Contains(t, err.Error(), "the server gave no reason")
	// The empty page is addressed at the number, and the number is what the check
	// would use without the alias translation; assert which addresses were asked
	// for so a regression cannot quietly go back to reading the number.
	for _, got := range driveFolders(drive) {
		require.NotEqual(t, "", got, "the walk has to name the parent it looked at")
	}
}

// TestRootIDLearnsTheFamilySpaceRootEvenUnderARootFolderID is the other half of the
// same fix: rootID() returns what the user named, but must still have read the
// drive's own top level, because that is the id listDirID translates back to the
// alias. Without it a remote opened at the top level of a family drive has an
// absence check that addresses a number the listing endpoint does not answer.
func TestRootIDLearnsTheFamilySpaceRootEvenUnderARootFolderID(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	f.opt.Space = spaceFamily
	f.opt.RootFolderID = famRoot
	f.familyID = famID
	f.familyRootID = famRoot
	drive.listingFn = func(r *http.Request) string {
		if r.URL.Query().Get("folderId") == famRoot {
			return emptyListing()
		}
		return listingJSON([]string{fileJSON("9000000000000055", "clip-01.mp4", famRoot)}, nil)
	}

	got, err := f.rootID(context.Background())
	require.NoError(t, err)
	require.Equal(t, famRoot, got, "what the user named is still what the remote is opened at")
	require.Equal(t, famRoot, f.familySpaceRoot, "and the drive's own top level is now known for the translation")
	require.Equal(t, familyRootAlias, f.listDirID(famRoot), "so a listing of it is addressed by the alias")
}

// rowRange builds a directory of count other objects with ids that never collide
// with the test object's.
func rowRange(count int) []string {
	rows := make([]string, 0, count)
	for i := 1; i <= count; i++ {
		rows = append(rows, fileJSON(fmt.Sprintf("700000000000000%04d", i), fmt.Sprintf("o%04d.mp4", i), rootID))
	}
	return rows
}

// TestADeleteOnTheSecondPageIsFoundByAWalkThatPages is the positive control for
// paging: the directory holds 250 objects and ours is the last of them, so it is
// only reachable on page two. Before the fixture could page, no case in this
// package could express "the row is past the first page" at all - which is the
// hole the truncated-walk bug fell through.
func TestADeleteOnTheSecondPageIsFoundByAWalkThatPages(t *testing.T) {
	rows := append(rowRange(249), fileJSON("6000000000000000006", "clip-01.mp4", rootID))
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}, pagedRows: rows}
	f := newFakeFs(t, drive)

	err := srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background())
	require.Error(t, err, "it is on page two of the listing, so it is still on the drive")
	require.Len(t, drivesTasks(drive), 3, "and it was resent, twice, as a leftover should be")
}

// TestADeleteAWalkCutShortByItsPageBudgetIsNotTakenAsGone is the safety half: the
// object genuinely is gone, the directory holds 250 others, and the walk is
// allowed one page. Not being allowed to read the rest is not evidence of
// absence, so the delete may not be called done from it - the cost of that rule
// is a resend, and the cost of not having it is reporting a leftover as deleted.
func TestADeleteAWalkCutShortByItsPageBudgetIsNotTakenAsGone(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}, pagedRows: rowRange(250)}
	f := newFakeFs(t, drive)
	old := deleteVerifyPages
	deleteVerifyPages = 1
	t.Cleanup(func() { deleteVerifyPages = old })

	err := srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background())
	require.Error(t, err, "a walk that was cut off after one page cannot certify an absence")
	require.Len(t, drivesTasks(drive), 3, "the undecidable answer still owes the resends")
}

// TestADeleteResendsOnlyWhatTheDriveDidNotHandle is the big-purge shape: one task
// of nine objects, one of them refused. The task names the eight it handled in
// successedFileIdList (captured 2026-10-22: DELETE answers it), so the repeat has
// to carry the ninth alone - sending all nine again repeats work the drive already
// did, and the first version of this backend did exactly that.
func TestADeleteResendsOnlyWhatTheDriveDidNotHandle(t *testing.T) {
	var rows []string
	var ids []string
	for i := 1; i <= 9; i++ {
		id := fmt.Sprintf("70000000000000001%02d", i)
		ids = append(ids, id)
		rows = append(rows, fileJSON(id, fmt.Sprintf("f%02d.mp4", i), rootID))
	}
	drive := &fakeDrive{t: t,
		deleteTaskFailures: []int64{1, 0},
		// The first task says which eight it finished with.
		deleteTaskHandled: [][]string{ids[:8]},
	}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string { return listingJSON(rows, nil) }

	require.NoError(t, f.Purge(context.Background(), ""), "the resend finished the one object the first task did not handle")

	sent := drivesTasks(drive)
	require.Len(t, sent, 2, "the first task and one resend")
	require.Len(t, sent[0].infos, 9, "the purge sent all nine at once")
	require.Len(t, sent[1].infos, 1, "the resend carries only the object the drive did not handle")
	require.Equal(t, ids[8], sent[1].infos[0].FileID, "and it is the ninth")
}

// TestADeleteAnIncompleteWalkIsNotReadAsGone is the false-success axis the whole
// absence check hangs on: the drive says a page carries 400 rows while giving 200
// of them, so the walk that "found nothing" did not look at everything. Concluding
// "gone" from that reports a leftover as deleted, which is the one answer that
// cannot be taken back - so the target stays outstanding and the delete is
// reported as a failure.
func TestADeleteAnIncompleteWalkIsNotReadAsGone(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		// count 400, rows on this page 1: the walk cannot be read as complete.
		return `{"res_code":0,"fileListAO":{"count":400,"fileList": [` +
			fileJSON("6000000000000000007", "other.mp4", rootID) + `],"folderList": []}}`
	}

	err := srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background())
	require.Error(t, err, "a walk that did not reach the drive's own total cannot say the object is gone")
	require.Contains(t, err.Error(), "the server gave no reason")
	require.Len(t, drivesTasks(drive), 3, "the unproven absence still owes the resends")
}

// TestADeleteRefusedWithAReasonIsNotSentAgain: a drive that did give a reason -
// here the day's flow being spent - is telling the client something a repeat
// cannot fix, so the resend is not spent on it.
func TestADeleteRefusedWithAReasonIsNotSentAgain(t *testing.T) {
	drive := &fakeDrive{t: t,
		deleteTaskFailures: []int64{1},
		deleteTaskCodes:    []string{"DayFlowOver"},
	}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", rootID)}, nil)
	}

	err := srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background())
	require.Error(t, err)
	require.Len(t, drivesTasks(drive), 1, "a refusal with a reason is not worth a second task")
}

// TestADeleteTheListingCannotAnswerIsNotTakenAsDone: an undecidable listing must
// not become a success. The resend still happens, and the object still comes back
// as a failure.
func TestADeleteTheListingCannotAnswerIsNotTakenAsDone(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}, listFailTimes: 20}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", rootID)}, nil)
	}

	err := srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background())
	require.Error(t, err, "the listing refused to say, so nothing was concluded from it")
	require.Len(t, drivesTasks(drive), 3, "the measured cure is owed twice")
}

// TestADeleteATaskTheDriveNeverClosesIsNotTakenAsDone covers the other way the
// wait ends. A task that keeps answering taskStatus 3 past taskPollWaitMax has
// not closed, so it has said nothing about the objects - and saying nothing is
// not success. The error has to name the deadline, no resend is owed (the wait
// returns before the evidence pass is reached), and the drive is not listed,
// because a listing whose question was never asked cannot be read either way.
//
// The error is deliberately not wrapped as NoRetry: the drive may still finish
// the task after the client gave up on it, and that is exactly what the message
// tells the caller to check before trying again. Guessing wrong costs a false
// red - the next pass of rclone lists nothing left to delete - which is the
// direction that can be taken back. It also skips the two steps deleteRows runs
// only after a task comes back clean: remembering the id for the stale-row
// window, and the recycle-bin clear that is sent only by a hard_delete remote.
// Neither of those is a claim that the object went.
func TestADeleteATaskTheDriveNeverClosesIsNotTakenAsDone(t *testing.T) {
	// The fixture answers the first poll "running" and every later one from its
	// default, so a wait that stopped honouring its deadline would see the task
	// finish and report success.
	drive := &fakeDrive{t: t, taskStates: []int64{taskStateRunning}}
	f := newFakeFs(t, drive)
	// The bound is a var for this reason: it is how long rclone waits before
	// deciding the task is never coming back, not a measured server limit.
	//
	// A second in the past rather than zero. The deadline is read with
	// time.Now().After, so a deadline of "now" is a coin flip on whether a clock
	// tick falls between the two calls: with zero this test failed 60 of 100 runs
	// on this host, every miss letting the fixture's default answer finish the
	// task. A deadline already behind the call cannot be missed.
	oldWaitMax := taskPollWaitMax
	taskPollWaitMax = -time.Second
	t.Cleanup(func() { taskPollWaitMax = oldWaitMax })

	err := srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background())
	require.Error(t, err, "a task the drive never closed settles nothing")
	require.Contains(t, err.Error(), "was still running after",
		"the deadline is what ended the wait, and the error has to say so")
	require.False(t, fserrors.IsNoRetryError(err),
		"the drive may still finish it, so the caller may ask again - after checking")
	drive.mu.Lock()
	unasked := len(drive.taskStates)
	drive.mu.Unlock()
	require.Zero(t, unasked,
		"the wait has to ask at least once before it decides the task is never coming back")
	require.Len(t, drivesTasks(drive), 1, "the wait ended before the evidence pass, so no resend is owed")
	require.Empty(t, driveFolders(drive), "and nothing was listed: the question was never asked")
}

// TestADeleteEvidenceWindowReachesPastTheLagItDistrusts is the one place the
// spacing of the looks is written down as a requirement rather than as four
// unrelated numbers. The absence check spans (looks-1)*wait between its first and
// last look; the same package distrusts a listing that was still stale seven
// seconds after the mutation landed (deletedRowWindow, and the capture of
// 2026-10-22 it comes from). A window that stops inside that period asks the
// question five times while the drive has not yet answered it.
func TestADeleteEvidenceWindowReachesPastTheLagItDistrusts(t *testing.T) {
	require.Equal(t, 5, deleteVerifyLooks, "five looks")
	require.Equal(t, 2*time.Second, deleteVerifyWait, "two seconds apart")
	window := time.Duration(deleteVerifyLooks-1) * deleteVerifyWait
	require.GreaterOrEqual(t, window, 8*time.Second,
		"the last look has to fall after the seven seconds the stale-row window allows, not inside it")
}

// TestADeleteLooksThatRunOutAreNotTakenAsGone covers the end of the look loop.
// Here every look says the object is missing, so the answer never gets the two
// agreements it asks for and the looks simply run out. That is not evidence of an
// absence: the targets have to stay outstanding, which costs a resend and, when
// the resends run out too, a reported failure.
func TestADeleteLooksThatRunOutAreNotTakenAsGone(t *testing.T) {
	drive := &fakeDrive{t: t, deleteTaskFailures: []int64{1, 1, 1}}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("6000000000000000007", "other.mp4", rootID)}, nil)
	}
	oldAgree, oldLooks := deleteVerifyAgree, deleteVerifyLooks
	deleteVerifyAgree, deleteVerifyLooks = 3, 2
	t.Cleanup(func() {
		deleteVerifyAgree, deleteVerifyLooks = oldAgree, oldLooks
	})

	err := srcObject(f, "clip-01.mp4", "6000000000000000006").Remove(context.Background())
	require.Error(t, err, "the looks ran out before they agreed, which settles nothing")
	require.Len(t, drivesTasks(drive), 3, "and the undecidable answer still owes the resends")
}

// TestADeleteOneRefusedChainSpendsTheListingOncePerParentPerLook is the cost
// contract. The directory holds 450 rows, which pages as 200+200+50, and a purge
// of it is one task of all 450 targets. Asking the drive which of those are still
// on the drive is answered by reading the directory once, not 450 times, because
// the walk groups every target under its parent - so what this chain may spend is
// the purge's own read of the directory plus one look. A check that asked per
// target, or a look that kept going after it had the answer, moves that number.
func TestADeleteOneRefusedChainSpendsTheListingOncePerParentPerLook(t *testing.T) {
	var rows []string
	for i := 1; i <= 450; i++ {
		rows = append(rows, fileJSON(fmt.Sprintf("710000000000000%04d", i), fmt.Sprintf("g%03d.mp4", i), rootID))
	}
	drive := &fakeDrive{t: t,
		deleteTaskFailures: []int64{1, 0},
		pagedRows:          rows,
	}
	f := newFakeFs(t, drive)

	require.NoError(t, f.Purge(context.Background(), ""), "the resend finished what the first task refused")

	sent := drivesTasks(drive)
	require.Equal(t, 2, len(sent), "one refusal, one resend, and the resend is answered clean")
	require.Equal(t, 450, len(sent[0].infos), "the purge sends the whole directory as one task")
	require.Equal(t, 7, len(driveFolders(drive)),
		"four requests read the directory for the purge (three pages and the empty one that ends it) "+
			"and three read it once for the look that answers all 450 targets together - not 450 walks")
}

// TestADeleteEvidenceReportsOnlyWhatItCounted pins the wording the resend line
// quotes. The review caught the log saying "the 2 target(s) are still on the
// drive" when the walk had stopped at the first hit and the second object was
// already gone - and that line is the only record an operator has of what the
// evidence pass concluded, so the number in it has to be what the drive was
// actually shown.
func TestADeleteEvidenceReportsOnlyWhatItCounted(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	drive.listingFn = func(*http.Request) string {
		return listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", rootID)}, nil)
	}
	parent, err := strconv.ParseInt(rootID, 10, 64)
	require.NoError(t, err)
	targets := []taskInfo{
		{FileID: "6000000000000000006", FileName: "clip-01.mp4", ParentID: &parent},
		{FileID: "6000000000000000007", FileName: "gone.mp4", ParentID: &parent},
	}
	st := &taskStatusResponse{TaskStatus: taskStateDone, FailedCount: 1, SubTaskCount: 2}

	still, why, err := f.deleteEvidenceOnce(context.Background(), targets, st)
	require.NoError(t, err)
	require.Equal(t, 1, len(still), "only the object the drive still lists is outstanding")
	require.Equal(t, "6000000000000000006", still[0].FileID, "and it is the one that is on the drive")
	require.Equal(t, "1 of the 2 target(s) are still on the drive", why,
		"the count it reports is what it found, not how many were sent")
}

// driveFolders copies the folderIds the drive was asked to list, under its lock.
func driveFolders(d *fakeDrive) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.listFolders...)
}
