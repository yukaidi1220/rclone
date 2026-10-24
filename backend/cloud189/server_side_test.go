package cloud189

import (
	"context"
	"fmt"
	"net/http"
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
		rootID:    {subFolder, subFolder, subFolder, subFolder},
		archiveID: {emptyListing(), emptyListing(), emptyListing(), listingJSON(nil, []string{folderJSON(subID, "sub", archiveID)})},
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

// TestMoveWithholdsTheDeleteWhenTheWantedNameCannotBeRead is the same guard one
// step later: the object did arrive under the name the task moved it with, the
// rename into the name rclone asked for went out and was accepted, and the listing
// still does not show it there. Reporting success would hand rclone a path the
// drive does not confirm, so the move is an error - and the object parked out of
// the arrival's way goes back where it was instead of being called replaced.
func TestMoveWithholdsTheDeleteWhenTheWantedNameCannotBeRead(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	unrelated := fileJSON("7000000000000000022", "clip-01.mp4", archiveID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(),                        // the create's verification
			listingJSON([]string{unrelated}, nil), // the holder of the arrival name
			// the proof: the task did move the object, under its own name
			listingJSON([]string{fileJSON("900000000000000006", "clip-01.mp4", archiveID)}, nil),
			emptyListing(), // nothing holds the wanted name, so the rename goes out
			// and the listing never shows it there, however long the proof waits
		},
	}).fn

	_, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/moved.mp4")
	require.ErrorIs(t, err, fs.ErrorObjectNotFound, "a path the drive does not confirm is not reported as a move")

	require.Len(t, drive.renames, 3, "parked, renamed into the wanted name, and the holder given its name back")
	require.Equal(t, "7000000000000000022", drive.renames[0].id)
	require.True(t, strings.HasPrefix(drive.renames[0].name, "rclone-park-"))
	require.Equal(t, "900000000000000006", drive.renames[1].id)
	require.Equal(t, "moved.mp4", drive.renames[1].name)
	require.Equal(t, "7000000000000000022", drive.renames[2].id)
	require.Equal(t, "clip-01.mp4", drive.renames[2].name)

	require.Len(t, drive.tasks, 1, "the move, and nothing deleted for it")
	require.Equal(t, taskMove, drive.tasks[0].taskType)
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
			listingJSON([]string{fileJSON("900000000000000006", "moved.mp4", archiveID)}, nil),
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
			emptyListing(), // and nothing holds the wanted name
			listingJSON([]string{fileJSON("900000000000000006", "moved.mp4", archiveID)}, nil),
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
			emptyListing(), // the wanted name is free
			// the readback that must precede any delete: our object, in place
			listingJSON([]string{fileJSON("900000000000000006", "other.txt", archiveID)}, nil),
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
			emptyListing(), // nothing holds the wanted name
			listingJSON([]string{fileJSON("900000000000000006", "moved.mp4", archiveID)}, nil),
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
		rootID:    {emptyListing(), subFolder, subFolder, subFolder},
		archiveID: {emptyListing(), emptyListing(), emptyListing(), listingJSON(nil, []string{folderJSON("900000000000000007", "sub", archiveID)})},
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
	drive.listingFn = newListingScript(map[string][]string{
		// every walk sees the same two rows: the source and the copy, both under
		// the name the readback asks for
		rootID: {
			listingJSON([]string{source, fileJSON("7000000000000000007", "clip-01.mp4", rootID)}, nil),
			listingJSON([]string{source, fileJSON("7000000000000000007", "clip-01.mp4", rootID)}, nil),
			listingJSON([]string{source, fileJSON("7000000000000000007", "clip-01.mp4", rootID)}, nil),
		},
	}).fn

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
	drive2.listingFn = newListingScript(map[string][]string{
		rootID: {listingJSON([]string{source, fileJSON("7000000000000000009", "clip-01.mp4", rootID)},
			[]string{folderJSON("5000000000000000009", "clip-01.mp4", rootID)})},
	}).fn
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
