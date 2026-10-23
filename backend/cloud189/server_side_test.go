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
		rootID:    {emptyListing()}, // nothing named archive yet
		archiveID: {emptyListing(), listingJSON([]string{fileJSON("900000000000000011", "clip-01.mp4", archiveID)}, nil)},
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

// TestServerSideCopyArrivesNamedAfterItsSource pins the follow-up rename: the
// task takes no destination name, so a copy that also renames is a task plus one
// rename. The source sits in the same directory under the same name, which is
// why the readback has to skip the source id.
func TestServerSideCopyArrivesNamedAfterItsSource(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	source := fileJSON("900000000000000006", "clip-01.mp4", rootID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {
			listingJSON([]string{source}, nil),
			listingJSON([]string{source, fileJSON("7000000000000000007", "clip-01.mp4", rootID)}, nil),
		},
	}).fn

	obj, err := f.Copy(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "holiday.MP4")
	require.NoError(t, err)
	require.Equal(t, "7000000000000000007", obj.(*Object).id, "the copy, not the source")

	require.Len(t, drive.renames, 1)
	require.Equal(t, "7000000000000000007", drive.renames[0].id)
	require.Equal(t, "holiday.MP4", drive.renames[0].name)
	require.Len(t, drive.tasks, 1, "the source is left alone by a copy")
	require.Equal(t, taskCopy, drive.tasks[0].taskType)
}

// TestServerSideCopyParksTheOccupierAndDeletesIt is the collision case: the name
// is held, the holder is moved aside, the copy lands, then the holder is deleted.
func TestServerSideCopyParksTheOccupierAndDeletesIt(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
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
		archiveID: {listingJSON(nil, []string{folderJSON("5000000000000000005", "clip-01.mp4", archiveID)})},
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

// TestDirMoveBetweenTwoRemotesIsRefused is why a directory move can never see two
// remotes disagree about how to spell a name: taskAuthFor refuses a server-side
// move between two remote instances of one account, so DirMove only ever runs with
// the source and the destination the same Fs and the same encoding.
func TestDirMoveBetweenTwoRemotesIsRefused(t *testing.T) {
	drive := &fakeDrive{t: t}
	src := newFakeFs(t, drive)
	dst := newFakeFs(t, drive)
	dst.opt.AccessToken = src.opt.AccessToken
	dst.opt.Enc = encoder.Standard // different on purpose

	err := dst.DirMove(context.Background(), src, "sub", "sub")
	require.ErrorIs(t, err, fs.ErrorCantDirMove)
	require.Empty(t, sequenceOf(drive), "and the drive is not asked")
}

// TestCopyNamesTheSourceAsTheSourceRemoteStoresIt is the case of two remotes of
// one account carrying different encoding options. The copy arrives spelled with
// the source's own bytes, so the name this run clears and the object it reads back
// have to be compared against the source remote's rendering. The destination's
// rendering belongs only to the name the destination was asked to give it.
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
		famRoot: {emptyListing(), listingJSON([]string{fileJSON("900000000000000011", "a＜b＞.mp4", famRoot)}, nil)},
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
		rootID:    {emptyListing()},
		archiveID: {emptyListing()},
	}).fn

	obj, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "archive/clip-01.mp4")
	require.NoError(t, err)
	require.Equal(t, "900000000000000006", obj.(*Object).id, "a move preserves the id")
	require.Equal(t, "archive/clip-01.mp4", obj.Remote())

	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskMove, drive.tasks[0].taskType)
	require.Contains(t, drive.createBodies[0], "targetFolderId="+archiveID)
	require.Empty(t, drive.renames, "the name did not change, so nothing was renamed")
	require.Len(t, drive.tasks, 1, "and nothing was deleted")
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

	require.Len(t, drive.tasks, 2, "the move, then the delete of the parked occupier")
	require.Equal(t, taskMove, drive.tasks[0].taskType)
	require.Equal(t, taskDelete, drive.tasks[1].taskType)
	require.Equal(t, "6000000000000000006", drive.tasks[1].infos[0].FileID)

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

// TestDirMoveIsOneRecursiveTask is the point of moving a directory on the
// server: the drive takes the subtree with it, in one task.
func TestDirMoveIsOneRecursiveTask(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	subFolder := listingJSON(nil, []string{folderJSON("900000000000000007", "sub", rootID)})
	drive.listingFn = newListingScript(map[string][]string{
		// The root is listed twice: once while looking for the destination's
		// parent, once for the source directory itself. The archive is listed
		// twice as well: the destination must not exist, and the name the move
		// arrives under must be free.
		rootID:    {subFolder, subFolder},
		archiveID: {emptyListing(), emptyListing()},
	}).fn

	require.NoError(t, f.DirMove(context.Background(), f, "sub", "archive/moved"))

	require.Len(t, drive.tasks, 1)
	require.Equal(t, taskMove, drive.tasks[0].taskType)
	require.Equal(t, "900000000000000007", drive.tasks[0].infos[0].FileID)
	require.EqualValues(t, 1, drive.tasks[0].infos[0].IsFolder, "a directory moves as one object")
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
	drive := &fakeDrive{t: t, taskStates: []int64{taskStateConflict}}
	f := newFakeFs(t, drive)
	source := fileJSON("900000000000000006", "clip-01.mp4", rootID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {listingJSON([]string{source}, nil)},
	}).fn

	_, err := f.Move(context.Background(), srcObject(f, "clip-01.mp4", "900000000000000006"), "clip-01.mp4")
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflict")
	require.True(t, fserrors.IsNoRetryError(err))
	require.Empty(t, drive.renames, "nothing was renamed on the way in")
}

// TestArrivedCopySkipsTheSource is the readback rule that keeps a same-directory
// copy from being mistaken for its own source.
func TestArrivedCopySkipsTheSource(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	source := fileJSON("900000000000000006", "clip-01.mp4", rootID)
	drive.listingFn = newListingScript(map[string][]string{
		rootID: {listingJSON([]string{source, fileJSON("7000000000000000007", "clip-01.mp4", rootID)}, nil)},
	}).fn

	row, err := f.arrivedCopy(context.Background(), rootID, "clip-01.mp4", "900000000000000006")
	require.NoError(t, err)
	require.Equal(t, "7000000000000000007", row.ID.String())

	_, err = f.arrivedCopy(context.Background(), rootID, "gone.MP4", "900000000000000006")
	require.Error(t, err)
	require.Contains(t, err.Error(), "other than its source")
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
		archiveID: {listingJSON([]string{fileJSON("6000000000000000006", "clip-01.mp4", archiveID)}, nil)},
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

// TestCopyRefusesToTakeADirectoryNameAndRemovesWhatItMade: the copy did land, so
// the object it made is the one thing this run created, and the rule is that a
// failure handler takes back what it made and never touches what was already on
// the drive.
func TestCopyRefusesToTakeADirectoryNameAndRemovesWhatItMade(t *testing.T) {
	drive := &fakeDrive{t: t, folders: map[string]string{"archive": archiveID}}
	f := newFakeFs(t, drive)
	script := newListingScript(map[string][]string{
		rootID: {emptyListing()},
		archiveID: {
			emptyListing(), // the name the copy arrives under is free
			listingJSON([]string{fileJSON("900000000000000011", "clip-01.mp4", archiveID)}, nil),
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
				emptyListing(), // nothing arrives under the source's name
				listingJSON([]string{fileJSON("900000000000000011", "clip-01.mp4", archiveID)}, nil),
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
		rootID: {emptyListing()},
	})
	drive.listingFn = script.fn

	require.NoError(t, f.Mkdir(context.Background(), "fresh"))
	require.Equal(t, []string{"fresh"}, drive.mkdirs)

	require.NoError(t, f.Mkdir(context.Background(), "fresh"))
	require.Equal(t, []string{"fresh"}, drive.mkdirs, "a directory the cache knows is not created twice")
	require.Equal(t, 1, script.walksOf(rootID), "nor is the parent listed again")
}

// TestSetModTimeReportsItCannotBeDone pins both halves of that answer: the
// sentinel rclone's callers look for, and the reason a user would need.
func TestSetModTimeReportsItCannotBeDone(t *testing.T) {
	drive := &fakeDrive{t: t}
	f := newFakeFs(t, drive)
	obj := srcObject(f, "clip.mp4", "900000000000000004")

	err := obj.SetModTime(context.Background(), time.Now())
	require.Error(t, err)
	require.ErrorIs(t, err, fs.ErrorCantSetModTime, "operations treats the sentinel as unsupported, not as a failed transfer")
	require.Contains(t, err.Error(), "stamps its own times")
	require.Empty(t, sequenceOf(drive), "and the drive is never asked")
}
