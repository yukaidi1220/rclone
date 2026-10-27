package cloudreve

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/rclone/rclone/backend/cloudreve/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/list"
)

// List the files and directories in dir
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	var policy *api.Policy
	next := ""
	for {
		query := url.Values{}
		query.Set("uri", f.uriFor(dir))
		query.Set("page_size", strconv.Itoa(defaultPageSize))
		query.Set("order_by", "name")
		query.Set("order_direction", "asc")
		if next != "" {
			query.Set("next_page_token", next)
		}
		page := new(api.ListResponse)
		if err := f.call(ctx, "GET", "/file", query, nil, page); err != nil {
			return nil, f.mapMissingDir(dir, err)
		}
		if page.StoragePolicy != nil {
			policy = page.StoragePolicy
		}
		for i := range page.Files {
			entry := &page.Files[i]
			if uploading(entry) {
				// A placeholder an unfinished upload left behind is not a file
				// anyone can read, and showing it as one makes rclone try to
				// overwrite a name the server has locked.
				continue
			}
			if !usableName(entry.Name) {
				return nil, fmt.Errorf("cloudreve: %q was listed with the unusable name %q", dir, entry.Name)
			}
			remote := path.Join(dir, f.opt.Enc.ToStandardName(entry.Name))
			if entry.Type == api.FileTypeFolder {
				entries = append(entries, fs.NewDir(remote, entry.UpdatedAt).SetID(entry.ID))
				continue
			}
			o, err := f.newObject(remote, entry)
			if err != nil {
				return nil, err
			}
			entries = append(entries, o)
		}
		if page.Pagination == nil || page.Pagination.NextToken == "" {
			break
		}
		next = page.Pagination.NextToken
	}
	if policy != nil {
		f.mu.Lock()
		f.policies[f.drivePath(dir)] = policy
		f.mu.Unlock()
	}
	return entries, nil
}

// usableName rejects names that cannot become one rclone path segment
func usableName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	return !strings.ContainsRune(name, '/')
}

// ListR lists the tree under dir, calling back with each batch
func (f *Fs) ListR(ctx context.Context, dir string, callback fs.ListRCallback) error {
	helper := list.NewHelper(callback)
	if err := f.listR(ctx, dir, helper); err != nil {
		return err
	}
	return helper.Flush()
}

func (f *Fs) listR(ctx context.Context, dir string, helper *list.Helper) error {
	entries, err := f.List(ctx, dir)
	if err != nil {
		if errors.Is(err, fs.ErrorIsFile) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if err := helper.Add(entry); err != nil {
			return err
		}
		if d, ok := entry.(fs.Directory); ok {
			// The directory is reported as well as descended into: a recursive
			// listing that dropped folders would make --fast-list operations
			// blind to them.
			if err := f.listR(ctx, d.Remote(), helper); err != nil {
				return err
			}
		}
	}
	return nil
}

// NewObject finds the Object at remote
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	if remote == "" {
		return nil, fs.ErrorIsDir
	}
	file, err := f.statFile(ctx, remote)
	if err != nil {
		return nil, f.mapMissingFile(remote, err)
	}
	if file.Type == api.FileTypeFolder {
		return nil, fs.ErrorIsDir
	}
	return f.newObject(remote, file)
}

// newObject builds an Object from a server response
func (f *Fs) newObject(remote string, file *api.File) (*Object, error) {
	std, err := f.fromURIPath(file.Path)
	if err == nil && std != f.drivePath(remote) {
		// The server answered with an entry kept under a different name, so
		// the content is not where the caller asked for it.
		return nil, fmt.Errorf("cloudreve: asked for %q but the entry is stored at %q", remote, std)
	}
	return &Object{
		fs:      f,
		remote:  remote,
		id:      file.ID,
		size:    file.Size,
		modTime: file.UpdatedAt,
	}, nil
}

// Put the object into the remote
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	remote := src.Remote()
	if remote == "" {
		return nil, errors.New("cloudreve: cannot upload to the root as a file")
	}
	o := &Object{fs: f, remote: remote}
	if err := o.Upload(ctx, in, src); err != nil {
		return nil, err
	}
	return o, nil
}

// Mkdir makes the directory dir, along with any parent of it that is missing
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	if dir == "" && f.rootExists {
		// The top of the drive, or a root already found there, needs no work.
		return nil
	}
	return f.mkdirDrivePath(ctx, f.drivePath(dir))
}

// mkdirDrivePath makes the folder at a path from the top of the drive, making
// the parents it finds missing on the way
func (f *Fs) mkdirDrivePath(ctx context.Context, drivePath string) error {
	err := f.createFolder(ctx, drivePath)
	switch codeOf(err) {
	case api.CodeObjectExist, api.CodeConflict:
		// Something is already there, and only a folder of the same name makes
		// this a success.
		file, statErr := f.statDrivePath(ctx, drivePath)
		if statErr != nil {
			return statErr
		}
		if file.Type != api.FileTypeFolder {
			return fmt.Errorf("cloudreve: cannot make directory %q, a file is in the way: %w", drivePath, err)
		}
		return nil
	case api.CodeParentNotExist:
		// The server makes one folder per call, so the missing ancestor has to be
		// asked for first: build the path above this one, then try again. A name
		// with no slash in it has only the top of the drive above it, which
		// rclone cannot make.
		segments := strings.Split(drivePath, "/")
		if len(segments) == 1 {
			return err
		}
		if err := f.mkdirDrivePath(ctx, strings.Join(segments[:len(segments)-1], "/")); err != nil {
			return err
		}
		return f.createFolder(ctx, drivePath)
	}
	return err
}

// createFolder asks the server for the single folder at a path from the top of
// the drive
func (f *Fs) createFolder(ctx context.Context, drivePath string) error {
	// A folder is metadata only and inherits its policy from the drive or the
	// pool, so no storage policy check applies to making one.
	return f.call(ctx, "POST", "/file/create", nil, &api.CreateFolderRequest{
		URI:  f.uriForDrivePath(drivePath),
		Type: "folder",
	}, nil)
}

// Rmdir removes the directory dir, which must be empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	if dir == "" && f.root == "" {
		return errors.New("cloudreve: cannot remove the root of the drive")
	}
	file, err := f.statFile(ctx, dir)
	if err != nil {
		return f.mapMissingDir(dir, err)
	}
	if file.Type != api.FileTypeFolder {
		return fs.ErrorIsFile
	}
	entries, err := f.List(ctx, dir)
	if err != nil {
		return err
	}
	if len(entries) > 0 {
		return fs.ErrorDirectoryNotEmpty
	}
	return f.deleteBatch(ctx, []string{f.uriFor(dir)})
}

// Purge removes the directory dir and everything under it
//
// The server deletes a folder along with its contents, so one request is
// enough however deep the tree is.
func (f *Fs) Purge(ctx context.Context, dir string) error {
	if dir == "" && f.root == "" {
		return errors.New("cloudreve: cannot purge the root of the drive")
	}
	if _, err := f.statFile(ctx, dir); err != nil {
		return f.mapMissingDir(dir, err)
	}
	return f.deleteBatch(ctx, []string{f.uriFor(dir)})
}

// deleteUris asks the site to remove the given URIs
//
// An entry an interrupted upload left behind cannot be deleted while its lock is
// held, so a conflict is answered the same way an upload answers it - release the
// lock, but only the ones this request is actually working on. A folder delete
// names every lock under it, and the ones whose placeholder is young belong to
// uploads that are running now.
func (f *Fs) deleteUris(ctx context.Context, uris []string) error {
	request := &api.DeleteRequest{Uris: uris, SkipSoftDelete: f.opt.SkipSoftDelete}
	err := f.call(ctx, "DELETE", "/file", nil, request, nil)
	if codeOf(err) == api.CodeLockConflict && f.opt.DropStaleLocks {
		dropped, why := f.dropLocks(ctx, err, serverPathsOf(uris), 0, true)
		if dropped > 0 {
			return f.call(ctx, "DELETE", "/file", nil, request, nil)
		}
		if why != "" {
			return fmt.Errorf("%w (%s)", err, why)
		}
	}
	return err
}

// deleteBatch removes the given URIs, a few hundred per request
func (f *Fs) deleteBatch(ctx context.Context, uris []string) error {
	for len(uris) > 0 {
		batch := uris
		if len(batch) > defaultDeleteBatch {
			batch = batch[:defaultDeleteBatch]
			uris = uris[defaultDeleteBatch:]
		} else {
			uris = nil
		}
		if err := f.deleteUris(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

// DirMove moves src, srcRemote to this remote at dstRemote using a server-side move
//
// Only a folder is moved here: a server-side move of a file stamps the entry with
// the time of the move, and no API gives the modified time back, so a file is
// transferred again by rclone rather than moved. A folder keeps the modified times
// of the files inside it, so moving one loses nothing.
func (f *Fs) DirMove(ctx context.Context, srcFs fs.Fs, srcRemote, dstRemote string) error {
	src, ok := srcFs.(*Fs)
	if !ok {
		return fs.ErrorCantDirMove
	}
	// A destination that is there, folder or file, has to be reported as taken so
	// that rclone copies and purges instead of moving. An empty dstRemote is this
	// Fs's own root, which is a real folder: that is the move-onto-itself case.
	if _, err := f.statFile(ctx, dstRemote); err == nil {
		return fs.ErrorDirExists
	} else if !errors.Is(f.mapMissingDir(dstRemote, err), fs.ErrorDirNotFound) {
		return err
	}
	if err := f.moveEntry(ctx, src.drivePath(srcRemote), f.drivePath(dstRemote)); err != nil {
		return err
	}
	if _, err := f.statFile(ctx, dstRemote); err != nil {
		return fmt.Errorf("cloudreve: moved the directory but cannot find it at %q: %w", dstRemote, err)
	}
	return nil
}

// moveEntry moves the entry at one drive path under another, renaming it when the
// destination names a different leaf
func (f *Fs) moveEntry(ctx context.Context, srcPath, dstPath string) error {
	dstDir, dstName := splitDrivePath(dstPath)
	srcDir, srcName := splitDrivePath(srcPath)

	if _, err := f.writableDirPolicy(ctx, dstDir); err != nil {
		return err
	}

	if srcDir == dstDir {
		if srcName == dstName {
			return nil
		}
		return f.rename(ctx, srcPath, dstName)
	}

	err := f.call(ctx, "POST", "/file/move", nil, &api.MoveRequest{
		Uris: []string{f.uriForDrivePath(srcPath)},
		Dst:  f.uriForDrivePath(dstDir),
	}, nil)
	if err != nil {
		return err
	}
	if dstName == srcName {
		return nil
	}
	// path.Join rather than a concatenation: moving to the top of the drive leaves
	// dstDir empty, and "/"+srcName there would address an empty first segment.
	// path.Join rather than a concatenation: moving to the top of the drive leaves
	// dstDir empty, and "/"+srcName there would address an empty first segment.
	return f.rename(ctx, path.Join(dstDir, srcName), dstName)
}

// rename gives the entry at a drive path a new leaf name
func (f *Fs) rename(ctx context.Context, drivePath, newName string) error {
	return f.call(ctx, "POST", "/file/rename", nil, &api.RenameRequest{
		URI:     f.uriForDrivePath(drivePath),
		NewName: f.opt.Enc.FromStandardName(newName),
	}, nil)
}

// splitDrivePath cuts a path from the top of the drive into its folder and leaf,
// where the folder of a top level name is the drive itself
func splitDrivePath(drivePath string) (dir, name string) {
	dir, name = path.Split(drivePath)
	return strings.TrimSuffix(dir, "/"), name
}

// About returns the account's storage: the quota of the group the account is in,
// against the space it holds now.
//
// The used figure covers files sitting in the trash bin too, since those keep
// counting against the quota until the site's retention period collects them.
// What the site calls storage_pack_total is left out: the answer does not say
// whether it is already inside the quota, and guessing would misreport the space.
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	capacity := new(api.Capacity)
	if err := f.call(ctx, "GET", "/user/capacity", nil, nil, capacity); err != nil {
		return nil, err
	}
	total, used := capacity.Total, capacity.Used
	usage := &fs.Usage{Total: &total, Used: &used}
	if free := total - used; free > 0 {
		usage.Free = &free
	}
	return usage, nil
}

// policyReport is what the policy command answers
//
// Path is the folder within the whole drive, not the path as rclone names it: a
// command run against a rooted remote has "" as its rclone path, which says
// nothing about what was bound. FolderID is what root_folder_id wants, so the
// answer to one command can feed the option it is asking about.
type policyReport struct {
	Path     string        `json:"path,omitempty"`
	FolderID string        `json:"folder_id,omitempty"`
	ID       string        `json:"id"`
	Name     string        `json:"name"`
	Type     string        `json:"type"`
	Writable bool          `json:"writable"`
	Refusal  string        `json:"refusal,omitempty"`
	Children []policyChild `json:"children,omitempty"`
}

// policyChild is one member of a pool: a policy rather than a folder, so it has
// no writability of its own - only the ID set_policy takes and the name to read.
type policyChild struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

// Command runs the backend's own commands
func (f *Fs) Command(ctx context.Context, name string, arg []string, opt map[string]string) (any, error) {
	switch name {
	case "policy":
		if len(opt) > 0 {
			return nil, errors.New("cloudreve: the policy command takes no options")
		}
		if len(arg) > 0 {
			// The folder belongs in the remote, so the command cannot be told
			// about two places at once.
			return nil, errors.New(`cloudreve: the policy command takes no arguments - name the folder in the remote, as remote:folder`)
		}
		return f.policyReportFor(ctx, "")
	case "set_policy":
		if len(opt) > 0 {
			return nil, errors.New("cloudreve: the set_policy command takes no options")
		}
		if len(arg) != 1 || arg[0] == "" {
			return nil, errors.New("cloudreve: set_policy needs one argument, the storage policy ID to bind this folder to")
		}
		return f.setPolicy(ctx, arg[0])
	}
	return nil, fs.ErrorCommandNotFound
}

// policyReportFor describes the storage policy that uploads into remote go through
func (f *Fs) policyReportFor(ctx context.Context, remote string) (any, error) {
	policy, err := f.policyForDrive(ctx, f.drivePath(remote))
	if err != nil {
		return nil, err
	}
	report := policyReport{
		Path:     f.drivePath(remote),
		ID:       policy.ID,
		Name:     policy.Name,
		Type:     policy.Type,
		Children: make([]policyChild, 0, len(policy.Children)),
	}
	for _, child := range policy.Children {
		report.Children = append(report.Children, policyChild{
			ID: child.ID, Name: child.Name, Type: child.Type,
		})
	}
	// The same question the upload asks, so this reports what rclone would do
	// here rather than what the site would allow in principle.
	report.FolderID = f.folderID(f.drivePath(remote))
	if _, err := f.writablePolicyForDir(ctx, f.drivePath(remote)); err != nil {
		report.Refusal = err.Error()
	} else {
		report.Writable = true
	}
	return report, nil
}

// setPolicy binds the folder this remote is rooted at to a storage policy
//
// The site keeps no "inherit again" value, so undoing a binding means naming the
// policy the policy command reported beforehand.
func (f *Fs) setPolicy(ctx context.Context, policyID string) (any, error) {
	if err := f.call(ctx, "PATCH", "/file/policy", nil, &api.BindPolicyRequest{
		URI:      f.uriFor(""),
		PolicyID: policyID,
	}, nil); err != nil {
		return nil, err
	}
	// Folders below this one inherited the policy it had, and the site does not
	// say which of them it re-resolves, so nothing cached is safe to keep.
	f.forgetPolicies()
	return f.policyReportFor(ctx, "")
}

// PublicLink returns the normal download link of the file at remote
//
// It is the link every read goes through (POST /file/url, which the site answers
// with its download_host rewrite applied): no share and no stored direct link is
// created, and the link carries the site's own expiry rather than one asked for
// here.
//
// The link is minted for this call rather than taken from the cache reads fill,
// because one with a few minutes left is of little use to whoever asked for it.
// The site hands out entity URLs for files only, so a folder has none.
//
// unlink is ignored, as rclone documents for a backend that cannot unlink, and a
// link is returned anyway. The stored direct links an earlier version of this
// backend created are permanent public URLs the site keeps serving until they are
// deleted, and revoking every one of them for a file is not what a link request
// asks for; how to do it is kept in the comment below.
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (string, error) {
	if unlink {
		// rclone's contract for a backend without unlink is to ignore the flag,
		// and a download link is stored nowhere, so nothing of this call's own is
		// left behind.
		//
		// An earlier version of this backend did create stored direct links, which
		// the site keeps serving as permanent public URLs until they are deleted.
		// Deleting them from here is deliberately not done: `rclone link --unlink`
		// is not a request to revoke every URL ever handed out for a file, and the
		// code that did it is kept below, commented out, rather than removed.
		fs.Debugf(f, "a download link is minted per request, so there is no stored link to unlink")
	}
	if expire.IsSet() {
		fs.Logf(f, "the site sets its own expiry on download links, so --expire is ignored")
	}
	// Ask about the named entry itself rather than going through NewObject: when
	// the name is not there, cmd/link leaves the whole path in the Fs root and
	// asks about "", so a plain NewObject would report a missing name as a folder.
	file, err := f.statFile(ctx, remote)
	if err != nil {
		return "", f.mapMissingFile(remote, err)
	}
	if file.Type == api.FileTypeFolder {
		return "", fs.ErrorCantShareDirectories
	}
	link, err := f.freshDownloadURL(ctx, remote)
	if err != nil {
		return "", err
	}
	return f.absoluteURL(link), nil
}

// An earlier version of this backend created stored direct links, which the site
// keeps serving as permanent public URLs until they are deleted, and this is how
// it deletes them. Nothing calls it: PublicLink ignores `--unlink`, so a run of
// `rclone link --unlink` leaves whatever earlier runs handed out alone. Kept as a
// comment rather than deleted, so nobody has to work out again how the site
// revokes one - and so nobody mistakes it for code that still runs.
//
// func (f *Fs) deleteDirectLinks(ctx context.Context, remote string) error {
// 	query := url.Values{}
// 	query.Set("uri", f.uriFor(remote))
// 	query.Set("extended", "true")
// 	info := new(api.FileExtended)
// 	if err := f.call(ctx, "GET", "/file/info", query, nil, info); err != nil {
// 		return err
// 	}
// 	for _, link := range info.ExtendedInfo.DirectLinks {
// 		endpoint := "/file/source/" + url.PathEscape(link.ID)
// 		if err := f.call(ctx, "DELETE", endpoint, nil, nil, nil); err != nil {
// 			return err
// 		}
// 	}
// 	return nil
// }

// absoluteURL makes a server-relative URL complete
//
// ResolveReference is what the URL rules say: an absolute path is taken from the
// host root, a relative one from the remote's own base, and a network-path
// reference keeps the host it names.
//
// An instance mounted under a sub-path is the exception. Its download_host
// rewrites produce paths relative to the application, and an absolute path from
// the host root would drop the prefix and land on the proxy's 404. When the
// configured URL has a path prefix and the answer does not already carry it, the
// prefix is put back. A complete URL is left alone either way: the site names the
// host it wants, and that is the one the signature covers.
func (f *Fs) absoluteURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.IsAbs() {
		return raw
	}
	base, err := url.Parse(f.opt.URL)
	if err != nil {
		return raw
	}
	if prefix := strings.TrimSuffix(base.Path, "/"); prefix != "" && u.Host == "" {
		if !strings.HasPrefix(u.Path, prefix+"/") && u.Path != prefix {
			u.Path = prefix + "/" + strings.TrimPrefix(u.Path, "/")
		}
	}
	return base.ResolveReference(u).String()
}

// mapMissingDir turns the server's not-found answers about a folder into
// rclone's sentinel
func (f *Fs) mapMissingDir(dir string, err error) error {
	if codeOf(err) == api.CodeNotFound || codeOf(err) == api.CodeParentNotExist {
		return fs.ErrorDirNotFound
	}
	return err
}

// mapMissingFile turns the server's not-found answers about a file into
// rclone's sentinel
func (f *Fs) mapMissingFile(remote string, err error) error {
	if codeOf(err) == api.CodeNotFound || codeOf(err) == api.CodeParentNotExist {
		return fs.ErrorObjectNotFound
	}
	return err
}
