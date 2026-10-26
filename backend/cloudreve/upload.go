package cloudreve

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/cloudreve/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/multipart"
	"github.com/rclone/rclone/lib/pacer"
	"golang.org/x/sync/errgroup"
)

// Upload writes the object
//
// The server decides how the file is cut into parts and hands out one presigned
// URL per part, so there is no single-request path to choose: a small file comes
// back as exactly one part.
func (o *Object) Upload(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	f := o.fs
	size := src.Size()
	if size < 0 {
		return errors.New("cloudreve: the size of an upload must be known, so streaming is not supported: copy from a source that reports a size")
	}
	// From here this process owns the name, so a lock conflict on this path is
	// never answered by releasing a session that is still running here.
	ticket, release := f.claimName(f.serverPath(o.remote))
	defer release()
	// An empty file goes through the same session as any other: the server hands
	// out one part URL for it, and this is the only path that carries
	// last_modified, so the modified time survives.
	policy, err := f.writablePolicy(ctx, o.remote)
	if err != nil {
		return err
	}

	session, err := f.createSession(ctx, o.remote, src, policy.ID, "", ticket)
	if codeOf(err) == api.CodeObjectExist {
		// An existing file is only written over as a new version, which the
		// server keeps or discards according to the account's version
		// retention.
		session, err = f.createSession(ctx, o.remote, src, policy.ID, "version", ticket)
	}
	if err != nil {
		return err
	}
	// The session names the policy the bytes land on. If that is not the S3
	// child asked for, the site chose otherwise and rclone cannot push through
	// whatever it picked.
	if session.StoragePolicy != nil && session.StoragePolicy.Type != PolicyTypeS3 {
		f.abandon(ctx, o, session)
		return fmt.Errorf("cloudreve: the upload of %q landed on storage policy %q, which rclone cannot write through",
			o.remote, session.StoragePolicy.Type)
	}
	if len(session.UploadURLs) == 0 {
		f.abandon(ctx, o, session)
		return fmt.Errorf("cloudreve: the session for %q carried no part URLs, so its storage policy is not one rclone uploads through", o.remote)
	}

	parts, err := f.uploadParts(ctx, session, size, in)
	if err != nil {
		f.abandon(ctx, o, session)
		return err
	}
	if err := f.finishSession(ctx, session, parts); err != nil {
		if codeOf(err) == api.CodeLockConflict && len(lockConflicts(err)) == 0 {
			// The server says no session holds the name any more, which is what it
			// answers when the file was already filed and filing it released the
			// name. Read the name before declaring the upload a failure: re-sending
			// the callback cannot work, and re-uploading the file would only fight
			// the version that is there.
			if filed, ok := f.confirmFiled(ctx, o.remote, src); ok {
				fs.Debugf(o, "cloudreve: %q is filed even though the callback failed: %v", o.remote, err)
				o.setFrom(filed)
				return nil
			}
		}
		f.abandon(ctx, o, session)
		return err
	}

	file, err := f.statFile(ctx, o.remote)
	if err != nil {
		return fmt.Errorf("cloudreve: uploaded %q but cannot find it afterwards: %w", o.remote, err)
	}
	o.setFrom(file)
	f.forgetURL(o.remote)
	return nil
}

// createSession asks the server for an upload session for remote, naming the
// storage policy the file should land on
//
// A name locked by an upload that never finished is released first, but only on
// the evidence dropLocks accepts. Releasing a lock whose upload is still running
// does not make this upload the winner: both sessions then live, and whichever
// callback arrives first files the name while the other is refused and its bytes
// recycled.
func (f *Fs) createSession(ctx context.Context, remote string, src fs.ObjectInfo, policyID, entityType string, self uint64) (*api.UploadSession, error) {
	session, err := f.startSession(ctx, remote, src, policyID, entityType)
	if codeOf(err) != api.CodeLockConflict || !f.opt.DropStaleLocks {
		return session, err
	}
	dropped, why := f.dropLocks(ctx, err, []string{f.serverPath(remote)}, self, false)
	if dropped == 0 {
		if why != "" {
			return nil, fmt.Errorf("%w (%s)", err, why)
		}
		return nil, err
	}
	return f.startSession(ctx, remote, src, policyID, entityType)
}

// claimName records that this upload is working on a path of this drive, and
// hands back the ticket it holds with the function that gives the name up
func (f *Fs) claimName(serverPath string) (uint64, func()) {
	f.uploadsMu.Lock()
	defer f.uploadsMu.Unlock()
	f.uploadSeq++
	ticket := f.uploadSeq
	if f.uploads[serverPath] == nil {
		f.uploads[serverPath] = map[uint64]bool{}
	}
	f.uploads[serverPath][ticket] = true
	return ticket, func() {
		f.uploadsMu.Lock()
		defer f.uploadsMu.Unlock()
		delete(f.uploads[serverPath], ticket)
		if len(f.uploads[serverPath]) == 0 {
			delete(f.uploads, serverPath)
		}
	}
}

// nameTakenHere reports whether an upload other than self is working on the path
// in this very process
//
// Releasing the lock of a name that is being uploaded here would cut off that
// upload, and the two sessions would then race for the name.
func (f *Fs) nameTakenHere(serverPath string, self uint64) bool {
	f.uploadsMu.Lock()
	defer f.uploadsMu.Unlock()
	for ticket := range f.uploads[serverPath] {
		if ticket != self {
			return true
		}
	}
	return false
}

// conflictPath turns the path a lock conflict names into the same form rclone
// addresses entries with
//
// Measured on a live site, the field carries a whole URI
// ("cloudreve://my/some/folder/name.txt") rather than a bare path, and a bare path
// is what the server's own lock node holds, so accept either. The percent escapes
// come off because the comparison is against decoded names.
func conflictPath(raw string) string {
	if _, rest, found := strings.Cut(raw, "://"); found {
		_, afterDrive, found := strings.Cut(rest, "/")
		if !found {
			return ""
		}
		raw = afterDrive
	}
	if unescaped, err := url.PathUnescape(raw); err == nil {
		raw = unescaped
	}
	return strings.Trim(raw, "/")
}

// withinAny reports whether path is one of prefixes or sits under one
func withinAny(path string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return false
}

// placeholderAge reports how old the unfinished upload behind a lock is, and
// false when the entry at that path is not an unfinished upload rclone can read
//
// The placeholder is created when the session begins, so its age is how long the
// upload claiming the name has been going. Nothing else on the wire says whether
// that upload is still alive: measured on a live site the entry exposes only its
// session id with no expiry, and the conflict payload carries no timestamp.
func (f *Fs) placeholderAge(ctx context.Context, serverPath string) (time.Duration, bool) {
	entry, err := f.entryAtServerPath(ctx, serverPath)
	if err != nil || !uploading(entry) {
		return 0, false
	}
	age := time.Since(entry.CreatedAt)
	if age < 0 {
		// A server clock behind this one proves nothing, and treating it as proof
		// would release locks that were just created.
		return 0, false
	}
	return age, true
}

// refuseToRelease explains why a lock may not be released, or returns "" when it
// may be
//
// Deleting is the looser rule: the request wants that very entry gone, so breaking
// the lock of an unfinished upload is doing what was asked - and leaving it locked
// is the deadlock where a folder cannot be purged because one half-written file
// inside it was interrupted. Writing is the stricter rule: taking over a name
// someone may be uploading now starts a race for it, so the operator has to ask
// for that by setting an age worth waiting for.
func (f *Fs) refuseToRelease(ctx context.Context, conflict api.LockConflict, prefixes []string, uid string, self uint64, byDeleting bool) string {
	path := conflictPath(conflict.Path)
	switch {
	case conflict.Token == "":
		return "a lock with no token was named"
	case conflict.Owner.Owner != uid:
		return path + ": held by another account"
	case conflict.Owner.Application.Type != "upload":
		return path + ": held by " + conflict.Owner.Application.Type + ", not by an upload"
	case !withinAny(path, prefixes):
		return path + ": not the path this request is writing"
	case f.nameTakenHere(path, self):
		return path + ": another upload of this run holds it"
	}
	age, known := f.placeholderAge(ctx, path)
	if !known {
		return path + ": the entry it locks is not an unfinished upload rclone can read"
	}
	if byDeleting {
		return ""
	}
	after := time.Duration(f.opt.StaleLockAfter)
	if after <= 0 {
		return path + ": held by an upload started " + age.Round(time.Second).String() +
			" ago, and rclone does not break those unless drop_stale_locks_after is set"
	}
	if age < after {
		return fmt.Sprintf("%s: uploaded only %s ago, under the %s rclone will release at",
			path, age.Round(time.Second), after)
	}
	return ""
}

// startSession sends one PUT /file/upload
func (f *Fs) startSession(ctx context.Context, remote string, src fs.ObjectInfo, policyID, entityType string) (*api.UploadSession, error) {
	session := new(api.UploadSession)
	request := &api.CreateSessionRequest{
		URI:        f.uriFor(remote),
		Size:       src.Size(),
		PolicyID:   policyID,
		EntityType: entityType,
		MimeType:   fs.MimeType(ctx, src),
	}
	if modTime := src.ModTime(ctx); !modTime.IsZero() {
		// The server reads this as milliseconds since the epoch.
		request.LastModified = modTime.UnixMilli()
	}
	err := f.call(ctx, "PUT", "/file/upload", nil, request, session)
	if err != nil {
		return nil, err
	}
	if session.SessionID == "" {
		return nil, fmt.Errorf("cloudreve: the upload session for %q has no ID", remote)
	}
	return session, nil
}

// confirmFiled answers the one question a failed callback leaves open: is the
// file there, carrying what this upload set out to write
func (f *Fs) confirmFiled(ctx context.Context, remote string, src fs.ObjectInfo) (*api.File, bool) {
	file, err := f.statFile(ctx, remote)
	if err != nil {
		return nil, false
	}
	if file.Size != src.Size() {
		return nil, false
	}
	// The same comparison rclone itself uses for "unchanged", at the precision
	// this backend documents: whole seconds.
	diff := src.ModTime(ctx).Sub(file.UpdatedAt)
	if diff < 0 {
		diff = -diff
	}
	if diff > f.Precision() {
		return nil, false
	}
	return file, true
}

// abandon tells the server an upload is giving up, so the name it claimed comes
// free again
//
// A failed abandon has to be heard: the lock outlives the session row if the
// request never lands, and the next run is told a name is held by an upload that
// stopped right here.
func (f *Fs) abandon(ctx context.Context, o *Object, session *api.UploadSession) {
	if err := f.deleteSession(ctx, session); err != nil {
		fs.Logf(o, "cloudreve: cannot release %q after the upload failed: %v", o.remote, err)
	}
}

// deleteSession abandons a session so the server can drop what it has of it
//
// The request is detached from the caller's context: abandoning is what releases
// the name's lock, and a cancelled upload has to release it most of all.
func (f *Fs) deleteSession(ctx context.Context, session *api.UploadSession) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultAbandonTimeout)
	defer cancel()
	return f.call(ctx, "DELETE", "/file/upload", nil, &api.DeleteSessionRequest{
		ID:  session.SessionID,
		URI: session.URI,
	}, nil)
}

// dropLocks releases the upload locks a conflict named, and reports how many the
// site let go together with why it refused the rest
//
// A lock is only released when every one of these holds: the conflict named it,
// this account owns it, an upload holds it rather than some other application,
// its path is one this request is working on, no upload in this process has that
// path, and the placeholder behind it is at least StaleLockAfter old. The server
// has no way to say whether a holder is still running, so that age is the only
// evidence that an upload has stopped.
func (f *Fs) dropLocks(ctx context.Context, err error, prefixes []string, self uint64, byDeleting bool) (int, string) {
	conflicts := lockConflicts(err)
	if len(conflicts) == 0 {
		return 0, "the server named no holder, so there is no lock to release"
	}
	uid, idErr := f.accountID(ctx)
	if idErr != nil {
		// Without the account's own ID nothing can be told apart, and the reason
		// has to be visible: this is where a lock we could have released is kept.
		fs.Logf(f, "cannot tell whose upload lock this is, so it is left alone: %v", idErr)
		return 0, "rclone cannot tell whose lock this is"
	}
	tokens := []string{}
	refused := []string{}
	for _, conflict := range conflicts {
		if reason := f.refuseToRelease(ctx, conflict, prefixes, uid, self, byDeleting); reason != "" {
			refused = append(refused, reason)
			continue
		}
		tokens = append(tokens, conflict.Token)
	}
	if len(refused) > 0 {
		fs.Debugf(f, "cloudreve: left upload lock(s) alone: %s", strings.Join(refused, "; "))
	}
	if len(tokens) == 0 {
		return 0, strings.Join(refused, "; ")
	}
	if err := f.call(ctx, "DELETE", "/file/lock", nil, &api.UnlockRequest{Tokens: tokens}, nil); err != nil {
		fs.Logf(f, "cannot release the upload lock left behind: %v", err)
		return 0, "the site refused to release it"
	}
	fs.Logf(f, "released %d upload lock(s) this account left behind; the unfinished uploads they came from lost their parts",
		len(tokens))
	return len(tokens), ""
}

// part is one piece the object store accepted
type part struct {
	number int
	etag   string
}

// uploadParts reads in and PUTs every part to the URL assigned to it
func (f *Fs) uploadParts(ctx context.Context, session *api.UploadSession, size int64, in io.Reader) ([]part, error) {
	urls := session.UploadURLs
	chunkSize := session.ChunkSize
	if chunkSize <= 0 {
		// The server said the file is not to be cut up.
		chunkSize = size
	}
	con := f.opt.UploadConcurrency
	if con > len(urls) {
		con = len(urls)
	}

	var (
		tokens = pacer.NewTokenDispenser(con)
		mu     sync.Mutex
		parts  = make([]part, 0, len(urls))
		off    int64
	)

	g, gCtx := errgroup.WithContext(ctx)
	for number, link := range urls {
		want := chunkSize
		if number == len(urls)-1 {
			want = size - off // the last part takes whatever is left
		}
		if want < 0 || (want == 0 && !(len(urls) == 1 && size == 0)) {
			// Parts already in flight hold the session, so it cannot be handed
			// back for the caller to abandon until they have finished.
			_ = g.Wait()
			return nil, fmt.Errorf("cloudreve: %q needs %d parts but only %d bytes were declared", path.Base(session.URI), len(urls), size)
		}
		tokens.Get()
		rw := multipart.NewRW()
		n, err := io.CopyN(rw, in, want)
		if err != nil && !errors.Is(err, io.EOF) {
			_ = rw.Close()
			tokens.Put()
			// Parts already in flight hold the session, so the caller cannot be
			// handed back a session it is free to abandon until they have finished.
			_ = g.Wait()
			return nil, fmt.Errorf("cloudreve: reading part %d: %w", number+1, err)
		}
		if n != want {
			_ = rw.Close()
			tokens.Put()
			_ = g.Wait()
			return nil, fmt.Errorf("cloudreve: the source gave %d bytes for part %d of %d: %w", n, number+1, want, io.ErrUnexpectedEOF)
		}
		off += n
		number, want, link := number, want, link
		g.Go(func() error {
			defer func() {
				_ = rw.Close()
				tokens.Put()
			}()
			etag, err := f.putPart(gCtx, link, rw, want)
			if err != nil {
				return fmt.Errorf("cloudreve: part %d of %q: %w", number+1, path.Base(session.URI), err)
			}
			mu.Lock()
			defer mu.Unlock()
			parts = append(parts, part{number: number + 1, etag: etag})
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if off != size {
		return nil, fmt.Errorf("cloudreve: the source gave %d bytes for a %d byte file: %w", off, size, io.ErrUnexpectedEOF)
	}
	if len(parts) != len(urls) {
		return nil, fmt.Errorf("cloudreve: uploaded %d parts of %q but the session asked for %d", len(parts), path.Base(session.URI), len(urls))
	}
	return parts, nil
}

// noCloseReader shows a part's buffer to the transport as readable and seekable
// but not closeable, so the buffer outlives one attempt.
type noCloseReader struct {
	io.ReadSeeker
}

// putPart uploads one part to its presigned URL, which needs no session
// credentials
func (f *Fs) putPart(ctx context.Context, link string, body io.ReadSeeker, size int64) (etag string, err error) {
	err = f.pacer.Call(func() (bool, error) {
		if _, seekErr := body.Seek(0, io.SeekStart); seekErr != nil {
			return false, fmt.Errorf("cloudreve: can't rewind part: %w", seekErr)
		}
		// Hide the buffer's Close from the transport, which otherwise closes it
		// after the first attempt and leaves nothing to rewind for a retry.
		req, err := http.NewRequestWithContext(ctx, "PUT", link, noCloseReader{body})
		if err != nil {
			return false, signedError("PUT", link, err)
		}
		req.ContentLength = size
		if size == 0 {
			// A zero-byte body with a length of 0 and a non-nil reader is read by
			// the transport as "length unknown", which it sends chunked - and some
			// object stores answer that with HTTP 400 "Content-Length is nil".
			// NoBody says empty on the wire instead.
			req.Body = http.NoBody
		}
		resp, err := f.bare.Do(req)
		if err != nil {
			err = signedError("PUT", link, err)
			return fserrors.ShouldRetry(err), err
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode >= 300 {
			detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			err := fmt.Errorf("cloudreve: PUT %s: HTTP %s: %s", redactLink(link), resp.Status, strings.TrimSpace(string(detail)))
			return resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500, err
		}
		etag = resp.Header.Get("ETag")
		if etag == "" {
			return false, fmt.Errorf("cloudreve: part upload to %s answered without an ETag", redactLink(link))
		}
		return false, nil
	})
	if err != nil {
		return "", err
	}
	return etag, nil
}

// finishSession tells the object store that the parts are all there, then tells
// Cloudreve to file the result
func (f *Fs) finishSession(ctx context.Context, session *api.UploadSession, parts []part) error {
	if session.CompleteURL != "" {
		if err := f.completeMultipart(ctx, session.CompleteURL, parts); err != nil {
			return err
		}
	}
	if session.CallbackSecret == "" {
		return errors.New("cloudreve: the session has no callback secret, so rclone cannot tell the server the file is ready")
	}
	// The callback is an API call, not a storage one: the server resolves the
	// session under the signed-in account, so it carries the session token. It is
	// sent once, because a lost reply has already been filed by the time a second
	// delivery arrives, and the server then answers as if the lock had gone.
	endpoint := "/callback/" + PolicyTypeS3 + "/" + url.PathEscape(session.SessionID) + "/" + url.PathEscape(session.CallbackSecret)
	return f.callOnce(ctx, "GET", endpoint, nil, nil, nil)
}

// completeXML is the body that finalises a multipart upload
type completeXML struct {
	XMLName xml.Name  `xml:"CompleteMultipartUpload"`
	Parts   []partXML `xml:"Part"`
}

type partXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// completeMultipartUpload POSTs the part list to the object store
func (f *Fs) completeMultipart(ctx context.Context, link string, parts []part) error {
	sorted := append([]part(nil), parts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].number < sorted[j].number })
	body := completeXML{Parts: make([]partXML, 0, len(sorted))}
	for _, p := range sorted {
		// S3 wants the ETag quoted, and answers it quoted, so send it as it
		// arrived.
		etag := p.etag
		if !strings.HasPrefix(etag, `"`) {
			etag = `"` + etag + `"`
		}
		body.Parts = append(body.Parts, partXML{PartNumber: p.number, ETag: etag})
	}
	raw, err := xml.Marshal(body)
	if err != nil {
		return fmt.Errorf("cloudreve: can't build the completion request: %w", err)
	}
	return f.pacer.Call(func() (bool, error) {
		req, err := http.NewRequestWithContext(ctx, "POST", link, strings.NewReader(string(raw)))
		if err != nil {
			return false, signedError("POST", link, err)
		}
		req.Header.Set("Content-Type", "application/xml")
		req.ContentLength = int64(len(raw))
		resp, err := f.bare.Do(req)
		if err != nil {
			err = signedError("POST", link, err)
			return fserrors.ShouldRetry(err), err
		}
		defer func() { _ = resp.Body.Close() }()
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if resp.StatusCode >= 300 {
			err := fmt.Errorf("cloudreve: completing the upload answered HTTP %s: %s", resp.Status, strings.TrimSpace(string(detail)))
			return resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500, err
		}
		// An S3 error can arrive with HTTP 200, in which case the reply is XML
		// starting with an Error element.
		if bytes.HasPrefix(bytes.TrimSpace(detail), []byte("<Error")) {
			return false, fmt.Errorf("cloudreve: completing the upload: %s", strings.TrimSpace(string(detail)))
		}
		return false, nil
	})
}
