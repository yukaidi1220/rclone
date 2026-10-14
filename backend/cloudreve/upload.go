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
	// An empty file goes through the same session as any other: the server hands
	// out one part URL for it, and this is the only path that carries
	// last_modified, so the modified time survives.
	policy, err := f.writablePolicy(ctx, o.remote)
	if err != nil {
		return err
	}

	session, err := f.createSession(ctx, o.remote, src, policy.ID, "")
	if codeOf(err) == api.CodeObjectExist {
		// An existing file is only written over as a new version, which the
		// server keeps or discards according to the account's version
		// retention.
		session, err = f.createSession(ctx, o.remote, src, policy.ID, "version")
	}
	if err != nil {
		return err
	}
	// The session names the policy the bytes land on. If that is not the S3
	// child asked for, the site chose otherwise and rclone cannot push through
	// whatever it picked.
	if session.StoragePolicy != nil && session.StoragePolicy.Type != PolicyTypeS3 {
		_ = f.deleteSession(ctx, session)
		return fmt.Errorf("cloudreve: the upload of %q landed on storage policy %q, which rclone cannot write through",
			o.remote, session.StoragePolicy.Type)
	}
	if len(session.UploadURLs) == 0 {
		_ = f.deleteSession(ctx, session)
		return fmt.Errorf("cloudreve: the session for %q carried no part URLs, so its storage policy is not one rclone uploads through", o.remote)
	}

	parts, err := f.uploadParts(ctx, session, size, in)
	if err != nil {
		_ = f.deleteSession(ctx, session)
		return err
	}
	if err := f.finishSession(ctx, session, parts); err != nil {
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
// An interrupted upload leaves the name locked, so a conflict is answered by
// releasing this account's own stale lock and asking once more.
func (f *Fs) createSession(ctx context.Context, remote string, src fs.ObjectInfo, policyID, entityType string) (*api.UploadSession, error) {
	session, err := f.startSession(ctx, remote, src, policyID, entityType)
	if codeOf(err) == api.CodeLockConflict && f.opt.DropStaleLocks && f.dropLocks(ctx, err) > 0 {
		return f.startSession(ctx, remote, src, policyID, entityType)
	}
	return session, err
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

// dropLocks releases the upload locks a conflict named, counting only the ones
// this account owns, and reports how many the site let go
//
// A lock held by another application, or by another account, is left alone: the
// server does not say whether its holder is still running, so dropping it could
// cut off an upload that is merely slow.
func (f *Fs) dropLocks(ctx context.Context, err error) int {
	conflicts := lockConflicts(err)
	if len(conflicts) == 0 {
		return 0
	}
	uid, idErr := f.accountID(ctx)
	if idErr != nil {
		// Without the account's own ID nothing can be told apart, and the reason
		// has to be visible: this is where a lock we could have released is kept.
		fs.Logf(f, "cannot tell whose upload lock this is, so it is left alone: %v", idErr)
		return 0
	}
	tokens := []string{}
	for _, conflict := range conflicts {
		if conflict.Token == "" || conflict.Owner.Owner != uid ||
			conflict.Owner.Application.Type != "upload" {
			continue
		}
		tokens = append(tokens, conflict.Token)
	}
	if len(tokens) == 0 {
		return 0
	}
	if err := f.call(ctx, "DELETE", "/file/lock", nil, &api.UnlockRequest{Tokens: tokens}, nil); err != nil {
		fs.Logf(f, "cannot release the upload lock left behind: %v", err)
		return 0
	}
	fs.Logf(f, "released %d upload lock(s) this account left behind; the unfinished uploads they came from lost their parts",
		len(tokens))
	return len(tokens)
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
	// session under the signed-in account, so it carries the session token.
	endpoint := "/callback/" + PolicyTypeS3 + "/" + url.PathEscape(session.SessionID) + "/" + url.PathEscape(session.CallbackSecret)
	return f.call(ctx, "GET", endpoint, nil, nil, nil)
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
