package cloudreve

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/rclone/rclone/backend/cloudreve/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/hash"
)

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info { return o.fs }

// Remote returns the remote path
func (o *Object) Remote() string { return o.remote }

// Size in bytes
func (o *Object) Size() int64 { return o.size }

// ModTime is what the server kept from the upload
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }

// String describes the object
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// uri is the cloudreve:// URI of this object
func (o *Object) uri() string { return o.fs.uriFor(o.remote) }

// ID is the server's identifier for the file
func (o *Object) ID() string { return o.id }

// Hash is unavailable: the API returns no content hash, so rclone compares
// objects by size and modified time.
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Storable says the object can be stored on this remote
func (o *Object) Storable() bool { return true }

// SetModTime is not supported: the server stamps the time of an upload and has
// no API to change it afterwards.
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

// setFrom fills the object from a server response
func (o *Object) setFrom(file *api.File) {
	o.id = file.ID
	o.size = file.Size
	o.modTime = file.UpdatedAt
}

// cachedURL is a link the server handed out for one file
type cachedURL struct {
	url     string
	expires time.Time
}

// downloadURL returns a link to the object's content, minting a new one when
// the cached link is close to stopping work
func (f *Fs) downloadURL(ctx context.Context, remote string) (string, error) {
	uri := f.uriFor(remote)
	f.mu.Lock()
	cached, found := f.links[uri]
	f.mu.Unlock()
	if found && time.Until(cached.expires) > urlExpiryMargin {
		return cached.url, nil
	}

	data := new(api.FileURLData)
	err := f.call(ctx, "POST", "/file/url", nil, &api.FileURLRequest{
		Uris: []string{uri},
	}, data)
	if err != nil {
		return "", err
	}
	if len(data.URLs) == 0 || data.URLs[0].URL == "" {
		return "", fmt.Errorf("cloudreve: no download URL returned for %q", remote)
	}
	expires := time.Now().Add(time.Hour)
	if data.Expires != nil {
		expires = *data.Expires
	}
	f.mu.Lock()
	f.links[uri] = &cachedURL{url: data.URLs[0].URL, expires: expires}
	// A long run over a huge tree would otherwise keep every link it ever
	// minted, so drop the stale ones once the map has grown.
	if len(f.links) > maxCachedLinks {
		for key, cached := range f.links {
			if time.Now().After(cached.expires) {
				delete(f.links, key)
			}
		}
	}
	f.mu.Unlock()
	return data.URLs[0].URL, nil
}

// forgetURL drops a cached link that the storage refused
func (f *Fs) forgetURL(remote string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.links, f.uriFor(remote))
}

// Open reads the object
//
// The link points into the object store, so it is fetched without the session
// credentials the API needs.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.size)
	headers := fs.OpenOptionHeaders(options)

	link, err := o.fs.downloadURL(ctx, o.remote)
	if err != nil {
		return nil, err
	}
	res, err := o.fs.fetch(ctx, link, headers)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusForbidden || res.StatusCode == http.StatusNotFound {
		// An expired link answers like a refusal, so ask for another one once
		// instead of failing the read.
		_ = res.Body.Close()
		o.fs.forgetURL(o.remote)
		if link, err = o.fs.downloadURL(ctx, o.remote); err != nil {
			return nil, err
		}
		if res, err = o.fs.fetch(ctx, link, headers); err != nil {
			return nil, err
		}
	}
	if res.StatusCode >= 300 {
		_ = res.Body.Close()
		return nil, fmt.Errorf("cloudreve: open %q: HTTP %s", o.remote, res.Status)
	}
	return res.Body, nil
}

// fetch gets a URL that carries its own credentials
// fetch sends one GET to a storage link and returns the response before any body
// is read
//
// A dropped connection before the reply is retried, as the rest of this backend's
// requests are: the site's own proxy cuts connections now and then, and one such
// cut must not fail a transfer that has not started streaming yet.
func (f *Fs) fetch(ctx context.Context, link string, headers map[string]string) (*http.Response, error) {
	var resp *http.Response
	err := f.pacer.Call(func() (bool, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", link, nil)
		if err != nil {
			return false, signedError("GET", link, err)
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}
		resp, err = f.bare.Do(req)
		if err != nil {
			err = signedError("GET", link, err)
			return fserrors.ShouldRetry(err), err
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			_ = resp.Body.Close()
			return true, fmt.Errorf("cloudreve: GET %s: HTTP %s", redactLink(link), resp.Status)
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Remove the object
func (o *Object) Remove(ctx context.Context) error {
	err := o.fs.deleteUris(ctx, []string{o.uri()})
	if err != nil {
		return err
	}
	o.fs.forgetURL(o.remote)
	return nil
}

// Update the object with the contents of in
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return o.Upload(ctx, in, src, options...)
}

// redactLink keeps the credentials of a signed URL out of the logs
func redactLink(link string) string {
	u, err := url.Parse(link)
	if err != nil {
		return "<unreadable link>"
	}
	if u.RawQuery != "" {
		u.RawQuery = "***"
	}
	return u.String()
}

// signedError reports a failure of a request to a URL that carries its own
// credentials, without repeating them
//
// The HTTP layer names the whole URL in its error, and a presigned URL's query is
// its signature, so the error it returns has to be taken apart rather than
// wrapped: printing it as %w would put a working link into the log.
func signedError(op, link string, err error) error {
	if urlErr, ok := err.(*url.Error); ok {
		err = urlErr.Err
	}
	return fmt.Errorf("cloudreve: %s %s: %w", op, redactLink(link), err)
}
