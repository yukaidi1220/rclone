package yun139

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rclone/rclone/backend/yun139/api"
	"github.com/rclone/rclone/fs"
)

// downloadURL returns a cached download URL for the object, fetching a fresh
// one when empty or expired.
func (o *Object) downloadURL(ctx context.Context) (string, error) {
	o.urlMu.Lock()
	defer o.urlMu.Unlock()
	if o.url != "" && time.Now().Before(o.urlExpiry) {
		return o.url, nil
	}
	url, err := o.fs.fetchDownloadURL(ctx, o)
	if err != nil {
		return "", err
	}
	o.url = url
	o.urlExpiry = time.Now().Add(downloadURLTTL)
	return url, nil
}

// invalidateURL drops the cached download URL.
func (o *Object) invalidateURL() {
	o.urlMu.Lock()
	defer o.urlMu.Unlock()
	o.url = ""
	o.urlExpiry = time.Time{}
}

// fetchDownloadURL requests a fresh download URL for the object.
func (f *Fs) fetchDownloadURL(ctx context.Context, o *Object) (string, error) {
	if f.space == spaceFamily {
		// POST .../getFileDownLoadURLV2 (captured 2026-09-03). The
		// server returns a single "downloadURL" string.
		body := map[string]any{
			"catalogType": 3,
			"cloudID":     f.opt.FamilyID,
			"cloudType":   1,
			"commonAccountInfo": map[string]any{
				"account":     f.account,
				"accountType": "1",
			},
			"contentID": o.id,
			"extInfo": map[string]string{
				"isReturnCdnDownloadUrl": "1",
			},
			"path": o.serverPath,
		}
		var resp struct {
			Result struct {
				ResultCode string `json:"resultCode"`
				ResultDesc string `json:"resultDesc"`
			} `json:"result"`
			DownloadURL string `json:"downloadURL"`
		}
		err := f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/getFileDownLoadURLV2", body, &resp)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return "", err
		}
		if resp.Result.ResultCode != "0" {
			return "", &apiError{Code: resp.Result.ResultCode, Message: resp.Result.ResultDesc}
		}
		if resp.DownloadURL == "" {
			return "", fmt.Errorf("yun139: no download URL returned for %q", o.remote)
		}
		return resp.DownloadURL, nil
	}
	// The official client requests a 24h link (expireSec:86400).
	body := map[string]any{"fileId": o.id, "expireSec": 86400}
	var resp api.PersonalDownloadResp
	err := f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, "/hcy/file/getDownloadUrl", body, &resp)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return "", err
	}
	// Prefer cdnUrl when the server says the CDN switch is on, otherwise url.
	if resp.Data.CDNURL != "" && resp.Data.CDNSwitch {
		return resp.Data.CDNURL, nil
	}
	if resp.Data.URL == "" {
		return "", fmt.Errorf("yun139: no download URL returned for %q", o.remote)
	}
	return resp.Data.URL, nil
}

// Open opens the file for read. Call Close() on the returned io.ReadCloser.
//
// A cached link that has been revoked (403) is refetched once before giving up.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	fs.FixRangeOption(options, o.size)
	headers := fs.OpenOptionHeaders(options)
	url, err := o.downloadURL(ctx)
	if err != nil {
		return nil, err
	}
	res, err := o.fs.getURL(ctx, url, headers)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == http.StatusForbidden {
		// The cached link is stale - drop it and fetch a fresh one once.
		_ = res.Body.Close()
		o.invalidateURL()
		url, err = o.downloadURL(ctx)
		if err != nil {
			return nil, err
		}
		res, err = o.fs.getURL(ctx, url, headers)
		if err != nil {
			return nil, err
		}
	}
	if res.StatusCode >= 300 {
		_ = res.Body.Close()
		return nil, fmt.Errorf("yun139: download: %s", res.Status)
	}
	return res.Body, nil
}

// ensure strings import is used
var _ = strings.TrimSpace
