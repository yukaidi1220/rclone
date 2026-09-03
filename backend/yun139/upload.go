package yun139

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"

	"github.com/rclone/rclone/backend/yun139/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/lib/random"
	"golang.org/x/sync/errgroup"
)

// uploadResult is the outcome of one Put/Update upload.
type uploadResult struct {
	fileID  string
	fileName string // server-side name after auto_rename
	hashHex string
}

// partPlan describes the byte range of one part of an upload.
type partPlan struct {
	index    int64 // 1-based part number
	offset   int64 // byte offset of the part start
	partSize int64 // bytes in this part
}

// planParts returns the part boundaries for a file of size bytes with the
// given part size. Mirrors the SDK's floor-division rule (last part absorbs
// the remainder), with a minimum of one part.
func planParts(size, partSize int64) []partPlan {
	if partSize <= 0 {
		partSize = personalPartSize
	}
	// total = ceil(size / partSize); minimum 1 part even for empty files.
	total := size / partSize
	if size%partSize != 0 {
		total++
	}
	if total <= 0 {
		total = 1
	}
	plans := make([]partPlan, 0, total)
	var sent int64
	for i := int64(1); i <= total; i++ {
		n := partSize
		if i == total {
			n = size - sent
			if n < 0 {
				n = 0
			}
		}
		plans = append(plans, partPlan{index: i, offset: sent, partSize: n})
		sent += n
	}
	return plans
}

// Put in to the remote path with the modTime given of the given size.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	size := src.Size()
	if size < 0 {
		return nil, errors.New("yun139: can't upload files of unknown size")
	}
	if size == 0 {
		return nil, fs.ErrorCantUploadEmptyFiles
	}
	leaf, dirID, err := f.dirCache.FindPath(ctx, src.Remote(), true)
	if err != nil {
		return nil, err
	}
	leaf = f.opt.Enc.FromStandardName(leaf)
	res, err := f.uploadFile(ctx, in, dirID, leaf, size)
	if err != nil {
		return nil, err
	}
	o := &Object{
		fs:      f,
		remote:  src.Remote(),
		id:      res.fileID,
		size:    size,
		modTime: src.ModTime(ctx),
		urlMu:   &sync.Mutex{},
	}
	return o, nil
}

// uploadFile is the single upload pipeline used by both Put and Update.
//
// The whole file is buffered in memory (or a TempFile for files larger than
// 64 MiB) so the SHA-256 and the multipart stream come from the same bytes.
func (f *Fs) uploadFile(ctx context.Context, in io.Reader, dirID, leaf string, size int64) (*uploadResult, error) {
	if size <= 0 {
		return nil, errors.New("yun139: size must be > 0")
	}
	if size <= int64(64*1024*1024) {
		// In-memory path: one Read+Hash, then uploadFromBuffer.
		data := make([]byte, size)
		h := sha256.New()
		if _, err := io.ReadFull(io.TeeReader(in, h), data); err != nil {
			return nil, fmt.Errorf("yun139: read: %w", err)
		}
		return f.uploadFromBuffer(ctx, data, dirID, leaf, size, hex.EncodeToString(h.Sum(nil)))
	}
	// Streaming path for large files: stream the data through a temp file
	// while hashing it, then drive /file/create + parallel part PUTs from
	// random-access SectionReader handles. This caps memory use to one
	// part (chunkSize) regardless of the file size.
	tmp, err := os.CreateTemp("", "yun139-upload-")
	if err != nil {
		return nil, fmt.Errorf("yun139: temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), in)
	if err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("yun139: hash: %w", err)
	}
	if n != size {
		_ = tmp.Close()
		return nil, fmt.Errorf("yun139: short read: got %d bytes, want %d", n, size)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	res, err := f.uploadFromRandom(ctx, tmp, dirID, leaf, size, hex.EncodeToString(h.Sum(nil)))
	_ = tmp.Close()
	return res, err
}

// uploadFromRandom drives a multi-part upload for data already on disk in
// the given *os.File (positioned at offset 0). The same file is used both
// for hashing (already done) and for reading each part on demand via
// io.SectionReader, so memory use is bounded by chunkSize.
func (f *Fs) uploadFromRandom(ctx context.Context, freader io.ReaderAt, dirID, leaf string, size int64, hashHex string) (*uploadResult, error) {
	chunkSize := int64(f.opt.PartSize)
	if chunkSize <= 0 {
		chunkSize = api.DefaultChunkSize
	}
	parts := planParts(size, chunkSize)
	// First, ask the server to create (or rapid-upload) the file.
	// The payload mirrors alist/139Strm: contentHash + type:file, no
	// rapidUpload flag (the server decides instant-upload by hash).
	partInfos := make([]api.PartInfo, 0, len(parts))
	for _, p := range parts {
		partInfos = append(partInfos, api.PartInfo{
			PartNumber: p.index,
			PartSize:   p.partSize,
			ParallelHashCtx: &api.ParallelHashCtx{
				PartOffset: p.offset,
			},
		})
	}
	body := api.PersonalCreateReq{
		CommonUpload: api.CommonUpload{
			ParentID: dirID,
			Name:     leaf,
			SHA256:   hashHex,
			Size:     size,
			MD5:      "",
			Type:     "file",
		},
		FileRenameMode: "auto_rename",
	}
	body.ContentHash = hashHex
	body.ContentHashAlgorithm = "SHA256"
	body.ContentType = "application/octet-stream"
	body.ParallelUpload = false
	body.PartInfos = partInfos
	if len(body.PartInfos) > maxPartsPerRequest {
		body.PartInfos = body.PartInfos[:maxPartsPerRequest]
	}
	var resp api.PersonalCreateResp
	if err := f.personalCall(ctx, "/file/create", body, &resp); err != nil {
		return nil, fmt.Errorf("create: %w", err)
	}
	fs.Debugf(f, "yun139: create returned %d part URLs, rapid=%v exists=%v", len(resp.Data.PartInfos), resp.Data.RapidUpload, resp.Data.Exists)
	// rapidUpload success path: server already has the content (no part
	// URLs to fetch and no upload body to send), or the file already
	// exists under this name.
	if resp.Success && resp.Data.FileID != "" &&
		(resp.Data.RapidUpload || resp.Data.Exists || len(resp.Data.PartInfos) == 0) {
		return &uploadResult{fileID: resp.Data.FileID, fileName: leaf}, nil
	}
	if !resp.Success {
		return nil, fmt.Errorf("create failed: %s %s", resp.Code, resp.Message)
	}
	if len(resp.Data.PartInfos) == 0 {
		return nil, errors.New("create returned no upload URL")
	}
	// Then PUT every part in parallel.
	var eg errgroup.Group
	eg.SetLimit(f.opt.UploadConcurrency)
	for i, p := range parts {
		i, p := i, p
		eg.Go(func() error {
			pi := resp.Data.PartInfos[i]
			rdr := io.NewSectionReader(freader, p.offset, p.partSize)
			return f.putPart(ctx, rdr, pi.UploadURL, p.partSize)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("put part: %w", err)
	}
	// Finally, mark the file complete.
	cmpl := api.PersonalCompleteReq{
		FileID:  resp.Data.FileID,
		SHA256:  hashHex,
		Size:    size,
		ResType: 1,
	}
	var cmplResp api.PersonalCompleteResp
	if err := f.personalCall(ctx, "/file/complete", cmpl, &cmplResp); err != nil {
		return nil, fmt.Errorf("complete: %w", err)
	}
	if !cmplResp.Success {
		return nil, fmt.Errorf("complete failed: %s %s", cmplResp.Code, cmplResp.Message)
	}
	return &uploadResult{fileID: resp.Data.FileID, fileName: leaf}, nil
}

// uploadFromBuffer uploads a file that is already in memory.
func (f *Fs) uploadFromBuffer(ctx context.Context, data []byte, dirID, leaf string, size int64, hashHex string) (*uploadResult, error) {
	plans := planParts(size, int64(f.opt.PartSize))
	// Step 1: create the upload session and fetch the first 100 part URLs.
	first := plans
	if len(first) > maxPartsPerRequest {
		first = first[:maxPartsPerRequest]
	}
	createBody, err := f.makeCreateReq(dirID, leaf, size, hashHex, first)
	if err != nil {
		return nil, err
	}
	var createResp api.PersonalUploadResp
	if err := f.pacer.Call(func() (bool, error) {
		err := f.uploadCreate(ctx, createBody, &createResp)
		return shouldRetry(ctx, err)
	}); err != nil {
		return nil, err
	}
	if createResp.Data.Exist {
		return &uploadResult{fileID: createResp.Data.FileId, fileName: leaf, hashHex: hashHex}, nil
	}
	if len(createResp.Data.PartInfos) == 0 {
		// Rapid upload: server already has the content, no parts to send.
		return f.completeUpload(ctx, createResp.Data.FileId, createResp.Data.UploadId, hashHex)
	}
	// Step 2: upload the first 100 parts.
	if err := f.uploadParts(ctx, data, plans[:len(createResp.Data.PartInfos)], createResp.Data.PartInfos); err != nil {
		return nil, err
	}
	// Step 3: get more part URLs in batches of 100 and upload the rest.
	allParts := createResp.Data.PartInfos
	for i := maxPartsPerRequest; i < len(plans); i += maxPartsPerRequest {
		end := i + maxPartsPerRequest
		if end > len(plans) {
			end = len(plans)
		}
		batch := plans[i:end]
		urls, err := f.fetchMoreURLs(ctx, createResp.Data.FileId, createResp.Data.UploadId, batch)
		if err != nil {
			return nil, err
		}
		if err := f.uploadParts(ctx, data, batch, urls); err != nil {
			return nil, err
		}
		allParts = append(allParts, urls...)
	}
	// Step 4: complete.
	return f.completeUpload(ctx, createResp.Data.FileId, createResp.Data.UploadId, hashHex)
}

// uploadStreamed uploads from a TempFile (no buffer copy). It is the path for
// files that did not fit in memory.
func (f *Fs) uploadStreamed(ctx context.Context, in io.ReaderAt, dirID, leaf string, size int64, sum []byte) (*uploadResult, error) {
	plans := planParts(size, int64(f.opt.PartSize))
	first := plans
	if len(first) > maxPartsPerRequest {
		first = first[:maxPartsPerRequest]
	}
	hashHex := hex.EncodeToString(sum)
	createBody, err := f.makeCreateReq(dirID, leaf, size, hashHex, first)
	if err != nil {
		return nil, err
	}
	var createResp api.PersonalUploadResp
	if err := f.pacer.Call(func() (bool, error) {
		err := f.uploadCreate(ctx, createBody, &createResp)
		return shouldRetry(ctx, err)
	}); err != nil {
		return nil, err
	}
	if createResp.Data.Exist {
		return &uploadResult{fileID: createResp.Data.FileId, fileName: leaf, hashHex: hashHex}, nil
	}
	if len(createResp.Data.PartInfos) == 0 {
		return f.completeUpload(ctx, createResp.Data.FileId, createResp.Data.UploadId, hashHex)
	}
	if err := f.uploadPartsRandom(ctx, in, plans[:len(createResp.Data.PartInfos)], createResp.Data.PartInfos, size); err != nil {
		return nil, err
	}
	allParts := createResp.Data.PartInfos
	for i := maxPartsPerRequest; i < len(plans); i += maxPartsPerRequest {
		end := i + maxPartsPerRequest
		if end > len(plans) {
			end = len(plans)
		}
		batch := plans[i:end]
		urls, err := f.fetchMoreURLs(ctx, createResp.Data.FileId, createResp.Data.UploadId, batch)
		if err != nil {
			return nil, err
		}
		if err := f.uploadPartsRandom(ctx, in, batch, urls, size); err != nil {
			return nil, err
		}
		allParts = append(allParts, urls...)
	}
	return f.completeUpload(ctx, createResp.Data.FileId, createResp.Data.UploadId, hashHex)
}

// makeCreateReq builds the create request for the active space.
//
// The server expects a parallelHashCtx with partOffset for every part. The
// SHA-256 midstate (H) is only needed for true parallel upload; the official
// client falls back to sequential upload when the midstate is missing, so
// leaving the H field empty is safe.
func (f *Fs) makeCreateReq(dirID, leaf string, size int64, hashHex string, plans []partPlan) (any, error) {
	parts := make([]api.PartInfo, 0, len(plans))
	for _, p := range plans {
		parts = append(parts, api.PartInfo{
			PartNumber: p.index,
			PartSize:   p.partSize,
			ParallelHashCtx: &api.ParallelHashCtx{
				PartOffset: p.offset,
			},
		})
	}
	if f.space == spaceFamily {
		body := api.FamilyUploadCreateReq{}
		body.FamilyCommon.CloudID = f.opt.FamilyID
		body.FamilyCommon.CommonAccountInfo.Account = f.account
		body.FamilyCommon.CommonAccountInfo.AccountType = 1
		body.ContentHash = hashHex
		body.ContentHashAlgorithm = "SHA256"
		body.ContentType = "application/octet-stream"
		body.ParallelUpload = false
		body.PartInfos = parts
		body.Size = size
		body.ParentFileID = dirID
		body.Name = leaf
		body.Type = "file"
		body.FileRenameMode = "auto_rename"
		body.GroupID = f.opt.FamilyID
		body.GroupType = 1 // family
		body.SeqNo = random.String(32)
		return body, nil
	}
	return map[string]any{
		"contentHash":          hashHex,
		"contentHashAlgorithm": "SHA256",
		"contentType":          "application/octet-stream",
		"parallelUpload":       false,
		"partInfos":            parts,
		"size":                 size,
		"parentFileId":         dirID,
		"name":                 leaf,
		"type":                 "file",
		"fileRenameMode":       "auto_rename",
	}, nil
}

// uploadCreate issues the /file/create call (personal or family).
func (f *Fs) uploadCreate(ctx context.Context, body any, out *api.PersonalUploadResp) error {
	if f.space == spaceFamily {
		req, ok := body.(api.FamilyUploadCreateReq)
		if !ok {
			return fmt.Errorf("yun139: family upload body has wrong type %T", body)
		}
		var resp api.FamilyUploadCreateResp
		if err := f.familyCall(ctx, "/dynamic/file/create", req, &resp); err != nil {
			return err
		}
		out.BaseResp = resp.BaseResp
		out.Data = resp.Data
		return nil
	}
	return f.personalCall(ctx, "/file/create", body, out)
}

// fetchMoreURLs calls /file/getUploadUrl (personal) or
// /dynamic/file/getUploadUrl (family) for a batch of partInfos.
func (f *Fs) fetchMoreURLs(ctx context.Context, fileID, uploadID string, plans []partPlan) ([]api.PersonalPartInfo, error) {
	parts := make([]api.PartInfo, 0, len(plans))
	for _, p := range plans {
		parts = append(parts, api.PartInfo{
			PartNumber:      p.index,
			PartSize:        p.partSize,
			ParallelHashCtx: &api.ParallelHashCtx{PartOffset: p.offset},
		})
	}
	if f.space == spaceFamily {
		body := api.FamilyUploadURLReq{}
		body.FamilyCommon.CloudID = f.opt.FamilyID
		body.FamilyCommon.CommonAccountInfo.Account = f.account
		body.FamilyCommon.CommonAccountInfo.AccountType = 1
		body.FileId = fileID
		body.UploadId = uploadID
		body.PartInfos = parts
		var resp api.FamilyUploadCreateResp
		if err := f.familyCall(ctx, "/dynamic/file/getUploadUrl", body, &resp); err != nil {
			return nil, err
		}
		return resp.Data.PartInfos, nil
	}
	body := map[string]any{
		"fileId":    fileID,
		"uploadId":  uploadID,
		"partInfos": parts,
		"commonAccountInfo": map[string]any{
			"account":     f.account,
			"accountType": 1,
		},
	}
	var resp api.PersonalUploadURLResp
	if err := f.personalCall(ctx, "/file/getUploadUrl", body, &resp); err != nil {
		return nil, err
	}
	return resp.Data.PartInfos, nil
}

// completeUpload calls /file/complete and returns the uploadResult.
func (f *Fs) completeUpload(ctx context.Context, fileID, uploadID, hashHex string) (*uploadResult, error) {
	if f.space == spaceFamily {
		body := api.FamilyUploadCompleteReq{}
		body.FamilyCommon.CloudID = f.opt.FamilyID
		body.FamilyCommon.CommonAccountInfo.Account = f.account
		body.FamilyCommon.CommonAccountInfo.AccountType = 1
		body.ContentHash = hashHex
		body.ContentHashAlgorithm = "SHA256"
		body.FileId = fileID
		body.UploadId = uploadID
		if err := f.familyCall(ctx, "/dynamic/file/complete", body, nil); err != nil {
			return nil, err
		}
		return &uploadResult{fileID: fileID, fileName: "", hashHex: hashHex}, nil
	}
	body := map[string]any{
		"contentHash":          hashHex,
		"contentHashAlgorithm": "SHA256",
		"fileId":               fileID,
		"uploadId":             uploadID,
	}
	if err := f.personalCall(ctx, "/file/complete", body, nil); err != nil {
		return nil, err
	}
	return &uploadResult{fileID: fileID, fileName: "", hashHex: hashHex}, nil
}

// uploadParts reads each part from data and PUTs it to its upload URL.
// parts and urls must be aligned by index. Concurrent, --upload-concurrency
// workers at most.
func (f *Fs) uploadParts(ctx context.Context, data []byte, parts []partPlan, urls []api.PersonalPartInfo) error {
	if len(parts) != len(urls) {
		return fmt.Errorf("yun139: parts/urls length mismatch: %d vs %d", len(parts), len(urls))
	}
	concurrency := f.opt.UploadConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > len(parts) {
		concurrency = len(parts)
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	var mu sync.Mutex
	var firstErr error
	for i := range parts {
		p := parts[i]
		u := urls[i]
		g.Go(func() error {
			err := f.putPart(gctx, bytes.NewReader(data[p.offset:p.offset+p.partSize]), u.UploadURL, p.partSize)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return err
			}
			return nil
		})
	}
	_ = g.Wait()
	return firstErr
}

// uploadPartsRandom is like uploadParts but reads each part from a random-
// access source (TempFile) at the part offset.
func (f *Fs) uploadPartsRandom(ctx context.Context, in io.ReaderAt, parts []partPlan, urls []api.PersonalPartInfo, total int64) error {
	if len(parts) != len(urls) {
		return fmt.Errorf("yun139: parts/urls length mismatch: %d vs %d", len(parts), len(urls))
	}
	concurrency := f.opt.UploadConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > len(parts) {
		concurrency = len(parts)
	}
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)
	var mu sync.Mutex
	var firstErr error
	for i := range parts {
		p := parts[i]
		u := urls[i]
		g.Go(func() error {
			section := io.NewSectionReader(in, p.offset, p.partSize)
			err := f.putPart(gctx, section, u.UploadURL, p.partSize)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return err
			}
			return nil
		})
	}
	_ = g.Wait()
	return firstErr
}

// putPart PUTs a single part to its pre-signed URL.
func (f *Fs) putPart(ctx context.Context, r io.Reader, url string, size int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Origin", yunBaseURL)
	req.Header.Set("Referer", yunBaseURL+"/")
	req.Header.Set("User-Agent", defaultUserAgent)
	// Content-Length is part of the CDN's S3 signature: leaving it out
	// makes the server reject the part with SignatureDoesNotMatch once
	// it tries to verify the body. The caller must know the part size.
	if size > 0 {
		req.ContentLength = size
		req.Header.Set("Content-Length", strconv.FormatInt(size, 10))
	}
	res, err := f.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode >= 300 {
		body, _ := io.ReadAll(res.Body)
		// 5xx and 4xx are both treated as business errors; the part URL
		// is single-use so a retry on a transient 5xx would just hit the
		// same dead URL.
		err := fmt.Errorf("yun139: upload part: %s: %s", res.Status, truncate(string(body), 500))
		if res.StatusCode >= 400 && res.StatusCode < 500 {
			return fserrors.NoRetryError(err)
		}
		return err
	}
	return nil
}

// Update in to the object with the modTime given of the given size.
//
// The 139 API has no in-place update, so the new content is uploaded under
// a temporary name, the old object is deleted, and the temp is renamed into
// place.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	size := src.Size()
	if size < 0 {
		return errors.New("yun139: can't upload files of unknown size")
	}
	if size == 0 {
		return fs.ErrorCantUploadEmptyFiles
	}
	leaf, dirID, err := o.fs.dirCache.FindPath(ctx, o.remote, true)
	if err != nil {
		return err
	}
	leaf = o.fs.opt.Enc.FromStandardName(leaf)
	tempLeaf := leaf + ".rclone-tmp-" + random.String(8)
	res, err := o.fs.uploadFile(ctx, in, dirID, tempLeaf, size)
	if err != nil {
		return err
	}
	// The new file may have been auto-renamed on the server; for the
	// post-rename step we want to address the just-created file by id and
	// rename it to the target name. If the new id is empty (the upload did
	// not return a usable id), treat as failure.
	if res.fileID == "" {
		// The complete response did not echo the id; refresh the parent to
		// look up the new file id by name.
		found, err := o.fs.findByNameInDir(ctx, dirID, res.fileName)
		if err != nil {
			return err
		}
		if found == "" {
			return errors.New("yun139: cannot resolve the new file id after upload")
		}
		res.fileID = found
	}
	// Delete the old object.
	if err := o.fs.deleteObject(ctx, o.id, o.serverPath, o.fs.space == spaceFamily); err != nil {
		return fserrors.NoLowLevelRetryError(fmt.Errorf("yun139: delete old object: %w", err))
	}
	// Rename the new file to the target name.
	if err := o.fs.renameObject(ctx, res.fileID, leaf, dirID, o.fs.space == spaceFamily); err != nil {
		return fserrors.NoLowLevelRetryError(fmt.Errorf("yun139: rename temp object: %w", err))
	}
	// Refresh the receiver in place.
	o.id = res.fileID
	o.size = size
	o.modTime = src.ModTime(ctx)
	return nil
}

// findByNameInDir lists dirID and returns the first file id matching name.
func (f *Fs) findByNameInDir(ctx context.Context, dirID, name string) (string, error) {
	var id string
	err := f.listAll(ctx, dirID, func(e listEntry) bool {
		if !e.isDir && strings.EqualFold(e.name, name) {
			id = e.id
			return true
		}
		return false
	})
	return id, err
}

// deleteObject removes a single object by id.
//
// family=true uses the family batch-delete endpoint; false uses the personal
// /recyclebin/batchTrash (or /file/batchDelete if hard_delete is on).
func (f *Fs) deleteObject(ctx context.Context, id, srvPath string, family bool) error {
	if family {
		body := api.FamilyDeleteReq{}
		body.FamilyCommon.CloudID = f.opt.FamilyID
		body.FamilyCommon.CommonAccountInfo.Account = f.account
		body.FamilyCommon.CommonAccountInfo.AccountType = 1
		body.ContentList = []string{id}
		body.SourceCloudID = f.opt.FamilyID
		body.SourceCatalogType = 1002
		body.TaskType = 2
		body.Path = srvPath
		return f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/orchestration/familyCloud-rebuild/batchOprTask/v1.0/createBatchOprTask", body, nil)
			return shouldRetry(ctx, err)
		})
	}
	if f.opt.HardDelete {
		return f.pacer.Call(func() (bool, error) {
			err := f.personalCall(ctx, "/file/batchDelete", api.PersonalTrashReq{FileIds: []string{id}}, nil)
			return shouldRetry(ctx, err)
		})
	}
	return f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, "/recyclebin/batchTrash", api.PersonalTrashReq{FileIds: []string{id}}, nil)
		return shouldRetry(ctx, err)
	})
}

// renameObject renames a file or folder.
func (f *Fs) renameObject(ctx context.Context, id, newName, dirID string, family bool) error {
	if family {
		body := api.FamilyModifyDocV2Req{}
		body.FamilyCommon.CloudID = f.opt.FamilyID
		body.FamilyCommon.CommonAccountInfo.Account = f.account
		body.FamilyCommon.CommonAccountInfo.AccountType = 1
		body.CatalogType = 3
		body.DocLibName = newName
		body.DocLibraryID = id
		// Path of the parent dir (dircache stores the family dirId here).
		_ = dirID
		return f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/modifyCloudDocV2", body, nil)
			return shouldRetry(ctx, err)
		})
	}
	return f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, "/file/update", api.PersonalUpdateReq{
			FileId:      id,
			Name:        newName,
			Description: "",
		}, nil)
		return shouldRetry(ctx, err)
	})
}

// Remove deletes the object
func (o *Object) Remove(ctx context.Context) error {
	return o.fs.deleteObject(ctx, o.id, o.serverPath, o.fs.space == spaceFamily)
}

// Move a file or directory
func (f *Fs) Move(ctx context.Context, src fs.Object, dst fs.Fs, dstDir string) error {
	if dst != f {
		return fs.ErrorCantMove
	}
	srcObj, ok := src.(*Object)
	if !ok {
		return fs.ErrorCantMove
	}
	dstDirID, _, err := f.dirCache.FindPath(ctx, path.Join(f.root, dstDir), true)
	if err != nil {
		return err
	}
	if f.space == spaceFamily {
		body := api.IsboBatchOprTaskReq{}
		body.AccountInfo.AccountName = f.account
		body.AccountInfo.AccountType = "1"
		body.DestCatalogID = dstDirID
		body.DestGroupID = f.opt.FamilyID
		body.DestType = 0
		body.SrcGroupID = f.opt.FamilyID
		body.SrcType = 0
		body.TaskType = 3
		if srcObj.isDir {
			body.CatalogList = []string{srcObj.serverPath}
		} else {
			body.ContentList = []string{path.Join(srcObj.serverPath, srcObj.id)}
		}
		return f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/orchestration/familyCloud-rebuild/batchOprTask/v1.0/createBatchOprTask", body, nil)
			return shouldRetry(ctx, err)
		})
	}
	return f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, "/file/batchMove", api.PersonalBatchMoveReq{
			FileIds:        []string{srcObj.id},
			ToParentFileID: dstDirID,
		}, nil)
		return shouldRetry(ctx, err)
	})
}

// DirMove moves a directory
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok || srcFs != f {
		return fs.ErrorCantDirMove
	}
	srcID, _, err := f.dirCache.FindPath(ctx, path.Join(f.root, srcRemote), false)
	if err != nil {
		return err
	}
	dstID, _, err := f.dirCache.FindPath(ctx, path.Join(f.root, dstRemote), true)
	if err != nil {
		return err
	}
	if f.space == spaceFamily {
		body := api.IsboBatchOprTaskReq{}
		body.AccountInfo.AccountName = f.account
		body.AccountInfo.AccountType = "1"
		body.CatalogList = []string{srcID}
		body.DestCatalogID = dstID
		body.DestGroupID = f.opt.FamilyID
		body.DestType = 0
		body.SrcGroupID = f.opt.FamilyID
		body.SrcType = 0
		body.TaskType = 3
		return f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/orchestration/familyCloud-rebuild/batchOprTask/v1.0/createBatchOprTask", body, nil)
			return shouldRetry(ctx, err)
		})
	}
	return f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, "/file/batchMove", api.PersonalBatchMoveReq{
			FileIds:        []string{srcID},
			ToParentFileID: dstID,
		}, nil)
		return shouldRetry(ctx, err)
	})
}

// Copy a file
func (f *Fs) Copy(ctx context.Context, src fs.Object, dst fs.Fs, dstDir string) error {
	if dst != f {
		return fs.ErrorCantCopy
	}
	srcObj, ok := src.(*Object)
	if !ok {
		return fs.ErrorCantCopy
	}
	dstDirID, _, err := f.dirCache.FindPath(ctx, path.Join(f.root, dstDir), true)
	if err != nil {
		return err
	}
	if f.space == spaceFamily {
		body := api.AndAlbumCopyReq{}
		body.CommonAccountInfo.AccountType = "1"
		body.CommonAccountInfo.AccountUserId = f.account
		body.DestCatalogID = dstDirID
		body.DestCloudID = f.opt.FamilyID
		body.SourceCloudID = f.opt.FamilyID
		if srcObj.isDir {
			body.SourceCatalogIDs = []string{srcObj.id}
		} else {
			body.SourceContentIDs = []string{srcObj.id}
		}
		return f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/copyContentCatalog", body, nil)
			return shouldRetry(ctx, err)
		})
	}
	return f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, "/file/batchCopy", api.PersonalBatchCopyReq{
			FileIds:        []string{srcObj.id},
			ToParentFileID: dstDirID,
		}, nil)
		return shouldRetry(ctx, err)
	})
}

// OpenChunkWriter is not implemented. 139's upload protocol needs the
// whole-file SHA-256 *before* /file/create can return the part upload
// URLs, so the chunked copy engine (which calls WriteChunk per part with
// a fresh io.ReadSeeker each time) cannot drive the upload without
// knowing the hash up front. rclone's fs.ObjectInfo interface does not
// give us a reader either, so we cannot pre-hash inside this method.
//
// The equivalent concurrency is exposed via PutUnchecked → Put, which
// runs the upload as temp-file + SHA-256 + parallel part PUTs (see
// uploadFromRandom).  Set --yun139-upload-concurrency to tune the
// per-file part PUT parallelism (default 4).
func (f *Fs) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (info fs.ChunkWriterInfo, writer fs.ChunkWriter, err error) {
	return info, nil, fs.ErrorNotImplemented
}

// PutUnchecked aliases Put: 139's /file/create handles the
// "file already exists" case via its fileRenameMode field, so Put
// already does the right thing for the chunked copy engine's
// "I have already confirmed the target will be overwritten" path.
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}