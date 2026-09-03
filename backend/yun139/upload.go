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
	"time"

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
// The input is streamed once through a temp file while SHA-256 is
// computed, then /hcy/file/create + parallel part PUTs are driven from
// random-access SectionReader handles. This caps memory use to one
// part (chunkSize) regardless of the file size, and the single pass
// doubles as the midstate scan for parallelUpload.
func (f *Fs) uploadFile(ctx context.Context, in io.Reader, dirID, leaf string, size int64) (*uploadResult, error) {
	if size <= 0 {
		return nil, errors.New("yun139: size must be > 0")
	}
	// Small files: keep the reader in memory, hash it once.
	if size <= int64(5*1024*1024) {
		data := make([]byte, size)
		h := sha256.New()
		if _, err := io.ReadFull(io.TeeReader(in, h), data); err != nil {
			return nil, fmt.Errorf("yun139: read: %w", err)
		}
		return f.uploadFromRandom(ctx, bytes.NewReader(data), dirID, leaf, size, hex.EncodeToString(h.Sum(nil)))
	}
	// Streaming path for large files: stream the data through a temp file
	// while hashing it, then drive /hcy/file/create + parallel part PUTs
	// from random-access SectionReader handles. This caps memory use to
	// one part (chunkSize) regardless of the file size.
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
	// The official PC client (captured 2026-09-03) sends
	// parallelUpload:true with a parallelHashCtx per part: the SHA-256
	// midstate (8 H registers) after all bytes before that part. The
	// server signs each part's context into its upload URL
	// (X-Amz-Iteration-Hash-Ctx) and accepts out-of-order concurrent
	// part PUTs. Part 1 carries no context (it starts from the IV).
	partInfos := make([]api.PartInfo, 0, len(parts))
	for i, p := range parts {
		pi := api.PartInfo{
			PartNumber: p.index,
			PartSize:   p.partSize,
		}
		// The official client always sets partOffset (we verified on
		// 2026-09-03 with mCloudDownload.zip) even on the first part;
		// only the h field is omitted for part 1.
		pi.ParallelHashCtx = &api.ParallelHashCtx{PartOffset: p.offset}
		if i > 0 {
			regs, _, err := api.Sha256Midstate(sha256MidstateHash(freader, p.offset))
			if err != nil {
				return nil, fmt.Errorf("yun139: midstate part %d: %w", p.index, err)
			}
			pi.ParallelHashCtx.H = regs
		}
		partInfos = append(partInfos, pi)
	}
	// Limit the create payload to the first maxPartsPerRequest parts; the
	// rest are covered by /hcy/file/getUploadUrl later (alist does the
	// same 100-part batches). The full list is kept for the fetch step.
	allPartInfos := partInfos
	if len(partInfos) > maxPartsPerRequest {
		partInfos = partInfos[:maxPartsPerRequest]
	}
	body := api.PersonalCreateReq{
		CommonUpload: api.CommonUpload{
			ParentID: dirID,
			Name:     leaf,
			Size:     size,
			Type:     "file",
		},
		FileRenameMode: "auto_rename",
	}
	body.ContentHash = hashHex
	body.ContentHashAlgorithm = "SHA256"
	body.ContentType = "application/octet-stream"
	body.ParallelUpload = true
	body.PartInfos = partInfos
	// The official client stamps local timestamps in RFC3339 UTC with
	// milliseconds, e.g. "2026-09-03T08:06:36.784Z". The server rejects
	// other formats with '04000002: 本地更新时间格式不符合标准'.
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	body.LocalCreatedAt = now
	body.LocalUpdatedAt = now
	if len(body.PartInfos) > maxPartsPerRequest {
		body.PartInfos = body.PartInfos[:maxPartsPerRequest]
	}
	var resp api.PersonalCreateResp
	if err := f.personalCall(ctx, "/hcy/file/create", body, &resp); err != nil {
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
		return nil, &apiError{Code: resp.Code, Message: resp.Message}
	}
	if len(resp.Data.PartInfos) == 0 {
		return nil, errors.New("create returned no upload URL")
	}
	// PUT every part in parallel, in batches of maxPartsPerRequest.
	// The first batch's URLs came from /file/create; the rest come from
	// /hcy/file/getUploadUrl (captured 2026-09-03: the client fetches
	// part 101+ exactly this way, with parallelUpload:true and the same
	// parallelHashCtx entries).
	allParts := make([]api.PartUploadInfo, 0, len(parts))
	allParts = append(allParts, resp.Data.PartInfos...)
	for i := maxPartsPerRequest; i < len(parts); i += maxPartsPerRequest {
		end := i + maxPartsPerRequest
		if end > len(parts) {
			end = len(parts)
		}
		// Reuse the precomputed partInfos (with parallelHashCtx) for
		// parts 101+ - the official client sends the same entries to
		// /hcy/file/getUploadUrl.
		urlBody := map[string]any{
			"fileId":         resp.Data.FileID,
			"uploadId":       resp.Data.UploadID,
			"parallelUpload": true,
			"partInfos":      allPartInfos[i:end],
		}
		var urlResp struct {
			api.BaseResp
			Data struct {
				PartInfos []api.PartUploadInfo `json:"partInfos"`
			} `json:"data"`
		}
		if err := f.pacer.Call(func() (bool, error) {
			err := f.personalCall(ctx, "/hcy/file/getUploadUrl", urlBody, &urlResp)
			return shouldRetry(ctx, err)
		}); err != nil {
			return nil, fmt.Errorf("getUploadUrl: %w", err)
		}
		if !urlResp.Success {
			return nil, &apiError{Code: urlResp.Code, Message: urlResp.Message}
		}
		allParts = append(allParts, urlResp.Data.PartInfos...)
	}
	var eg errgroup.Group
	eg.SetLimit(f.opt.UploadConcurrency)
	for i, p := range parts {
		i, p := i, p
		eg.Go(func() error {
			if i >= len(allParts) {
				return fmt.Errorf("yun139: no upload URL for part %d", p.index)
			}
			rdr := io.NewSectionReader(freader, p.offset, p.partSize)
			return f.putPart(ctx, rdr, allParts[i].UploadURL, p.partSize)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("put part: %w", err)
	}
	// Finally, mark the file complete. Mirrors the official client:
	// contentHash + contentHashAlgorithm + fileId + uploadId.
	cmpl := api.PersonalCompleteReq{
		FileID:               resp.Data.FileID,
		UploadID:             resp.Data.UploadID,
		ContentHash:          hashHex,
		ContentHashAlgorithm: "SHA256",
	}
	var cmplResp api.PersonalCompleteResp
	if err := f.personalCall(ctx, "/hcy/file/complete", cmpl, &cmplResp); err != nil {
		return nil, fmt.Errorf("complete: %w", err)
	}
	if !cmplResp.Success {
		return nil, &apiError{Code: cmplResp.Code, Message: cmplResp.Message}
	}
	return &uploadResult{fileID: resp.Data.FileID, fileName: leaf}, nil
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
		return f.deleteTask(ctx, "/hcy/file/batchDelete", id)
	}
	return f.deleteTask(ctx, "/hcy/recyclebin/batchTrash", id)
}

// deleteTask performs one batch-delete call and polls the returned task.
func (f *Fs) deleteTask(ctx context.Context, endpoint, id string) error {
	var out struct {
		api.BaseResp
		Data struct {
			TaskID string `json:"taskId"`
		} `json:"data"`
	}
	err := f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, endpoint, api.PersonalTrashReq{FileIds: []string{id}, BusinessType: 0}, &out)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return err
	}
	if out.Data.TaskID == "" {
		// Some endpoints complete synchronously; nothing to poll.
		return nil
	}
	return f.taskGet(ctx, out.Data.TaskID, "delete")
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
		err := f.personalCall(ctx, "/hcy/file/update", api.PersonalUpdateReq{
			FileId: id,
			Name:   newName,
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
	taskID, err := f.moveTaskID(ctx, srcObj.id, dstDirID)
	if err != nil {
		return err
	}
	return f.taskGet(ctx, taskID, "move")
}

// moveTaskID is a helper that performs one batchMove and returns the task id.
func (f *Fs) moveTaskID(ctx context.Context, id, dstDirID string) (string, error) {
	var out struct {
		api.BaseResp
		Data struct {
			TaskID string `json:"taskId"`
		} `json:"data"`
	}
	err := f.pacer.Call(func() (bool, error) {
		err := f.personalCall(ctx, "/hcy/file/batchMove", api.PersonalBatchMoveReq{
			FileIds:        []string{id},
			ToParentFileID: dstDirID,
			UserID:         f.account,
			EventType:      "move",
			BusinessType:   0,
		}, &out)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return "", err
	}
	return out.Data.TaskID, nil
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
	taskID, err := f.moveTaskID(ctx, srcID, dstID)
	if err != nil {
		return err
	}
	return f.taskGet(ctx, taskID, "dirmove")
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
	taskID, err := f.copyTaskID(ctx, srcObj.id, dstDirID)
	if err != nil {
		return err
	}
	return f.taskGet(ctx, taskID, "copy")
}

// copyTaskID performs one batchCopy and returns the task id.
func (f *Fs) copyTaskID(ctx context.Context, id, dstDirID string) (string, error) {
	var out struct {
		api.BaseResp
		Data struct {
			TaskID string `json:"taskId"`
		} `json:"data"`
	}
	err := f.pacer.Call(func() (bool, error) {
		// The official client sends userId = userDomainId (the
		// 1301956522699563527-style id) for copy; we only know the
		// phone number at this point, so fall back to it. If the server
		// rejects it, userDomainID discovery is needed.
		userID := f.userDomainID
		if userID == "" {
			userID = f.account
		}
		err := f.personalCall(ctx, "/hcy/file/batchCopy", api.PersonalBatchCopyReq{
			FileIds:        []string{id},
			ToParentFileID: dstDirID,
			UserID:         userID,
			UserDomainID:   userID,
		}, &out)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return "", err
	}
	return out.Data.TaskID, nil
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
// sha256MidstateHash hashes the first n bytes of src through a fresh
// SHA-256 and returns the digest. Caller exports the midstate via
// api.Sha256Midstate. The hash is never re-used: each part of the file
// needs its own digest up to its starting offset.
func sha256MidstateHash(src io.ReaderAt, n int64) interface {
	Write(p []byte) (int, error)
	Sum(b []byte) []byte
} {
	h := sha256.New()
	// Copy in 1 MiB chunks. With 5 MiB parts and typical 64 KiB Go
	// buffer defaults this stays comfortably off the GC.
	buf := make([]byte, 1<<20)
	off := int64(0)
	for off < n {
		want := n - off
		if want > int64(len(buf)) {
			want = int64(len(buf))
		}
		nr, err := src.ReadAt(buf[:want], off)
		if nr > 0 {
			h.Write(buf[:nr])
		}
		if err != nil {
			break
		}
		off += int64(nr)
	}
	return h
}
