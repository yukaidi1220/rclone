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

// buildCreateBody assembles the /hcy/file/create payload exactly as the
// official PC client posts it (captured 2026-09-03, v8.8.6.20260829):
// contentHash + contentHashAlgorithm:SHA256 + contentType + parallelUpload:true
// + partInfos[:100] + fileRenameMode:auto_rename + localCreatedAt/localUpdatedAt
// in RFC3339 millisecond UTC.
func buildCreateBody(dirID, leaf string, size int64, hashHex string, partInfos []api.PartInfo) api.PersonalCreateReq {
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
	// The official client sends localCreatedAt/localUpdatedAt as
	// RFC3339 UTC with milliseconds (captured 2026-09-03,
	// v8.8.6.20260829 - e.g. "2026-09-03T08:06:36.784Z"). The server
	// REJECTS empty strings with '04000002: 本地创建时间格式不符合标准',
	// but ignores the actual value in favour of its own clock. So we
	// send a valid-format stamp from time.Now() and let the server
	// overwrite the read-back value (mirrors official client + keeps
	// us within the format spec).
	now := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	body.LocalCreatedAt = now
	body.LocalUpdatedAt = now
	if len(body.PartInfos) > maxPartsPerRequest {
		body.PartInfos = body.PartInfos[:maxPartsPerRequest]
	}
	return body
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
	body := buildCreateBody(dirID, leaf, size, hashHex, partInfos)
	// Family space uses a different create endpoint
	// (/hcy/group/dynamic/file/create, captured 2026-09-03) on the
	// group.yun.139.com host, with extra fields {groupId, groupType,
	// seqNo, expireSec}. The response envelope is the same shape
	// (fileId / uploadId / partInfos / rapidUpload) but inside data
	// plus an extra "file" object. We treat the two flows as parallel
	// by mapping both into a unified PartInfos[] result.
	var resp struct {
		api.BaseResp
		Data struct {
			FileID      string               `json:"fileId"`
			FileName    string               `json:"fileName"`
			UploadID    string               `json:"uploadId"`
			RapidUpload bool                 `json:"rapidUpload"`
			Exists      *bool                `json:"exist"`
			PartInfos   []api.PartUploadInfo `json:"partInfos"`
		} `json:"data"`
	}
	var callPath string
	var callBody any = body
	if f.space == spaceFamily {
		callPath = "/hcy/group/dynamic/file/create"
		fb := map[string]any{
			"contentHash":          body.ContentHash,
			"contentHashAlgorithm": body.ContentHashAlgorithm,
			"expireSec":            86400,
			"fileRenameMode":       body.FileRenameMode,
			"groupId":              f.opt.FamilyID,
			"groupType":            1,
			"localCreatedAt":       body.LocalCreatedAt,
			"localUpdatedAt":       body.LocalUpdatedAt,
			"name":                 body.Name,
			"parallelUpload":       body.ParallelUpload,
			"parentFileId":         body.ParentID,
			"partInfos":            partInfos,
			"seqNo":                familySeqNo(),
			"size":                 body.Size,
			"type":                 body.Type,
		}
		if body.UserRegion != nil {
			fb["userRegion"] = map[string]any{
				"cityCode":     body.UserRegion.CityCode,
				"provinceCode": body.UserRegion.ProvinceCode,
			}
		}
		callBody = fb
	} else {
		callPath = "/hcy/file/create"
	}
	if err := f.familyCall(ctx, callPath, callBody, &resp); err != nil {
		return nil, fmt.Errorf("create: %w", err)
	}
	fs.Debugf(f, "yun139: create returned %d part URLs, rapid=%v exists=%v", len(resp.Data.PartInfos), resp.Data.RapidUpload, resp.Data.Exists)
	// rapidUpload success path: server already has the content (no part
	// URLs to fetch and no upload body to send), or the file already
	// exists under this name.
	if resp.Success && resp.Data.FileID != "" &&
		(resp.Data.RapidUpload || (resp.Data.Exists != nil && *resp.Data.Exists) || len(resp.Data.PartInfos) == 0) {
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
		// /hcy/file/getUploadUrl (personal) or
		// /hcy/group/dynamic/file/getUploadUrl (family).
		urlBody := map[string]any{
			"fileId":         resp.Data.FileID,
			"uploadId":       resp.Data.UploadID,
			"parallelUpload": true,
			"partInfos":      allPartInfos[i:end],
		}
		if f.space == spaceFamily {
			urlBody["groupId"] = f.opt.FamilyID
			urlBody["groupType"] = 1
		}
		var urlResp struct {
			api.BaseResp
			Data struct {
				PartInfos []api.PartUploadInfo `json:"partInfos"`
			} `json:"data"`
		}
		urlPath := "/hcy/file/getUploadUrl"
		if f.space == spaceFamily {
			urlPath = "/hcy/group/dynamic/file/getUploadUrl"
		}
		if err := f.pacer.Call(func() (bool, error) {
			err := f.call(ctx, f.callHost()+urlPath, urlBody, &urlResp)
			return shouldRetry(ctx, err)
		}); err != nil {
			return nil, fmt.Errorf("getUploadUrl: %w", err)
		}
		if !urlResp.Success {
			return nil, &apiError{Code: urlResp.Code, Message: urlResp.Message}
		}
		allParts = append(allParts, urlResp.Data.PartInfos...)
	}
	// The server does not guarantee partInfos ordering in the
	// getUploadUrl response (captured 2026-09-03: [110, 111, 112, 101,
	// 113, ...]), so index-aligning allParts with parts would put
	// chunks on the wrong URLs. Match by partNumber instead.
	urlOfPart := make(map[int]string, len(parts))
	for _, pi := range allParts {
		if pi.PartNumber > 0 {
			urlOfPart[pi.PartNumber] = pi.UploadURL
		}
	}
	var eg errgroup.Group
	eg.SetLimit(f.opt.UploadConcurrency)
	for _, p := range parts {
		p := p
		eg.Go(func() error {
			url, ok := urlOfPart[int(p.index)]
			if !ok {
				return fmt.Errorf("yun139: no upload URL for part %d", p.index)
			}
			rdr := io.NewSectionReader(freader, p.offset, p.partSize)
			return f.putPart(ctx, rdr, url, p.partSize)
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
	if f.space == spaceFamily {
		cmpl.GroupID = f.opt.FamilyID
		cmpl.AccountUserID = f.userDomainID
	}
	cmplPath := "/hcy/file/complete"
	if f.space == spaceFamily {
		cmplPath = "/hcy/group/dynamic/file/complete"
	}
	var cmplResp api.PersonalCompleteResp
	if err := f.call(ctx, f.callHost()+cmplPath, cmpl, &cmplResp); err != nil {
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
		taskID, err := f.familyBatchOprTask(ctx, familyBatchReq{
			ContentList:       []string{id},
			DestCloudID:       "",    // delete - no dest
			DestCatalogType:   1002,
			DestType:          "1",
			DestPath:          "",
			SourceCatalogType: 1002,
			SourceCloudID:     f.opt.FamilyID,
			SourceType:        "1",
			Path:              srvPath,
			TaskType:          2, // delete
			BusinessType:      2,
		})
		if err != nil {
			return err
		}
		return f.familyTaskPoll(ctx, taskID)
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
		// The PC client no longer has a "move" button inside the family
		// cloud (transfers are copy + manual delete). rclone's Move
		// falls back to "copy then delete" which the server surfaces
		// as taskType 1 (copy) followed by taskType 2 (delete).
		if err := f.familyCopy(ctx, srcObj, dstDirID); err != nil {
			return err
		}
		return f.deleteObject(ctx, srcObj.id, srcObj.serverPath, true)
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
		// No native family move - fall back to copy+delete.
		if err := f.familyCopyID(ctx, srcID, dstID, true /*isDir*/); err != nil {
			return err
		}
		return f.familyDeleteID(ctx, srcID, f.familySrvPath(srcID))
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
		return f.familyCopy(ctx, srcObj, dstDirID)
	}
	taskID, err := f.copyTaskID(ctx, srcObj.id, dstDirID)
	if err != nil {
		return err
	}
	return f.taskGet(ctx, taskID, "copy")
}

// familyCopy copies srcObj (file or dir) into dstDirID inside the family
// cloud via createBatchOprTaskV2 taskType=1, then polls the task
// (captured 2026-09-03).
func (f *Fs) familyCopy(ctx context.Context, srcObj *Object, dstDirID string) error {
	return f.familyCopyID(ctx, srcObj.id, dstDirID, srcObj.isDir)
}

// familyCopyID copies the given content/catalog id into dstDirID.
func (f *Fs) familyCopyID(ctx context.Context, id, dstDirID string, isDir bool) error {
	req := familyBatchReq{
		DestCatalogType:   1002,
		DestCloudID:       f.opt.FamilyID,
		DestPath:          f.familySrvPath(dstDirID),
		DestType:          "1",
		Path:              "",
		SourceCatalogType: 1002,
		SourceCloudID:     f.opt.FamilyID,
		SourceType:        "1",
		TaskType:          1, // copy
		BusinessType:      2,
	}
	if isDir {
		req.CatalogList = []string{id}
	} else {
		req.ContentList = []string{id}
	}
	taskID, err := f.familyBatchOprTask(ctx, req)
	if err != nil {
		return err
	}
	return f.familyTaskPoll(ctx, taskID)
}

// familyDeleteID deletes the given family catalog id (taskType=2).
func (f *Fs) familyDeleteID(ctx context.Context, id, srvPath string) error {
	taskID, err := f.familyBatchOprTask(ctx, familyBatchReq{
		CatalogList:       []string{id},
		DestCatalogType:   1002,
		DestType:          "1",
		Path:              srvPath,
		SourceCatalogType: 1002,
		SourceCloudID:     f.opt.FamilyID,
		SourceType:        "1",
		TaskType:          2, // delete
		BusinessType:      2,
	})
	if err != nil {
		return err
	}
	return f.familyTaskPoll(ctx, taskID)
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
		// <user-domain-id>-style id) for copy; we only know the
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

// OpenChunkWriter supports the multi-thread copy engine by staging
// chunks in a temp file and running the real upload at Close.
//
// 139's protocol needs the whole-file SHA-256 *before* /file/create can
// return the part upload URLs, so the chunked copy engine cannot drive
// the parts directly. Instead, WriteChunk stages each chunk into a temp
// file at its part offset (safe under concurrent out-of-order writes),
// and Close hashes the file and runs the same pipeline as Put
// (temp file + SHA-256 + /hcy/file/create + parallel part PUTs).
// The cost vs Put is one extra disk round-trip.
func (f *Fs) OpenChunkWriter(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (info fs.ChunkWriterInfo, writer fs.ChunkWriter, err error) {
	size := src.Size()
	if size < 0 {
		return info, nil, errors.New("yun139: can't upload files of unknown size")
	}
	leaf, dirID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return info, nil, err
	}
	leaf = f.opt.Enc.FromStandardName(leaf)
	chunkSize := int64(f.opt.PartSize)
	if chunkSize <= 0 {
		chunkSize = api.DefaultChunkSize
	}
	if size < chunkSize {
		chunkSize = size
	}
	if size == 0 {
		chunkSize = 1 // zero-size files still need a temp file to hash
	}
	tmp, err := os.CreateTemp("", "yun139-chunkwriter-")
	if err != nil {
		return info, nil, fmt.Errorf("yun139: chunk writer temp: %w", err)
	}
	// Preallocate so WriteAt never hits EOF errors on sparse regions.
	if err := tmp.Truncate(size); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
		return info, nil, fmt.Errorf("yun139: chunk writer truncate: %w", err)
	}
	w := &yun139ChunkWriter{
		f:         f,
		tmp:       tmp,
		path:      tmp.Name(),
		dirID:     dirID,
		leaf:      leaf,
		size:      size,
		chunkSize: chunkSize,
		total:     (size + chunkSize - 1) / chunkSize,
	}
	info = fs.ChunkWriterInfo{
		ChunkSize:   chunkSize,
		Concurrency: f.opt.UploadConcurrency,
	}
	return info, w, nil
}

// yun139ChunkWriter stages chunks into a temp file for OpenChunkWriter.
type yun139ChunkWriter struct {
	f         *Fs
	tmp       *os.File
	path      string
	dirID     string
	leaf      string
	size      int64
	chunkSize int64
	total     int64
	closed    bool
}

// WriteChunk writes chunkNumber at chunkNumber*chunkSize in the temp
// file. The copy engine seeks the reader to the chunk start before
// calling; concurrent out-of-order calls are safe because WriteAt is
// position-addressed.
func (w *yun139ChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (int64, error) {
	if w.closed {
		return 0, errors.New("yun139: chunk writer closed")
	}
	if int64(chunkNumber) >= w.total {
		return 0, fmt.Errorf("yun139: chunk %d out of range (total %d)", chunkNumber, w.total)
	}
	offset := int64(chunkNumber) * w.chunkSize
	// Read exactly this chunk's worth (the last chunk may be short).
	limit := w.chunkSize
	if remaining := w.size - offset; remaining < limit {
		limit = remaining
	}
	// The engine positions the reader at the chunk start; read from the
	// current position.
	buf := make([]byte, limit)
	m, err := io.ReadFull(reader, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, err
	}
	if _, err := w.tmp.WriteAt(buf[:m], offset); err != nil {
		return 0, err
	}
	return int64(m), nil
}

// Close hashes the staged file and runs the standard upload pipeline.
func (w *yun139ChunkWriter) Close(ctx context.Context) error {
	if w.closed {
		return errors.New("yun139: chunk writer already closed")
	}
	w.closed = true
	defer func() {
		_ = w.tmp.Close()
		_ = os.Remove(w.path)
	}()
	if err := w.tmp.Sync(); err != nil {
		return err
	}
	h := sha256.New()
	if _, err := w.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.Copy(h, w.tmp); err != nil {
		return fmt.Errorf("yun139: chunk writer hash: %w", err)
	}
	if _, err := w.tmp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := w.f.uploadFromRandom(ctx, w.tmp, w.dirID, w.leaf, w.size, hex.EncodeToString(h.Sum(nil)))
	if err != nil {
		return fmt.Errorf("yun139: chunk writer upload: %w", err)
	}
	return nil
}

// Abort removes the temp file without uploading.
func (w *yun139ChunkWriter) Abort(ctx context.Context) error {
	if w.closed {
		return nil
	}
	w.closed = true
	_ = w.tmp.Close()
	_ = os.Remove(w.path)
	return nil
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
