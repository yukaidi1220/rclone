// Package api has type definitions for the wopan (China Unicom cloud drive)
// backend.
//
// The field names and types below follow the protocol specification exactly.
// Note in particular that some fields have inconsistent types between the
// listing and recycle-bin APIs: `type` is numeric in QueryAllFiles but a
// string in QueryRecycleData. `fileVersion` arrives sometimes as a number and
// sometimes as a quoted string even within one listing page (verified against
// the live server), so it is decoded through FlexString.
package api

import (
	"bytes"
	"encoding/json"
	"time"
)

// FlexString accepts a JSON string or a JSON number and yields a string.
//
// The wopan server is inconsistent about quoting numeric fields (fileVersion
// is the known case), so fields with that behaviour decode through this type
// instead of failing the whole listing.
type FlexString string

// UnmarshalJSON implements json.Unmarshaler.
func (s *FlexString) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		*s = ""
		return nil
	}
	if b[0] == '"' {
		var v string
		if err := json.Unmarshal(b, &v); err != nil {
			return err
		}
		*s = FlexString(v)
		return nil
	}
	// A JSON number: keep the raw literal, which is already the decimal text.
	*s = FlexString(b)
	return nil
}

// TimeFormat is the time layout used for all timestamps returned by the API.
//
// wopan returns every timestamp as a 14-digit string "20060102150405" in
// UTC+8 (Beijing time). Callers must convert through the fixed zone before
// formatting.
const TimeFormat = "20060102150405"

// Beijing is the fixed UTC+8 zone used for all wopan timestamps.
//
// It must be a fixed zone (time.FixedZone) rather than the local time zone, so
// that parsing is correct regardless of the CI or host machine's local time.
var Beijing = time.FixedZone("UTC+8", 8*3600)

// ParseTime parses a 14-digit wopan timestamp into time.Time in the UTC+8 zone.
//
// An empty string yields the zero time.
func ParseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.ParseInLocation(TimeFormat, s, Beijing)
}

// ------------------------------------------------------------ envelope ----

// Header is the request header carried inside the dispatcher envelope.
//
// Key is the method name (e.g. "QueryAllFiles"). It is used both as the
// header key and as the first component of the signature input, but it is
// unrelated to the AES encryption key (which is selected by channel).
type Header struct {
	Key     string `json:"key"`
	ResTime int64  `json:"resTime"`
	ReqSeq  int    `json:"reqSeq"`
	Channel string `json:"channel"`
	Sign    string `json:"sign"`
	Version string `json:"version"`
}

// The dispatcher response envelope is decoded by responseEnvelope in the
// parent package, which keeps DATA as json.RawMessage: DATA may arrive as a
// quoted ciphertext string, the empty string, or a plaintext JSON object, and
// the three cases must be disambiguated on the raw bytes before decoding.

// ------------------------------------------------------------ api-user ----

// QueryUserRequest is the param for api-user/AppQueryUser (token liveness).
type QueryUserRequest struct {
	AccessToken string `json:"accessToken"`
}

// QueryUserResponse is the DATA returned by api-user/AppQueryUser.
//
// DATA is returned as a plaintext JSON object (not encrypted).
type QueryUserResponse struct {
	UserID        string `json:"userId"`
	HeadURL       string `json:"headUrl"`
	UserName      string `json:"userName"`
	Sex           string `json:"sex"`
	Birthday      string `json:"birthday"`
	IsModify      string `json:"isModify"`
	IsHeadModify  string `json:"isHeadModify"`
	IsSetPassword string `json:"isSetPassword"`
	RegisterTime  string `json:"registerTime"`
}

// RefreshTokenRequest is the param for api-user/AppRefreshToken.
type RefreshTokenRequest struct {
	RefreshToken string `json:"refreshToken"`
	ClientSecret string `json:"clientSecret"`
}

// RefreshTokenResponse is the plaintext DATA returned by api-user/AppRefreshToken.
//
// DATA is returned as a plaintext JSON object (not encrypted).
type RefreshTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
}

// ------------------------------------------------------------ wohome ------

// ListRequest is the param for wohome/QueryAllFiles.
type ListRequest struct {
	SpaceType         string `json:"spaceType"`
	ParentDirectoryID string `json:"parentDirectoryId"`
	PageNum           int    `json:"pageNum"`
	PageSize          int    `json:"pageSize"`
	SortRule          int    `json:"sortRule"`
	ClientID          string `json:"clientId"`
	FamilyID          string `json:"familyId,omitempty"` // omitted for personal space
}

// File is a single file/directory entry returned by QueryAllFiles.
//
// `Type` is numeric: 0=directory, 1=file. `FileVersion` decodes through
// FlexString because the server quotes it inconsistently.
type File struct {
	ID                string     `json:"id"`
	Fid               string     `json:"fid"`
	Name              string     `json:"name"`
	Type              int        `json:"type"`
	Size              int64      `json:"size"`
	CreateTime        string     `json:"createTime"`
	CollectTime       string     `json:"collectTime"`
	ShootingTime      string     `json:"shootingTime"`
	Creator           string     `json:"creator"`
	FileType          string     `json:"fileType"`
	FileUniqueValue   string     `json:"fileUniqueValue"`
	FileVersion       FlexString `json:"fileVersion"`
	SHA256            string     `json:"sha256"`
	ThumbURL          string     `json:"thumbUrl"`
	PreviewURL        string     `json:"previewUrl"`
	ParentDirectoryID string     `json:"parentDirectoryId"`
	SpaceType         string     `json:"spaceType"`
	IsCollected       int        `json:"isCollected"`
	SyncMark          string     `json:"syncMark"`
	SyncRootMark      string     `json:"syncRootMark"`
	UnitID            string     `json:"unitId"`
}

// ListResponse is the decrypted DATA returned by QueryAllFiles.
type ListResponse struct {
	Files      []File `json:"files"`
	SystemDirs []any  `json:"systemDirs"`
}

// DownloadURLRequest is the param for wohome/GetDownloadUrlV2.
type DownloadURLRequest struct {
	Type     string   `json:"type"`
	FidList  []string `json:"fidList"`
	ClientID string   `json:"clientId"`
}

// DownloadURLItem is a single fid -> download URL mapping.
type DownloadURLItem struct {
	Fid         string `json:"fid"`
	DownloadURL string `json:"downloadUrl"`
}

// DownloadURLResponse is the decrypted DATA returned by GetDownloadUrlV2.
type DownloadURLResponse struct {
	Type int               `json:"type"`
	List []DownloadURLItem `json:"list"`
}

// CreateDirectoryRequest is the param for wohome/CreateDirectory.
type CreateDirectoryRequest struct {
	SpaceType         string `json:"spaceType"`
	ParentDirectoryID string `json:"parentDirectoryId"`
	DirectoryName     string `json:"directoryName"`
	ClientID          string `json:"clientId"`
	FamilyID          string `json:"familyId,omitempty"`
}

// CreateDirectoryResponse is the decrypted DATA returned by CreateDirectory.
type CreateDirectoryResponse struct {
	ID string `json:"id"`
}

// DeleteFileRequest is the param for wohome/DeleteFile.
type DeleteFileRequest struct {
	SpaceType string   `json:"spaceType"`
	VipLevel  string   `json:"vipLevel"`
	DirList   []string `json:"dirList"`
	FileList  []string `json:"fileList"`
	ClientID  string   `json:"clientId"`
	FamilyID  string   `json:"familyId,omitempty"`
}

// MoveCopyRequest is the param for wohome/MoveFile and wohome/CopyFile.
type MoveCopyRequest struct {
	TargetDirID  string   `json:"targetDirId"`
	SourceType   string   `json:"sourceType"`
	TargetType   string   `json:"targetType"`
	DirList      []string `json:"dirList"`
	FileList     []string `json:"fileList"`
	Secret       bool     `json:"secret"`
	ClientID     string   `json:"clientId"`
	FamilyID     string   `json:"familyId,omitempty"`
	FromFamilyID string   `json:"fromFamilyId,omitempty"` // sent when sourceType=="1"
}

// RenameRequest is the param for wohome/RenameFileOrDirectory.
type RenameRequest struct {
	SpaceType string `json:"spaceType"`
	Type      int    `json:"type"` // 0=directory, 1=file
	FileType  string `json:"fileType"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	ClientID  string `json:"clientId"`
	FamilyID  string `json:"familyId,omitempty"`
}

// UsageRequest is the param for wohome/QueryCloudUsageInfo.
type UsageRequest struct {
	PhoneNum string `json:"phoneNum"`
	ClientID string `json:"clientId"`
}

// UsageInfo nests the quota numbers returned by QueryCloudUsageInfo.
//
// `ByteTotalSize` is a string (must be parsed with strconv.ParseInt);
// `ByteUsedSize` is numeric. The sibling `TotalSize`/`UsedSize` fields are
// not used because their unit is unspecified.
type UsageInfo struct {
	TotalSize     string `json:"totalSize"`
	UsedSize      int64  `json:"usedSize"`
	ByteUsedSize  int64  `json:"byteUsedSize"`
	ByteTotalSize string `json:"byteTotalSize"`
}

// UsageResponse is the decrypted DATA returned by QueryCloudUsageInfo.
//
// The quota fields are nested inside UsageInfo, not at the top level.
type UsageResponse struct {
	Code      string    `json:"code"`
	UsageInfo UsageInfo `json:"usageInfo"`
	VipLevel  string    `json:"vipLevel"`
}

// GetZoneInfoRequest is the param for wohome/GetZoneInfo.
type GetZoneInfoRequest struct {
	AppID string `json:"appId"`
}

// GetZoneInfoResponse is the decrypted DATA returned by GetZoneInfo.
type GetZoneInfoResponse struct {
	URL string `json:"url"`
}

// FamilyUserRequest is the param for wohome/FamilyUserCurrentEncode.
type FamilyUserRequest struct {
	ClientID string `json:"clientId"`
}

// FamilyUserResponse is the decrypted DATA returned by FamilyUserCurrentEncode.
type FamilyUserResponse struct {
	DefaultHomeID   int64  `json:"defaultHomeId"`
	Count           string `json:"count"`
	DefaultHomeName string `json:"defaultHomeName"`
	FamilyUserInfos []any  `json:"familyUserInfos"`
}

// RecycleListRequest is the param for wohome/QueryRecycleData.
type RecycleListRequest struct {
	SpaceType string `json:"spaceType"`
	VipLevel  string `json:"vipLevel"`
	PageNum   int    `json:"pageNum"`
	PageSize  int    `json:"pageSize"`
	ClientID  string `json:"clientId"`
	FamilyID  string `json:"familyId,omitempty"`
}

// RecycleItem is a single recycle-bin entry returned by QueryRecycleData.
//
// `Type` is a string here ("0"/"1"), in contrast to the numeric `type` in
// QueryAllFiles. `DeleteNo` is the handle used by DeleteRecycleData, and
// `ID`/`Fid` equal the original object id.
type RecycleItem struct {
	DeleteNo   string `json:"deleteNo"`
	DeleteTime string `json:"deleteTime"`
	Fid        string `json:"fid"`
	ID         string `json:"id"`
	FileSize   int64  `json:"fileSize"`
	FileType   string `json:"fileType"`
	Name       string `json:"name"`
	KeepDays   int    `json:"keepDays"`
	ThumbURL   string `json:"thumbUrl"`
	Type       string `json:"type"`
}

// RecycleListResponse is the decrypted DATA returned by QueryRecycleData.
//
// The top-level DATA is a JSON array, not an object.
type RecycleListResponse []RecycleItem

// DeleteRecycleRequest is the param for wohome/DeleteRecycleData.
//
// The parameter name is `deleteNos` (plural, array), supporting batch.
type DeleteRecycleRequest struct {
	DeleteNos []string `json:"deleteNos"`
	ClientID  string   `json:"clientId"`
}

// ------------------------------------------------------------ upload ------

// UploadFileInfo is the plaintext JSON encrypted into the `fileInfo`
// multipart field of upload2C, using the wohome key.
type UploadFileInfo struct {
	SpaceType    string `json:"spaceType"`
	DirectoryID  string `json:"directoryId"`
	BatchNo      string `json:"batchNo"`
	FileName     string `json:"fileName"`
	FileSize     int64  `json:"fileSize"`
	FileType     string `json:"fileType"`
	ShootingTime string `json:"shootingTime,omitempty"`
	FamilyID     string `json:"familyId,omitempty"`
}

// UploadData is the `data` object returned by upload2C on the last part.
//
// `FileVersion` decodes through FlexString because the server quotes it
// inconsistently (the spec records it as a string here but a number in
// QueryAllFiles). `WcFileID` equals the object's `id` in the listing.
type UploadData struct {
	Fid         string     `json:"fid"`
	FileVersion FlexString `json:"fileVersion"`
	WcFileID    string     `json:"wcFileId"`
}

// UploadResponse is the upload2C response envelope, which differs from the
// dispatcher envelope (`code`/`data`/`msg` instead of STATUS/RSP).
type UploadResponse struct {
	Code string     `json:"code"`
	Data UploadData `json:"data"`
	Msg  string     `json:"msg"`
}

// TotalParts returns the number of parts for an upload of size bytes,
// following the SDK's floor-division rule (last part absorbs the remainder)
// with a minimum of one part.
func TotalParts(size int64, partSize int64) int64 {
	if partSize <= 0 {
		partSize = 8 * 1024 * 1024
	}
	total := size / partSize
	if total == 0 {
		total = 1
	}
	return total
}

// PartPlan describes one part of a chunked upload.
type PartPlan struct {
	Index    int64 // 1-based
	Offset   int64 // byte offset of the part start
	PartSize int64 // bytes in this part (last part absorbs the remainder)
}

// PlanParts returns the part boundaries for an upload of size bytes.
//
// It implements the SDK's exact algorithm: totalPart = size/partSize floored
// (minimum 1), each part is partSize bytes except the last, which carries the
// full remainder. This means e.g. 20 MiB -> two parts of 8 MiB and 12 MiB,
// not a ceil-based split.
func PlanParts(size int64, partSize int64) []PartPlan {
	total := TotalParts(size, partSize)
	plans := make([]PartPlan, 0, total)
	var sent int64
	for i := int64(1); i <= total; i++ {
		n := partSize
		if i == total {
			n = size - sent
		}
		plans = append(plans, PartPlan{Index: i, Offset: sent, PartSize: n})
		sent += n
	}
	return plans
}

// ContentETag converts a raw ETag header value (quotes already stripped) into
// a content MD5, or the empty string when the ETag is an S3-style multipart
// digest (contains "-N" suffix) which is not a content hash.
func ContentETag(etag string) string {
	for i := 0; i < len(etag); i++ {
		if etag[i] == '-' {
			return ""
		}
	}
	return etag
}
