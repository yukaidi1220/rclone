// Package api contains the HTTP type definitions for the yun139 (中国移动云盘)
// backend.
//
// The 139 cloud drive exposes two families of APIs, both JSON-over-HTTP:
//
//   - PersonalNew (新个人云): a modern REST-ish API rooted at a per-account
//     host discovered via the route-policy endpoint. All endpoints are
//     `/file/*`, `/dynamic/file/*`, `/recyclebin/*`. Requests carry the
//     mcloud-sign header; bodies are plaintext JSON.
//
//   - Family (新家庭云): the orchestration API rooted at
//     https://yun.139.com/orchestration/familyCloud-rebuild/... Requests are
//     signed the same way (x-SvcType: 2 instead of 1) and bodies are also
//     plaintext JSON.
//
// Every request needs:
//   - Authorization: "Basic " + base64("pc:<account>:<token>|<...>")
//   - mcloud-sign: "<ts>,<randStr>,<sign>" where sign is a double-MD5 of the
//     URI-encoded, char-sorted, base64'd body plus the ts:randStr pair.
package api

import (
	"time"
)

// TimeFormat is the layout used by orchestration (family/group) timestamps:
// "20060102150405" in UTC+8 (Beijing time).
const TimeFormat = "20060102150405"

// TimeLayoutRFC3339 is the layout used by the PersonalNew API: RFC3339 with a
// numeric timezone offset, e.g. "2024-01-02T15:04:05.999+08:00".
const TimeLayoutRFC3339 = "2006-01-02T15:04:05.999-07:00"

// Beijing is the fixed UTC+8 zone used for orchestration timestamps.
var Beijing = time.FixedZone("UTC+8", 8*3600)

// ParseTime parses a 14-digit orchestration timestamp into time.Time in the
// UTC+8 zone. An empty string yields the zero time.
func ParseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.ParseInLocation(TimeFormat, s, Beijing)
}

// ParseRFC3339 parses a PersonalNew timestamp (RFC3339 with numeric offset).
func ParseRFC3339(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(TimeLayoutRFC3339, s)
}

// ------------------------------------------------------------ base -------

// BaseResp is the common envelope of the PersonalNew API. success=true means
// the request was understood; business errors set success=false and a message.
type BaseResp struct {
	Success bool   `json:"success"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Result is the nested result object of the orchestration API. resultCode
// "0" means success.
type Result struct {
	ResultCode string `json:"resultCode"`
	ResultDesc string `json:"resultDesc"`
}

// ------------------------------------------------------ route policy ------

// RoutePolicyURL is the endpoint that maps an account to its per-module hosts.
const RoutePolicyURL = "https://user-njs.yun.139.com/user/route/qryRoutePolicy"

// QueryRoutePolicyReq is the request body for the route-policy endpoint.
type QueryRoutePolicyReq struct {
	UserInfo    UserInfo `json:"userInfo"`
	ModAddrType int      `json:"modAddrType"`
}

// UserInfo identifies the account in route-policy and other requests.
type UserInfo struct {
	UserType    int    `json:"userType"`
	AccountType int    `json:"accountType"`
	AccountName string `json:"accountName"`
}

// QueryRoutePolicyResp is the response of the route-policy endpoint.
type QueryRoutePolicyResp struct {
	Success bool   `json:"success"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		RoutePolicyList []struct {
			SiteID      string `json:"siteID"`
			SiteCode    string `json:"siteCode"`
			ModName     string `json:"modName"`
			HttpURL     string `json:"httpUrl"`
			HttpsURL    string `json:"httpsUrl"`
			EnvID       string `json:"envID"`
			ExtInfo     string `json:"extInfo"`
			HashName    string `json:"hashName"`
			ModAddrType int    `json:"modAddrType"`
		} `json:"routePolicyList"`
	} `json:"data"`
}

// ---------------------------------------------------------- refresh ------

// AuthTokenRefreshURL is the SSO token-refresh endpoint.
const AuthTokenRefreshURL = "https://aas.caiyun.feixin.10086.cn:443/tellin/authTokenRefresh.do"

// RefreshTokenResp is the XML response of the token-refresh endpoint.
type RefreshTokenResp struct {
	Return      string `xml:"return"`
	Token       string `xml:"token"`
	ExpireTime  int32  `xml:"expiretime"`
	AccessToken string `xml:"accessToken"`
	Desc        string `xml:"desc"`
}

// ------------------------------------------------------- personal -------

// PersonalFileItem is one entry in the PersonalNew listing.
type PersonalFileItem struct {
	FileId     string              `json:"fileId"`
	Name       string              `json:"name"`
	Size       int64               `json:"size"`
	Type       string              `json:"type"` // "folder" or "file"
	CreatedAt  string              `json:"createdAt"`
	UpdatedAt  string              `json:"updatedAt"`
	Thumbnails []PersonalThumbnail `json:"thumbnailUrls"`
}

// PersonalThumbnail is one thumbnail URL variant.
type PersonalThumbnail struct {
	Style string `json:"style"`
	URL   string `json:"url"`
}

// PersonalListResp is the response of POST /file/list.
type PersonalListResp struct {
	BaseResp
	Data struct {
		Items          []PersonalFileItem `json:"items"`
		NextPageCursor string             `json:"nextPageCursor"`
	} `json:"data"`
}

// PersonalDownloadResp is the response of POST /file/getDownloadUrl.
type PersonalDownloadResp struct {
	BaseResp
	Data struct {
		URL       string `json:"url"`
		CDNURL    string `json:"cdnUrl"`
		CDNSwitch bool   `json:"cdnSwitch"`
		FileName  string `json:"fileName"`
	} `json:"data"`
}

// PartInfo describes one upload part in the /file/create request.
type PartInfo struct {
	PartNumber      int64            `json:"partNumber"`
	PartSize        int64            `json:"partSize"`
	ParallelHashCtx *ParallelHashCtx `json:"parallelHashCtx,omitempty"`
}

// ParallelHashCtx carries the SHA-256 midstate of the file prefix so the
// server can verify each part independently (parallel upload).
type ParallelHashCtx struct {
	H          []uint32 `json:"h"`
	PartOffset int64    `json:"partOffset"`
}

// PersonalUploadResp is the response of POST /file/create.
type PersonalUploadResp struct {
	BaseResp
	Data struct {
		FileId      string             `json:"fileId"`
		FileName    string             `json:"fileName"`
		PartInfos   []PersonalPartInfo `json:"partInfos"`
		Exist       bool               `json:"exist"`
		RapidUpload bool               `json:"rapidUpload"`
		UploadId    string             `json:"uploadId"`
	} `json:"data"`
}

// PersonalPartInfo maps a part number to its pre-signed upload URL.
type PersonalPartInfo struct {
	PartNumber int    `json:"partNumber"`
	UploadURL  string `json:"uploadUrl"`
}

// PersonalUploadURLResp is the response of POST /file/getUploadUrl.
type PersonalUploadURLResp struct {
	BaseResp
	Data struct {
		FileId    string             `json:"fileId"`
		UploadId  string             `json:"uploadId"`
		PartInfos []PersonalPartInfo `json:"partInfos"`
	} `json:"data"`
}

// ---------------------------------------------------------- family -------

// FamilyCommon is the envelope shared by every family-cloud orchestration
// request.
type FamilyCommon struct {
	CatalogType       int    `json:"catalogType"`
	CloudID           string `json:"cloudID"`
	CloudType         int    `json:"cloudType"`
	CommonAccountInfo struct {
		Account     string `json:"account"`
		AccountType int    `json:"accountType"`
	} `json:"commonAccountInfo"`
}

// QueryContentListReq is the request for the family content listing.
type QueryContentListReq struct {
	FamilyCommon
	CatalogID       string `json:"catalogID"`
	ContentSortType int    `json:"contentSortType"`
	SortDirection   int    `json:"sortDirection"`
	PageInfo        struct {
		PageNum  int `json:"pageNum"`
		PageSize int `json:"pageSize"`
	} `json:"pageInfo"`
}

// CloudCatalog is a folder in the family listing.
type CloudCatalog struct {
	CatalogID      string `json:"catalogID"`
	CatalogName    string `json:"catalogName"`
	CreateTime     string `json:"createTime"`
	LastUpdateTime string `json:"lastUpdateTime"`
}

// CloudContent is a file in the family listing.
type CloudContent struct {
	ContentID      string `json:"contentID"`
	ContentName    string `json:"contentName"`
	ContentSize    int64  `json:"contentSize"`
	CreateTime     string `json:"createTime"`
	LastUpdateTime string `json:"lastUpdateTime"`
	ThumbnailURL   string `json:"thumbnailURL"`
}

// QueryContentListResp is the response of the family content listing.
type QueryContentListResp struct {
	BaseResp
	Data struct {
		Result           Result         `json:"result"`
		Path             string         `json:"path"`
		CloudContentList []CloudContent `json:"cloudContentList"`
		CloudCatalogList []CloudCatalog `json:"cloudCatalogList"`
		TotalCount       int            `json:"totalCount"`
	} `json:"data"`
}

// FamilyDownloadReq is the request for the family download-URL endpoint.
type FamilyDownloadReq struct {
	FamilyCommon
	ContentID string `json:"contentID"`
	Path      string `json:"path"`
}

// FamilyDownloadResp is the response of the family download-URL endpoint.
type FamilyDownloadResp struct {
	BaseResp
	Data struct {
		Result      Result `json:"result"`
		DownloadURL string `json:"downloadURL"`
	} `json:"data"`
}

// FamilyCreateFolderReq is the request for creating a family folder.
type FamilyCreateFolderReq struct {
	FamilyCommon
	DocLibName string `json:"docLibName"`
	Path       string `json:"path"`
}

// FamilyCreateFolderResp is the response of the family create-folder endpoint.
type FamilyCreateFolderResp struct {
	BaseResp
	Data struct {
		Result Result `json:"result"`
	} `json:"data"`
}

// FamilyRenameReq is the request for renaming a family file or folder.
type FamilyRenameReq struct {
	FamilyCommon
	CatalogType   int    `json:"catalogType"`
	DocLibName    string `json:"docLibName"`
	DocLibraryID  string `json:"docLibraryID"`
	Path          string `json:"path"`
	ContentID     string `json:"contentID"`
	ContentName   string `json:"contentName"`
}

// FamilyRenameResp is the response of the family rename endpoint.
type FamilyRenameResp struct {
	BaseResp
	Data struct {
		Result Result `json:"result"`
	} `json:"data"`
}

// FamilyBatchOprTaskReq is the request for the family batch-operation task
// endpoint (delete / move).
type FamilyBatchOprTaskReq struct {
	FamilyCommon
	CatalogList       []string `json:"catalogList"`
	ContentList       []string `json:"contentList"`
	SourceCloudID     string   `json:"sourceCloudID"`
	SourceCatalogType int      `json:"sourceCatalogType"`
	TaskType          int      `json:"taskType"`
	Path              string   `json:"path"`
}

// FamilyBatchOprTaskResp is the response of the family batch-operation task
// endpoint.
type FamilyBatchOprTaskResp struct {
	BaseResp
	Data struct {
		Result Result `json:"result"`
		TaskID string `json:"taskID"`
	} `json:"data"`
}

// FamilyUploadCreateReq is the request for the family /dynamic/file/create
// endpoint.
type FamilyUploadCreateReq struct {
	FamilyCommon
	ContentHash          string     `json:"contentHash"`
	ContentHashAlgorithm string     `json:"contentHashAlgorithm"`
	ContentType          string     `json:"contentType"`
	ParallelUpload       bool       `json:"parallelUpload"`
	PartInfos            []PartInfo `json:"partInfos"`
	Size                 int64      `json:"size"`
	ParentFileID         string     `json:"parentFileId"`
	Name                 string     `json:"name"`
	Type                 string     `json:"type"`
	FileRenameMode       string     `json:"fileRenameMode"`
	GroupID              string     `json:"groupId"`
	GroupType            int        `json:"groupType"`
	SeqNo                string     `json:"seqNo"`
}

// FamilyUploadCreateResp mirrors PersonalUploadResp (same data shape).
type FamilyUploadCreateResp struct {
	BaseResp
	Data struct {
		FileId      string             `json:"fileId"`
		FileName    string             `json:"fileName"`
		PartInfos   []PersonalPartInfo `json:"partInfos"`
		Exist       bool               `json:"exist"`
		RapidUpload bool               `json:"rapidUpload"`
		UploadId    string             `json:"uploadId"`
	} `json:"data"`
}

// FamilyUploadURLReq is the request for fetching more part URLs
// (/dynamic/file/getUploadUrl).
type FamilyUploadURLReq struct {
	FamilyCommon
	FileId    string             `json:"fileId"`
	UploadId  string             `json:"uploadId"`
	PartInfos []PartInfo         `json:"partInfos"`
}

// FamilyUploadCompleteReq is the request for /dynamic/file/complete.
type FamilyUploadCompleteReq struct {
	FamilyCommon
	ContentHash          string `json:"contentHash"`
	ContentHashAlgorithm string `json:"contentHashAlgorithm"`
	FileId               string `json:"fileId"`
	UploadId             string `json:"uploadId"`
}

// ------------------------------------------------------------ quota ------

// DiskQuotaDetailResp is the response of the disk-quota endpoint.
type DiskQuotaDetailResp struct {
	BaseResp
	Data struct {
		FreeDiskSize int64 `json:"freeDiskSize"`
		DiskSize     int64 `json:"diskSize"`
	} `json:"data"`
}

// ------------------------------------------------------- mutations ------

// PersonalCreateFolderReq is the request for POST /hcy/file/create with
// type=folder. The official client sends exactly {name, type, parentFileId}.
type PersonalCreateFolderReq struct {
	ParentFileID string `json:"parentFileId"`
	Name         string `json:"name"`
	Type         string `json:"type"`
}

// PersonalBatchMoveReq is the request for POST /hcy/file/batchMove.
// Mirrors the official client (captured 2026-09-03):
// {fileIds, toParentFileId, userId, eventType:"move", businessType:0}.
type PersonalBatchMoveReq struct {
	FileIds        []string `json:"fileIds"`
	ToParentFileID string   `json:"toParentFileId"`
	UserID         string   `json:"userId"`
	EventType      string   `json:"eventType"`
	BusinessType   int      `json:"businessType"`
}

// PersonalBatchCopyReq is the request for POST /hcy/file/batchCopy.
// Mirrors the official client (captured 2026-09-03):
// {userId, userDomainId, fileIds, toParentFileId}.
type PersonalBatchCopyReq struct {
	FileIds        []string `json:"fileIds"`
	ToParentFileID string   `json:"toParentFileId"`
	UserID         string   `json:"userId"`
	UserDomainID   string   `json:"userDomainId"`
}

// PersonalUpdateReq is the request for POST /hcy/file/update (rename).
// The official client sends exactly {fileId, name} (captured 2026-09-03).
type PersonalUpdateReq struct {
	FileId string `json:"fileId"`
	Name   string `json:"name"`
}

// PersonalTrashReq is the request for POST /hcy/recyclebin/batchTrash.
// businessType:0 is what the official client sends.
type PersonalTrashReq struct {
	FileIds      []string `json:"fileIds"`
	BusinessType int      `json:"businessType"`
}

// FamilyModifyContentReq is the request for renaming a family file
// (photoContent/modifyContentInfo).
type FamilyModifyContentReq struct {
	FamilyCommon
	ContentID   string `json:"contentID"`
	ContentName string `json:"contentName"`
	Path        string `json:"path"`
}

// FamilyModifyDocV2Req is the request for renaming a family folder
// (modifyCloudDocV2).
type FamilyModifyDocV2Req struct {
	FamilyCommon
	CatalogType   int    `json:"catalogType"`
	DocLibName    string `json:"docLibName"`
	DocLibraryID  string `json:"docLibraryID"`
	Path          string `json:"path"`
}

// FamilyDeleteReq is the request for the family batch-delete endpoint.
type FamilyDeleteReq struct {
	FamilyCommon
	CatalogList       []string `json:"catalogList"`
	ContentList       []string `json:"contentList"`
	SourceCloudID     string   `json:"sourceCloudID"`
	SourceCatalogType int      `json:"sourceCatalogType"`
	TaskType          int      `json:"taskType"`
	Path              string   `json:"path"`
}

// IsboBatchOprTaskReq is the request for the isbo openApi batch-operation
// endpoint used by family move.
type IsboBatchOprTaskReq struct {
	CatalogList   []string `json:"catalogList"`
	ContentList   []string `json:"contentList"`
	AccountInfo   struct {
		AccountName string `json:"accountName"`
		AccountType string `json:"accountType"`
	} `json:"accountInfo"`
	DestCatalogID string `json:"destCatalogID"`
	DestGroupID   string `json:"destGroupID"`
	DestPath      string `json:"destPath"`
	DestType      int    `json:"destType"`
	SrcGroupID    string `json:"srcGroupID"`
	SrcType       int    `json:"srcType"`
	TaskType      int    `json:"taskType"`
}

// AndAlbumCopyReq is the request for the family copy endpoint
// (andAlbum/openApi/copyContentCatalog).
type AndAlbumCopyReq struct {
	CommonAccountInfo struct {
		AccountType   string `json:"accountType"`
		AccountUserId string `json:"accountUserId"`
	} `json:"commonAccountInfo"`
	DestCatalogID    string   `json:"destCatalogID"`
	DestCloudID      string   `json:"destCloudID"`
	SourceCatalogIDs []string `json:"sourceCatalogIDs"`
	SourceCloudID    string   `json:"sourceCloudID"`
	SourceContentIDs []string `json:"sourceContentIDs"`
}

// DefaultChunkSize is the default multi-part upload size used by the
// upload pipeline when no --yun139-part-size option is supplied. Matches
// the official PC client's 5 MiB part size.
const DefaultChunkSize int64 = 5 * 1024 * 1024

// CommonUpload groups the fields shared by /file/create across the file
// types (regular, folder, etc.). The hash lives in PersonalCreateReq's
// ContentHash (set explicitly so it serialises as "contentHash", not
// "sha256" or "md5", which the server reads as legacy and can reject).
type CommonUpload struct {
	ParentID string `json:"parentFileId"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Type     string `json:"type,omitempty"`
}

// PersonalCreateReq is the request for POST /hcy/file/create (regular file).
// Field names mirror what the official PC client posts (captured
// 2026-09-03, client 8.8.6.20260829): contentHash + parallelUpload:true +
// partInfos with parallelHashCtx, and localCreatedAt/localUpdatedAt.
type PersonalCreateReq struct {
	CommonUpload
	FileRenameMode       string     `json:"fileRenameMode"`
	ContentHash          string     `json:"contentHash"`
	ContentHashAlgorithm string     `json:"contentHashAlgorithm"`
	ContentType          string     `json:"contentType"`
	ParallelUpload       bool       `json:"parallelUpload"`
	PartInfos            []PartInfo `json:"partInfos"`
	LocalCreatedAt       string     `json:"localCreatedAt"`
	LocalUpdatedAt       string     `json:"localUpdatedAt"`
}

// PartUploadInfo is one part URL returned by /file/create.
type PartUploadInfo struct {
	PartNumber int    `json:"partNumber"`
	UploadURL  string `json:"uploadURL"`
}

// PersonalCreateResp is the response of POST /file/create.
type PersonalCreateResp struct {
	BaseResp
	Data struct {
		FileID      string           `json:"fileId"`
		FileName    string           `json:"fileName"`
		PartInfos   []PartUploadInfo `json:"partInfos"`
		UploadID    string           `json:"uploadId"`
		Exists      bool             `json:"exists"`
		RapidUpload bool             `json:"rapidUpload"`
	} `json:"data"`
}

// PersonalCompleteReq is the request for POST /hcy/file/complete.
// Mirrors the official client (captured 2026-09-03):
// {contentHash, contentHashAlgorithm, fileId, uploadId} - no size.
type PersonalCompleteReq struct {
	FileID               string `json:"fileId"`
	UploadID             string `json:"uploadId"`
	ContentHash          string `json:"contentHash"`
	ContentHashAlgorithm string `json:"contentHashAlgorithm"`
}

// PersonalCompleteResp is the response of POST /file/complete.
type PersonalCompleteResp struct {
	BaseResp
	Data struct {
		FileID string `json:"fileId"`
	} `json:"data"`
}