package api

import "time"

// Response codes, from the Cloudreve server's serializer package.
//
// Success is the zero value of the code field: the server builds responses
// without setting it.
const (
	CodeSuccess              = 0
	CodeNotFullySuccess      = 203
	CodeCheckLogin           = 401
	CodeNoPermission         = 403
	CodeNotFound             = 404
	CodeConflict             = 409
	CodeUploadFailed         = 40002
	CodeCreateFolderFailed   = 40003
	CodeObjectExist          = 40004
	CodeSignExpired          = 40005
	CodePolicyNotAllowed     = 40006
	CodeGroupNotAllowed      = 40007
	CodeUploadSessionExpired = 40011
	CodeInvalidChunkIndex    = 40012
	CodeInvalidContentLength = 40013
	CodeParentNotExist       = 40016
	CodeIllegalObjectName    = 40052
	CodeLockConflict         = 40073
	CodeFeatureNotEnabled    = 40019
	CodeCredentialInvalid    = 40020
	CodeCaptchaError         = 40026
	CodeCaptchaRefreshNeeded = 40027
	CodeInvalidPassword      = 40080
)

// LoginRequest is the body of POST /session/token.
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// LoginConfig is the login section of GET /site/config/login.
//
// The server omits login_captcha when it is false, so an absent key means no
// captcha is asked for.
type LoginConfig struct {
	LoginCaptcha bool `json:"login_captcha"`
}

// PrepareLogin is the answer of GET /session/prepare, which reports how an
// account can prove itself.
//
// PasswordEnabled is false for an account created through SSO or OIDC that has
// no local password, which is otherwise indistinguishable from a wrong
// password.
type PrepareLogin struct {
	PasswordEnabled bool `json:"password_enabled"`
	WebauthnEnabled bool `json:"webauthn_enabled"`
}

// Token is the JWT pair issued by the session endpoints.
//
// The expiry fields are RFC3339 timestamps here, unlike the OAuth token
// endpoint, which reports lifetimes as a number of seconds.
type Token struct {
	AccessToken    string    `json:"access_token"`
	RefreshToken   string    `json:"refresh_token"`
	AccessExpires  time.Time `json:"access_expires"`
	RefreshExpires time.Time `json:"refresh_expires"`
}

// RefreshRequest is the body of POST /session/token/refresh.
type RefreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// LoginData is the data field of a successful login.
type LoginData struct {
	User  interface{} `json:"user"`
	Token Token       `json:"token"`
}

// FileType values of FileResponse.Type.
const (
	FileTypeFile   = 0
	FileTypeFolder = 1
)

// Pagination is the cursor state of a directory listing.
type Pagination struct {
	Page       int    `json:"page"`
	PageSize   int    `json:"page_size"`
	TotalItems int    `json:"total_items"`
	NextToken  string `json:"next_token"`
	IsCursor   bool   `json:"is_cursor"`
}

// Policy describes the storage policy in force for a directory.
//
// A load_balance policy is a pool: it lists the child policies an upload can
// land on, and the client picks one and names it in the upload session. The
// server chooses for a client that does not.
type Policy struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	Type             string   `json:"type"`
	MaxSize          int64    `json:"max_size"`
	Relay            bool     `json:"relay,omitempty"`
	ChunkConcurrency int      `json:"chunk_concurrency,omitempty"`
	AllowedSuffix    []string `json:"allowed_suffix,omitempty"`
	DeniedSuffix     []string `json:"denied_suffix,omitempty"`
	Children         []Policy `json:"children,omitempty"`
}

// File is one entry of a listing or a stat response.
//
// There is no modified-time field: UpdatedAt carries it, and no MIME type is
// returned, so it must be derived from the name.
type File struct {
	Type          int               `json:"type"`
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	Size          int64             `json:"size"`
	Metadata      map[string]string `json:"metadata"`
	Path          string            `json:"path,omitempty"`
	Shared        bool              `json:"shared,omitempty"`
	Owned         bool              `json:"owned,omitempty"`
	PrimaryEntity string            `json:"primary_entity,omitempty"`
}

// ListResponse is the data field of GET /file.
type ListResponse struct {
	Files         []File      `json:"files"`
	Parent        File        `json:"parent,omitempty"`
	Pagination    *Pagination `json:"pagination"`
	ContextHint   *string     `json:"context_hint"`
	MixedType     bool        `json:"mixed_type"`
	StoragePolicy *Policy     `json:"storage_policy,omitempty"`
}

// FileURL is one signed entity URL.
type FileURL struct {
	URL string `json:"url"`
}

// FileURLData is the data field of POST /file/url when redirect is false.
//
// Expires is when the earliest URL stops working.
type FileURLData struct {
	URLs    []FileURL  `json:"urls"`
	Expires *time.Time `json:"expires,omitempty"`
}

// FileURLRequest is the body of POST /file/url.
type FileURLRequest struct {
	Uris      []string `json:"uris"`
	Download  bool     `json:"download,omitempty"`
	Redirect  bool     `json:"redirect,omitempty"`
	Entity    string   `json:"entity,omitempty"`
	SkipError bool     `json:"skip_error,omitempty"`
	NoCache   bool     `json:"no_cache,omitempty"`
}

// DirectLinkRequest is the body of PUT /file/source.
type DirectLinkRequest struct {
	Uris []string `json:"uris"`
}

// DirectLinkResponse is one entry of the PUT /file/source reply.
//
// Link is the address that opens the file from the drive and FileURL the
// content address behind it.
type DirectLinkResponse struct {
	ID      string `json:"id,omitempty"`
	Name    string `json:"name,omitempty"`
	Link    string `json:"link"`
	FileURL string `json:"file_url"`
}

// DirectLink is a stored direct link of a file.
type DirectLink struct {
	ID         string `json:"id"`
	URL        string `json:"url"`
	Downloaded int    `json:"downloaded"`
}

// FileExtended is a stat response carrying the information asked for with
// ?extended=true.
type FileExtended struct {
	File
	ExtendedInfo struct {
		StoragePolicy *Policy      `json:"storage_policy,omitempty"`
		DirectLinks   []DirectLink `json:"direct_links,omitempty"`
	} `json:"extended_info,omitempty"`
}

// CreateSessionRequest is the body of PUT /file/upload.
//
// EntityType must be "version" to write over an existing file; an empty
// EntityType there is answered with CodeObjectExist.
type CreateSessionRequest struct {
	Uri          string            `json:"uri"`
	Size         int64             `json:"size"`
	LastModified int64             `json:"last_modified,omitempty"`
	MimeType     string            `json:"mime_type,omitempty"`
	PolicyID     string            `json:"policy_id,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	EntityType   string            `json:"entity_type,omitempty"`
}

// UploadSession is the data field of PUT /file/upload.
//
// For an S3 policy, UploadURLs holds one presigned PUT URL per part and
// CompleteURL the POST that finalises the multipart upload; both are empty
// for a relayed policy, which takes POST /file/upload/{session}/{index}
// instead.
type UploadSession struct {
	SessionID      string   `json:"session_id"`
	UploadID       string   `json:"upload_id"`
	ChunkSize      int64    `json:"chunk_size"`
	Expires        int64    `json:"expires"`
	UploadURLs     []string `json:"upload_urls,omitempty"`
	Credential     string   `json:"credential,omitempty"`
	CompleteURL    string   `json:"completeURL,omitempty"`
	URI            string   `json:"uri"`
	CallbackSecret string   `json:"callback_secret"`
	MimeType       string   `json:"mime_type,omitempty"`
	UploadPolicy   string   `json:"upload_policy,omitempty"`
	StoragePolicy  *Policy  `json:"storage_policy,omitempty"`
}

// DeleteSessionRequest is the body of DELETE /file/upload.
type DeleteSessionRequest struct {
	ID  string `json:"id"`
	Uri string `json:"uri"`
}

// MetadataUploadSessionID is the entry metadata that marks a placeholder left by
// an unfinished upload. The id it names is the session holding the file's lock.
const MetadataUploadSessionID = "sys:upload_session_id"

// CurrentUser is the part of GET /user/me this backend reads: the account's own
// ID, which is what a lock holder gives as its owner.
type CurrentUser struct {
	ID string `json:"id"`
}

// LockConflict is one holder named by the data of a 40073 answer.
type LockConflict struct {
	Path  string `json:"path"`
	Token string `json:"token"`
	Owner struct {
		Owner       string `json:"owner"`
		Application struct {
			Type string `json:"type"`
		} `json:"application"`
	} `json:"owner"`
	Type int `json:"type"`
}

// UnlockRequest is the body of DELETE /file/lock, which releases lock tokens
// without needing the session behind them.
type UnlockRequest struct {
	Tokens []string `json:"tokens"`
}

// Capacity is the data field of GET /user/capacity, the account's storage.
//
// Total is the quota of the account's group and Used is what it holds, trash bin
// included. The site may also answer storage_pack_total, which rclone ignores
// because nothing says whether it is inside total.
type Capacity struct {
	Total int64 `json:"total"`
	Used  int64 `json:"used"`
}

// BindPolicyRequest is the body of PATCH /file/policy, which binds a folder to a
// storage policy instead of the one its group gives it.
//
// Both fields are required and policy_id names a policy the site knows: there is
// no value here meaning "inherit again", so undoing a binding means naming the
// policy the folder reported beforehand.
type BindPolicyRequest struct {
	Uri      string `json:"uri"`
	PolicyID string `json:"policy_id"`
}

// CreateFolderRequest is the body of POST /file/create.
type CreateFolderRequest struct {
	Uri  string `json:"uri"`
	Type string `json:"type"`
}

// RenameRequest is the body of POST /file/rename.
type RenameRequest struct {
	Uri     string `json:"uri"`
	NewName string `json:"new_name"`
}

// MoveRequest is the body of POST /file/move, which moves one entry into a
// destination folder while keeping its own name.
type MoveRequest struct {
	Uris []string `json:"uris"`
	Dst  string   `json:"dst"`
}

// DeleteRequest is the body of DELETE /file.
//
// Without SkipSoftDelete the server soft-deletes: the file leaves the listing but
// sits in the trash bin, still using quota, until the site's retention period
// collects it. With SkipSoftDelete the file goes at once and cannot be recovered.
type DeleteRequest struct {
	Uris           []string `json:"uris"`
	SkipSoftDelete bool     `json:"skip_soft_delete,omitempty"`
}
