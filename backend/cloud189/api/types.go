package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// Response types for the read side of the API.
//
// The shapes here come from captures plus the live probes of 2026-10-22. Two
// details are load-bearing rather than cosmetic: ids must keep every digit, and
// a listing arrives wrapped for listFiles but flat for the recycle bin.

// FlexID is a JSON scalar kept exactly as the server wrote it.
//
// Ids are 18 digits (for example 123456789012345678), which is past the range a
// float64 represents exactly, so decoding through an interface{} silently
// rewrites the last digit or two. An id that lost precision would make Delete,
// Rename and Move address the wrong object. The server also writes some ids as
// JSON strings - commit's userFileId was seen both quoted and bare - so both
// forms are accepted. The same reason makes this the type for revision stamps:
// listFiles answers "rev":20261022163512 as a bare number while a createFolder
// answer quotes it.
type FlexID string

// UnmarshalJSON accepts a quoted string, a bare number, or null.
func (f *FlexID) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return errors.New("cloud189: empty id value")
	}
	if string(b) == "null" {
		*f = ""
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		*f = FlexID(s)
		return nil
	}
	// A number: keep its text. json.RawMessage's own bytes are already the
	// exact digits the server sent.
	*f = FlexID(b)
	return nil
}

// String returns the id as text.
func (f FlexID) String() string { return string(f) }

// Row is one entry of a directory listing, a recycle bin listing, or a copy of
// either. Fields the two listings disagree about are all optional.
type Row struct {
	ID       FlexID `json:"id"`
	Name     string `json:"name"`
	ParentID FlexID `json:"parentId"`
	// Size is only present on file rows; folder rows carry FileCount instead.
	Size      int64  `json:"size"`
	MD5       string `json:"md5"`
	FileCount int    `json:"fileCount"`
	// Rev is the server's own revision stamp, formatted yyyyMMddHHmmss. It is
	// what a change-detecting client compares, and listings return a lastRev.
	Rev        FlexID `json:"rev"`
	CreateDate string `json:"createDate"`
	LastOpTime string `json:"lastOpTime"`
	// FileCata is the server's own kind flag: 1 for a file row, 0 for a folder
	// row. It is kept because the two arrays it appears in are the primary
	// signal, and a disagreement between them is worth being able to see.
	FileCata int `json:"fileCata"`
}

// Listing is a directory or recycle bin page, flattened out of whichever
// envelope it arrived in.
type Listing struct {
	Count      int
	Files      []Row
	Folders    []Row
	LastRev    FlexID
	StatusCode json.RawMessage `json:"-"` // res_code when the response carries one
}

// listingEnvelope matches both shapes: the wrapped one listFiles answers with
// and the flat one listRecycleBinFilesV3 answers with.
type listingEnvelope struct {
	Count      int    `json:"count"`
	FileList   []Row  `json:"fileList"`
	FolderList []Row  `json:"folderList"`
	LastRev    FlexID `json:"lastRev"`
	// FileListAO wraps the same fields for the listFiles endpoints.
	FileListAO *listingEnvelope `json:"fileListAO"`
	// ResCode is present on personal responses and absent on family ones,
	// which is why judging a family listing needs HTTP status plus field
	// presence rather than this code.
	ResCode json.RawMessage `json:"res_code"`
}

// ParseListing decodes either envelope.
func ParseListing(body []byte) (*Listing, error) {
	var e listingEnvelope
	if err := json.Unmarshal(body, &e); err != nil {
		return nil, fmt.Errorf("cloud189: decoding listing: %w", err)
	}
	inner := &e
	if e.FileListAO != nil {
		inner = e.FileListAO
	}
	// The wrapped form carries count inside fileListAO while lastRev and
	// res_code sit outside it, so read each from wherever it actually is.
	l := &Listing{
		Count:      inner.Count,
		Files:      inner.FileList,
		Folders:    inner.FolderList,
		LastRev:    inner.LastRev,
		StatusCode: e.ResCode,
	}
	// Which array a row came from is the authoritative kind: the server's own
	// fileCata flag agrees with it in every capture (1 for file rows, 0 for
	// folder rows) but a row that omits the flag would otherwise decode as a
	// directory.
	for i := range l.Files {
		l.Files[i].FileCata = 1
	}
	for i := range l.Folders {
		l.Folders[i].FileCata = 0
	}
	if l.LastRev == "" {
		l.LastRev = e.LastRev
	}
	return l, nil
}

// FamilyInfo is one family drive the account belongs to.
//
// familyId arrives as a bare number, which is why its type is FlexID rather
// than string: a string field fails to decode outright, and routing it through
// a float would drop digits. Discovered live 2026-10-22 the hard way.
type FamilyInfo struct {
	FamilyID   FlexID `json:"familyId"`
	RemarkName string `json:"remarkName"`
	Type       int    `json:"type"`
	UseFlag    int    `json:"useFlag"`
	UserRole   int    `json:"userRole"`
}

// FamilyListResponse is the answer to
// family/manage/getFamilyList.action.
type FamilyListResponse struct {
	FamilyInfo []FamilyInfo `json:"familyInfoResp"`
}

// Capacity is what the account reports about the space it has.
//
// Both drives answer this, but only the personal endpoint answers res_code as
// well; the family one has no such field, so a failure there can only be seen in
// the HTTP status and in the fields that are missing.
//
// Available is the space that is FREE, and it goes negative once the drive is
// over full: -49,582,543,299 was measured on an account that had been filled
// past its quota. Anyone reading it as an unsigned or as "used" gets the
// opposite answer.
type Capacity struct {
	Available   int64  `json:"available"`
	Capacity    int64  `json:"capacity"`
	MaxFilesize int64  `json:"maxFilesize"`
	LoginName   string `json:"loginName"`
	// ResCode is present on the personal answer and absent on the family one,
	// which is why it is raw: a missing field must not read as a refusal, and a
	// present one must not be ignored.
	ResCode json.RawMessage `json:"res_code"`
}

// Privilege is one entry of the account's privilege list.
//
// Only the name and the value are read. Values are kept as text through FlexID
// because the answer mixes a bare number (transFileSize: 2147483648) with things
// that are not numbers at all, and a float64 would quietly round the big ones.
type Privilege struct {
	Name      FlexID `json:"privilegeName"`
	Value     FlexID `json:"privilegeValue"`
	ValueUnit string `json:"privilegeValueUnit"`
}

// PrivilegesResponse is the answer to getUserPrivilegesV2.action?flag=1.
//
// The 42 entries in data are one list, and the live value of the day's usage is
// not in it: it sits in extData.usedDayFlow, which is a real running total and
// changes as bytes move (measured 2026-10-22: it rose by exactly the size of a
// file that had just been uploaded).
type PrivilegesResponse struct {
	Data    []Privilege `json:"data"`
	ExtData struct {
		UsedDayFlow FlexID `json:"usedDayFlow"`
	} `json:"extData"`
	ResCode json.RawMessage `json:"res_code"`
}

// RefusedByResCode reports whether a response carried a res_code that says the
// call did not do what was asked.
//
// The field is a bare number on a listing, a string on a refusal, and missing
// altogether on the family endpoints, so absence is not a refusal while an
// unreadable value is treated as one rather than passed over in silence.
func RefusedByResCode(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}
	var code FlexID
	if err := json.Unmarshal(raw, &code); err != nil {
		return true
	}
	return code != "" && code != "0"
}

// ValueOf returns a privilege's value in its own units, and whether the account
// named it at all.
func (p *PrivilegesResponse) ValueOf(name string) (int64, bool) {
	for _, item := range p.Data {
		if item.Name.String() != name {
			continue
		}
		n, err := strconv.ParseInt(item.Value.String(), 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// UsedDayFlow returns how much of the day's transfer allowance has gone, as the
// account reports it right now.
func (p *PrivilegesResponse) UsedDayFlow() (int64, bool) {
	n, err := strconv.ParseInt(p.ExtData.UsedDayFlow.String(), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
