package api

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnitFlexIDKeepsEveryDigit(t *testing.T) {
	// 18-digit ids sit past the range a float64 represents exactly. A decode
	// that routes them through interface{} rewrites the tail, and a rewritten
	// id addresses the wrong object on Delete, Rename and Move.
	var control struct {
		ID float64 `json:"id"`
	}
	require.NoError(t, json.Unmarshal([]byte(`{"id":123456789012345678}`), &control))
	assert.NotEqual(t, "123456789012345678", strconv.FormatFloat(control.ID, 'f', 0, 64),
		"the float64 route is expected to corrupt the tail; if it stops corrupting, this test's premise changed")

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"bare number", `{"id":123456789012345678}`, "123456789012345678"},
		{"quoted string", `{"id":"900000000000000011"}`, "900000000000000011"},
		{"small number", `{"id":0}`, "0"},
		{"negative root alias", `{"id":-11}`, "-11"},
		{"null", `{"id":null}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var v struct {
				ID FlexID `json:"id"`
			}
			require.NoError(t, json.Unmarshal([]byte(tc.input), &v))
			assert.Equal(t, tc.want, v.ID.String())
		})
	}
}

func TestUnitParseListingWrapped(t *testing.T) {
	// The personal shape: res_code outside, count and the two arrays inside
	// fileListAO, lastRev outside.
	body := `{"res_code":0,"res_message":"ok","fileListAO":{"count":3,` +
		`"fileList":[{"id":123456789012345678,"name":"a.bin","size":4096,"md5":"ABC","fileCata":1,"parentId":-11,"rev":"20261022201529"}],` +
		`"fileListSize":1,` +
		`"folderList":[{"id":223456789012345678,"name":"dir","fileCata":0,"fileCount":7,"parentId":-11}],` +
		`"folderListSize":1},"lastRev":"20261022201530"}`
	l, err := ParseListing([]byte(body))
	require.NoError(t, err)
	assert.Equal(t, 3, l.Count)
	require.Len(t, l.Files, 1)
	require.Len(t, l.Folders, 1)
	assert.Equal(t, "123456789012345678", l.Files[0].ID.String())
	assert.Equal(t, int64(4096), l.Files[0].Size)
	assert.Equal(t, "-11", l.Files[0].ParentID.String())
	assert.Equal(t, "20261022201529", l.Files[0].Rev.String())
	assert.Equal(t, "223456789012345678", l.Folders[0].ID.String())
	assert.Equal(t, 7, l.Folders[0].FileCount)
	assert.Equal(t, "20261022201530", l.LastRev.String())
	assert.JSONEq(t, "0", string(l.StatusCode))
}

func TestUnitParseListingFamilyHasNoResCode(t *testing.T) {
	// The family endpoints answer without res_code at all, so a family listing
	// cannot be judged by a status field - only HTTP status and the absence of
	// fileListAO mean anything here.
	body := `{"fileListAO":{"count":1,"fileList":[{"id":900000000000000011,"name":"clip-01.mp4","size":1234567890,"md5":"553E","parentId":900000000000000009}],"folderList":[]},"lastRev":20261022191923}`
	l, err := ParseListing([]byte(body))
	require.NoError(t, err)
	assert.Equal(t, 1, l.Count)
	require.Len(t, l.Files, 1)
	assert.Equal(t, "900000000000000011", l.Files[0].ID.String())
	assert.Empty(t, l.StatusCode, "family responses carry no res_code field")
	assert.Equal(t, "20261022191923", l.LastRev.String())
}

func TestUnitParseListingRecycleIsFlat(t *testing.T) {
	// The recycle bin answers flat: count and both arrays sit at the top level,
	// with no fileListAO wrapper. One parser has to serve both shapes.
	body := `{"res_code":0,"res_message":"ok","count":2,"familyId":900000001,` +
		`"fileList":[{"id":900000000000000013,"name":"probe-child","pathStr":"personal/probe-a"}],` +
		`"fileListSize":1,` +
		`"folderList":[{"id":900000000000000012,"name":"probe-a","pathStr":"personal"}],"folderListSize":1}`
	l, err := ParseListing([]byte(body))
	require.NoError(t, err)
	assert.Equal(t, 2, l.Count)
	require.Len(t, l.Files, 1)
	require.Len(t, l.Folders, 1)
	assert.Equal(t, "900000000000000012", l.Folders[0].ID.String())
	assert.Empty(t, l.LastRev)
}

func TestUnitFamilyListResponseKeepsIDText(t *testing.T) {
	// familyId arrives as a bare number. A string field fails to decode it
	// outright - which is exactly how this was found on a live account - and a
	// float would silently rewrite the tail.
	var resp FamilyListResponse
	require.NoError(t, json.Unmarshal([]byte(`{"familyInfoResp":[`+
		`{"familyId":1234567890123456,"remarkName":"fixture","type":1,"useFlag":1,"userRole":1}]}`), &resp))
	require.Len(t, resp.FamilyInfo, 1)
	assert.Equal(t, "1234567890123456", resp.FamilyInfo[0].FamilyID.String())
	assert.Equal(t, "fixture", resp.FamilyInfo[0].RemarkName)
	assert.Equal(t, 1, resp.FamilyInfo[0].UseFlag)
}

func TestUnitParseListingRejectsNonJSON(t *testing.T) {
	_, err := ParseListing([]byte("<html>gate error</html>"))
	assert.ErrorContains(t, err, "decoding listing")
}

// TestUnitPrivilegesResponsePinsTheCapturedShape is written from the answer
// §13 of the capture notes records, field for field. It exists because the
// parsing above and the reading of it would otherwise agree with each other and
// nothing else: a shape invented here would still pass.
func TestUnitPrivilegesResponsePinsTheCapturedShape(t *testing.T) {
	const captured = `{"res_code":0,"res_message":"成功","data":[` +
		`{"caculateType":0,"clientTypeId":"DEFAULTTYPE","ext2":"","privilegeName":"transFileSize","privilegeType":1,"privilegeValue":2147483648,"privilegeValueUnit":"B"},` +
		`{"caculateType":0,"clientTypeId":"DEFAULTTYPE","ext2":"","privilegeName":"transDayFlow","privilegeType":1,"privilegeValue":2147483648,"privilegeValueUnit":"B"},` +
		`{"caculateType":0,"clientTypeId":"DEFAULTTYPE","ext2":"","privilegeName":"transSpeed","privilegeType":1,"privilegeValue":12800,"privilegeValueUnit":"KB/s"},` +
		`{"caculateType":0,"clientTypeId":"DEFAULTTYPE","ext2":"","privilegeName":"recycleDay","privilegeType":1,"privilegeValue":10,"privilegeValueUnit":"D"}` +
		`],"extData":{"msgTaskStatus":-1,"picEditTimes":0,"usedDayFlow":1455497820}}`

	var resp PrivilegesResponse
	require.NoError(t, json.Unmarshal([]byte(captured), &resp))
	assert.Len(t, resp.Data, 4)

	limit, ok := resp.ValueOf("transDayFlow")
	require.True(t, ok)
	assert.EqualValues(t, 2147483648, limit, "2GiB, the value the ordinary account reported")

	fileSize, ok := resp.ValueOf("transFileSize")
	require.True(t, ok)
	assert.EqualValues(t, 2147483648, fileSize)

	used, ok := resp.UsedDayFlow()
	require.True(t, ok)
	assert.EqualValues(t, 1455497820, used, "1.36GB used of the 2GB day")

	_, ok = resp.ValueOf("transFileSizeThatDoesNotExist")
	assert.False(t, ok, "a name the account did not list is not a zero")

	// A value the account wrote as text has to read the same, because the field
	// is a string on some privileges and the upgrade path is not observable
	// from here.
	var quoted PrivilegesResponse
	require.NoError(t, json.Unmarshal([]byte(`{"data":[{"privilegeName":"transDayFlow","privilegeValue":"9223372036854775807"}]}`), &quoted))
	big, ok := quoted.ValueOf("transDayFlow")
	require.True(t, ok)
	assert.EqualValues(t, int64(9223372036854775807), big, "int64 max on an upgraded account, without rounding")

	var notANumber PrivilegesResponse
	require.NoError(t, json.Unmarshal([]byte(`{"data":[{"privilegeName":"transDayFlow","privilegeValue":"unlimited"}]}`), &notANumber))
	_, ok = notANumber.ValueOf("transDayFlow")
	assert.False(t, ok, "a value that is not a number is not read as zero")
}

// TestUnitCapacityReadsAvailableAsFreeSpace pins the reading that was once got
// wrong: available is what is left, and it is negative on a drive past full.
func TestUnitCapacityReadsAvailableAsFreeSpace(t *testing.T) {
	var personal Capacity
	require.NoError(t, json.Unmarshal([]byte(`{"res_code":0,"res_message":"成功","available":31415592476,`+
		`"capacity":32212254720,"maxFilesize":4294967296,"loginName":"fixture"}`), &personal))
	assert.EqualValues(t, 32212254720, personal.Capacity)
	assert.EqualValues(t, 31415592476, personal.Available)
	assert.False(t, RefusedByResCode(personal.ResCode))

	var full Capacity
	require.NoError(t, json.Unmarshal([]byte(`{"res_code":0,"available":-49582543299,"capacity":32212254720}`), &full))
	assert.EqualValues(t, -49582543299, full.Available, "the negative number is the drive saying it is past full")

	// The family endpoint answers with no res_code at all.
	var family Capacity
	require.NoError(t, json.Unmarshal([]byte(`{"available":30968443148,"capacity":32212254720,"maxFilesize":4294967296}`), &family))
	assert.EqualValues(t, 30968443148, family.Available)
	assert.False(t, RefusedByResCode(family.ResCode), "an absent res_code is the family shape, not a refusal")
}

func TestUnitRefusedByResCode(t *testing.T) {
	// The literal table, in both forms the field arrives in.
	cases := []struct {
		raw    string
		refuse bool
	}{{``, false}, {`0`, false}, {`"0"`, false}, {`1`, true}, {`"InvalidArgument"`, true}, {`null`, false}, {`{`, true}}
	for _, tc := range cases {
		got := RefusedByResCode(json.RawMessage(tc.raw))
		assert.Equalf(t, tc.refuse, got, "res_code %s", tc.raw)
	}
}
