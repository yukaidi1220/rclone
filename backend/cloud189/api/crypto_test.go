package api

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The expected values below were produced by an independent implementation (a
// Node script using its own AES/HMAC primitives) over a synthetic session
// secret, not by this package. Real session secrets from the captures never
// enter the repository.

const (
	fixtureSecret    = "0123456789ABCDEF0123456789ABCDEF"
	fixtureSessionID = "11111111-2222-3333-4444-555555555555"
	fixtureDate      = "Thu, 22 Oct 2026 17:57:41 GMT"
	fixturePlain     = "parentFolderId=-11&fileName=fixture.tar&fileMd5=0123456789ABCDEF0123456789ABCDEF" +
		"&fileSize=20914176&sliceMd5=FEDCBA9876543210FEDCBA9876543210&sliceSize=10485760"
	fixtureParamsHex = "fd97c4c491c5e59ba1cc606cdab173b4223f9992d94d9075487dc835347bc73f" +
		"2b47d57eec8446b3f7af42c25f4d89398d835d0cffd8bad585fe8b4294c42188" +
		"8d835d0cffd8bad585fe8b4294c42188" +
		"8001c889e16a4aa839c4fdb5fc373868d1c8e7c4d125aee2d7ea393142a2753d" +
		"33ff3fa4d1d1e3f3dc6f31d73482e7ee06985b98c249c3a9b31ec48d1a727c30" +
		"d2bee4bc5cf2dff7b132e3368ff4625d"
)

// md5Bytes returns the raw MD5 digest of s, the form part digests take on the
// wire.
func md5Bytes(t *testing.T, s string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(strings.ToLower(MD5Upper([]byte(s))))
	require.NoError(t, err)
	require.Len(t, raw, 16)
	return raw
}

func TestUnitSignatureVectors(t *testing.T) {
	cases := []struct {
		name    string
		operate string
		uri     string
		params  string
		want    string
	}{
		{
			// A POST carries params in the form body; they still sign.
			name:    "postWithParams",
			operate: "POST",
			uri:     "/person/initMultiUpload",
			params:  fixtureParamsHex,
			want:    "62f9985dd78be19516b3ca273e5a5c1ab1a17de1",
		},
		{
			// A GET with params in the query signs the same way.
			name:    "getWithParams",
			operate: "GET",
			uri:     "/person/getMultiUploadUrls",
			params:  fixtureParamsHex,
			want:    "7e6990575cd06a43238f0f2e0f4503bb9f6f380d",
		},
		{
			// A plain query-parameter request has no params clause at all.
			name:    "getWithoutParams",
			operate: "GET",
			uri:     "/listFiles.action",
			params:  "",
			want:    "3dd0bc869b3d7081ebbe1dd5e27038b50d2e07ca",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, Sign(fixtureSecret, fixtureSessionID, tc.operate, tc.uri, fixtureDate, tc.params))
		})
	}
}

func TestUnitSignatureMessageShape(t *testing.T) {
	// The assembled message is where the query-free RequestURI and the
	// conditional params clause are decided, so pin the string itself.
	got := SignatureMessage(fixtureSessionID, "POST", "/person/initMultiUpload", fixtureDate, "abc123")
	assert.Equal(t, "SessionKey=11111111-2222-3333-4444-555555555555&Operate=POST"+
		"&RequestURI=/person/initMultiUpload&Date=Thu, 22 Oct 2026 17:57:41 GMT&params=abc123", got)

	// Without params the clause is absent, not empty: a trailing "&params="
	// would sign a different string.
	noParams := SignatureMessage(fixtureSessionID, "GET", "/listFiles.action", fixtureDate, "")
	assert.Equal(t, "SessionKey=11111111-2222-3333-4444-555555555555&Operate=GET"+
		"&RequestURI=/listFiles.action&Date=Thu, 22 Oct 2026 17:57:41 GMT", noParams)
	assert.NotContains(t, noParams, "&params=")
}

func TestUnitEncryptParams(t *testing.T) {
	got, err := EncryptParams(fixtureSecret, fixturePlain)
	require.NoError(t, err)
	assert.Equal(t, fixtureParamsHex, got)

	back, err := DecryptParams(fixtureSecret, fixtureParamsHex)
	require.NoError(t, err)
	assert.Equal(t, fixturePlain, back)

	// A short secret cannot yield a key at all and must be reported rather
	// than panicking inside the AES constructor.
	_, err = EncryptParams("short", fixturePlain)
	assert.ErrorIs(t, err, ErrShortSecret)
	_, err = DecryptParams("short", fixtureParamsHex)
	assert.ErrorIs(t, err, ErrShortSecret)
}

func TestUnitDecryptParamsRejectsMalformedCiphertext(t *testing.T) {
	cases := []struct {
		name       string
		ciphertext string
	}{
		{"notHex", "zzzz"},
		{"empty", ""},
		{"partialBlock", "00112233445566778899aabb"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecryptParams(fixtureSecret, tc.ciphertext)
			assert.Error(t, err)
		})
	}
}

func TestUnitPKCS7UnPadRejects(t *testing.T) {
	blockSize := 16
	cases := []struct {
		name  string
		input []byte
	}{
		{"empty", []byte{}},
		{"padByteZero", bytes16(0x00)},
		{"padLargerThanBlock", bytes16(0x11)},
		{"nonUniformPad", append(bytesN(15, 0x05), 0x05)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := pkcs7UnPad(tc.input, blockSize)
			assert.Error(t, err)
		})
	}
}

func TestUnitPartDigests(t *testing.T) {
	hello := md5Bytes(t, "hello")
	world := md5Bytes(t, "world")
	// MD5Raw and the hex round trip must agree, or partInfo would carry a
	// digest that is not the one the file hashes to.
	assert.Equal(t, hello, MD5Raw([]byte("hello")))
	require.Equal(t, "5D41402ABC4B2A76B9719D911017C592", strings.ToUpper(hex.EncodeToString(hello)))
	require.Equal(t, "7D793037A0760186574B0282F2F435E7", strings.ToUpper(hex.EncodeToString(world)))

	// sliceMd5 for more than one part is the MD5 of the upper-case hex part
	// digests joined by "\n". This is the definition a capture reproduces;
	// every other candidate (binary concat, lower-case hex, "\r\n", ",", ";", a
	// single digest, the S3 "hex-N" multipart ETag) was measured against the
	// same capture and failed.
	assert.Equal(t, "DE0B449E96B0190C806DDF9E5AA1D462", SliceAggMD5([][]byte{hello, world}, ""))

	// sliceMd5 for exactly one part is the whole-file digest itself. The
	// server refuses anything else with SliceMd5DoesNotMatch, so this is not a
	// convenience branch: getting it wrong fails the upload.
	assert.Equal(t, "C5CF04A94E4B397F2DD215A56D2E5BB2", SliceAggMD5([][]byte{hello}, "C5CF04A94E4B397F2DD215A56D2E5BB2"))

	// partInfo numbers parts from 1 and base64-encodes the raw digests.
	assert.Equal(t, "1-XUFAKrxLKna5cZ2REBfFkg==,2-fXkwN6B2AYZXSwKC8vQ15w==", PartInfo([][]byte{hello, world}))

	assert.Equal(t, "", SliceAggMD5(nil, "C5CF04A94E4B397F2DD215A56D2E5BB2"))
	assert.Empty(t, PartInfo(nil))
}

func TestUnitInitParams(t *testing.T) {
	// The client writes parentFolderId first, not alphabetically, and this is
	// the exact plaintext the AES vector above was made from.
	got := EncodeParams(InitParams("-11", "fixture.tar",
		"0123456789ABCDEF0123456789ABCDEF", 20914176,
		"FEDCBA9876543210FEDCBA9876543210", 10485760))
	assert.Equal(t, fixturePlain, got)
}

// TestUnitInitParamsEscapesName is the guard on the one field a user controls.
//
// Sent raw, an ampersand in a name is read back as the separator between two
// params, and the file lands named only up to that character with no error
// anywhere. Measured 2026-10-22 with a real upload: "probe-a&b.bin" was stored
// as "probe-a".
func TestUnitInitParamsEscapesName(t *testing.T) {
	got := EncodeParams(InitParams("-11", "a&b=c.txt",
		"0123456789ABCDEF0123456789ABCDEF", 4096,
		"0123456789ABCDEF0123456789ABCDEF", 10485760))
	assert.Equal(t,
		"parentFolderId=-11&fileName=a%26b%3Dc.txt&fileMd5=0123456789ABCDEF0123456789ABCDEF"+
			"&fileSize=4096&sliceMd5=0123456789ABCDEF0123456789ABCDEF&sliceSize=10485760", got)

	// The name must survive as one field: three separators, not five.
	assert.Equal(t, 6, strings.Count(got, "="), "every param is one k=v pair")

	// The escape is the whole percent-form, not only the grammar characters.
	// This is what was measured: a name uploaded with url.QueryEscape was
	// stored intact for all 54 cases the character table probed, parentheses
	// included, because the server percent-decodes the value it reads. The
	// capture's raw parentheses are the same name from the other side: a client
	// that only escapes what needs escaping.
	captured := EncodeParams(InitParams("-11", "setup(20260610104409).exe",
		"0123456789ABCDEF0123456789ABCDEF", 4096,
		"0123456789ABCDEF0123456789ABCDEF", 10485760))
	assert.Contains(t, captured, "fileName=setup%2820260610104409%29.exe&")

	// A name that needs no escaping keeps its exact captured spelling, which is
	// what the AES plaintext vector above pins.
	assert.NotContains(t, fixturePlain, "%")
}

func TestUnitParseRequestHeader(t *testing.T) {
	// Two values legitimately contain "=": a base64 padding tail and
	// x-amz-limit's "rate=12800". Splitting on anything but the first "="
	// truncates one and mangles the other.
	const raw = "Content-Type=application/octet-stream" +
		"&Authorization=AWS fixtureaccesskeyid00:FIXTURESIGATUREVALUE=" +
		"&Content-MD5=+UnYOYsFkQIppM/TNrpmCw==" +
		"&Date=Thu, 22 Oct 2026 08:45:21 GMT" +
		"&x-amz-limit=rate=12800"
	got := ParseRequestHeader(raw)
	assert.Equal(t, map[string]string{
		"Content-Type":  "application/octet-stream",
		"Authorization": "AWS fixtureaccesskeyid00:FIXTURESIGATUREVALUE=",
		"Content-MD5":   "+UnYOYsFkQIppM/TNrpmCw==",
		"Date":          "Thu, 22 Oct 2026 08:45:21 GMT",
		"x-amz-limit":   "rate=12800",
	}, got)
	assert.Equal(t, []string{"Authorization", "Content-MD5", "Content-Type", "Date", "x-amz-limit"}, SortHeaders(got))

	// A pair with no "=" at all is dropped rather than becoming an empty name.
	assert.Equal(t, map[string]string{"a": "b"}, ParseRequestHeader("a=b&junk"))
}

func TestUnitPlanParts(t *testing.T) {
	// The capture: a 21,059,100-byte file at the client's 10 MiB sliceSize
	// becomes two full parts and an 87,580-byte tail.
	plans := PlanParts(21059100, 10485760)
	require.Len(t, plans, 3)
	assert.Equal(t, []PartPlan{{1, 10485760}, {2, 10485760}, {3, 87580}}, plans)

	// Two whole slices of data produce two parts, not three.
	assert.Equal(t, []PartPlan{{1, 10485760}, {2, 10428416}}, PlanParts(20914176, 10485760))

	assert.Equal(t, []PartPlan{{1, 10}}, PlanParts(10, 10485760))
	assert.Empty(t, PlanParts(0, 10485760))
	assert.Nil(t, PlanParts(100, 0), "a zero sliceSize must not spin")
}

// bytes16 returns a 16-byte block whose last byte is b.
func bytes16(b byte) []byte {
	return bytesN(16, b)
}

// bytesN returns n bytes, the last one set to b and the rest zeroed.
func bytesN(n int, b byte) []byte {
	out := make([]byte, n)
	out[n-1] = b
	return out
}
