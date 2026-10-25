// Package api contains the request signing scheme, the encrypted parameter
// form and the HTTP type definitions for the China Telecom cloud drive
// (天翼云盘, "cloud189") backend.
//
// Everything here is pinned to captures of the official PC client ecloud
// 7.2.4.0 taken 2026-10-22. The captures are the source of truth for the wire
// format; nothing is inferred from a third-party driver.
package api

import (
	"bytes"
	"crypto/aes"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// AESKeyLen is the number of leading characters of the session secret used as
// the AES-128 key.
//
// The key is the first 16 ASCII characters of the secret, not the secret
// hex-decoded: a captured initMultiUpload body only reproduces byte-for-byte
// under the ASCII reading. Measured 2026-10-22.
const AESKeyLen = 16

// ErrShortSecret reports a session secret too short to derive an AES key from.
// It can never become usable, so callers must treat it as a configuration
// error rather than a transient one.
var ErrShortSecret = errors.New("cloud189: session secret shorter than 16 characters")

// SignatureMessage builds the string that gets HMAC-SHA1'd into the signature
// header.
//
// RequestURI carries the path only, never the query, even though the query is
// what the request is about. Operate is the upper-case HTTP method. The Date
// component must be the exact bytes of the date header, not a re-formatted
// timestamp: the server signs what it received.
//
// The params clause is appended last, and only when the request really carries
// an encrypted params field - for a POST the form body value, for a GET the
// query value. Both were verified byte-for-byte against captured signatures on
// 2026-10-22, along with 97 requests that carry no params clause at all.
func SignatureMessage(sessionKey, operate, requestURI, date, params string) string {
	var sb strings.Builder
	sb.WriteString("SessionKey=")
	sb.WriteString(sessionKey)
	sb.WriteString("&Operate=")
	sb.WriteString(operate)
	sb.WriteString("&RequestURI=")
	sb.WriteString(requestURI)
	sb.WriteString("&Date=")
	sb.WriteString(date)
	if params != "" {
		sb.WriteString("&params=")
		sb.WriteString(params)
	}
	return sb.String()
}

// Sign returns the lower-case hex HMAC-SHA1 of the signature message, keyed
// with the whole session secret.
//
// The client sends lower-case hex. Upper-case is accepted by the server as
// well (verified 2026-10-22 by re-computing a captured signature both ways),
// but rclone follows the capture.
func Sign(secret, sessionKey, operate, requestURI, date, params string) string {
	m := hmac.New(sha1.New, []byte(secret))
	m.Write([]byte(SignatureMessage(sessionKey, operate, requestURI, date, params)))
	return hex.EncodeToString(m.Sum(nil))
}

// EncryptParams encrypts an encoded parameter string as the client sends it:
// AES-128-ECB, PKCS7 padded, lower-case hex out.
//
// ECB is not ours to choose - it is what the server decrypts, and it is why
// equal plaintext blocks show up as equal ciphertext blocks in the capture.
// That repetition leaks structure, and it is also what let the plaintext field
// order be inferred before any key was available.
func EncryptParams(secret, plaintext string) (string, error) {
	if len(secret) < AESKeyLen {
		return "", ErrShortSecret
	}
	block, err := aes.NewCipher(aesKey(secret))
	if err != nil {
		return "", err
	}
	padded := pkcs7Pad([]byte(plaintext), block.BlockSize())
	if len(padded)%block.BlockSize() != 0 {
		return "", fmt.Errorf("cloud189: padded params length %d is not a multiple of the block size", len(padded))
	}
	out := make([]byte, len(padded))
	for i := 0; i < len(padded); i += block.BlockSize() {
		block.Encrypt(out[i:i+block.BlockSize()], padded[i:i+block.BlockSize()])
	}
	return hex.EncodeToString(out), nil
}

// DecryptParams reverses EncryptParams. It exists so a captured request can be
// read back with a session secret in hand, which is how the plaintext field
// sets in this backend were established; the backend itself never needs it.
func DecryptParams(secret, ciphertext string) (string, error) {
	if len(secret) < AESKeyLen {
		return "", ErrShortSecret
	}
	raw, err := hex.DecodeString(ciphertext)
	if err != nil {
		return "", fmt.Errorf("cloud189: params is not hex: %w", err)
	}
	block, err := aes.NewCipher(aesKey(secret))
	if err != nil {
		return "", err
	}
	if len(raw) == 0 || len(raw)%block.BlockSize() != 0 {
		return "", fmt.Errorf("cloud189: params ciphertext length %d is not a positive multiple of the block size", len(raw))
	}
	out := make([]byte, len(raw))
	for i := 0; i < len(raw); i += block.BlockSize() {
		block.Decrypt(out[i:i+block.BlockSize()], raw[i:i+block.BlockSize()])
	}
	plain, err := pkcs7UnPad(out, block.BlockSize())
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

// aesKey returns the AES-128 key material: the first AESKeyLen characters of
// the secret as ASCII bytes. Callers must have checked ErrShortSecret.
func aesKey(secret string) []byte {
	return []byte(secret[:AESKeyLen])
}

// pkcs7Pad appends PKCS7 padding so b is a multiple of blockSize.
func pkcs7Pad(b []byte, blockSize int) []byte {
	pad := blockSize - len(b)%blockSize
	return append(b, bytes.Repeat([]byte{byte(pad)}, pad)...)
}

// pkcs7UnPad strips PKCS7 padding, rejecting a pad that is out of range or
// not uniformly its own value - which is what a wrong key produces.
func pkcs7UnPad(b []byte, blockSize int) ([]byte, error) {
	if len(b) == 0 {
		return nil, errors.New("cloud189: plaintext is empty")
	}
	pad := int(b[len(b)-1])
	if pad == 0 || pad > blockSize {
		return nil, fmt.Errorf("cloud189: invalid PKCS7 pad byte %d", pad)
	}
	if pad > len(b) {
		return nil, fmt.Errorf("cloud189: PKCS7 pad %d exceeds the %d bytes on hand", pad, len(b))
	}
	if !bytes.Equal(b[len(b)-pad:], bytes.Repeat([]byte{byte(pad)}, pad)) {
		return nil, errors.New("cloud189: invalid PKCS7 padding")
	}
	return b[:len(b)-pad], nil
}

// Param is one encrypted parameter. The client does not sort them: it writes
// parentFolderId or uploadFileId first, so order is carried explicitly rather
// than recovered by sorting a map.
type Param struct {
	Key   string
	Value string
}

// EncodeParams joins params as the client encrypts them: "k=v&k=v".
//
// Values arrive already in the form the server expects: the capture shows a
// fileName with its parentheses intact, because those characters never needed
// escaping, while the ones that do are escaped by the caller that owns the
// field (InitParams for fileName). See that function for the measurement.
func EncodeParams(params []Param) string {
	parts := make([]string, 0, len(params))
	for _, p := range params {
		parts = append(parts, p.Key+"="+p.Value)
	}
	return strings.Join(parts, "&")
}

// InitParams returns the six fields a capture-verified initMultiUpload carries,
// in the order the client writes them.
//
// fileMd5 and sliceMd5 are both present even for a chunked upload that will
// miss the rapid-transfer check: the client hashes the whole file before it
// asks to upload anything, which is what makes fileDataExists answerable in
// the first response. Captured 2026-10-22.
//
// fileName is URL escaped here, and that is not decoration: sent raw, an ampersand
// in a name is read as the separator between two params and the file lands named
// only up to that character. Measured 2026-10-22 with a real upload, where
// "probe-a&b.bin" was stored as "probe-a" - silently, with no error anywhere in
// the response. Escaped, the same name and every other character the table probed
// (& = % # + , ; @ ( ) [ ] { } $ ! ^) is stored intact, because the server
// percent-decodes the value it reads.
func InitParams(parentFolderID, fileName, fileMd5Hex string, fileSize int64, sliceMd5Hex string, sliceSize int64) []Param {
	return []Param{
		{"parentFolderId", parentFolderID},
		{"fileName", url.QueryEscape(fileName)},
		{"fileMd5", fileMd5Hex},
		{"fileSize", fmt.Sprintf("%d", fileSize)},
		{"sliceMd5", sliceMd5Hex},
		{"sliceSize", fmt.Sprintf("%d", sliceSize)},
	}
}

// DecodeFields is the value of the decodefields request header, which names
// every plaintext field the server may find inside the encrypted params.
//
// It accompanies the POSTs (initMultiUpload, commitMultiUploadFile) as sent by
// the client; it is not part of the signature. Captured 2026-10-22.
const DecodeFields = "familyId,parentFolderId,fileName,fileMd5,fileSize,sliceMd5,sliceSize,albumId,extend,lazyCheck,isLog"

// MD5Raw returns the raw MD5 digest of b, the form a part digest takes inside
// partInfo and inside the Content-MD5 the server signs into the storage PUT.
func MD5Raw(b []byte) []byte {
	sum := md5.Sum(b)
	return sum[:]
}

// MD5Upper returns the upper-case hex MD5 of b, the form every MD5 field in
// this protocol uses: fileMd5 in the encrypted params, md5 in a listing, and
// fileMd5 in an upload response.
func MD5Upper(b []byte) string {
	sum := md5.Sum(b)
	return strings.ToUpper(hex.EncodeToString(sum[:]))
}

// SliceAggMD5 returns the sliceMd5 for a file whose part digests are
// partDigests, given the whole-file digest fileMd5Hex.
//
// Two rules, both measured against the live server:
//
//   - A file that fits in one part sends fileMd5Hex as sliceMd5. The server
//     says this out loud when it is violated: a mismatch is refused with
//     HTTP 403 and code SliceMd5DoesNotMatch, whose message reads "fileSize
//     <= 10MB, fileMD5 should equal sliceMd5". Measured 2026-10-22 on a
//     1,331,200-byte file, where the equal value was accepted and the digested
//     value was refused.
//   - A file with more than one part sends the upper-case hex MD5 over the
//     upper-case hex digests joined by a single "\n". Verified 2026-10-22
//     against a captured initMultiUpload whose part digests came from the same
//     session's getMultiUploadUrls plaintext; the seven other candidate
//     definitions tested alongside it - binary concatenation, lower-case hex,
//     "\r\n", "," and ";" separators, the first digest alone, and the S3
//     "hex-N" multipart ETag - all failed.
//
// The server words its threshold as "10MB", which does not settle whether it
// means 10,000,000 or 10,485,760 bytes. Keying on the part count instead of
// that constant leaves only files between those two sizes in question, and a
// wrong guess there surfaces as a refused upload rather than as damaged data.
//
// A file with no parts returns the empty string, because an empty digest list
// is a caller bug and the MD5 of nothing is a plausible-looking value the
// server would happily hash against.
func SliceAggMD5(partDigests [][]byte, fileMd5Hex string) string {
	switch len(partDigests) {
	case 0:
		return ""
	case 1:
		return fileMd5Hex
	}
	hexes := make([]string, 0, len(partDigests))
	for _, d := range partDigests {
		hexes = append(hexes, strings.ToUpper(hex.EncodeToString(d)))
	}
	return MD5Upper([]byte(strings.Join(hexes, "\n")))
}

// PartInfo encodes the per-part digests for getMultiUploadUrls:
// "1-<base64(md5)>,2-<base64(md5)>" with the part numbers one-based and
// ascending.
//
// The server turns each of these base64 digests into the Content-MD5 of the
// presigned PUT it hands back for that part, so a part's digest has to be known
// before its URL exists. Captured 2026-10-22.
func PartInfo(partDigests [][]byte) string {
	return PartInfoFrom(partDigests, 1)
}

// PartInfoFrom is PartInfo for a page of a file's parts, starting at first.
//
// The number in each entry is the part the URL will be for, not the entry's
// position in this list: asking for "21-<digest>,22-<digest>" gets URLs whose
// partNumber is 21 and 22, while asking for the same digests as "1-<digest>,..."
// gets URLs for parts 1 and 2. Measured 2026-10-25 against the family space,
// which is what makes a page of a large file addressable at all.
func PartInfoFrom(partDigests [][]byte, first int) string {
	parts := make([]string, 0, len(partDigests))
	for i, d := range partDigests {
		parts = append(parts, fmt.Sprintf("%d-%s", first+i, base64.StdEncoding.EncodeToString(d)))
	}
	return strings.Join(parts, ",")
}

// ParseRequestHeader splits the requestHeader string the server returns for a
// part into headers to replay on the PUT.
//
// Splitting is on the first "=" of each "&"-separated pair and nothing else,
// because two of the values legitimately contain "=" themselves:
// "x-amz-limit=rate=12800" and a Content-MD5 or Authorization ending in "==".
// Splitting on every "=", or on the last one, mangles both. Verified
// 2026-10-22 against the exact strings a capture put in that field.
func ParseRequestHeader(s string) map[string]string {
	h := make(map[string]string)
	for _, kv := range strings.Split(s, "&") {
		i := strings.Index(kv, "=")
		if i < 0 {
			continue
		}
		h[kv[:i]] = kv[i+1:]
	}
	return h
}

// PartPlan is one part of a multipart upload.
type PartPlan struct {
	Index int   // one-based, as the server numbers parts
	Size  int64 // bytes in this part
}

// PlanParts splits size into parts of sliceSize bytes.
//
// The client uses a fixed 10 MiB sliceSize and lets the last part carry the
// remainder, so a 21,059,100-byte file becomes 10485760 + 10485760 + 87580.
// Captured 2026-10-22.
func PlanParts(size, sliceSize int64) []PartPlan {
	if sliceSize <= 0 {
		return nil
	}
	var plans []PartPlan
	remaining := size
	index := 1
	for remaining > 0 {
		n := sliceSize
		if n > remaining {
			n = remaining
		}
		plans = append(plans, PartPlan{Index: index, Size: n})
		remaining -= n
		index++
	}
	return plans
}

// SortHeaders returns header names in a stable order, so an error message can
// list which headers the server asked for without the ordering changing
// between runs.
func SortHeaders(h map[string]string) []string {
	names := make([]string, 0, len(h))
	for k := range h {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
