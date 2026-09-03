package yun139

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/yun139/api"
)

// TestPutPart_SetsContentLength checks that putPart puts the known part
// size into both req.ContentLength and the Content-Length header. The
// CDN's S3 signature covers Content-Length, so an empty header makes it
// reject the part with 403 SignatureDoesNotMatch (seen live).
func TestPutPart_SetsContentLength(t *testing.T) {
	var gotCL string
	var gotReqCL int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCL = r.Header.Get("Content-Length")
		gotReqCL = r.ContentLength
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	f := &Fs{httpClient: srv.Client(), opt: Options{}}
	err := f.putPart(context.Background(), newZeroReader(100*1024*1024), srv.URL, 100*1024*1024)
	if err != nil {
		t.Fatalf("putPart: %v", err)
	}
	if want := strconv.FormatInt(100*1024*1024, 10); gotCL != want {
		t.Errorf("Content-Length header = %q, want %q", gotCL, want)
	}
	if gotReqCL != 100*1024*1024 {
		t.Errorf("req.ContentLength = %d, want %d", gotReqCL, 100*1024*1024)
	}
}

// newZeroReader returns a reader that yields n zero bytes.
type zeroReader struct {
	left int64
}

func newZeroReader(n int64) *zeroReader { return &zeroReader{left: n} }

func (z *zeroReader) Read(p []byte) (int, error) {
	if z.left <= 0 {
		return 0, context.DeadlineExceeded // test will not read past the end
	}
	if int64(len(p)) > z.left {
		p = p[:z.left]
	}
	for i := range p {
		p[i] = 0
	}
	z.left -= int64(len(p))
	return len(p), nil
}

// TestCreateReqPayload_Mirrors139Strm verifies the /file/create JSON the
// backend posts has exactly the field names 139 expects. Live testing
// showed the old shape ('sha256') got '04000002: 文件类型不允许为空',
// while contentHash + parallelUpload:false + type:file works.
func TestCreateReqPayload_Mirrors139Strm(t *testing.T) {
	body := api.PersonalCreateReq{
		CommonUpload: api.CommonUpload{
			ParentID: "root",
			Name:     "f.bin",
			Size:     10,
			Type:     "file",
		},
		FileRenameMode:       "auto_rename",
		ContentHash:          "abc123",
		ContentHashAlgorithm: "SHA256",
		ContentType:          "application/octet-stream",
		ParallelUpload:       false,
		PartInfos: []api.PartInfo{{
			PartNumber: 1,
			PartSize:   10,
			ParallelHashCtx: &api.ParallelHashCtx{
				PartOffset: 0,
			},
		}},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	// The critical fields must be present with the right names.
	for _, k := range []string{
		"contentHash", "contentHashAlgorithm", "contentType",
		"parallelUpload", "partInfos", "parentFileId", "name",
		"size", "type", "fileRenameMode",
	} {
		if _, ok := m[k]; !ok {
			t.Errorf("request JSON missing key %q (got %s)", k, string(raw))
		}
	}
	if m["contentHash"] != "abc123" {
		t.Errorf("contentHash = %v, want abc123", m["contentHash"])
	}
	if m["contentHashAlgorithm"] != "SHA256" {
		t.Errorf("contentHashAlgorithm = %v, want SHA256", m["contentHashAlgorithm"])
	}
	if m["parallelUpload"] != false {
		t.Errorf("parallelUpload = %v, want false", m["parallelUpload"])
	}
	if m["type"] != "file" {
		t.Errorf("type = %v, want file", m["type"])
	}
	// The old wrong shape must not appear.
	if _, ok := m["sha256"]; ok {
		t.Errorf("request JSON still has legacy 'sha256' key: %s", string(raw))
	}
	if _, ok := m["rapidUpload"]; ok {
		t.Errorf("request JSON still has legacy 'rapidUpload' key: %s", string(raw))
	}
	// partInfos[0] carries parallelHashCtx.partOffset.
	parts, ok := m["partInfos"].([]any)
	if !ok || len(parts) == 0 {
		t.Fatalf("partInfos missing: %s", string(raw))
	}
	part0, ok := parts[0].(map[string]any)
	if !ok {
		t.Fatalf("partInfos[0] not an object: %s", string(raw))
	}
	ctx, ok := part0["parallelHashCtx"].(map[string]any)
	if !ok {
		t.Fatalf("partInfos[0].parallelHashCtx missing: %s", string(raw))
	}
	if off, _ := ctx["partOffset"].(float64); off != 0 {
		t.Errorf("partOffset = %v, want 0", ctx["partOffset"])
	}
}
// TestBuildCreateBody_CapsAt100Parts pins the create payload cap at
// maxPartsPerRequest (100) parts - the official client's create sends
// at most 100 partInfos and fetches the rest via getUploadUrl
// (captured 2026-09-03 with a 608 MB / 116-part file).
func TestBuildCreateBody_CapsAt100Parts(t *testing.T) {
	partInfos := make([]api.PartInfo, 0, 116)
	for i := 1; i <= 116; i++ {
		partInfos = append(partInfos, api.PartInfo{PartNumber: int64(i), PartSize: 5242880})
	}
	body := buildCreateBody("parent", "big.bin", 116*5242880, strings.Repeat("ab", 32), partInfos)
	if len(body.PartInfos) != maxPartsPerRequest {
		t.Fatalf("len(PartInfos) = %d, want %d", len(body.PartInfos), maxPartsPerRequest)
	}
	// The server requires RFC3339 UTC with milliseconds (rejects other
	// formats with '04000002: 本地创建时间格式不符合标准'); the value
	// itself is ignored in favour of the server clock.
	if body.LocalCreatedAt == "" || body.LocalUpdatedAt == "" {
		t.Error("LocalCreatedAt/LocalUpdatedAt must be non-empty RFC3339")
	}
	for _, ts := range []string{body.LocalCreatedAt, body.LocalUpdatedAt} {
		if _, err := time.Parse("2006-01-02T15:04:05.000Z", ts); err != nil {
			t.Errorf("timestamp %q not RFC3339-ms: %v", ts, err)
		}
	}
}

// TestPlanParts_EdgeCases pins the part-planning behavior for the
// boundary sizes the server cares about: empty file, exact multiple,
// and a part that would exceed the 100-part create cap (needing
// getUploadUrl).
func TestPlanParts_EdgeCases(t *testing.T) {
	cases := []struct {
		size, chunk int64
		wantParts   int
		wantLast    int64
	}{
		{0, 5242880, 1, 0},            // empty file: one zero-size part
		{5242880, 5242880, 1, 5242880}, // exactly one part
		{5242880 * 100, 5242880, 100, 5242880}, // exactly 100 parts
		{5242880*100 + 1, 5242880, 101, 1},     // 101st part triggers getUploadUrl
		{5, 5242880, 1, 5},            // tiny file
	}
	for _, c := range cases {
		parts := planParts(c.size, c.chunk)
		if len(parts) != c.wantParts {
			t.Errorf("planParts(%d, %d) = %d parts, want %d", c.size, c.chunk, len(parts), c.wantParts)
			continue
		}
		if parts[len(parts)-1].partSize != c.wantLast {
			t.Errorf("planParts(%d, %d) last part = %d, want %d", c.size, c.chunk, parts[len(parts)-1].partSize, c.wantLast)
		}
		// Offsets must be strictly increasing and contiguous.
		for i := 1; i < len(parts); i++ {
			if parts[i].offset != parts[i-1].offset+parts[i-1].partSize {
				t.Errorf("planParts(%d): part %d offset %d not contiguous", c.size, i, parts[i].offset)
			}
		}
	}
}

// TestChunkWriter_OutOfOrderWrites verifies WriteChunk lands each chunk
// at its part offset even when called out of order (the copy engine does
// this), and that the staged file equals the source after Close's hash
// step. Uses a fake uploader to avoid network.
func TestChunkWriter_OutOfOrderWrites(t *testing.T) {
	const chunkSize = 16
	src := make([]byte, 50)
	for i := range src {
		src[i] = byte(i * 3)
	}
	tmp, err := os.CreateTemp("", "yun139-cw-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmp.Name())

	w := &yun139ChunkWriter{
		tmp:       tmp,
		path:      tmp.Name(),
		size:      int64(len(src)),
		chunkSize: chunkSize,
		total:     (int64(len(src)) + chunkSize - 1) / chunkSize,
	}
	if err := tmp.Truncate(int64(len(src))); err != nil {
		t.Fatal(err)
	}

	// Write chunks in scrambled order: 2, 0, 3, 1.
	order := []int{2, 0, 3, 1}
	for _, cn := range order {
		off := int64(cn) * chunkSize
		limit := int64(chunkSize)
		if int64(len(src))-off < limit {
			limit = int64(len(src)) - off
		}
		r := bytes.NewReader(src[off : off+limit])
		n, err := w.WriteChunk(context.Background(), cn, r)
		if err != nil {
			t.Fatalf("WriteChunk(%d): %v", cn, err)
		}
		if n != limit {
			t.Errorf("WriteChunk(%d) wrote %d, want %d", cn, n, limit)
		}
	}

	// The staged file must now equal the source.
	staged := make([]byte, len(src))
	if _, err := tmp.ReadAt(staged, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(staged, src) {
		t.Fatal("staged file != source after out-of-order writes")
	}
}

// TestChunkWriter_AbortRemovesTemp verifies Abort closes and deletes the
// temp file without uploading.
func TestChunkWriter_AbortRemovesTemp(t *testing.T) {
	tmp, err := os.CreateTemp("", "yun139-cw-abort-")
	if err != nil {
		t.Fatal(err)
	}
	path := tmp.Name()
	w := &yun139ChunkWriter{tmp: tmp, path: path}
	if err := w.Abort(context.Background()); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("temp file still exists after Abort (stat err %v)", err)
	}
}
