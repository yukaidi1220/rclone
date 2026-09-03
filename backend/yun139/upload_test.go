package yun139

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

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