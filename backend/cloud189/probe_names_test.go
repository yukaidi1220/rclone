package cloud189

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/cloud189/api"
)

// Character table for file names.
//
// wopan and yun139 both carry a written-down set of names that may be rejected
// locally (validateName plus the encoder default), and neither of those was
// invented either. This is the same table for cloud189: every case is uploaded
// as a 4 KiB single-part file, read back out of the listing, and deleted again,
// so the answer is "what the server does" rather than a guess from a
// third-party driver.
//
// Names are sent URL-escaped because that is the client's own form - measured
// 2026-10-22: a raw "&" in a name silently truncates it at the ampersand,
// while an escaped one lands intact. A case that lands a *different* name is
// therefore server-side mangling, not an escaping bug.

// nameCase is one row of the table.
type nameCase struct {
	label string
	name  string
}

func TestProbeNameCharacterTable(t *testing.T) {
	s := liveSession(t)
	stamp := time.Now().Format("150405")
	prefix := "c189t-" + stamp + "-"
	// A distinct size per case so no case can be answered by rapid transfer:
	// every row must really send its part and really commit.
	digestOf := func(i int) []byte { return patternBytes(4096 + i) }
	cases := []nameCase{
		{"plain", "plain.bin"},
		{"space", "two words.bin"},
		{"leading-space", " lead.bin"},
		{"trailing-space", "trail .bin"},
		{"leading-dot", ".hidden.bin"},
		{"trailing-dot", "traildot..bin"},
		{"leading-tilde", "~tilde.bin"},
		{"double-quote", `dq".bin`},
		{"single-quote", "sq'.bin"},
		{"asterisk", "star*.bin"},
		{"colon", "co:lon.bin"},
		{"lt-gt", "angle<>.bin"},
		{"question", "quest?.bin"},
		{"backslash", "back\\slash.bin"},
		{"pipe", "pipe|.bin"},
		{"slash", "sla/sh.bin"},
		{"amp", "amp&and.bin"},
		{"equals", "eq=uals.bin"},
		{"percent", "pct-100%.bin"},
		{"hash", "hash#tag.bin"},
		{"plus", "plus+sign.bin"},
		{"comma", "com,ma.bin"},
		{"semicolon", "semi;colon.bin"},
		{"at", "at@sign.bin"},
		{"brackets", "bra(ck)ets[2].bin"},
		{"brace", "brace{}.bin"},
		{"dollar", "dollar$.bin"},
		{"bang", "bang!.bin"},
		{"caret", "caret^.bin"},
		{"tab", "tab\tsep.bin"},
		{"newline", "new\nline.bin"},
		{"cjk", "中文 文件名.bin"},
		{"cjk-punct", "中文（全角）.bin"},
		{"japanese", "日本語.bin"},
		{"cyrillic", "Русский.bin"},
		{"emoji-bmp", "bmp-☃.bin"},
		{"emoji-nonbmp", "emoji-😀.bin"},
		{"variation-selector", "vs-❤️.bin"},
		{"zwsp", "zero\u200bwidth.bin"},
		{"rtl-mark", "rtl\u200fmark.bin"},
		{"combining", "combining-é.bin"},
		{"invalid-utf8", "bad\xffutf8.bin"},
		{"only-dots", "..."},
		{"dot-dot-slash-safe", "..tails.bin"},
		{"len-100", strings.Repeat("a", 90) + ".bin"},
		{"len-101", strings.Repeat("b", 91) + ".bin"},
		{"len-150", strings.Repeat("c", 140) + ".bin"},
		{"len-200", strings.Repeat("d", 190) + ".bin"},
		{"len-255", strings.Repeat("e", 245) + ".bin"},
		{"len-256", strings.Repeat("f", 246) + ".bin"},
		{"len-300", strings.Repeat("g", 290) + ".bin"},
		{"cjk-len-100", strings.Repeat("漢", 100) + ".bin"},
		{"cjk-len-200", strings.Repeat("漢", 200) + ".bin"},
	}

	var created []probeEntry

	for i, tc := range cases {
		content := digestOf(i)
		fileMd5 := api.MD5Upper(content)
		name := prefix + tc.name
		// InitParams escapes the name itself, so the wire form here is exactly
		// the one measured; passing an already escaped name would double it.
		params := api.InitParams("-11", name, fileMd5, int64(len(content)), fileMd5, probeSliceSize)
		rec := runOnePartUpload(t, s, params, content)
		switch {
		case rec.refuse != "":
			rec.landed = "（未落盘）"
			rec.verdict = "refused: " + rec.refuse
		case rec.landed == name:
			rec.verdict = "stored verbatim"
		case rec.landed == "":
			rec.verdict = "committed but not found in the listing"
		default:
			rec.verdict = fmt.Sprintf("MANGLED to %q (len %d)", rec.landed, len(rec.landed))
		}
		if rec.id != "" {
			created = append(created, probeEntry{id: rec.id, name: rec.landed, parent: "-11"})
		}
		t.Logf("%-18s %-28q -> %s", tc.label, trunc(name, 46), rec.verdict)
	}

	t.Run("cleanup", func(t *testing.T) {
		if len(created) == 0 {
			t.Log("nothing landed")
			return
		}
		deleteTargets(t, s, false, created, true)
		t.Logf("DELETE submitted for %d probe files, left in the recycle bin", len(created))
	})
}

// uploadReceipt is what one probe upload came back with.
type uploadReceipt struct {
	verdict string
	refuse  string
	landed  string
	id      string
}

// runOnePartUpload completes an already-built init for a single-part file and
// reports the name the object ended up under.
func runOnePartUpload(t *testing.T, s probeSession, params []api.Param, content []byte) uploadReceipt {
	t.Helper()
	enc, err := api.EncryptParams(s.SessionSecret, api.EncodeParams(params))
	if err != nil {
		return uploadReceipt{refuse: "encrypt: " + err.Error()}
	}
	status, body := signedCall(t, s, uploadHost, false, http.MethodPost, "/person/initMultiUpload", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m := decode(t, body)
	data := subMap(m, "data")
	uploadFileID := field(data, "uploadFileId")
	if uploadFileID == "" {
		code := field(m, "code")
		if code == "" {
			code = field(m, "res_code")
		}
		return uploadReceipt{refuse: fmt.Sprintf("init http=%d code=%s", status, code)}
	}

	if field(data, "fileDataExists") == "1" {
		t.Log("rapid hit, skipping the PUT")
	}
	digest := api.MD5Raw(content)
	if status == http.StatusOK && field(data, "fileDataExists") != "1" {
		enc, err = api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
			{Key: "uploadFileId", Value: uploadFileID},
			{Key: "partInfo", Value: api.PartInfo([][]byte{digest})},
		}))
		if err != nil {
			return uploadReceipt{refuse: "encrypt partInfo: " + err.Error()}
		}
		status, body = signedCall(t, s, uploadHost, false, http.MethodGet, "/person/getMultiUploadUrls", probeQuery("params="+enc), "")
		m = decode(t, body)
		part := subMap(subMap(m, "uploadUrls"), "partNumber_1")
		partURL := field(part, "requestURL")
		if partURL == "" {
			return uploadReceipt{refuse: fmt.Sprintf("no part URL http=%d %s", status, trunc(string(body), 120))}
		}
		req, err := http.NewRequest(http.MethodPut, partURL, strings.NewReader(string(content)))
		if err != nil {
			return uploadReceipt{refuse: err.Error()}
		}
		for k, v := range api.ParseRequestHeader(field(part, "requestHeader")) {
			req.Header.Set(k, v)
		}
		req.Header.Set("user-agent", dartUserAgent)
		res, err := doScrubbed(req)
		if err != nil {
			return uploadReceipt{refuse: "PUT: " + err.Error()}
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return uploadReceipt{refuse: fmt.Sprintf("PUT http=%d", res.StatusCode)}
		}
	}

	enc, err = api.EncryptParams(s.SessionSecret, api.EncodeParams([]api.Param{
		{Key: "uploadFileId", Value: uploadFileID},
	}))
	if err != nil {
		return uploadReceipt{refuse: "encrypt commit: " + err.Error()}
	}
	status, body = signedCall(t, s, uploadHost, false, http.MethodPost, "/person/commitMultiUploadFile", probeQuery(""), "params="+enc,
		hdr{"decodefields", api.DecodeFields})
	m = decode(t, body)
	file := subMap(m, "file")
	code := field(m, "code")
	if code != "SUCCESS" {
		return uploadReceipt{refuse: fmt.Sprintf("commit http=%d code=%s", status, code)}
	}
	id := field(file, "userFileId")
	_, files, _ := listDir(t, s, false, "-11")
	for _, e := range files {
		if e.id == id {
			return uploadReceipt{landed: e.name, id: id}
		}
	}
	return uploadReceipt{id: id}
}

// TestProbeNameFullwidthSubstitutes checks the second half of the encoder
// question: rclone escapes the characters the server drops with their fullwidth
// forms, so those must survive the round trip intact. A fullwidth form that got
// mangled too would mean the character cannot be stored at all and the encoder
// flag would have to be dropped from the default rather than relied on.
func TestProbeNameFullwidthSubstitutes(t *testing.T) {
	s := liveSession(t)
	stamp := time.Now().Format("150405")
	prefix := "c189w-" + stamp + "-"
	names := []struct {
		label string
		name  string
	}{
		{"solidus", "s／l／a.bin"},
		{"dquote", "dq＂.bin"},
		{"squote", "sq＇.bin"},
		{"asterisk", "st＊ar.bin"},
		{"colon", "co：lon.bin"},
		{"ltgt", "an＜＞gle.bin"},
		{"question", "q？u.bin"},
		{"bslash", "b＼s.bin"},
		{"pipe", "p｜i.bin"},
		{"amp", "a＆b.bin"},
		{"percent", "p＠100％.bin"},
		{"space-ctl", "t＊a：b.bin"},
	}
	var created []probeEntry
	for i, tc := range names {
		content := patternBytes(5120 + i)
		fileMd5 := api.MD5Upper(content)
		full := prefix + tc.name
		params := api.InitParams("-11", full, fileMd5, int64(len(content)), fileMd5, probeSliceSize)
		rec := runOnePartUpload(t, s, params, content)
		verdict := "stored verbatim"
		switch {
		case rec.refuse != "":
			verdict = "refused: " + rec.refuse
		case rec.landed != full:
			verdict = fmt.Sprintf("MANGLED to %q", rec.landed)
		}
		t.Logf("%-12s -> %s", tc.label, verdict)
		if rec.id != "" {
			created = append(created, probeEntry{id: rec.id, name: rec.landed, parent: "-11"})
		}
	}
	if len(created) > 0 {
		deleteTargets(t, s, false, created, true)
		t.Logf("DELETE submitted for %d files, left in the recycle bin", len(created))
	}
}
