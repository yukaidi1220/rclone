package yun139

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/require"
)

// personalListTransport 按预设的"每页条数 + 下一页游标"应答 /hcy/file/list,
// 用来模拟服务端的少给、空页、游标不前进等形态。
type personalListTransport struct {
	items   []int    // 每页返回多少条
	cursors []string // 每页返回的 nextPageCursor

	mu        sync.Mutex
	n         int
	pageSizes []int
	cursorsIn []string
}

func (tr *personalListTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	raw, _ := io.ReadAll(r.Body)
	var body struct {
		PageInfo struct {
			PageCursor string `json:"pageCursor"`
			PageSize   int    `json:"pageSize"`
		} `json:"pageInfo"`
	}
	_ = json.Unmarshal(raw, &body)
	tr.pageSizes = append(tr.pageSizes, body.PageInfo.PageSize)
	tr.cursorsIn = append(tr.cursorsIn, body.PageInfo.PageCursor)

	idx := tr.n
	if idx >= len(tr.items) {
		idx = len(tr.items) - 1
	}
	cursor := ""
	if idx < len(tr.cursors) {
		cursor = tr.cursors[idx]
	}
	tr.n++
	items := make([]map[string]any, 0, tr.items[idx])
	for i := 0; i < tr.items[idx]; i++ {
		id := fmt.Sprintf("F%03d%03d", idx, i)
		items = append(items, map[string]any{
			"fileId":    id,
			"name":      id + ".bin",
			"size":      1,
			"type":      "file",
			"updatedAt": "2026-10-23T00:00:00+08:00",
		})
	}
	out, _ := json.Marshal(map[string]any{
		"success": true,
		"code":    "0000",
		"data":    map[string]any{"items": items, "nextPageCursor": cursor},
	})
	return &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Header:     http.Header{},
		Body:       io.NopCloser(bytes.NewReader(out)),
	}, nil
}

func newPersonalTestFs(tr http.RoundTripper) *Fs {
	f := &Fs{
		name:         "test",
		opt:          Options{Enc: encoder.Standard | encoder.EncodeInvalidUtf8},
		space:        spacePersonal,
		account:      "13900000000",
		auth:         "dG9rZW4=",
		ts:           &tokenState{auth: "dG9rZW4="},
		tokMu:        &sync.Mutex{},
		hostMu:       &sync.Mutex{},
		personalHost: "https://yun.139.com",
		httpClient:   &http.Client{Transport: tr},
		pacer:        fs.NewPacer(context.Background(), pacer.NewDefault(pacer.MinSleep(time.Millisecond))),
	}
	f.dirCache = dircache.New("", defaultRootID, f)
	return f
}

// 游标说了还有下一页就必须继续, 哪怕这一页比请求的窗口短。个人云是 sync 的源端,
// 早停会少列对象, 目标端多出来的会被 sync --delete 当成"多余"删掉。
func TestUnitPersonalListFollowsCursorNotShortPage(t *testing.T) {
	tr := &personalListTransport{items: []int{150, 150, 150}, cursors: []string{"c1", "c2", ""}}
	f := newPersonalTestFs(tr)

	// 本用例的前提: 请求窗口大于模拟的每页条数(150), 否则"短页"根本不会出现,
	// 终止判据也就没被覆盖到。
	require.Greater(t, listPageSize, 150, "模拟页长必须小于 listPageSize, 否则本用例测不到短页")

	entries, err := f.List(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, entries, 450, "三页各 150 条, 靠游标才能取全; 只拿到 150 说明短页被判成了末页")
	require.Equal(t, []int{200, 200, 200}, tr.pageSizes, "每页请求的大小应等于 listPageSize")
	require.Equal(t, []string{"", "c1", "c2"}, tr.cursorsIn, "应逐页带上服务端给的游标")
}

// 空页但游标非空是协议自相矛盾: 静默停会丢掉后面的对象, 继续走会空转。
// 这条语义必须响亮失败, 因为个人云是 sync 的源端。
func TestUnitPersonalListEmptyPageWithLiveCursorFails(t *testing.T) {
	tr := &personalListTransport{items: []int{150, 0, 150}, cursors: []string{"c1", "c2", ""}}
	f := newPersonalTestFs(tr)

	_, err := f.List(context.Background(), "")
	require.ErrorContains(t, err, "got an empty page", "空页加活游标必须报错, 静默停等于把这次要修的截断换个位置留着")
}

// 游标不前进说明服务端没在往前走, 继续翻只会撞 1000 页上限再报一个看不懂的错。
func TestUnitPersonalListStuckCursorFails(t *testing.T) {
	tr := &personalListTransport{items: []int{150, 150, 150}, cursors: []string{"c1", "c1", "c1"}}
	f := newPersonalTestFs(tr)

	_, err := f.List(context.Background(), "")
	require.ErrorContains(t, err, "cursor did not advance", "必须由游标护栏报错; 若是 1000 页上限的报错说明护栏没生效")
}
