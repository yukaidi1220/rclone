package yun139

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// 群盘列目录窗口有服务端硬上限 200(1909011500)。这条用例把两件事同时钉住:
// 一是 450 条的目录必须完整取回(短页=末页的终止判据在窗口内是对的),
// 二是我们的请求窗口一次都没超过 200 —— 常量被往上调时, 假服务器会照真服务端
// 回 1909011500, List 直接报错, 用例变红而不是静默少数。
func TestUnitGroupListWindowStaysWithinServerCap(t *testing.T) {
	const total = 450
	contents := make([]map[string]any, 0, total)
	for i := 0; i < total; i++ {
		contents = append(contents, gcontent(fmt.Sprintf("F%03d", i), fmt.Sprintf("f%03d.bin", i), 1))
	}
	tr := &groupTransport{dirs: map[string]*gdir{gtestRootCat: {contents: contents}}}
	f := newGroupTestFs(tr)

	entries, err := f.List(context.Background(), "")
	require.NoError(t, err, "窗口超过 200 时假服务器会回 1909011500; 报错说明 listPageSize 被调到了上限之上")
	require.Len(t, entries, total)
	// 450 条按 200 切: 200 + 200 + 50, 三次请求且每次都按上限要。
	require.Equal(t, []int{200, 200, 200}, tr.windows)
}
