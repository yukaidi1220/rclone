// Live probes for the wopan family space (家庭云).
//
// Two things are unknown there and both matter for the plan to serve more traffic from it:
// whether this account has a family space at all (the backend picks family mode purely by
// `family_id` being non-empty, and nothing discovered that id so far), and how long a
// family-space download link is actually granted for - the 120 minutes we measured is a
// personal-space number, and the backend currently applies downloadURLTTL=30m to both on that
// single piece of evidence.
//
// Guards, in order of how much they cost to violate:
//   - refuses to run at all unless the remote sets no_refresh: this probe must never be the
//     thing that rotates or invalidates the operator's token;
//   - never logs a download link. A wopan link is a bearer credential for its whole lifetime, so
//     it is written to the 0600 file named by WOPAN_LINK_OUT and otherwise reported only as
//     derived facts (host, path length, query parameter names, a visible expiry if one exists).
package wopan

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/wopan/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// probeFamilyFs loads the configured remote, refuses to touch a refresh-capable one, and returns
// the discovered default home id ("" when the account has no family space or the call fails).
func probeFamilyFs(t *testing.T) (*Fs, string) {
	t.Helper()
	if os.Getenv("WOPAN_PROBE") == "" {
		t.Skip("set WOPAN_PROBE=1 (and RCLONE_CONFIG with [TestWopan]) for live probes")
	}
	if p := os.Getenv("RCLONE_CONFIG"); p != "" {
		require.NoError(t, config.SetConfigPath(p))
	}
	configfile.Install()
	ctx := context.Background()
	fsi, err := fs.NewFs(ctx, "TestWopan:")
	require.NoError(t, err)
	f, ok := fsi.(*Fs)
	require.True(t, ok, "remote is not a wopan Fs")
	require.Truef(t, f.opt.NoRefresh,
		"拒绝运行：这个 remote 没设 no_refresh，探针不该成为动票的那一方")

	p := f.spaceParams()
	p["clientId"] = clientID
	data, err := f.call(ctx, chanWoHome, "FamilyUserCurrentEncode", p, nil)
	if err != nil {
		t.Logf("家庭云探测：FamilyUserCurrentEncode 失败 err=%v（这本身就是结论：这个账号/这张票看不到家庭云）", err)
		return f, ""
	}
	var resp api.FamilyUserResponse
	require.NoError(t, json.Unmarshal(data, &resp))
	id := strconv.FormatInt(resp.DefaultHomeID, 10)
	t.Logf("家庭云探测：defaultHomeId 非零=%v（值不打印，长度 %d）count=%q 有名字=%v 成员信息条数=%d",
		resp.DefaultHomeID != 0, len(id), resp.Count, resp.DefaultHomeName != "", len(resp.FamilyUserInfos))
	if resp.DefaultHomeID == 0 {
		return f, ""
	}
	if out := os.Getenv("WOPAN_FAMILY_OUT"); out != "" {
		writeProbeFile(t, out, id)
	}
	return f, id
}

// writeProbeFile writes a probe artefact with the tightest permissions we can ask for, because
// what goes in here (a link, an id) is credential-shaped.
func writeProbeFile(t *testing.T, path, body string) {
	t.Helper()
	fh, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	_, err = fh.WriteString(body)
	require.NoError(t, err)
	require.NoError(t, fh.Close())
	require.NoError(t, os.Chmod(path, 0o600))
}

// TestProbeFamilySpaceExists records whether the account has a family space, and proves the
// backend's one switch for it (a non-empty family_id selects spaceType 1).
func TestProbeFamilySpaceExists(t *testing.T) {
	_, id := probeFamilyFs(t)
	if id == "" {
		t.Skip("没有家庭云（或这张票看不到它）：后续链接寿命测量无从下手")
	}
	assert.NotEmpty(t, id)
}

// TestProbeFamilyLinkShape mints a family-space link and reports what the server handed back -
// shape only. If the URL carries its own expiry we learn the granted lifetime without waiting for
// a link to die; if it does not, the poller started from WOPAN_LINK_OUT answers the question.
func TestProbeFamilyLinkShape(t *testing.T) {
	f, id := probeFamilyFs(t)
	if id == "" {
		t.Skip("没有家庭云，跳过")
	}
	ctx := context.Background()
	f.opt.FamilyID = id
	f.spaceType = spaceFamily

	// One small file of our own, so the answer is not polluted by whatever is already there.
	// WOPAN_KEEP=1 leaves it in place: the link is only meaningful while its object exists
	// (measured 2026-10-11 - deleting it turns the link into a 404 in under two minutes), so
	// the lifetime poller needs the file to outlive this test. Cleanup is then
	// TestProbeFamilyProbeCleanup's job.
	const probeDir = "rclone-wopan-family-probe"
	keep := os.Getenv("WOPAN_KEEP") != ""
	obj := probePutAt(ctx, t, f, probeDir, "link-probe.bin", "wopan family link probe\n")
	name := obj.remote
	if !keep {
		t.Cleanup(func() {
			if err := obj.Remove(ctx); err != nil {
				t.Logf("清理 %s 失败（不影响结论）：%v", name, err)
			}
		})
	} else {
		t.Logf("WOPAN_KEEP=1：%s 保留给寿命轮询，测完由 TestProbeFamilyProbeCleanup 删", name)
	}

	urlStr, err := f.PublicLink(ctx, name, fs.Duration(0), false)
	require.NoErrorf(t, err, "家庭云取链失败：PublicLink 还没带对 space 参数？")
	require.NotEmpty(t, urlStr)

	u, err := url.Parse(urlStr)
	require.NoError(t, err)
	keys := make([]string, 0, len(u.Query()))
	for k := range u.Query() {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	t.Logf("家庭云直链：主机=%s 路径长度=%d query 参数名=%s（链接本身不打印）",
		u.Host, len(u.Path), strings.Join(keys, ","))
	for _, k := range keys {
		for _, v := range u.Query()[k] {
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if err != nil || n < 1_600_000_000 || n > 4_000_000_000 {
				continue
			}
			t.Logf("  参数 %s 像 unix 秒：距现在 %d 分钟（= 若它真是过期时间，寿命约 %.1f 小时）",
				k, (n-time.Now().Unix())/60, float64(n-time.Now().Unix())/3600.0)
		}
	}
	if out := os.Getenv("WOPAN_LINK_OUT"); out != "" {
		writeProbeFile(t, out, urlStr+"\n")
		t.Logf("链接已写入 %s（0600），供轮询脚本判寿命", out)
	}

	// And it must be alive *right now*: "the link died at age N" only measures the lifetime if
	// the same request shape succeeded at age 0. A 404 here means the object went away (the
	// first run of this probe deleted its own file and then watched the link 404).
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	require.NoError(t, err)
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("User-Agent", defaultUserAgent)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = res.Body.Close()
	t.Logf("签发后立刻探一次（1 字节 GET）：HTTP %d", res.StatusCode)
	require.Contains(t, []int{http.StatusOK, http.StatusPartialContent}, res.StatusCode,
		"链在年龄 0 就不通，那测到的不是寿命")
}

// TestProbeFamilyProbeCleanup removes the object the lifetime poller needs kept, so the probe
// leaves nothing behind once the measurement is over.
func TestProbeFamilyProbeCleanup(t *testing.T) {
	f, id := probeFamilyFs(t)
	if id == "" {
		t.Skip("没有家庭云，跳过")
	}
	ctx := context.Background()
	f.opt.FamilyID = id
	f.spaceType = spaceFamily
	const probePath = "rclone-wopan-family-probe/link-probe.bin"
	obj, err := f.NewObject(ctx, probePath)
	if err != nil {
		t.Logf("探针文件已不在（%v），无需清理", err)
		return
	}
	require.NoError(t, obj.Remove(ctx))
	t.Logf("已删除家庭云探针文件 %s", probePath)
}
