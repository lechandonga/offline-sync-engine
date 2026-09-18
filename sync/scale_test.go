package sync

import (
	"fmt"
	"testing"
	"time"
)

// TestScaleSyncAndCompact 验证在大量历史变更下：
// 增量同步、压缩、快照恢复的开销随规模合理增长，不出现无界退化，
// 且压缩后增量日志有界（不随历史无限增长）。
func TestScaleSyncAndCompact(t *testing.T) {
	if testing.Short() {
		t.Skip("短模式跳过规模测试")
	}
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	const n = 5000
	for i := 0; i < n; i++ {
		mustSet(t, a, fmt.Sprintf("k%05d", i), `"v"`)
	}

	// 全量同步 5000 条变更应在秒级完成。
	start := time.Now()
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("全量同步耗时异常: %v", d)
	}
	if !Converged(a, b) {
		t.Fatal("大规模同步后未收敛")
	}

	// 压缩后增量日志应有界（只剩压缩期间的新变更，这里为 0）。
	if err := a.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if got := len(a.ChangesSince(VersionVector{})); got != 0 {
		t.Fatalf("压缩后增量日志无界: %d 条", got)
	}

	// 压缩后再写少量变更，增量同步只传输增量部分。
	for i := 0; i < 10; i++ {
		mustSet(t, a, fmt.Sprintf("post%02d", i), `"p"`)
	}
	if got := len(a.ChangesSince(b.Vector())); got != 10 {
		t.Fatalf("增量同步应只传 10 条, 得到 %d", got)
	}
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if !Converged(a, b) {
		t.Fatal("压缩后增量同步未收敛")
	}

	// 快照恢复：重启加载应远快于全量回放，且状态完整。
	a.Close()
	start = time.Now()
	a2 := open(t, dir+"/a", "A")
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("快照恢复耗时异常: %v", d)
	}
	if got := len(a2.ChangesSince(VersionVector{})); got != 10 {
		t.Fatalf("恢复后日志尾部应为 10 条, 得到 %d", got)
	}
	if v, d := a2.Get("k00000"); d || string(v) != `"v"` {
		t.Fatalf("快照内容恢复错误: %q deleted=%v", v, d)
	}
	if v, d := a2.Get("post09"); d || string(v) != `"p"` {
		t.Fatalf("日志尾部恢复错误: %q deleted=%v", v, d)
	}
	if !Converged(a2, b) {
		t.Fatal("快照恢复后与对端未收敛")
	}
}

// TestLaggingReplicaCatchesUpViaSnapshot 验证落后太多的副本
// 在对端已压缩、增量日志不再覆盖其进度时，能通过基线快照追赶收敛。
func TestLaggingReplicaCatchesUpViaSnapshot(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	// b 只同步到第 2 条变更后长期离线。
	mustSet(t, a, "k1", `"1"`)
	mustSet(t, a, "k2", `"2"`)
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}

	// a 继续产生大量变更并压缩（增量日志被截断）。
	for i := 3; i <= 20; i++ {
		mustSet(t, a, fmt.Sprintf("k%d", i), `"v"`)
	}
	mustSet(t, a, "k1", `"updated"`)
	if err := a.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// b 重新上线：基线快照 + 增量补齐，必须收敛且不丢不重。
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	if !Converged(a, b) {
		t.Fatalf("落后副本未收敛: A=%v B=%v", a.Vector(), b.Vector())
	}
	assertNoDuplicateIDs(t, b)
	if v, d := b.Get("k1"); d || string(v) != `"updated"` {
		t.Fatalf("k1 追赶错误: %q deleted=%v", v, d)
	}
	if _, d := b.Get("k20"); d {
		t.Fatal("k20 在快照追赶中丢失")
	}

	// 追赶完成后重启 b：状态来自本地持久化的基线，无需再次传快照。
	b.Close()
	b2 := open(t, dir+"/b", "B")
	if !Converged(a, b2) {
		t.Fatal("追赶后重启未保持收敛")
	}
	if v, d := b2.Get("k1"); d || string(v) != `"updated"` {
		t.Fatalf("重启后 k1 错误: %q deleted=%v", v, d)
	}
}
