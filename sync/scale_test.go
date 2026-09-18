package sync

import (
	"fmt"
	"testing"
)

// 大量历史变更下，加载与增量同步应随规模线性增长（无烟道式退化）。
func TestLargeHistoryLoadAndSync(t *testing.T) {
	dir := t.TempDir()
	a, err := Open(dir+"/A", "A")
	if err != nil {
		t.Fatal(err)
	}
	const n = 5000
	for i := 0; i < n; i++ {
		mustSet(t, a, fmt.Sprintf("k%05d", i), "v")
	}
	a.Close()

	// 重新加载
	a2, err := Open(dir+"/A", "A")
	if err != nil {
		t.Fatal(err)
	}
	defer a2.Close()
	if got := len(a2.ChangesSince(nil)); got != n {
		t.Fatalf("reload lost changes: %d/%d", got, n)
	}

	// 增量同步到新副本
	b := openTemp(t, "B")
	if err := SyncPair(a2, b); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(stateOf(t, a2)) != fmt.Sprint(stateOf(t, b)) {
		t.Fatal("diverged")
	}
	// 快照恢复后状态一致
	if err := a2.Snapshot(); err != nil {
		t.Fatal(err)
	}
	a2.Close()
	a3, err := Open(dir+"/A", "A")
	if err != nil {
		t.Fatal(err)
	}
	defer a3.Close()
	if fmt.Sprint(stateOf(t, a3)) != fmt.Sprint(stateOf(t, b)) {
		t.Fatal("diverged after snapshot reload")
	}
}

func BenchmarkReplay(b *testing.B) {
	dir := b.TempDir()
	e, _ := Open(dir+"/A", "A")
	for i := 0; i < 2000; i++ {
		e.Set(fmt.Sprintf("k%d", i), []byte(`"v"`))
	}
	e.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e, err := Open(dir+"/A", "A")
		if err != nil {
			b.Fatal(err)
		}
		e.Close()
	}
}
