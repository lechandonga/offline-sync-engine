package sync

import (
	"fmt"
	"sync"
	"testing"
)

// 压缩与同步并发进行：不得丢失或重复应用变更，最终收敛。
func TestCompactionDuringSync(t *testing.T) {
	a := openTemp(t, "A")
	b := openTemp(t, "B")

	const n = 200
	for i := 0; i < n; i++ {
		mustSet(t, a, fmt.Sprintf("k%03d", i), "v1")
	}

	// 先同步一轮，让 B 有数据
	if err := SyncPair(a, b); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	// A 上并发：持续写入 + 反复压缩
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			mustSet(t, a, fmt.Sprintf("k%03d", i), "v2")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			if err := a.Snapshot(); err != nil {
				t.Errorf("snapshot: %v", err)
				return
			}
		}
	}()
	wg.Wait()
	// 写入全部完成后再做一次最终压缩
	if err := a.Snapshot(); err != nil {
		t.Fatal(err)
	}

	// 压缩后再同步，B 必须收敛到 A 的最终状态
	if err := SyncPair(a, b); err != nil {
		t.Fatal(err)
	}
	sa, sb := stateOf(t, a), stateOf(t, b)
	if fmt.Sprint(sa) != fmt.Sprint(sb) {
		t.Fatal("diverged after compaction+sync")
	}
	for i := 0; i < n; i++ {
		if sa[fmt.Sprintf("k%03d", i)] != "v2" {
			t.Fatalf("lost update at k%03d", i)
		}
	}
	// 压缩生效：A 的日志应远小于 2n
	if got := len(a.ChangesSince(nil)); got >= 2*n {
		t.Fatalf("compaction ineffective: %d changes", got)
	}
}

// 压缩后旧副本用陈旧版本向量再来同步：不得重复应用或分叉。
func TestSyncFromStalePeerAfterCompaction(t *testing.T) {
	a := openTemp(t, "A")
	b := openTemp(t, "B")
	for i := 0; i < 10; i++ {
		mustSet(t, a, fmt.Sprintf("k%d", i), "v")
	}
	if err := SyncPair(a, b); err != nil {
		t.Fatal(err)
	}
	staleVV := b.VersionVector() // B 此刻的版本向量
	if err := a.Snapshot(); err != nil {
		t.Fatal(err)
	}
	mustSet(t, a, "k-new", "v")
	// 压缩后的 A 用陈旧的 known 向量计算增量：可能多发旧变更，
	// 但 B 端幂等去重，状态不得分叉。
	for _, c := range a.ChangesSince(staleVV) {
		if err := b.Receive(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := SyncPair(a, b); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(stateOf(t, a)) != fmt.Sprint(stateOf(t, b)) {
		t.Fatal("diverged")
	}
	if _, ok := b.Get("k-new"); !ok {
		t.Fatal("post-compaction change not synced")
	}
}
