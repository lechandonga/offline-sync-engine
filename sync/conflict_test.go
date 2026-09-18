package sync

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
)

// 并发修改同一 key：无论投递顺序如何，所有副本必须得到相同的获胜者。
func TestConcurrentSetDeterministic(t *testing.T) {
	mk := func(replica string, seq uint64, val string) Change {
		return Change{
			ID: fmt.Sprintf("%s-%d", replica, seq), Replica: replica, Seq: seq,
			Key: "hot", Type: OpSet, Value: json.RawMessage(fmt.Sprintf("%q", val)),
		}
	}
	cs := []Change{mk("A", 1, "va"), mk("B", 1, "vb"), mk("C", 2, "vc")}

	// 所有排列下结果一致
	perm := rand.New(rand.NewSource(1)).Perm
	var want string
	for i := 0; i < 20; i++ {
		s := NewStore()
		for _, idx := range perm(len(cs)) {
			s.Apply(cs[idx])
		}
		v, ok := s.Get("hot")
		if !ok {
			t.Fatal("hot missing")
		}
		if i == 0 {
			want = string(v)
		} else if string(v) != want {
			t.Fatalf("order-dependent result: %s vs %s", v, want)
		}
	}
	// Seq 最大者 (C,2) 应获胜
	if want != `"vc"` {
		t.Fatalf("expected vc to win, got %s", want)
	}
}

// 并发删除与并发更新：delete 作为带排序键的墓碑参与同一全序，
// 结果与到达顺序无关。
func TestConcurrentDeleteVsUpdate(t *testing.T) {
	set := Change{ID: "A-1", Replica: "A", Seq: 1, Key: "doc", Type: OpSet, Value: json.RawMessage(`"v"`)}
	del := Change{ID: "B-1", Replica: "B", Seq: 1, Key: "doc", Type: OpDelete}

	// 两种到达顺序
	s1 := NewStore()
	s1.Apply(set)
	s1.Apply(del)
	s2 := NewStore()
	s2.Apply(del)
	s2.Apply(set)

	_, ok1 := s1.Get("doc")
	_, ok2 := s2.Get("doc")
	if ok1 != ok2 {
		t.Fatalf("order-dependent delete semantics: %v vs %v", ok1, ok2)
	}
	// 按全序 (Seq,Replica,ID)：A-1 < B-1，删除获胜
	if ok1 {
		t.Fatal("expected delete (B-1) to win over set (A-1)")
	}

	// 若更新因果上晚于删除（Seq 更大），则更新获胜（delete-then-recreate）
	s3 := NewStore()
	late := Change{ID: "A-2", Replica: "A", Seq: 2, Key: "doc", Type: OpSet, Value: json.RawMessage(`"v2"`)}
	s3.Apply(del)
	s3.Apply(late)
	if _, ok := s3.Get("doc"); !ok {
		t.Fatal("expected later set to win over earlier delete")
	}
}

// 端到端：两个副本并发改同一 key，同步后收敛到同一值，且与同步方向无关。
func TestConflictConvergesAcrossReplicas(t *testing.T) {
	a := openTemp(t, "A")
	b := openTemp(t, "B")
	mustSet(t, a, "k", "from-a")
	mustSet(t, b, "k", "from-b")

	if err := SyncPair(a, b); err != nil {
		t.Fatal(err)
	}
	va, _ := a.Get("k")
	vb, _ := b.Get("k")
	if string(va) != string(vb) {
		t.Fatalf("diverged: %s vs %s", va, vb)
	}
	// 反向再同步一次，结果不变（幂等）
	if err := SyncPair(b, a); err != nil {
		t.Fatal(err)
	}
	va2, _ := a.Get("k")
	if string(va2) != string(va) {
		t.Fatalf("unstable after re-sync: %s vs %s", va2, va)
	}
}
