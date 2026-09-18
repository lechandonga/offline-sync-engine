package sync

import (
	"encoding/json"
	"testing"
)

// TestUnknownFieldsPreserved 验证旧版本副本在解码-存储-转发变更时，
// 不认识的 JSON 字段被无损保留，新版本副本之后能读到这些字段。
func TestUnknownFieldsPreserved(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")

	// 模拟新版本副本产生的变更：带有本版本不认识的字段。
	raw := `{"id":{"replica":"NEW","seq":1},"type":"set","key":"k",` +
		`"value":"\"v\"","hlc":{"ms":1,"ctr":0,"r":"NEW"},` +
		`"futureFlag":true,"futureMeta":{"tier":3}}`
	var c Change
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := a.Apply(c); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// 经 WAL 落盘-重启后未知字段仍保留。
	a.Close()
	a = open(t, dir+"/a", "A")

	// 同步给 b，b 收到的变更必须携带完整未知字段。
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce: %v", err)
	}
	var got *Change
	for i, ch := range b.ChangesSince(VersionVector{}) {
		if ch.ID.Replica == "NEW" {
			got = &b.ChangesSince(VersionVector{})[i]
		}
	}
	if got == nil {
		t.Fatal("未知来源变更未同步到 b")
	}
	if string(got.UnknownFields["futureFlag"]) != "true" {
		t.Fatalf("futureFlag 丢失: %v", got.UnknownFields)
	}
	if string(got.UnknownFields["futureMeta"]) != `{"tier":3}` {
		t.Fatalf("futureMeta 丢失: %v", got.UnknownFields)
	}
	// 编码结果与原始字段一致（可被新版本副本正确解析）。
	out, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("再解码: %v", err)
	}
	if string(m["futureMeta"]) != `{"tier":3}` {
		t.Fatalf("编码后 futureMeta 不符: %s", m["futureMeta"])
	}
}

// TestUnknownChangeTypeForwarded 验证未知类型的变更：
// 不作用于本地状态、不破坏本地状态，但会被持久化并继续复制给其他副本。
func TestUnknownChangeTypeForwarded(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir+"/a", "A")
	b := open(t, dir+"/b", "B")
	c := open(t, dir+"/c", "C")

	unknown := Change{
		ID:    ChangeID{Replica: "FUTURE", Seq: 1},
		Type:  ChangeType("crdt-merge"),
		Key:   "counter",
		Value: json.RawMessage(`{"delta":5}`),
		HLC:   HLC{Millis: 1, Replica: "FUTURE"},
	}
	if err := a.Apply(unknown); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	// 未知类型不作用于本地状态。
	if _, d := a.Get("counter"); !d {
		t.Fatal("未知类型变更不应作用于本地状态")
	}

	// 经 b 中转同步到 c：旧版本副本可无损中转。
	if err := SyncOnce(a, b); err != nil {
		t.Fatalf("SyncOnce a-b: %v", err)
	}
	if err := SyncOnce(b, c); err != nil {
		t.Fatalf("SyncOnce b-c: %v", err)
	}
	if !Converged(a, c) {
		t.Fatalf("经中转后未收敛: A=%v C=%v", a.Vector(), c.Vector())
	}
	found := false
	for _, ch := range c.ChangesSince(VersionVector{}) {
		if ch.ID.Replica == "FUTURE" && ch.Type == ChangeType("crdt-merge") {
			found = true
			if string(ch.Value) != `{"delta":5}` {
				t.Fatalf("未知变更载荷被篡改: %s", ch.Value)
			}
		}
	}
	if !found {
		t.Fatal("未知类型变更未中转到 c")
	}
	// 中转方 b 的本地状态同样不受未知类型影响。
	if _, d := b.Get("counter"); !d {
		t.Fatal("中转副本状态被未知类型污染")
	}
}

// TestUpgradeMergesPreservedData 验证版本升级场景：
// 旧副本保留的未知类型变更，在副本"升级"（能识别该类型）后可被正确合并。
// 这里通过自定义应用函数模拟升级后的新版本逻辑。
func TestUpgradeMergesPreservedData(t *testing.T) {
	dir := t.TempDir()
	old := open(t, dir+"/r", "R")

	// 旧版本时期收到并保留两条未知类型变更。
	for i, delta := range []int{3, 4} {
		c := Change{
			ID:    ChangeID{Replica: "FUTURE", Seq: uint64(i + 1)},
			Type:  ChangeType("counter-add"),
			Key:   "counter",
			Value: json.RawMessage(json.Number(string(rune('0'+delta))).String()),
			HLC:   HLC{Millis: int64(i + 1), Replica: "FUTURE"},
		}
		if err := old.Apply(c); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	old.Close()

	// 升级后重新打开：回放日志时新版本逻辑能识别并合并这些变更。
	store, err := OpenStore(dir + "/r/changes.log")
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	defer store.Close()
	sum := 0
	err = store.Replay(func(c Change) error {
		if c.Type == "counter-add" {
			var d int
			if err := json.Unmarshal(c.Value, &d); err != nil {
				return err
			}
			sum += d
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if sum != 7 {
		t.Fatalf("升级后合并结果错误: 期望 7, 得到 %d", sum)
	}
}
