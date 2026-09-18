package sync

// SyncOnce 在两个副本之间执行一轮双向增量同步：
// 双方交换版本向量，各自发送对方缺失的变更。
//
// 容错性质：
//   - 变更应用幂等（按 ChangeID 去重），重复投递安全；
//   - 接收方缓存乱序变更，乱序到达安全；
//   - 同步进度完全由版本向量与日志推导，不依赖易损坏的外部状态，
//     中途断连后再次调用 SyncOnce 即可继续，直至双方版本向量一致；
//   - 若发送方的压缩基线已超过接收方进度（增量日志不够用），
//     先传输基线快照再增量补齐，保证任何进度的副本都能收敛。
func SyncOnce(a, b *Engine) error {
	if err := push(a, b); err != nil {
		return err
	}
	return push(b, a)
}

// push 把 from 中 to 尚未覆盖的内容发送给 to（必要时先传基线快照）。
func push(from, to *Engine) error {
	snap := from.ExportSnapshot()
	if len(snap.Vector) > 0 && !covers(to.Vector(), snap.Vector) {
		if err := to.InstallSnapshot(snap); err != nil {
			return err
		}
	}
	for _, c := range from.ChangesSince(to.Vector()) {
		if err := to.Apply(c); err != nil {
			return err
		}
	}
	return nil
}

// covers 报告 v 是否覆盖了 base 的每个分量。
func covers(v, base VersionVector) bool {
	for r, s := range base {
		if v[r] < s {
			return false
		}
	}
	return true
}

// Converged 报告两个副本是否已覆盖同一变更集。
func Converged(a, b *Engine) bool {
	va, vb := a.Vector(), b.Vector()
	return covers(va, vb) && covers(vb, va)
}
