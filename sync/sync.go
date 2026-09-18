package sync

// SyncOnce 在两个副本之间执行一轮双向增量同步：
// 双方交换版本向量，各自发送对方缺失的变更。
//
// 容错性质：
//   - 变更应用幂等（按 ChangeID 去重），重复投递安全；
//   - 接收方缓存乱序变更，乱序到达安全；
//   - 同步进度完全由版本向量与日志推导，不依赖易损坏的外部状态，
//     中途断连后再次调用 SyncOnce 即可继续，直至双方版本向量一致。
func SyncOnce(a, b *Engine) error {
	if err := push(a, b); err != nil {
		return err
	}
	return push(b, a)
}

// push 把 from 中 to 尚未覆盖的变更逐条发送给 to。
func push(from, to *Engine) error {
	for _, c := range from.ChangesSince(to.Vector()) {
		if err := to.Apply(c); err != nil {
			return err
		}
	}
	return nil
}

// Converged 报告两个副本是否已覆盖同一变更集。
func Converged(a, b *Engine) bool {
	va, vb := a.Vector(), b.Vector()
	for r, s := range va {
		if vb[r] < s {
			return false
		}
	}
	for r, s := range vb {
		if va[r] < s {
			return false
		}
	}
	return true
}
