package sync

// SyncPair 在两个副本之间执行一次双向增量同步：
// 双方交换版本向量，各自把对方缺失的变更推送给对方。
// 接收端按变更 ID 幂等去重，因此重复投递、乱序到达、
// 中途断连后重试均可安全完成，不会造成状态分叉或重复应用。
func SyncPair(a, b *Engine) error {
	avv := a.VersionVector()
	bvv := b.VersionVector()
	for _, c := range a.ChangesSince(bvv) {
		if err := b.Receive(c); err != nil {
			return err
		}
	}
	for _, c := range b.ChangesSince(avv) {
		if err := a.Receive(c); err != nil {
			return err
		}
	}
	return nil
}
