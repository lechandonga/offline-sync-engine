package sync

// SyncOnce 在两个副本之间执行一轮双向增量同步：
// 双方交换版本向量，互相发送对方缺失的变更。
// 变更应用幂等，因此重复投递、乱序到达与中途断连均安全：
// 断连后再次调用 SyncOnce 即可继续完成同步（占位实现）。
func SyncOnce(a, b *Engine) error { return nil }
