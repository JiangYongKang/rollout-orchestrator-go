package orchestrator

import (
	"sync"
)

// keyedLocks 为每个 plan / target 提供独立互斥锁：
// 不同 plan、不同 target 之间完全并行；
// 同一 plan 或同一 target 上的并发命令串行化，
// 杜绝半更新、重复放量、重复回滚与状态越级。
type keyedLocks struct {
	mu sync.Mutex
	m  map[string]*sync.Mutex
}

func newKeyedLocks() *keyedLocks {
	return &keyedLocks{m: make(map[string]*sync.Mutex)}
}

// withLock 串行执行 fn。key 集合受 MaxPlans 上限约束，
// 且空闲条目会被回收，map 不会随运行时间无限增长。
func (k *keyedLocks) withLock(key string, fn func()) {
	k.mu.Lock()
	l, ok := k.m[key]
	if !ok {
		l = &sync.Mutex{}
		k.m[key] = l
	}
	k.mu.Unlock()

	l.Lock()
	defer func() {
		l.Unlock()
		k.mu.Lock()
		if l.TryLock() {
			// 没有其他等待者，安全回收条目。
			l.Unlock()
			if k.m[key] == l {
				delete(k.m, key)
			}
		}
		k.mu.Unlock()
	}()
	fn()
}
