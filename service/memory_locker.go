package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hecc-blot/core/util"
	lock "github.com/hecc-blot/lock/contract"
)

// memoryLocker 基于进程内 map + 互斥锁实现的内存锁。
// 单进程互斥 / 单元测试用，不跨进程。无过期时间，Renew 为无操作。
type memoryLocker struct {
	mu            sync.Mutex
	held          map[string]string // key -> 持有者 token
	retryInterval time.Duration
}

// NewMemoryLocker 创建内存后端分布式锁。
func NewMemoryLocker(opts ...Option) lock.ILocker {
	o := options{retryInterval: 50 * time.Millisecond}
	for _, opt := range opts {
		opt(&o)
	}
	return &memoryLocker{
		held:          make(map[string]string),
		retryInterval: o.retryInterval,
	}
}

func (m *memoryLocker) TryLock(_ context.Context, key string, ttl time.Duration) (lock.ILock, error) {
	if ttl <= 0 {
		return nil, errors.New("lock: ttl 必须大于 0")
	}
	token := util.RandomHex(16)
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.held[key]; ok {
		return nil, lock.ErrLocked
	}
	m.held[key] = token
	return &memoryLock{locker: m, key: key, token: token}, nil
}

func (m *memoryLocker) Lock(ctx context.Context, key string, ttl, wait time.Duration) (lock.ILock, error) {
	if wait <= 0 {
		return m.TryLock(ctx, key, ttl)
	}
	return retryLock(ctx, m.retryInterval, wait, func() (lock.ILock, error) {
		return m.TryLock(ctx, key, ttl)
	})
}

// memoryLock 一把已获取的内存锁。
type memoryLock struct {
	locker *memoryLocker
	key    string
	token  string
	done   atomic.Bool
}

func (l *memoryLock) Unlock(_ context.Context) error {
	if l.done.Swap(true) {
		return lock.ErrNotHeld
	}
	l.locker.mu.Lock()
	defer l.locker.mu.Unlock()
	if l.locker.held[l.key] != l.token {
		return lock.ErrNotHeld
	}
	delete(l.locker.held, l.key)
	return nil
}

func (l *memoryLock) Renew(_ context.Context) error {
	// 内存锁无过期，续期无操作
	return nil
}
