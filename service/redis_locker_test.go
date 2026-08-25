package service

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	lock "github.com/hecc-blot/lock/contract"

	"github.com/stretchr/testify/assert"
)

// fakeAtomic 内存模拟 IRedisAtomic：SetNX 按 map 判定，Eval 解析锁的两段脚本语义。
// 用于无 Redis 环境单测 lock 的逻辑（token 比对、错误映射、阻塞重试、watchdog）。
type fakeAtomic struct {
	mu    sync.Mutex
	store map[string]*fakeEntry
}

type fakeEntry struct {
	token    string
	expireAt time.Time // 零值表示无过期
}

func newFakeAtomic() *fakeAtomic {
	return &fakeAtomic{store: make(map[string]*fakeEntry)}
}

func (f *fakeAtomic) SetNX(_ context.Context, key string, val interface{}, expire time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if e, ok := f.store[key]; ok && (e.expireAt.IsZero() || e.expireAt.After(time.Now())) {
		return false, nil
	}
	var exp time.Time
	if expire > 0 {
		exp = time.Now().Add(expire)
	}
	f.store[key] = &fakeEntry{token: val.(string), expireAt: exp}
	return true, nil
}

func (f *fakeAtomic) Eval(_ context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.store[keys[0]]
	if !ok {
		return int64(0), nil
	}
	// 过期清理，模拟 redis 的惰性删除
	if !e.expireAt.IsZero() && e.expireAt.Before(time.Now()) {
		delete(f.store, keys[0])
		return int64(0), nil
	}
	token := args[0].(string)
	if strings.Contains(script, "pexpire") {
		// 续期：token 匹配则重置过期
		if e.token == token {
			e.expireAt = time.Now().Add(time.Duration(args[1].(int64)) * time.Millisecond)
			return int64(1), nil
		}
		return int64(0), nil
	}
	// 解锁：token 匹配则删除
	if e.token == token {
		delete(f.store, keys[0])
		return int64(1), nil
	}
	return int64(0), nil
}

func TestRedisLocker_TryLockAndUnlock(t *testing.T) {
	ctx := context.Background()
	l := NewRedisLocker(newFakeAtomic())

	lk, err := l.TryLock(ctx, "k", time.Minute)
	assert.NoError(t, err)

	// 重复加锁失败
	_, err = l.TryLock(ctx, "k", time.Minute)
	assert.True(t, errors.Is(err, lock.ErrLocked))

	// 解锁成功后，其他持有者可再次加锁
	assert.NoError(t, lk.Unlock(ctx))
	lk2, err := l.TryLock(ctx, "k", time.Minute)
	assert.NoError(t, err)
	assert.NoError(t, lk2.Unlock(ctx))
}

func TestRedisLocker_DoubleUnlock(t *testing.T) {
	ctx := context.Background()
	l := NewRedisLocker(newFakeAtomic())

	lk, _ := l.TryLock(ctx, "k", time.Minute)
	assert.NoError(t, lk.Unlock(ctx))
	// 二次解锁返回 ErrNotHeld
	assert.True(t, errors.Is(lk.Unlock(ctx), lock.ErrNotHeld))
}

func TestRedisLocker_UnlockExpired(t *testing.T) {
	ctx := context.Background()
	l := NewRedisLocker(newFakeAtomic())

	lk, _ := l.TryLock(ctx, "k", 20*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	// 锁已过期，解锁返回 ErrNotHeld
	assert.True(t, errors.Is(lk.Unlock(ctx), lock.ErrNotHeld))
}

func TestRedisLocker_Renew(t *testing.T) {
	ctx := context.Background()
	l := NewRedisLocker(newFakeAtomic())

	lk, _ := l.TryLock(ctx, "k", time.Minute)
	assert.NoError(t, lk.Renew(ctx))

	// 短 ttl 锁过期后，续期失败
	lk2, _ := l.TryLock(ctx, "k2", 20*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	assert.True(t, errors.Is(lk2.Renew(ctx), lock.ErrNotHeld))
}

func TestRedisLocker_LockBlocking(t *testing.T) {
	ctx := context.Background()
	l := NewRedisLocker(newFakeAtomic())

	lk, _ := l.TryLock(ctx, "k", time.Minute)
	// 40ms 后释放
	go func() {
		time.Sleep(40 * time.Millisecond)
		_ = lk.Unlock(ctx)
	}()

	got, err := l.Lock(ctx, "k", time.Minute, time.Second)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	_ = got.Unlock(ctx)
}

func TestRedisLocker_LockTimeout(t *testing.T) {
	ctx := context.Background()
	l := NewRedisLocker(newFakeAtomic())

	_, _ = l.TryLock(ctx, "k", time.Minute)
	_, err := l.Lock(ctx, "k", time.Minute, 30*time.Millisecond)
	assert.True(t, errors.Is(err, lock.ErrTimeout))
}

func TestRedisLocker_LockWaitZero(t *testing.T) {
	ctx := context.Background()
	l := NewRedisLocker(newFakeAtomic())

	_, _ = l.TryLock(ctx, "k", time.Minute)
	// wait=0 等价于 TryLock，返回 ErrLocked 而非 ErrTimeout
	_, err := l.Lock(ctx, "k", time.Minute, 0)
	assert.True(t, errors.Is(err, lock.ErrLocked))
}

func TestRedisLocker_AutoRenew(t *testing.T) {
	ctx := context.Background()
	l := NewRedisLocker(newFakeAtomic(), WithAutoRenew())

	lk, _ := l.TryLock(ctx, "k", 60*time.Millisecond)
	// 睡多个续期周期（ttl/3=20ms），watchdog 若生效则锁仍被持有
	time.Sleep(150 * time.Millisecond)

	// 锁仍被当前持有者持有
	_, err := l.TryLock(ctx, "k", time.Minute)
	assert.True(t, errors.Is(err, lock.ErrLocked))

	assert.NoError(t, lk.Unlock(ctx))
}
