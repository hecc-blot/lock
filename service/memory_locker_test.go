package service

import (
	"context"
	"errors"
	"testing"
	"time"

	lock "github.com/hecc-blot/lock/contract"

	"github.com/stretchr/testify/assert"
)

func TestMemoryLocker_TryLockAndUnlock(t *testing.T) {
	ctx := context.Background()
	l := NewMemoryLocker()

	lk, err := l.TryLock(ctx, "k", time.Minute)
	assert.NoError(t, err)

	_, err = l.TryLock(ctx, "k", time.Minute)
	assert.True(t, errors.Is(err, lock.ErrLocked))

	assert.NoError(t, lk.Unlock(ctx))
	// 二次解锁返回 ErrNotHeld
	assert.True(t, errors.Is(lk.Unlock(ctx), lock.ErrNotHeld))
}

func TestMemoryLocker_LockBlocking(t *testing.T) {
	ctx := context.Background()
	l := NewMemoryLocker()

	lk, _ := l.TryLock(ctx, "k", time.Minute)
	go func() {
		time.Sleep(40 * time.Millisecond)
		_ = lk.Unlock(ctx)
	}()

	got, err := l.Lock(ctx, "k", time.Minute, time.Second)
	assert.NoError(t, err)
	assert.NotNil(t, got)
	_ = got.Unlock(ctx)
}

func TestMemoryLocker_LockTimeout(t *testing.T) {
	ctx := context.Background()
	l := NewMemoryLocker()

	_, _ = l.TryLock(ctx, "k", time.Minute)
	_, err := l.Lock(ctx, "k", time.Minute, 30*time.Millisecond)
	assert.True(t, errors.Is(err, lock.ErrTimeout))
}

func TestMemoryLocker_Renew(t *testing.T) {
	ctx := context.Background()
	l := NewMemoryLocker()

	lk, _ := l.TryLock(ctx, "k", time.Minute)
	// 内存锁无过期，续期为无操作
	assert.NoError(t, lk.Renew(ctx))
}
