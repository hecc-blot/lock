package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	lock "github.com/hecc-blot/lock/contract"
)

// unlockScript 解锁脚本：仅当 value 仍为当前持有者 token 时才删除，
// 防止锁过期后被他人抢占时误删别人的锁。
var unlockScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
`

// renewScript 续期脚本：仅当 value 仍为当前持有者 token 时重置过期时间（毫秒）。
var renewScript = `
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`

// Option 配置分布式锁行为。
type Option func(*options)

type options struct {
	// retryInterval 阻塞获取（Lock）的重试间隔
	retryInterval time.Duration
	// autoRenew 是否开启自动续期（watchdog）
	autoRenew bool
}

// WithRetryInterval 设置阻塞获取的重试间隔，默认 50ms。
func WithRetryInterval(d time.Duration) Option {
	return func(o *options) { o.retryInterval = d }
}

// WithAutoRenew 开启自动续期（watchdog）：后台 goroutine 以 ttl/3 为周期续期，
// 直到 Unlock 或锁丢失。适用于临界区执行时间可能超过 ttl 的长任务。
func WithAutoRenew() Option {
	return func(o *options) { o.autoRenew = true }
}

// redisLocker 基于 Redis 原子原语（SetNX + Lua）实现的分布式锁。
// 通过 IRedisAtomic 窄接口依赖后端，不直接依赖 cache / go-redis，后端可替换。
type redisLocker struct {
	atomic lock.IRedisAtomic
	opts   options
}

// NewRedisLocker 创建 Redis 后端分布式锁。atomic 由调用方注入
// （如 cache 模块的 IRedisCache，或自定义 redis 封装），不重复建连。
func NewRedisLocker(atomic lock.IRedisAtomic, opts ...Option) lock.ILocker {
	if atomic == nil {
		panic("lock: redis 原子操作接口不能为空")
	}
	o := options{retryInterval: 50 * time.Millisecond}
	for _, opt := range opts {
		opt(&o)
	}
	return &redisLocker{atomic: atomic, opts: o}
}

func (l *redisLocker) TryLock(ctx context.Context, key string, ttl time.Duration) (lock.ILock, error) {
	if ttl <= 0 {
		return nil, errors.New("lock: ttl 必须大于 0")
	}
	token := newToken()
	ok, err := l.atomic.SetNX(ctx, key, token, ttl)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, lock.ErrLocked
	}
	lk := &redisLock{
		locker: l,
		ctx:    ctx,
		key:    key,
		token:  token,
		ttl:    ttl,
		stopCh: make(chan struct{}),
	}
	if l.opts.autoRenew {
		lk.startWatchdog()
	}
	return lk, nil
}

func (l *redisLocker) Lock(ctx context.Context, key string, ttl, wait time.Duration) (lock.ILock, error) {
	if wait <= 0 {
		return l.TryLock(ctx, key, ttl)
	}
	return retryLock(ctx, l.opts.retryInterval, wait, func() (lock.ILock, error) {
		return l.TryLock(ctx, key, ttl)
	})
}

// redisLock 一把已获取的 Redis 锁。
type redisLock struct {
	locker *redisLocker
	ctx    context.Context
	key    string
	token  string
	ttl    time.Duration

	stopCh   chan struct{}
	stopOnce sync.Once
	done     atomic.Bool
}

func (l *redisLock) Unlock(ctx context.Context) error {
	// 防止重复解锁
	if l.done.Swap(true) {
		return lock.ErrNotHeld
	}
	l.stopWatchdog()
	res, err := l.locker.atomic.Eval(ctx, unlockScript, []string{l.key}, l.token)
	if err != nil {
		return err
	}
	if n, ok := res.(int64); ok && n == 1 {
		return nil
	}
	return lock.ErrNotHeld
}

func (l *redisLock) Renew(ctx context.Context) error {
	res, err := l.locker.atomic.Eval(ctx, renewScript, []string{l.key}, l.token, l.ttl.Milliseconds())
	if err != nil {
		return err
	}
	if n, ok := res.(int64); ok && n == 1 {
		return nil
	}
	return lock.ErrNotHeld
}

// startWatchdog 启动自动续期，周期为 ttl/3，直到 Unlock 或锁丢失。
func (l *redisLock) startWatchdog() {
	interval := l.ttl / 3
	if interval <= 0 {
		interval = time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-l.stopCh:
				return
			case <-ticker.C:
				// 续期失败（锁已丢失）即停止，避免空转
				if err := l.Renew(l.ctx); err != nil {
					return
				}
			}
		}
	}()
}

func (l *redisLock) stopWatchdog() {
	l.stopOnce.Do(func() { close(l.stopCh) })
}

// retryLock 阻塞获取锁的通用重试循环，供 Redis / 内存等后端复用。
func retryLock(ctx context.Context, retryInterval, wait time.Duration, try func() (lock.ILock, error)) (lock.ILock, error) {
	deadline := time.Now().Add(wait)
	for {
		lk, err := try()
		if err == nil {
			return lk, nil
		}
		if !errors.Is(err, lock.ErrLocked) {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, lock.ErrTimeout
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryInterval):
		}
	}
}

// newToken 生成唯一持有者 token。用 crypto/rand 保证跨进程不可预测，
// 防止其他进程伪造 token 解锁。
func newToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 几乎不会失败，兜底用时间戳
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
