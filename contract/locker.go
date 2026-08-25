package lock

import (
	"context"
	"errors"
	"time"
)

// 分布式锁的语义错误，调用方用 errors.Is 判定。
var (
	// ErrLocked 锁已被其他持有者占用（TryLock 失败）。
	ErrLocked = errors.New("lock: 已被其他持有者占用")
	// ErrTimeout 阻塞获取锁在等待时间内未成功。
	ErrTimeout = errors.New("lock: 等待超时")
	// ErrNotHeld 锁未被当前持有者持有（重复解锁 / 锁已过期 / 续期失败）。
	ErrNotHeld = errors.New("lock: 锁未被当前持有者持有")
)

// ILock 一把已获取的锁，持有者须在临界区结束后调用 Unlock 释放。
type ILock interface {
	// Unlock 释放锁。重复调用或锁已过期时返回 ErrNotHeld。
	Unlock(ctx context.Context) error
	// Renew 手动续期，将锁的过期时间重置为加锁时的 ttl。
	// 锁已丢失（token 不匹配 / 已过期）时返回 ErrNotHeld。
	Renew(ctx context.Context) error
}

// ILocker 分布式锁工厂，后端可替换（Redis / 内存 / DB / etcd 等）。
//
// 锁的语义（锁什么 key、ttl 多长、失败策略）由业务方决定；框架只提供通用机制。
type ILocker interface {
	// TryLock 非阻塞获取：拿不到立即返回 ErrLocked。
	TryLock(ctx context.Context, key string, ttl time.Duration) (ILock, error)
	// Lock 阻塞获取：在 wait 内持续重试，超时返回 ErrTimeout；wait <= 0 等价于 TryLock。
	Lock(ctx context.Context, key string, ttl, wait time.Duration) (ILock, error)
}

// IRedisAtomic 分布式锁所需的 Redis 原子操作最小集。
// 由 lock 模块自己定义（消费方接口），而非依赖 cache 模块：任何 redis 客户端封装
// （如 cache 模块的 IRedisCache，或单机/集群 go-redis client）只要实现这两个方法即可复用。
// 注意：锁只做单 key 操作，保证在 Redis Cluster 下脚本落在同一 slot、可原子执行。
type IRedisAtomic interface {
	// SetNX 仅当 key 不存在时写入并设过期，返回是否写入成功。
	SetNX(ctx context.Context, key string, val interface{}, expire time.Duration) (bool, error)
	// Eval 执行 Lua 脚本，数字结果以 int64 返回。
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error)
}
