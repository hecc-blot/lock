# hecc-blot-lock

面向接口的分布式锁组件：SetNX 加锁 + Lua 原子解锁/续期，token 防误删，支持阻塞获取与自动续期（watchdog）。提供 Redis 与内存两种后端，通过 IOC 按需加载。

## 安装

```bash
go get github.com/hecc-blot/lock
```

## 接口定义

```go
import (
    "context"
    lockContract "github.com/hecc-blot/lock/contract"
)

type ILock interface {
    Unlock(ctx context.Context) error
    Renew(ctx context.Context) error
}

type ILocker interface {
    TryLock(ctx context.Context, key string, ttl time.Duration) (ILock, error)
    Lock(ctx context.Context, key string, ttl, wait time.Duration) (ILock, error)
}
```

## 语义错误

| 错误 | 含义 |
|------|------|
| `lockContract.ErrLocked` | 锁已被其他持有者占用（TryLock 失败） |
| `lockContract.ErrTimeout` | 阻塞获取在等待时间内未成功 |
| `lockContract.ErrNotHeld` | 锁未被当前持有者持有（重复解锁 / 已过期 / 续期失败） |

用 `errors.Is(err, lockContract.ErrLocked)` 判定。

## 后端

| 后端 | 构造方式 | 适用场景 |
|------|---------|---------|
| Redis | `lockSvc.NewRedisLocker(atomic, opts...)` | 跨进程互斥（默认首选） |
| 内存 | `lockSvc.NewMemoryLocker(opts...)` | 单进程互斥 / 单测 |

Redis 后端通过窄接口 `IRedisAtomic`（只需 `SetNX` + `Eval`）依赖后端，**不直接依赖 cache / go-redis**：任何实现了这两个方法的封装（如 cache 模块的 `IRedisCache`，或单机/集群 go-redis client）都可复用。锁只做单 key 操作，在 Redis Cluster 下脚本落在同一 slot、可原子执行，因此后端从单机升集群时锁无需改动。

## 使用

### 1. 构造锁（按需加载，复用 cache 的 redis 连接）

```go
import (
    lockContract "github.com/hecc-blot/lock/contract"
    lockSvc "github.com/hecc-blot/lock/service"
)

// cacheFactory.Redis() 返回 cache 的 IRedisCache，已实现 SetNX/Eval，满足 IRedisAtomic
locker := lockSvc.NewRedisLocker(cacheFactory.Redis(), lockSvc.WithAutoRenew())
container.Set(new(lockContract.ILocker), locker)
```

### 2. 业务使用

```go
lk, err := locker.TryLock(ctx, "order:1001", 30*time.Second)
if errors.Is(err, lockContract.ErrLocked) {
    // 已被占用，快速失败
}
defer lk.Unlock(context.Background()) // 用独立 ctx，避免请求 ctx 取消导致释放失败

// 临界区业务逻辑（长任务可 lk.Renew(ctx) 手动续期，或构造时 WithAutoRenew 自动续期）

// 阻塞获取：等待直到拿到锁或超时
lk2, err := locker.Lock(ctx, "order:1001", 30*time.Second, time.Second)
if errors.Is(err, lockContract.ErrTimeout) {
    // 等待超时
}
defer lk2.Unlock(context.Background())
```

## 选项

| 选项 | 说明 |
|------|------|
| `WithRetryInterval(d)` | 阻塞获取的重试间隔，默认 50ms |
| `WithAutoRenew()` | 开启自动续期（watchdog），以 ttl/3 为周期续期，适用于临界区执行时间可能超过 ttl 的长任务 |

## 一致性说明

单实例 / 集群 Redis 锁是「尽力而为」的互斥（failover 时可能丢锁），适用于去重、限流、防重复触发；涉及资金、库存等正确性敏感场景，改用 etcd / DB / Redlock 等强一致后端。框架只提供机制，锁的语义（锁什么 key、ttl 多长、失败策略）由业务方决定。

## 相关模块

| 模块 | 说明 |
|------|------|
| [cache](https://github.com/hecc-blot/cache) | `IRedisCache` 已实现 `SetNX`/`Eval`，可作为 `IRedisAtomic` 复用连接 |
| [scheduler](https://github.com/hecc-blot/scheduler) | 多副本守卫 `IJobGuard` 可基于本模块实现 |
