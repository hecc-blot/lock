# hecc-blot-lock

分布式锁模块：Redis / 内存两种后端，支持阻塞获取、自动续期与手动续期。

## 安装

```bash
go get github.com/hecc-blot/lock
```

## 快速开始

```go
import (
	"context"
	"errors"
	"time"

	cacheContract "github.com/hecc-blot/cache/contract"
	lockContract "github.com/hecc-blot/lock/contract"
	lock "github.com/hecc-blot/lock/service"
)

func main() {
	// 1. 构造（无 cleanup、无 error）
	//    窄接口 IRedisAtomic 由 cache 的 IRedisCache 直接满足，复用同一个 Redis 连接
	locker := lock.NewRedisLocker(cacheFactory.Redis(), lock.WithAutoRenew())

	// 2. 注册到容器（须早于路由注册）
	container.Set(new(lockContract.ILocker), locker)

	// 3. 使用
	ctx := context.Background()
	lk, err := locker.TryLock(ctx, "order:1001", 30*time.Second)
	switch {
	case errors.Is(err, lockContract.ErrLocked):
		return // 已被占用，快速失败
	case err != nil:
		panic(err)
	}
	// ⚠️ 只有拿到锁才 Unlock —— 失败时 lk 是 nil，直接 defer 会 panic
	defer lk.Unlock(context.Background())

	// ... 临界区
}
```

完整的应用组装顺序（日志 → trace → cache → lock → 容器 → 路由）见[组织主页](https://github.com/hecc-blot)。

## 接口定义

```go
import lockContract "github.com/hecc-blot/lock/contract"

type ILocker interface {
	// TryLock 非阻塞获取：拿不到立即返回 ErrLocked
	TryLock(ctx context.Context, key string, ttl time.Duration) (ILock, error)
	// Lock 阻塞获取：在 wait 内持续重试，超时返回 ErrTimeout；wait <= 0 等价于 TryLock
	Lock(ctx context.Context, key string, ttl, wait time.Duration) (ILock, error)
}

type ILock interface {
	// Unlock 释放锁。重复调用或锁已过期时返回 ErrNotHeld
	Unlock(ctx context.Context) error
	// Renew 手动续期，把过期时间重置为加锁时的 ttl；锁已丢失返回 ErrNotHeld
	Renew(ctx context.Context) error
}

// 语义错误，用 errors.Is 判定
var (
	ErrLocked  = errors.New("lock: 已被其他持有者占用")
	ErrTimeout = errors.New("lock: 等待超时")
	ErrNotHeld = errors.New("lock: 锁未被当前持有者持有")
)
```

## 使用

### 构造

```go
// Redis 后端：复用 cache 的 Redis 连接
locker := lock.NewRedisLocker(cacheFactory.Redis(), lock.WithAutoRenew())

// 或注入自己的 redis 客户端封装（只要实现 SetNX + Eval 两个方法）
locker := lock.NewRedisLocker(myRedisAtomic)

// 内存后端：单机场景 / 单测
locker := lock.NewMemoryLocker()
```

> `IRedisAtomic` 是 lock 模块**自己定义**的窄接口（消费方接口），不依赖 cache 模块。任何 Redis 客户端封装只要实现 `SetNX` + `Eval` 即可复用，包括 `cache` 的 `IRedisCache` 或自建的 go-redis 封装。
>
> 锁只做单 key 操作，Lua 脚本在 Redis Cluster 下落在同一 slot、可原子执行。

### 非阻塞获取

```go
lk, err := locker.TryLock(ctx, "order:1001", 30*time.Second)
if errors.Is(err, lockContract.ErrLocked) {
	return // 已占用，快速失败（lk 为 nil，不要 Unlock）
}
if err != nil {
	return err
}
defer lk.Unlock(context.Background()) // 用独立 ctx，避免请求 ctx 取消导致释放失败
```

### 阻塞获取

```go
lk, err := locker.Lock(ctx, "order:1001", 30*time.Second, 5*time.Second)
if errors.Is(err, lockContract.ErrTimeout) {
	return // 等待 5s 仍未拿到
}
if err != nil {
	return err
}
defer lk.Unlock(context.Background())
```

### 续期

长任务超过 TTL 时，锁会在中途过期。两种续期方式：

```go
// 方式一：构造时开启自动续期（后台按 ttl/3 定时续期）
locker := lock.NewRedisLocker(atomic, lock.WithAutoRenew())

// 方式二：手动续期
if err := lk.Renew(ctx); errors.Is(err, lockContract.ErrNotHeld) {
	// 锁已丢失（超时 / 被他人抢走），应中止临界区
}
```

## 选项

| 选项 | 默认 | 说明 |
|------|------|------|
| `WithRetryInterval(d)` | 50ms | `Lock` 阻塞获取时的重试间隔 |
| `WithAutoRenew()` | 关闭 | 后台自动续期，周期为 ttl 的 1/3 |

## 语义与约定

- **失败时返回 nil 锁**：`TryLock` / `Lock` 出错时第一个返回值为 nil。**必须在错误分支 return，不要无条件 `defer lk.Unlock(...)`**。
- **`Unlock` 用独立 ctx**：释放动作不应受请求 ctx 取消影响，否则请求超时会导致锁泄漏到 TTL 到期。
- **`Unlock` / `Renew` 不是幂等的**：重复解锁、锁已过期、token 不匹配都返回 `ErrNotHeld`（这恰恰是判断「我是否还持锁」的手段）。
- **内存后端语义有差异**：不做过期处理，**TTL 不生效**；`Renew` 恒为无操作（返回 nil）。仅适合单机与单测。
- **panic 条件**：`NewRedisLocker(nil, ...)` 会 panic。
- **无释放语义**：`ILocker` 不持有需关闭的资源（Redis 连接由 cache 侧管理），不需要 cleanup。

## 相关模块

| 模块 | 说明 |
|------|------|
| [cache](https://github.com/hecc-blot/cache) | `IRedisCache` 直接满足 `IRedisAtomic`，复用同一 Redis 连接 |
| [scheduler](https://github.com/hecc-blot/scheduler) | 多副本执行守卫 `IJobGuard` 可基于本模块实现 |
