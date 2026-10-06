# leaselock — 完全本地的 Lease 分布式锁模拟器

一个纯本地实现的 lease（租约）分布式锁模拟器。多客户端以 goroutine 模拟，
锁状态、租约、fencing token 与日志只保存在**内存或本地 JSON 文件**中，
不依赖 Redis、etcd、ZooKeeper 或任何外部服务。

## 目录结构

```
lease/          锁服务核心：租约状态机、fencing token、可注入时钟、持久化
client/         模拟客户端：可暂停/恢复，演示旧持有者恢复场景
cmd/sim/        确定性演示程序（ManualClock，无真实 sleep）
```

## Lease 状态机

每个资源（resource）的租约处于以下状态之一：

```
                 Acquire 成功                 Release
   (无租约)  ──────────────────►  HELD ──────────────────► (无租约)
                  ▲                │
                  │                │ now >= ExpiresAt
                  │                ▼
                  └──────────  EXPIRED ──► (视为无租约，可被他人 Acquire)
```

- **Acquire**：仅当资源无有效租约（不存在或已过期）时成功，返回携带
  fencing token 的租约，过期时间为 `now + TTL`。
- **Renew**：仅当租约未过期、且 holder 与 token 均匹配时成功，
  过期时间顺延为 `now + TTL`。**过期租约永远不可续期**。
- **Release**：holder 与 token 匹配时删除租约；释放已过期租约为无害空操作。

同一资源任意时刻至多存在一个有效 lease；不同资源互不影响。

## Fencing token 原理

仅靠租约无法防止"暂停-恢复"问题：持有者 A 被 GC/网络暂停超过 TTL，
租约过期，B 获得锁；A 恢复后仍以为自己持有锁，若直接写下游资源会造成
双写损坏。解决方案：

1. 锁服务每次成功 Acquire 发出**严格递增**的 fencing token
   （单调计数器，随状态一起持久化，重启后不回退）。
2. 下游资源（`lease.FencedStore`）记录每个资源见过的最大 token，
   只接受 token 更大的写请求。
3. A 恢复后携带旧 token 的写被 `ErrStaleFencingToken` 拒绝，
   旧持有者的续期也因租约已易主/过期而失败。

## 时间模型

所有时间判断通过可注入的 `lease.Clock` 接口完成：

- `lease.RealClock`：生产环境的墙钟。
- `lease.ManualClock`：测试/演示用，通过 `Advance(d)` 显式推进。

**测试不使用任何真实 sleep**：租约超时、客户端暂停等场景都通过
推进 `ManualClock` 模拟，测试确定且瞬时完成。

## 持久化与重启

`lease.NewService(clock, path)` 传入非空路径即启用持久化：
每次状态变更以"临时文件 + rename"原子写入 JSON（含租约表与 token 计数器）。
重启加载时，**已过期租约会被直接丢弃，不会被错误恢复**；
token 计数器恢复后继续递增，保证 fencing token 全局单调。

## 运行

```bash
# 全部测试（互斥、竞争获取、续约、超时、旧客户端恢复、
# token 单调性、释放后重获、重启恢复等场景）
go test ./... -v -count=1

# 带竞态检测
go test -race ./... -count=1

# 确定性演示：暂停-恢复 + fencing 全流程
go run ./cmd/sim
```

## 测试覆盖

| 测试 | 场景 |
| --- | --- |
| `TestAcquireMutualExclusion` | 同一资源互斥，不同资源独立 |
| `TestConcurrentAcquireExactlyOneWinner` | 32 个 goroutine 竞争，恰好一个获胜 |
| `TestRenewExtendsLease` | 续期顺延过期时间 |
| `TestRenewAfterExpiryFails` | 过期租约不可续期 |
| `TestRenewWrongHolderOrTokenFails` | holder/token 校验 |
| `TestReleaseThenReacquire` | 释放后可被他人重获，token 递增 |
| `TestFencingTokensStrictlyIncreasing` | token 严格单调递增 |
| `TestFencedStoreRejectsStaleToken` | 下游存储拒绝旧 token 写入 |
| `TestPausedClientLosesLeaseAndIsFencedOut` | 客户端暂停超期恢复：新持有者得锁，旧客户端被 fence |
| `TestClientRenewKeepsLeaseAlive` | 健康客户端持续续约保持锁 |
| `TestPersistenceSurvivesRestart` | 重启后有效租约与 token 计数器保留 |
| `TestExpiredLeaseNotRestoredAfterRestart` | 重启后不恢复已过期租约 |
