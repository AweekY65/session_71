# leaselock — 完全本地的 Lease 分布式锁模拟器

一个纯本地的 lease（租约）分布式锁模拟器：多客户端以 goroutine 模拟，
锁状态、租约、fencing token 与日志只保存在**本地内存或本地 JSON 文件**中，
不依赖 Redis、etcd、ZooKeeper 或任何外部服务。

## 功能

- `Acquire` / `Renew` / `Release`：每个 lease 都有明确的过期时间（`ExpiresAt`）。
- 同一资源任意时刻最多一个有效 lease；过期租约**不能**续约，只能重新获取。
- 每次成功获取都签发**严格递增的 fencing token**，旧持有者恢复后无法获得更新的 token。
- 时间逻辑全部通过可注入的 `Clock` 接口完成，测试使用 `FakeClock`，**不依赖真实 sleep**。
- 客户端暂停（模拟 GC 停顿 / 网络分区）超过 lease 后恢复：新持有者可正常获锁，
  旧客户端的写入因 fencing token 过时被下游资源拒绝。
- `FileStore` 持久化到本地 JSON 文件（临时文件 + rename 原子写入），
  重启后未过期 lease 仍然有效，**已过期 lease 不会被错误恢复**，token 计数器不回退。

## 代码结构

| 文件 | 说明 |
| --- | --- |
| `clock.go` | `Clock` 接口、`RealClock`、可手动推进的 `FakeClock`（含 ticker） |
| `store.go` | `Lease` 结构、`Store` 接口、`MemoryStore` 与 `FileStore`（JSON 持久化） |
| `service.go` | `LockService`：acquire / renew / release 的核心状态机 |
| `client.go` | `Client`：以 goroutine 后台续约的模拟客户端，支持 `Pause` / `Resume` |
| `resource.go` | `GuardedResource`：执行 fencing 检查的下游资源 |
| `cmd/demo` | 真实时钟的端到端演示 |

## Lease 状态机

```
                 Acquire 成功
        Free ───────────────────► Held
          ▲                       │  │
          │                       │  ├── Release（owner+token 匹配）──► Free
          │                       │  │
          │        过期前 Renew    │  │ 时钟越过 ExpiresAt
          │      （延长 ExpiresAt）◄┘  ▼
          └────────── Acquire ◄── Expired
           （过期记录被覆盖，签发新 token）
```

- **Free → Held**：仅当资源无记录或记录已过期（以注入时钟判断）时允许获取；
  否则返回 `ErrLeaseHeld`。
- **Held → Held**：`Renew` 要求 owner 与 fencing token 完全匹配且**未过期**，
  成功后以 `now + ttl` 刷新 `ExpiresAt`。
- **Held → Free**：`Release` 要求 owner 与 token 匹配。
- **Held → Expired**：不需要任何操作，时间推进自然发生；过期记录续约返回
  `ErrLeaseExpired`，此后任何客户端都可重新 `Acquire`（记录被覆盖）。

## Fencing token 原理

每次 `Acquire` 成功，服务从**持久化的单调计数器**取出新 token（每资源独立、
只增不减、重启不回退）。持有者操作下游资源时必须携带该 token。
`GuardedResource` 记录所见过的最大 token，拒绝任何更小的 token：

```
token < maxSeen(token)  ⇒  ErrStaleToken（拒绝写入）
```

因此即使旧持有者 A 在租约过期、B 已获锁之后恢复，A 也**无法获得更新的 token**，
它携带旧 token 的写入会被下游资源识别并拒绝——锁服务与下游资源双重防护，
避免“脑裂”写入。

## 时间模型

- 所有过期判断都基于 `Clock.Now()`，服务不直接读取系统时间。
- 生产路径使用 `RealClock`；测试注入 `FakeClock`，通过 `Advance(d)` 确定性
  地推进时间并触发 ticker，**测试中没有任何真实 sleep**。
- 客户端续约循环由 `clock.NewTicker` 驱动；`Pause()` 模拟进程冻结（跳过续约），
  `Resume()` 恢复。测试用 `Client.Renewed()` 通道与续约循环同步，
  而非依赖时序巧合。

## 持久化与重启

`FileStore` 把 `{leases, tokens}` 以 JSON 原子写入本地文件。重启时原样加载，
**过期判断延迟到操作发生时用当前时钟进行**：过期记录只是数据，不会被当作有效
lease 恢复；token 计数器持久化保证重启后 token 继续单调递增。

## 运行测试

```bash
go test ./...          # 全部测试
go test -race ./...    # 带竞态检测
go test -v ./...       # 查看各场景
go run ./cmd/demo      # 真实时钟端到端演示
```

测试覆盖：竞争获取（32 goroutine 仅 1 成功）、续约延长、过期租约拒绝续约、
错误 owner/token 防护、释放后重获、token 单调性、客户端暂停超时后恢复
（旧 token 写入被 fencing 拒绝）、暂停短于 TTL 不丢锁、重启保留有效 lease、
重启不恢复过期 lease、token 计数器跨重启单调。
