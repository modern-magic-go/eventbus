# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-11-18

对应 [issue #1](https://github.com/modern-magic-go/eventbus/issues/1)（v0.1.1 可靠性 review）的 P0 修复。

### Added
- `ConsumerConfig.PendingReclaim` 与 `PendingReclaimConfig`（`MinIdle` / `Batch` / `Interval`），支持 pending 消息自动回收；零值禁用，任一字段非零即启用
- 可选接口 `PendingReclaimer`（`ClaimPending`），根包 re-export；`RedisRuntime` 基于 Redis 6.2+ 的 `XAUTOCLAIM` 实现
- `ErrPendingReclaimUnsupported`：启用 `PendingReclaim` 但运行时未实现 `PendingReclaimer` 时，`Subscribe` 返回该错误
- `RetryTask.MessageID` / `DeadLetterMessage.MessageID`：重试与死信消息保留原始消息 ID，便于审计与追踪
- `ErrHandlerPanic`：handler panic 被 recover 后返回的错误，可用 `errors.Is` 判断
- `DefaultReclaimMinIdle`（30s）、`DefaultReclaimInterval`（10s）默认值常量
- `StopWithTimeout(timeout)` 支持优雅停机超时控制
- `WithCloseRuntime(bool)` 选项控制 Stop 时是否关闭底层 Runtime
- `examples/` 目录提供完整可运行示例

### Changed
- **Breaking**: 失败处理语义变更：handler 失败（含 panic）不再立即 `XACK`，而是先写重试/死信任务（达到 `MaxRetries` 时写 DLQ），任务写入成功后才 `XACK` 原 stream 消息（所有权转移，避免 PEL 泄漏）；任务写入失败则不 `XACK`，留给 reclaim 或人工处理
- handler panic 不再导致进程退出，统一走失败处理路径（retry → DLQ），并触发 `OnHandleFailed`（首次投递与 pending 认领路径；retry worker 路径保持基线行为）
- `RedisRuntime` 默认 consumer name 在构造时生成一次（`eventbus-<pid>-<随机后缀>`），同一 Runtime 生命周期内保持稳定
- README 补充投递语义、pending 回收、已知限制章节；`examples/basic` 修复 module 路径并加入 pending 回收配置示例
- `Stop()` 默认 30s 超时等待 worker 结束
- 升级注记：v0.1.1 已入队的 `RetryTask` 不含 `message_id`，升级后这些旧任务重试时 `Message.ID` 为空、进入 DLQ 时 `MessageID` 为空；旧任务处理完后恢复

### Fixed
- 修复消费者崩溃/重启后 pending 消息永久滞留 PEL、无副本接管的问题
- 修复 handler 失败后原消息未 `XACK`，导致 PEL 泄漏且与重试任务重复消费的问题
- 修复 handler panic 导致整个进程退出的问题
- 修复 `RetryTask` / `DeadLetterMessage` 丢失原始消息 ID 的问题
- 修复 pending 认领丢弃 `XAUTOCLAIM` 返回的 next cursor、PEL 尾部消息可能长期无法被认领的问题
- 修复 retry worker 在 `ScheduleRetry` / `MoveToDeadLetter` 失败时仍删除旧重试任务、可能导致消息丢失的问题
- 修正 `Message.PublishedAt` 语义：消费时取 stream 中记录的发布时间，而不是消费时刻（重试路径仍为当前时间；属可观测行为变化）
- `PollRetry` 使用 `ZRangeArgs` 替代已废弃的 `ZRangeByScore`
- `cloneHeaders` 使用 `maps.Copy` 替代手动循环
