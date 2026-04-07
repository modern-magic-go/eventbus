# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `StopWithTimeout(timeout)` 支持优雅停机超时控制
- `WithCloseRuntime(bool)` 选项控制 Stop 时是否关闭底层 Runtime
- `examples/` 目录提供完整可运行示例

### Changed
- **Breaking**: 消费失败时不再立即 Ack，消息交由重试/死信机制处理
- `Stop()` 默认 30s 超时等待 worker 结束

### Fixed
- `PollRetry` 使用 `ZRangeArgs` 替代已废弃的 `ZRangeByScore`
- `cloneHeaders` 使用 `maps.Copy` 替代手动循环
