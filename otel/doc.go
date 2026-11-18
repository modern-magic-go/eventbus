// Package otel 提供 eventbus 的可选 OpenTelemetry（OTel）链路追踪集成。
//
// # 独立子模块
//
// 本目录是独立 Go module：github.com/modern-magic-go/eventbus/otel。它
// require 上级 eventbus 模块并通过 replace ../ 指向本地目录，而 eventbus
// 根模块的 go.mod 不包含任何 OTel 依赖。因此本包是严格可选的：不导入它的
// 应用既不会下载也不会编译 OTel。
//
// # 提供的能力
//
//   - InjectTrace：把当前 context 中的 trace context 按配置的传播格式
//     （通常为 W3C TraceContext，即 traceparent/tracestate）写入消息
//     headers；
//   - ExtractTrace：从消息 headers 恢复 trace context 到新的 context；
//   - Publish：在 producer span 内调用 eventbus.Bus.Publish，自动注入
//     traceparent 并透传调用方的业务 headers；
//   - WrapHandler：把 eventbus.Handler 包成带 consumer span 的 handler，
//     从 msg.Headers 恢复上游 trace context，形成 producer → consumer 的
//     完整链路。
//
// # 配置
//
// WrapHandler 通过 Option 配置（WithTracerProvider、WithPropagator、
// WithSpanNameFormatter），配置在包装时固定。InjectTrace、ExtractTrace 与
// Publish 使用 OTel 全局值（otel.GetTracerProvider 与
// otel.GetTextMapPropagator），在调用时读取。
//
// 注意：OTel Go 的全局 TextMapPropagator 在应用未显式设置时不注入任何字段。
// 因此接入方必须在进程启动时配置一次传播器，否则 traceparent 不会被注入：
//
//	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
//		propagation.TraceContext{}, propagation.Baggage{},
//	))
//
// TracerProvider 同理，通常通过 otel.SetTracerProvider 或 OTel SDK 的初始化
// 代码配置。本包不会在 init 中修改任何全局状态。
//
// # Header 约定
//
// 本包只负责 OTel 传播字段：常见配置为 W3C TraceContext 的
// traceparent/tracestate，启用 Baggage 时还包括 baggage。event_type、event_version、aggregate_id、
// dedup_key 等业务 header 由调用方（业务层）定义和维护，本包不会写入、覆盖
// 或解释它们；Publish 会复制调用方传入的 headers，其中所有业务键都会原样
// 保留。
package otel
