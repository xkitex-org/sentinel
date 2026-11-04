package runtime

import "github.com/cloudwego/kitex/pkg/klog"

// sdkLogger 将 Sentinel 内部诊断交给 klog，不输出完整规则、配置值或原始错误正文。
type sdkLogger struct{}

// Debug 输出固定调试消息，不回显 SDK 附带的配置值。
func (sdkLogger) Debug(string, ...interface{}) { klog.Debug("sentinel: SDK 调试诊断") }

// DebugEnabled 保留 SDK 的调试级别检查，由统一 klog 实现决定实际输出。
func (sdkLogger) DebugEnabled() bool { return false }

// Info 内部规则成功日志由框架按 Key 和规则数量记录，避免 SDK 重复输出全部规则。
func (sdkLogger) Info(string, ...interface{}) {}

// InfoEnabled 关闭 SDK 重复的信息日志。
func (sdkLogger) InfoEnabled() bool { return false }

// Warn 将 SDK 警告转入统一日志，不输出规则内容。
func (sdkLogger) Warn(string, ...interface{}) { klog.Warn("sentinel: SDK 内部警告") }

// WarnEnabled 启用安全警告输出。
func (sdkLogger) WarnEnabled() bool { return true }

// Error 输出安全诊断，详细业务错误仍由原调用链返回。
func (sdkLogger) Error(error, string, ...interface{}) { klog.Error("sentinel: SDK 内部操作失败") }

// ErrorEnabled 启用安全错误输出。
func (sdkLogger) ErrorEnabled() bool { return true }
