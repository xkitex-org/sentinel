// Package sentinel 提供显式接入 Kitex 的 Sentinel 入站保护与 Consul 规则动态更新。
// 引入包和 New 不读取应用配置、查询 Consul 或启动规则监听。
package sentinel

import (
	"context"

	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/hashicorp/consul/api"
	"go.chaoxizhongzhi.com/xkitex/sentinel/internal/runtime"
)

// Controller 是供 core 管理的接入协议，沿用已经校验的 Consul 客户端和应用服务名。
type Controller interface {
	ConfigureSentinel([]byte) error
	Start(context.Context, *api.Client, string) error
	Middleware() endpoint.Middleware
	Close() error
}

// New 创建独立控制器，基础配置与全局 SDK 资源只在显式启动时处理。
func New() Controller { return runtime.New() }
