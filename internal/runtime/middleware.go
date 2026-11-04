package runtime

import (
	"context"
	"errors"

	"github.com/alibaba/sentinel-golang/api"
	"github.com/alibaba/sentinel-golang/core/base"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
)

// Middleware 以应用名/IDL Service/方法名建立入站资源，阻断时不调用业务 Handler。
func (c *Controller) Middleware() endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) error {
			c.mu.RLock()
			if !c.running {
				c.mu.RUnlock()
				return errors.New("sentinel: 控制器尚未启动或已经停止")
			}
			if ctx == nil {
				c.mu.RUnlock()
				return errors.New("sentinel: context 不能为空")
			}
			entry, block := api.Entry(resourceName(ctx, c.service), api.WithResourceType(base.ResTypeRPC), api.WithTrafficType(base.Inbound))
			c.mu.RUnlock()
			if block != nil {
				return block
			}
			defer entry.Exit()
			defer func() {
				if failure := recover(); failure != nil {
					api.TraceError(entry, errors.New("业务处理 panic"))
					panic(failure)
				}
			}()
			err := next(ctx, req, resp)
			if err != nil {
				api.TraceError(entry, err)
			}
			return err
		}
	}
}

// resourceName 使用 core 已校验的应用身份，避免客户端目标别名改变保护资源名。
func resourceName(ctx context.Context, application string) string {
	service, method := "unknown", "unknown"
	if info := rpcinfo.GetRPCInfo(ctx); info != nil && info.Invocation() != nil {
		if name := info.Invocation().ServiceName(); name != "" {
			service = name
		}
		if name := info.Invocation().MethodName(); name != "" {
			method = name
		}
	}
	return application + "/" + service + "/" + method
}
