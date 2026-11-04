package runtime

import (
	"errors"
	"sync"

	"github.com/alibaba/sentinel-golang/core/config"
	"github.com/alibaba/sentinel-golang/core/system_metric"
	"github.com/alibaba/sentinel-golang/logging"
)

// sdkState 维护 SDK 的进程级归属，拒绝多个运行实例相互覆盖规则。
var sdkState = struct {
	sync.Mutex
	owner   *Controller
	once    sync.Once
	err     error
	service string
}{}

// acquireSDK 使用 SDK 公开配置和采集器接口初始化一次，避开高层 Init 的环境覆盖与文件日志。
func acquireSDK(owner *Controller, service string) error {
	sdkState.Lock()
	defer sdkState.Unlock()
	if sdkState.owner != nil {
		return errors.New("sentinel: 进程中已有运行中的 Sentinel 控制器")
	}
	sdkState.once.Do(func() {
		cfg := config.NewDefaultConfig()
		// SDK 身份来自 core 的启动快照，不能被 SDK 环境变量覆盖。
		cfg.Sentinel.App.Name = service
		cfg.Sentinel.Log.Logger = sdkLogger{}
		cfg.Sentinel.Log.Metric.FlushIntervalSec = 0
		cfg.Sentinel.Exporter.Metric.HttpAddr = ""
		if err := config.CheckValid(cfg); err != nil {
			sdkState.err = errors.New("sentinel: SDK 默认配置无效")
			return
		}
		config.ResetGlobalConfig(cfg)
		if err := logging.ResetGlobalLogger(sdkLogger{}); err != nil {
			sdkState.err = errors.New("sentinel: SDK 日志接入失败")
			return
		}
		// SDK 采集器自身由 sync.Once 管理，生命周期为整个进程，不能按服务反复启动或停止。
		system_metric.InitLoadCollector(cfg.Sentinel.Stat.System.CollectLoadIntervalMs)
		system_metric.InitCpuCollector(cfg.Sentinel.Stat.System.CollectCpuIntervalMs)
		system_metric.InitMemoryCollector(cfg.Sentinel.Stat.System.CollectMemoryIntervalMs)
		sdkState.service = service
	})
	if sdkState.err != nil {
		return sdkState.err
	}
	if sdkState.service != service {
		return errors.New("sentinel: SDK 已绑定其他应用身份，请使用独立进程")
	}
	sdkState.owner = owner
	return nil
}

// releaseSDK 在监听已停止、RPC 已排空后清理本模块规则并释放归属，不重复初始化 SDK。
func releaseSDK(owner *Controller) error {
	sdkState.Lock()
	defer sdkState.Unlock()
	if sdkState.owner != owner {
		return nil
	}
	err := clearRules()
	sdkState.owner = nil
	return err
}
