package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/hashicorp/consul/api"
)

// Controller 持有单次基础配置和四个规则监听任务，SDK 的进程级归属由 sdkState 管理。
type Controller struct {
	mu                                   sync.RWMutex
	cfg                                  settings
	configured, started, running, closed bool
	service                              string
	client                               *api.Client
	cancel                               context.CancelFunc
	wg                                   sync.WaitGroup
	closeDone                            chan struct{}
	closeErr                             error
}

// New 只分配控制状态，不读取环境或 Consul，不初始化 SDK。
func New() *Controller { return &Controller{closeDone: make(chan struct{})} }

// ConfigureSentinel 严格接收一次合并后的 sentinel 节。
func (c *Controller) ConfigureSentinel(body []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.configured || c.closed {
		return errors.New("sentinel: 配置只允许在启动前应用一次")
	}
	cfg, err := parseSettings(body)
	if err != nil {
		return err
	}
	c.cfg, c.configured = cfg, true
	return nil
}

// Start 先读取全部初始规则，再整体校验、应用并启动监听，失败不会进入业务保护中间件。
func (c *Controller) Start(ctx context.Context, client *api.Client, service string) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.configured || c.started || c.closed {
		return errors.New("sentinel: 启动生命周期无效")
	}
	if ctx == nil || client == nil || !validService.MatchString(service) {
		return errors.New("sentinel: context、Consul 客户端和服务名必须明确提供")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	initial := make([]ruleSnapshot, len(ruleKinds))
	for i, kind := range ruleKinds {
		key := c.cfg.RulePrefix + "/" + kind
		initial[i], err = readRule(ctx, client, key, 0, 0)
		if err != nil {
			return fmt.Errorf("sentinel: 初始规则读取失败：key=[%s]：%w", key, err)
		}
	}
	if err := acquireSDK(c, service); err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			err = errors.Join(err, releaseSDK(c))
		}
	}()
	updates := make([]ruleUpdate, len(ruleKinds))
	for i, kind := range ruleKinds {
		updates[i], err = parseRules(kind, initial[i].body)
		if err != nil {
			return fmt.Errorf("sentinel: 初始规则校验失败：key=[%s/%s]：%w", c.cfg.RulePrefix, kind, err)
		}
	}
	for i, update := range updates {
		if err := update.commit(); err != nil {
			return err
		}
		klog.Infof("sentinel: 初始规则已生效：service=[%s] key=[%s/%s] rules=[%d]", service, c.cfg.RulePrefix, ruleKinds[i], update.count)
	}
	watchCtx, cancel := context.WithCancel(ctx)
	c.client, c.service, c.cancel = client, service, cancel
	c.started, c.running = true, true
	for i, kind := range ruleKinds {
		c.wg.Add(1)
		go func() { defer c.wg.Done(); c.watch(watchCtx, kind, initial[i]) }()
	}
	success = true
	klog.Infof("sentinel: 规则监听已启动：service=[%s] prefix=[%s]", service, c.cfg.RulePrefix)
	return nil
}

// Close 取消并等待全部 Consul 请求，再清理规则、释放 SDK 归属；并发关闭等待同一次清理。
func (c *Controller) Close() error {
	c.mu.Lock()
	if c.closed {
		done := c.closeDone
		c.mu.Unlock()
		<-done
		c.mu.RLock()
		defer c.mu.RUnlock()
		return c.closeErr
	}
	c.closed, c.running = true, false
	cancel, started, service := c.cancel, c.started, c.service
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.wg.Wait()
	err := releaseSDK(c)
	if started {
		klog.Infof("sentinel: 规则监听已停止：service=[%s]", service)
	}
	c.mu.Lock()
	c.closeErr = err
	close(c.closeDone)
	c.mu.Unlock()
	return err
}
