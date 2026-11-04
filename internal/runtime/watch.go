package runtime

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/cloudwego/kitex/pkg/klog"
	"github.com/hashicorp/consul/api"
)

// ruleSnapshot 记录一个规则 Key 的 Consul 阻塞查询索引和正文，不写入日志。
type ruleSnapshot struct {
	body  []byte
	index uint64
}

// readRule 使用 core 提供的同一 SDK 客户端，错误不回显 ACL、响应正文或规则内容。
func readRule(ctx context.Context, client *api.Client, key string, index uint64, wait time.Duration) (ruleSnapshot, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	pair, meta, err := client.KV().Get(key, (&api.QueryOptions{WaitIndex: index, WaitTime: wait}).WithContext(queryCtx))
	if err != nil {
		if ctx.Err() != nil {
			return ruleSnapshot{}, ctx.Err()
		}
		return ruleSnapshot{}, errors.New("Consul 请求失败")
	}
	if meta == nil || meta.LastIndex == 0 {
		return ruleSnapshot{}, errors.New("Consul 规则索引缺失")
	}
	value := ruleSnapshot{index: meta.LastIndex}
	if pair != nil {
		value.body = pair.Value
	}
	if len(value.body) == 0 || len(value.body) > maxRuleBytes {
		return value, errors.New("规则缺失、为空或超过 1 MiB；无规则请写入 []")
	}
	return value, nil
}

// watch 用阻塞查询监听一类规则，所有失败均保留上一版有效规则，不重新加载 application.yaml。
func (c *Controller) watch(ctx context.Context, kind string, initial ruleSnapshot) {
	key := c.cfg.RulePrefix + "/" + kind
	index := initial.index
	lastGood := sha256.Sum256(initial.body)
	failed := false
	for ctx.Err() == nil {
		start := time.Now()
		current, err := readRule(ctx, c.client, key, index, c.cfg.wait)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if current.index > 0 && current.index < index {
				index = 0
			} else if current.index > index {
				index = current.index
			}
			if !failed {
				klog.Warnf("sentinel: 规则读取失败，保留当前规则：service=[%s] key=[%s]", c.service, key)
				failed = true
			}
			if !pause(ctx, c.cfg.retry) {
				return
			}
			continue
		}
		if current.index < index {
			index = 0
			continue
		}
		if current.index == index {
			// 真实 Consul 会阻塞等待；对立即返回的异常代理或服务端限速，避免空转。
			if time.Since(start) < 100*time.Millisecond && !pause(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		index = current.index
		digest := sha256.Sum256(current.body)
		if digest == lastGood {
			if failed {
				klog.Infof("sentinel: 规则监听已恢复：service=[%s] key=[%s]", c.service, key)
			}
			failed = false
			continue
		}
		update, err := parseRules(kind, current.body)
		if err == nil && ctx.Err() == nil {
			err = update.commit()
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			klog.Warnf("sentinel: 规则更新无效，保留当前规则：service=[%s] key=[%s] index=[%d]", c.service, key, index)
			failed = true
			continue
		}
		lastGood = digest
		failed = false
		klog.Infof("sentinel: 规则已更新：service=[%s] key=[%s] index=[%d] rules=[%d]", c.service, key, index, update.count)
	}
}

// pause 在错误退避期间响应取消，不额外创建等待协程。
func pause(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
