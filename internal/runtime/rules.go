package runtime

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/alibaba/sentinel-golang/core/circuitbreaker"
	"github.com/alibaba/sentinel-golang/core/flow"
	"github.com/alibaba/sentinel-golang/core/isolation"
	"github.com/alibaba/sentinel-golang/core/system"
)

const maxRuleBytes = 1 << 20

// ruleUpdate 是已完成整体校验、尚未交给 SDK 的单类规则快照。
type ruleUpdate struct {
	count int
	apply func() error
}

var ruleKinds = []string{"flow", "circuitBreaker", "isolation", "system"}

// ruleStateMu 统一串行提交、清理规则，并允许诊断在读锁内取得一致快照。
var ruleStateMu sync.RWMutex

// commit 在同一规则状态锁下应用已完成校验的快照，不改变其他类别的规则。
func (u ruleUpdate) commit() error {
	ruleStateMu.Lock()
	defer ruleStateMu.Unlock()
	return u.apply()
}

// parseRules 严格验证整个 JSON 数组，任意一项非法都不会替换现有保护规则。
func parseRules(kind string, body []byte) (ruleUpdate, error) {
	switch kind {
	case "flow":
		return decodeRules(body, func(rule *flow.Rule) error {
			if rule == nil {
				return errors.New("nil rule")
			}
			if rule.TokenCalculateStrategy < flow.Direct || rule.TokenCalculateStrategy > flow.MemoryAdaptive || rule.ControlBehavior < flow.Reject || rule.ControlBehavior > flow.Throttling {
				return errors.New("unknown strategy")
			}
			return flow.IsValidRule(rule)
		}, flow.LoadRules)
	case "circuitBreaker":
		return decodeRules(body, func(rule *circuitbreaker.Rule) error {
			if rule == nil || rule.Strategy > circuitbreaker.ErrorCount {
				return errors.New("unknown strategy")
			}
			return circuitbreaker.IsValidRule(rule)
		}, circuitbreaker.LoadRules)
	case "isolation":
		return decodeRules(body, isolation.IsValidRule, isolation.LoadRules)
	case "system":
		return decodeRules(body, func(rule *system.Rule) error {
			// SDK 未指定策略时使用 0，保留该默认语义，同时拒绝其他未定义值。
			if rule == nil || (rule.Strategy != 0 && rule.Strategy != system.NoAdaptive && rule.Strategy != system.BBR) {
				return errors.New("unknown strategy")
			}
			return system.IsValidSystemRule(rule)
		}, system.LoadRules)
	default:
		return ruleUpdate{}, errors.New("sentinel: 未知规则类型")
	}
}

// decodeRules 拒绝 null、对象、混合非法项及未知字段；明确的 [] 才表示清空一类规则。
func decodeRules[T any](body []byte, validate func(*T) error, load func([]*T) (bool, error)) (ruleUpdate, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 || len(body) > maxRuleBytes || body[0] != '[' {
		return ruleUpdate{}, errors.New("sentinel: 规则必须是非空 JSON 数组文本且不能超过 1 MiB")
	}
	var rules []*T
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&rules); err != nil {
		return ruleUpdate{}, errors.New("sentinel: JSON 规则格式或字段无效")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ruleUpdate{}, errors.New("sentinel: 规则只允许一个 JSON 数组")
	}
	for _, rule := range rules {
		if err := validate(rule); err != nil {
			return ruleUpdate{}, errors.New("sentinel: 规则项或参数校验失败")
		}
	}
	return ruleUpdate{count: len(rules), apply: func() error {
		_, err := load(rules)
		if err != nil {
			return errors.New("sentinel: SDK 应用规则失败")
		}
		return nil
	}}, nil
}

// clearRules 清理本模块接管的四类全局规则，不改变其他 Sentinel 功能模块。
func clearRules() error {
	ruleStateMu.Lock()
	defer ruleStateMu.Unlock()
	return errors.Join(flow.ClearRules(), circuitbreaker.ClearRules(), isolation.ClearRules(), system.ClearRules())
}
