package runtime

import (
	"bytes"
	"errors"
	"gopkg.in/yaml.v3"
	"io"
	"path"
	"regexp"
	"strings"
	"time"
)

// settings 只保存一次启动的 Sentinel 基础配置；只有规则正文可以动态更新。
type settings struct {
	RulePrefix  string `yaml:"rule_prefix"`
	wait, retry time.Duration
}

var validPrefix = regexp.MustCompile(`^config/[A-Za-z0-9][A-Za-z0-9._/-]*$`)

var validService = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// parseSettings 严格校验配置，禁止环境覆盖及向配置目录之外监听。
func parseSettings(body []byte) (settings, error) {
	var cfg settings
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, errors.New("sentinel: 配置必须是合法对象且不能含未知或重复字段")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return cfg, errors.New("sentinel: 不允许多个 YAML 文档")
	}
	if !validPrefix.MatchString(cfg.RulePrefix) || path.Clean(cfg.RulePrefix) != cfg.RulePrefix || strings.HasSuffix(cfg.RulePrefix, "/") {
		return cfg, errors.New("sentinel: rule_prefix 必须为 config/ 下的规范 Key 前缀")
	}
	// 与 core 共享的客户端具有 10 秒 HTTP 上限，阻塞查询留出网络及 Consul 抖动余量。
	cfg.wait, cfg.retry = 5*time.Second, 2*time.Second
	return cfg, nil
}
