package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibaba/sentinel-golang/core/base"
	"github.com/alibaba/sentinel-golang/core/circuitbreaker"
	"github.com/alibaba/sentinel-golang/core/config"
	"github.com/alibaba/sentinel-golang/core/flow"
	"github.com/alibaba/sentinel-golang/core/isolation"
	"github.com/alibaba/sentinel-golang/core/system"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/hashicorp/consul/api"
)

const testPrefix = "config/demo/api/sentinel"
const testResource = "demo-api/ProductService/Get"

// TestMain 在第一次 SDK 初始化之前设置冲突环境，验证实现没有进入 SDK 环境覆盖路径。
func TestMain(m *testing.M) {
	_ = os.Setenv("SENTINEL_APP_NAME", "must-not-override")
	_ = os.Setenv("SENTINEL_APP_TYPE", "not-an-integer")
	os.Exit(m.Run())
}

// loaded 在规则提交锁内查询 SDK 实际规则，规避 SDK GetRules 在锁外读取 map 长度的缺陷。
func loaded(kind string) int {
	ruleStateMu.RLock()
	defer ruleStateMu.RUnlock()
	switch kind {
	case "flow":
		return len(flow.GetRules())
	case "circuitBreaker":
		return len(circuitbreaker.GetRules())
	case "isolation":
		return len(isolation.GetRules())
	case "system":
		return len(system.GetRules())
	}
	return -1
}

// consulFixture 提供带索引、认证及可取消阻塞查询的真实 HTTP 协议替身。
type consulFixture struct {
	mu        sync.Mutex
	values    map[string][]byte
	index     uint64
	status    int
	changed   chan struct{}
	seen      map[string]uint64
	requests  map[string]int
	unindexed map[string]int
	active    atomic.Int32
	server    *httptest.Server
}

// newConsulFixture 创建四类明确空数组，并为每次请求验证 ACL 请求头和规则 Key。
func newConsulFixture(t *testing.T) (*consulFixture, *api.Client) {
	t.Helper()
	f := &consulFixture{values: map[string][]byte{}, index: 10, changed: make(chan struct{}), seen: map[string]uint64{}, requests: map[string]int{}, unindexed: map[string]int{}}
	for _, kind := range ruleKinds {
		f.values[kind] = []byte("[]")
	}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		kind := strings.TrimPrefix(r.URL.Path, "/v1/kv/"+testPrefix+"/")
		if r.Method != "GET" || kind == r.URL.Path || r.Header.Get("X-Consul-Token") != "test-token" {
			t.Error("unexpected rule request")
			w.WriteHeader(400)
			return
		}
		f.active.Add(1)
		defer f.active.Add(-1)
		index, _ := strconv.ParseUint(r.URL.Query().Get("index"), 10, 64)
		f.mu.Lock()
		f.requests[kind]++
		if index == 0 {
			f.unindexed[kind]++
		}
		changed := f.changed
		wait := index > 0 && index >= f.index && f.status == 0
		f.mu.Unlock()
		if wait {
			select {
			case <-r.Context().Done():
				return
			case <-changed:
			case <-time.After(30 * time.Millisecond):
			}
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Header().Set("X-Consul-Index", strconv.FormatUint(f.index, 10))
		f.seen[kind] = f.index
		if f.status != 0 {
			w.WriteHeader(f.status)
			_, _ = w.Write([]byte("secret-response-marker"))
			return
		}
		body, exists := f.values[kind]
		if !exists {
			w.WriteHeader(404)
			return
		}
		_ = json.NewEncoder(w).Encode([]map[string]any{{"Value": body, "ModifyIndex": f.index, "Key": testPrefix + "/" + kind}})
	}))
	t.Cleanup(f.server.Close)
	client, err := api.NewClient(&api.Config{Address: f.server.URL, Token: "test-token", HttpClient: &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}})
	if err != nil {
		t.Fatal(err)
	}
	return f, client
}

// set 更新一类规则并唤醒所有阻塞读；nil 表示删除 Key。
func (f *consulFixture) set(kind string, body []byte) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if body == nil {
		delete(f.values, kind)
	} else {
		f.values[kind] = body
	}
	f.index++
	close(f.changed)
	f.changed = make(chan struct{})
	return f.index
}

// unavailable 模拟 ACL 或服务端故障，不改变最后一版规则正文。
func (f *consulFixture) unavailable(status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = status
	f.index++
	close(f.changed)
	f.changed = make(chan struct{})
}

// waitUntil 等待真实异步规则生效，不依赖某次协程调度。
func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("rule update timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

// seen 等待指定规则版本已经到达客户端，规则是否生效由 SDK 状态另外验证。
func (f *consulFixture) waitSeen(t *testing.T, kind string, index uint64) {
	t.Helper()
	waitUntil(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.seen[kind] >= index })
}

// testController 使用真实 SDK 与 HTTP Consul 替身，缩短错误退避以便验证恢复路径。
func testController(t *testing.T, f *consulFixture, client *api.Client) *Controller {
	t.Helper()
	c := New()
	if err := c.ConfigureSentinel([]byte("rule_prefix: " + testPrefix)); err != nil {
		t.Fatal(err)
	}
	c.cfg.retry = 5 * time.Millisecond
	if err := c.Start(context.Background(), client, "demo-api"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	})
	return c
}

// rpcContext 构造多 Service 场景，目标别名不能改变应用身份。
func rpcContext(service, method string) context.Context {
	return rpcinfo.NewCtxWithRPCInfo(context.Background(), rpcinfo.NewRPCInfo(nil, rpcinfo.NewEndpointInfo("client-alias", method, nil, nil), rpcinfo.NewInvocation(service, method), nil, nil))
}

// flowBody 使用 SDK 原生字段生成可执行的流控规则。
func flowBody(threshold float64) []byte {
	return []byte(fmt.Sprintf(`[{"resource":%q,"threshold":%g,"tokenCalculateStrategy":0,"controlBehavior":0,"statIntervalInMs":1000}]`, testResource, threshold))
}

// TestDynamicFlowAndLastGoodRetention 验证更新实际阻断请求，坏规则、删除和 ACL 故障不会解除保护。
func TestDynamicFlowAndLastGoodRetention(t *testing.T) {
	f, client := newConsulFixture(t)
	c := testController(t, f, client)
	var calls atomic.Int32
	next := c.Middleware()(func(context.Context, interface{}, interface{}) error { calls.Add(1); return nil })
	ctx := rpcContext("ProductService", "Get")
	if err := next(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	f.set("flow", flowBody(0))
	waitUntil(t, func() bool { return loaded("flow") == 1 })
	if err := next(ctx, nil, nil); err == nil {
		t.Fatal("flow update did not block")
	}
	if calls.Load() != 1 {
		t.Fatal("blocked request reached handler")
	}
	if err := next(rpcContext("CategoryService", "Get"), nil, nil); err != nil {
		t.Fatal("one IDL Service blocked another Service's same-named method")
	}
	for _, body := range [][]byte{[]byte("["), []byte("null"), flowBody(-1), []byte(`[null]`), nil, []byte("")} {
		version := f.set("flow", body)
		f.waitSeen(t, "flow", version)
		if err := next(ctx, nil, nil); err == nil {
			t.Fatal("bad update disabled last good rules")
		}
	}
	f.unavailable(http.StatusForbidden)
	time.Sleep(35 * time.Millisecond)
	if err := next(ctx, nil, nil); err == nil {
		t.Fatal("ACL failure disabled protection")
	}
	f.unavailable(0)
	f.set("flow", []byte("[]"))
	waitUntil(t, func() bool { return loaded("flow") == 0 })
	if err := next(ctx, nil, nil); err != nil {
		t.Fatal("explicit [] did not clear protection")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for key := range f.requests {
		if !strings.Contains("flow circuitBreaker isolation system", key) {
			t.Fatal("ordinary configuration was watched")
		}
	}
}

// TestAllRuleKindsUpdateAndRollbackIndex 验证四类 Key 均动态更新，并处理 Consul 恢复后的索引回退。
func TestAllRuleKindsUpdateAndRollbackIndex(t *testing.T) {
	f, client := newConsulFixture(t)
	_ = testController(t, f, client)
	f.set("isolation", []byte(`[{"resource":"demo-api/ProductService/Get","metricType":0,"threshold":1}]`))
	f.set("circuitBreaker", []byte(`[{"resource":"demo-api/ProductService/Get","strategy":2,"retryTimeoutMs":100,"minRequestAmount":1,"statIntervalMs":1000,"threshold":1}]`))
	f.set("system", []byte(`[{"metricType":4,"triggerCount":1,"strategy":-1}]`))
	f.set("flow", flowBody(0))
	waitUntil(t, func() bool {
		return loaded("isolation") == 1 && loaded("circuitBreaker") == 1 && loaded("system") == 1 && loaded("flow") == 1
	})
	f.mu.Lock()
	f.index = 1
	f.values["flow"] = []byte("[]")
	before := f.unindexed["flow"]
	close(f.changed)
	f.changed = make(chan struct{})
	f.mu.Unlock()
	waitUntil(t, func() bool { return loaded("flow") == 0 })
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unindexed["flow"] <= before {
		t.Fatal("blocking index was not reset")
	}
}

// TestInitialRulesFailClosedAndErrorsAreSafe 验证初始缺失和非法规则阻止启动且不会部分应用。
func TestInitialRulesFailClosedAndErrorsAreSafe(t *testing.T) {
	for _, bad := range [][]byte{nil, []byte(""), []byte("null"), []byte(`[null]`), []byte(`[{"resource":"secret-marker","strategy":999}]`)} {
		f, client := newConsulFixture(t)
		f.set("flow", flowBody(0))
		f.set("circuitBreaker", bad)
		c := New()
		if err := c.ConfigureSentinel([]byte("rule_prefix: " + testPrefix)); err != nil {
			t.Fatal(err)
		}
		err := c.Start(context.Background(), client, "demo-api")
		if err == nil || strings.Contains(err.Error(), "secret-marker") {
			t.Fatalf("bad initial rule accepted or leaked: %v", err)
		}
		if loaded("flow") != 0 {
			t.Fatal("partially loaded initial rules")
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	f, client := newConsulFixture(t)
	f.unavailable(http.StatusForbidden)
	c := New()
	_ = c.ConfigureSentinel([]byte("rule_prefix: " + testPrefix))
	if err := c.Start(context.Background(), client, "demo-api"); err == nil || strings.Contains(err.Error(), "secret-response-marker") {
		t.Fatalf("unsafe ACL error: %v", err)
	}
	_ = c.Close()
}

// TestCloseCancelsWatchesAndExclusiveSDK 验证多个控制器不能覆盖规则，关闭能取消请求并幂等清理。
func TestCloseCancelsWatchesAndExclusiveSDK(t *testing.T) {
	f, client := newConsulFixture(t)
	c := testController(t, f, client)
	other := New()
	_ = other.ConfigureSentinel([]byte("rule_prefix: " + testPrefix))
	if err := other.Start(context.Background(), client, "demo-api"); err == nil {
		t.Fatal("second SDK owner was accepted")
	}
	_ = other.Close()
	var group sync.WaitGroup
	for i := 0; i < 5; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	waitUntil(t, func() bool { return f.active.Load() == 0 })
	if loaded("flow") != 0 || loaded("isolation") != 0 || loaded("circuitBreaker") != 0 || loaded("system") != 0 {
		t.Fatal("rules not released")
	}
	if err := c.Middleware()(func(context.Context, interface{}, interface{}) error { return nil })(rpcContext("ProductService", "Get"), nil, nil); err == nil {
		t.Fatal("closed middleware admitted a call")
	}
	// SDK 按进程初始化一次，后续同进程的控制器可以在释放归属后重新接管。
	replacement := testController(t, f, client)
	if err := replacement.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestIsolationAndPanicExit 验证原生并发隔离阻断，以及 panic 后入口一定释放。
func TestIsolationAndPanicExit(t *testing.T) {
	f, client := newConsulFixture(t)
	f.set("isolation", []byte(`[{"resource":"demo-api/ProductService/Get","metricType":0,"threshold":1}]`))
	c := testController(t, f, client)
	ctx := rpcContext("ProductService", "Get")
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic did not propagate")
			}
		}()
		_ = c.Middleware()(func(context.Context, interface{}, interface{}) error { panic("business panic") })(ctx, nil, nil)
	}()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var once sync.Once
	defer once.Do(func() { close(release) })
	blocked := c.Middleware()(func(context.Context, interface{}, interface{}) error { close(entered); <-release; return nil })
	go func() { done <- blocked(ctx, nil, nil) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("panic leaked entry concurrency")
	}
	err := c.Middleware()(func(context.Context, interface{}, interface{}) error {
		t.Error("isolated request reached business")
		return nil
	})(ctx, nil, nil)
	var block *base.BlockError
	if !errors.As(err, &block) {
		t.Fatalf("isolation did not block: %v", err)
	}
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestBusinessErrorsFeedCircuitBreaker 验证业务原始错误被返回且确实参与熔断统计。
func TestBusinessErrorsFeedCircuitBreaker(t *testing.T) {
	f, client := newConsulFixture(t)
	f.set("circuitBreaker", []byte(`[{"resource":"demo-api/ProductService/Breaker","strategy":2,"retryTimeoutMs":10000,"minRequestAmount":1,"statIntervalMs":2000,"threshold":1}]`))
	c := testController(t, f, client)
	failure := errors.New("business failure")
	var calls int
	next := c.Middleware()(func(context.Context, interface{}, interface{}) error { calls++; return failure })
	blocked := false
	for i := 0; i < 5; i++ {
		err := next(rpcContext("ProductService", "Breaker"), nil, nil)
		var block *base.BlockError
		if errors.As(err, &block) {
			blocked = true
			break
		}
		if !errors.Is(err, failure) {
			t.Fatalf("business error changed: %v", err)
		}
	}
	if !blocked || calls == 0 {
		t.Fatal("business errors did not open circuit breaker")
	}
}

// TestStrictConfigAndSDKEnvironmentIndependence 验证单次基础配置和多 Service 资源名，不读取 SDK 环境覆盖。
func TestStrictConfigAndSDKEnvironmentIndependence(t *testing.T) {
	for _, body := range []string{"", "{}", "[]", "rule_prefix: ../secret", "rule_prefix: config/demo/../prod", "rule_prefix: config/demo/", "rule_prefix: config/demo\nlog_dir: secret-marker", "rule_prefix: config/demo\n---\n{}"} {
		if _, err := parseSettings([]byte(body)); err == nil || strings.Contains(err.Error(), "secret-marker") {
			t.Fatalf("invalid config accepted or leaked: %v", err)
		}
	}
	f, client := newConsulFixture(t)
	t.Setenv("SENTINEL_APP_NAME", "must-not-override")
	c := testController(t, f, client)
	if config.AppName() != "demo-api" {
		t.Fatal("SDK environment overrode controlled configuration")
	}
	if err := c.ConfigureSentinel([]byte("rule_prefix: config/other")); err == nil {
		t.Fatal("ordinary configuration was reloaded")
	}
	for _, service := range []string{"ProductService", "CategoryService"} {
		if name := resourceName(rpcContext(service, "Get"), "demo-api"); name != "demo-api/"+service+"/Get" {
			t.Fatalf("wrong resource name: %s", name)
		}
	}
}
