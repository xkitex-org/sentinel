# sentinel：Consul 动态规则与 Kitex 入站保护

独立模块 `go.chaoxizhongzhi.com/xkitex/sentinel`，基于 Sentinel Go SDK v1.0.4。当前为未发布工作稿，参考 xkitex-bak 的四类规则、阻塞查询和多 Service 资源命名。

## 接入

```go
import (
    "go.chaoxizhongzhi.com/xkitex/core"
    "go.chaoxizhongzhi.com/xkitex/sentinel"
)

err := x.Server(
    x.WithSentinel(sentinel.New()),
    x.WithService(productservice.RegisterService, productHandler),
    x.WithService(categoryservice.RegisterService, categoryHandler),
)
```

通过 `WithSentinel` 显式启用，与 database、cache、monitor 可以分别或同时使用。New 不读取应用配置、查询 Consul、初始化运行时采集器或启动规则监听。没有选项时，core 不处理 sentinel 节，不安装保护中间件。

## 基础配置只读一次

普通配置仍由 core 从 `config/GROUP/NAME/application.yaml` 和 imports 单次读取、合并，然后只将 sentinel 节传入模块：

```yaml
sentinel:
  rule_prefix: config/zmall/product/sentinel
```

rule_prefix 必须是 config/ 下的规范前缀，不能包含目录跳转、重复斜线或末尾斜线。未知及重复字段、多个 YAML 文档和缺少前缀会阻止启动。基础配置不动态更新，修改后需要重启；模块不读取 X_SENTINEL_* 或 SENTINEL_* 作为配置覆盖。

规则读取使用 core 已校验的同一个 Consul SDK 客户端、端点和 ACL Token，不另读环境、不创建默认地址客户端。普通应用配置读取和规则监听使用不同请求：只有下面四个规则 Key 会动态读取。

## 四类动态规则

沿用备份中的 Key 名称，Value 为 SDK 原生 JSON 数组：

| Consul Key | 含义 |
| --- | --- |
| `config/zmall/product/sentinel/flow` | 流控 |
| `config/zmall/product/sentinel/circuitBreaker` | 熔断 |
| `config/zmall/product/sentinel/isolation` | 并发隔离 |
| `config/zmall/product/sentinel/system` | 系统保护 |

**四个 Key 启动前都必须存在。没有规则时明确写 `[]`。** Key 不存在、为空、null、JSON 非法、未知字段、非法规则项或超过 1 MiB，都会阻止启动。先读取并校验全部初始规则，再应用，成功后才启动监听和 RPC。

例如，flow 配置 ProductService.GetProduct 的每秒阈值：

```json
[
  {
    "resource": "zmall-product/ProductService/GetProduct",
    "tokenCalculateStrategy": 0,
    "controlBehavior": 0,
    "threshold": 100,
    "statIntervalInMs": 1000
  }
]
```

其他规则类型使用 Sentinel v1.0.4 对应的 JSON 字段。框架先校验整个数组及规则参数，再交给 SDK，避免 SDK 默默忽略非法项后意外减少保护。

每个 Key 有一个可取消的 Consul 阻塞查询任务，携带 WaitIndex 和 5 秒 WaitTime，适配 core 共享客户端的 10 秒 HTTP 上限。规则变更后重新校验并更新对应类别，不重启服务，不监听 application.yaml 或 imports。

- 网络或 ACL 错误：保留上一版有效规则，退避 2 秒后重试。
- 删除、空内容或非法更新：保留上一版有效规则，不能通过删 Key 清空保护。
- 明确写入合法 `[]`：清空该类规则。
- Consul 重启或恢复导致索引回退：重置阻塞查询索引，重新获取当前版本。
- 正文未变：只推进查询索引，不重复加载相同规则。
- 异常服务端立即返回相同索引：短暂退避，避免查询空转。

ACL 需要允许读取规则前缀。例子位于 config/zmall/ 下，可复用该应用现有配置读取权限；启动期缺少权限直接失败，运行期权限暂时不可用继续使用旧规则。

## RPC 保护与日志

core 在创建真实 Kitex Server 时安装中间件。资源名固定为 `应用名/IDL Service 名/方法名`，应用名来自已经校验的 GROUP-NAME，不使用客户端的目标别名。例如：

```text
zmall-product/ProductService/Get
zmall-product/CategoryService/Get
```

同名方法不串资源。流控、熔断或隔离阻断时返回 SDK 阻断错误，不执行业务 Handler；放行后的业务错误原样返回并进入 SDK 熔断统计。正常返回和 panic 都释放 Sentinel Entry，panic 保持原样传播。本次不引入业务错误码映射。

所有运行时诊断走 Kitex klog，包含服务名、规则 Key、索引或数量，不打印 Token、完整配置、规则正文或原始 Consul 错误响应。SDK 内部日志通过安全适配器接入 klog；成功规则日志由框架统一记录，避免 SDK 反复输出规则全文。

SDK 高层初始化会额外读取系统环境变量，因此这里使用 SDK 公开的配置、日志与系统采集接口进行一次初始化。SDK 应用身份来自 core 的启动快照；未启用 SDK 文件指标日志、缓存时钟或额外 HTTP exporter，日志输出由 logging/klog 管理。

## 生命周期

初始规则成功生效后才启动 RPC。运行期间 Consul 暂时不可达不撤销服务 readiness，因为有效旧规则仍在保护请求；不会绕过 Sentinel 放行业务。

core 在发出停机信号时取消规则请求的运行上下文，保留已加载规则供 RPC 排空使用；RPC 结束后 Close 等待所有监听任务退出，再清理四类规则、释放 SDK 归属，最后释放共享 Consul 连接。配置、启动或注册失败也执行清理，Close 可以并发、重复调用。

Sentinel SDK 是进程级状态，当前一个进程只允许一个活动控制器，不支持并行实例覆盖彼此规则。SDK 只初始化一次，应用身份不能在同一进程改成另一应用；控制器关闭后同一应用可以重新接入。不要在业务中另行初始化 SDK 或装载全局规则。

SDK 自带的系统采集器是进程级任务，没有按控制器停止的公开接口，会存续到进程退出。模块关闭会停止全部 Consul 监听并释放规则归属，不宣称停止这些 SDK 内部任务。

## 本地验证

```sh
GOWORK=off go test -race -timeout 60s ./...
```

完整未发布模块验证复用相邻 database 的脚本：

```sh
python3 ../database/scripts/test-local.py
```

脚本在临时代理中读取源码快照，不创建 go.work、replace 或上层 go.mod，也不发布版本。测试使用真实 Sentinel SDK、本机 Consul HTTP 协议替身和真实 Kitex RPC，覆盖动态放行/阻断、熔断统计、多 Service、非法与删除规则保留、ACL 故障、索引回退及监听取消；这不等于已经连接测试环境 Consul。

根包只保留 New 和 Controller 接入协议；配置、规则校验、监听、中间件和日志实现位于 internal/runtime，运行时不依赖 core、database、cache 或 monitor。
