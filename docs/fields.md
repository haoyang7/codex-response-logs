# 解析字段

详情中的「查看全部解析字段」和 `--once --json` 使用相同的响应字段。时间戳以 Unix 秒表示，毫秒字段单独标明；界面按 `Asia/Shanghai` 格式化。

`null` 表示没有可信记录或尚未关联，不代表零值。字段仅依据日志和明确关联的会话记录，缺失时不推断。

## 标识与状态

| 字段 | 来源与含义 |
| --- | --- |
| `response_id` | 响应的 `response.id`，记录去重与会话关联的主键 |
| `thread_id` | 会话记录中明确关联的对话 ID |
| `turn_id` | 明确关联的轮次 ID |
| `title` | 会话数据库中的自定义名称，或原始标题 |
| `turn` | 对话内按 `rollout_ordinal` 排序得到的轮次序号；一轮可有多次响应 |
| `transport` | 事件日志对应的 `SSE` 或 `WebSocket` |
| `response_event` | 采用的响应事件类型，终态优先于中间事件 |
| `response_status` | `response.status` 原值，可能缺失 |
| `status` | 查看器根据事件类型生成的状态，例如已完成、失败、未完成、响应中或仅耗时日志 |
| `match` | `mismatch`：已知可比字段有差异；`match`：模型、首档、终档均有请求与返回值且一致；`null`：比较证据不足 |

## 请求与响应参数

| 字段 | 来源与含义 |
| --- | --- |
| `request_model` | 对应请求的本地 `turn_context.model` |
| `request_effort` | 对应请求的本地 `turn_context.effort` |
| `response_model` | 终态响应的 `response.model`；终态未到达时采用已收到的最新事件 |
| `first_effort` | 第一条 `response.created` 的 `response.reasoning.effort` |
| `final_effort` | 第一条 completed / failed / incomplete 终态的 `response.reasoning.effort` |
| `service_tier` | 采用的响应事件中的 `response.service_tier` 原值 |

请求参数通过会话 `token_usage_record.response_id` 关联，在对应响应处绑定当时的上下文。同轮后续上下文变化不会覆写之前响应的请求参数。

首档与最终档独立显示。终态失败仍可能包含档位字段；这些值不能证明请求成功或实际使用了对应强度的推理。

## 时间

| 字段 | 来源与含义 |
| --- | --- |
| `timestamp` | 内部排序时间，可能使用解析时刻兜底；不要当作请求开始时间 |
| `request_started_at` | 仅在日志明确提供对应请求开始时间时记录 |
| `response_created_at` | 服务端 `response.created_at`，优先使用 created 事件中的值 |
| `response_completed_at` | 终态 `response.completed_at`，没有该字段时保持 `null` |
| `first_logged_at` | 该响应已观察到的最早日志时间 |
| `final_logged_at` | 采用的终态事件的日志时间 |
| `response_duration_ms` | 创建时间与完成时间均有效且顺序正确时计算的差值，单位为毫秒 |
| `elapsed_ms` | 完整客户端请求耗时；当前没有可靠来源，保持 `null` |

服务端时间和本机日志时间可能来自不同的时钟，不能混合相减得到完整请求耗时。

## Token 用量

均取自终态响应的 `usage`。仅接受非负整数，保留 `0`，不将缺失项补零。

| 字段 | 对应原始字段 |
| --- | --- |
| `input_tokens` | `usage.input_tokens` |
| `cached_input_tokens` | `usage.input_tokens_details.cached_tokens` |
| `cache_write_tokens` | `usage.input_tokens_details.cache_write_tokens`；部分提供方可能不返回 |
| `output_tokens` | `usage.output_tokens` |
| `reasoning_tokens` | `usage.output_tokens_details.reasoning_tokens` |
| `total_tokens` | `usage.total_tokens`；不会通过输入与输出相加补齐 |

缓存和推理是用量明细，不应再与对应输入、输出重复相加。

## 错误

| 字段 | 来源与含义 |
| --- | --- |
| `error_code` | 终态 `response.error.code` |
| `error_message` | 终态 `response.error.message` |
| `incomplete_reason` | 终态 `response.incomplete_details.reason` |
| `http_status` | HTTP 响应状态码；当前没有可靠来源，保持 `null` |

上述 34 个字段是解析后的元数据。完整对话正文和逐 token 事件不加入表格或详情 JSON。
