# 贡献指南

项目使用 Python 标准库与原生浏览器能力。运行程序和测试均不需要安装第三方依赖。

## 代码结构

```text
codex-response-logs/
├── codex_sse_watch.py          # 解析、会话关联、缓存、HTTP 服务与 CLI
├── codex_sse_dashboard.html    # 页面、样式与交互，无构建步骤
├── docs/
│   ├── fields.md               # 34 个解析字段的来源与缺失语义
│   └── logging.md              # 日志配置与常见问题
├── scripts/                   # macOS 启动、状态与停止脚本
├── tests/                     # 按职责划分的合成数据回归测试
├── .github/workflows/tests.yml
├── .editorconfig
├── .gitattributes
├── .gitignore
├── CONTRIBUTING.md
├── LICENSE
└── README.md
```

后端主要入口：

| 代码 | 职责 |
| --- | --- |
| `read_appended`、`parse_events`、`Summary.decode_events` | 增量读取、日志来源识别、跨块事件解析 |
| `Summary` | 响应保留、会话与请求关联、行数据生成和复用 |
| `RowSnapshot` | 可见窗口、ETag 与完整 / 增量 JSON 编码 |
| `serve`、`control` | 本机 HTTP、采集线程、长轮询与认证控制 |
| `main` | 命令行参数与目录配置 |

Python 与 HTML 保持相邻，脚本可直接运行。页面头部的主题初始化脚本在样式生效前设置外观，整理前端时应保留其执行时机。

## 本地验证

在项目根目录运行：

```sh
python3 -B -m unittest discover -s tests -v
```

测试创建临时目录、SQLite 数据库和动态端口，不依赖个人 Codex 数据或正在运行的桌面应用。修改测试时继续使用合成数据与现有共享 fixture。GitHub Actions 使用相同命令，覆盖 macOS / Ubuntu 与 Python 3.11 / 3.12。

仅修改说明文字时，核对链接、命令和 diff 即可。代码变更应运行覆盖实际影响的测试；涉及多个采集、关联或 HTTP 流程时运行完整回归。

修改前端后，通过合成日志启动独立实例，在浏览器验证：主题切换和跟随系统、窄屏布局、搜索与筛选、详情展开与复制、Esc 和焦点恢复、滚动阅读与暂停更新。HTML 在服务启动时读入，修改后需重启该测试实例。

## 保持的行为约定

- Codex 原始日志和会话数据库只读；查看器的临时关联索引由自身管理，使用私有权限并在正常退出时清理。服务仅监听本机，保留 Host / Origin 检查与状态、停止接口的令牌验证。
- 请求、首档和终档分别取自明确证据。缺失是 `null`，零是 `0`；不猜测关联、耗时或 HTTP 状态。
- 不把普通日志正文或内嵌 JSON 升格为事件。损坏输入恢复时保留外层来源边界。
- 新缓存应有明确生命周期；留意行替换、详情关闭、日志移除和连接取消后的资源释放。
- 页面中的日志与错误信息按文本渲染，避免将输入内容拼入可执行 HTML。
- 沿用 `.editorconfig` 的 UTF-8、LF 和空格缩进；`.runtime/` 与缓存等运行产物只保留在本地。

## 提交与问题报告

提交源码、文档、脚本、测试和 CI 配置。提交前检查：

```sh
git diff --check
git diff --cached --check
git diff --cached --stat
```

`.gitignore` 排除常见运行数据，但不能清除已进入 Git 历史的内容。不要提交 `.runtime/`、原始日志、会话 JSONL、SQLite 数据库、认证信息、个人绝对路径或真实会话截图。

提交说明应写清行为变化与验证结果。反馈问题时提供合成或充分脱敏的最小样本，避免上传整份 Codex 配置或数据目录。贡献内容按项目的 [MIT License](LICENSE) 提供。
