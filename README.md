# GoServer — WebSocket 匹配服

为 Godot 等客户端提供匹配队列、组房间（2～4 人）、分配专服端口并拉起 **Dedicated Server** 进程的 Go 服务。

## 环境要求

- Go **1.22+**
- 依赖：`github.com/gorilla/websocket`（`go mod tidy` 自动拉取）

## Dedicated Server（专服）可执行文件

匹配成功后会启动专服，等价于命令行：

```text
DedicatedServer.exe --port <端口号>
```

专服需支持 **`--port`**，后跟匹配服分配的 TCP 端口（整数）。

### 默认文件名

默认可执行文件名为 **`DedicatedServer.exe`**。若要改名，请改 `matchmaker/matchmaker.go` 里 `resolveDedicatedServerExe` 中的常量 `defaultName`。

### 为什么「在资源管理器里能双击 / 在 cmd 里能跑」，匹配服却报错？

从 **Go 1.19** 起，在 Windows 上用 `exec.Command("DedicatedServer.exe", ...)` 这种**只有文件名、依赖当前目录**的写法会被拒绝，报错类似：

```text
exec: "DedicatedServer.exe": cannot run executable found relative to current directory
```

这是 Go 的安全策略：**必须用绝对路径**启动。本项目已用 `resolveDedicatedServerExe()` 自动解析为绝对路径。

### 专服要放在哪里？（解析顺序）

1. **环境变量 `DEDICATED_SERVER`**（推荐，路径随意）  
   设为专服的**完整路径**，例如：  
   `C:\Users\Administrator\Desktop\Server\DedicatedServer.exe`

2. **与匹配服 `matchserver.exe` 同一目录**  
   把 `DedicatedServer.exe` 和 `matchserver.exe` 放在一起；用 **`go build` 后再运行**，不要用 `go run` 测专服拉起（`go run` 生成的 exe 在临时目录，同目录下往往没有你的专服）。

3. **当前工作目录**  
   若上面都没命中，则用「启动匹配服时**当前目录**」下的 `DedicatedServer.exe`（已转为绝对路径，满足 Go 要求）。

**建议部署**：`matchserver.exe` 与 `DedicatedServer.exe` 放在同一文件夹（如 `Desktop\Server`），在该文件夹打开终端执行 `.\matchserver.exe`。

> 若仍失败，日志会打印实际使用的 `exe` 绝对路径，便于核对。

### 专服控制台输出（写入本项目目录）

专服的 **stdout / stderr** 会重定向到日志文件（匹配服自动创建目录）：

- **默认目录**：与 `matchserver.exe` 同级的 **`logs/`**（例如 `Desktop\Server\logs\`）
- **文件名**：`DedicatedServer_YYYYMMDD_HHMMSS_<纳秒>_port<端口>.log`（每次启动一个新文件）
- **文件内容**：开头写入启动时间与命令行；专服运行期间的输出；**进程退出时**在末尾追加一行 **退出时间** 与运行时长（以及 `wait error` 若有）

可通过环境变量 **`DEDICATED_SERVER_LOG_DIR`** 指定日志目录（绝对路径或相对路径均可，会 `filepath.Abs`）。

## 构建与运行

```powershell
cd d:\software\GoDot\PrjA\GoServer
go mod tidy
go build -o matchserver.exe .
go build -o http_download.exe ./http_download

.\matchserver.exe
```

下载服务（HTTP 80 端口）单独运行：

```powershell
# 需要管理员权限（80 端口）
.\http_download.exe
```

开发调试：

```powershell
go run .
```

- 监听地址：**127.0.0.1:8765**
- WebSocket 路径：**/ws**  
  完整 URL：`ws://127.0.0.1:8765/ws`

优雅退出：`Ctrl+C`（SIGINT）或发送 SIGTERM。

## 协议说明

客户端与服务端 JSON 消息格式、状态广播等，见 **`protocol/protocol.go` 文件顶部的注释文档**（与代码同仓，避免文档漂移）。

### 客户端固定对接约定（不做字段猜测）

为避免两端“靠兼容分支兜底”导致的问题，客户端按以下固定字段解析：

- `{"type":1,"num":<port>,"player_count":<n>}`：匹配成功（端口只读 `num`）
- `{"type":2,"ok":true}`：取消匹配确认
- `{"type":4,"reason":"..."}`：房间解散
- `{"type":5,"online":<n>,"matching":<n>}`：状态广播（大厅人数显示只读 `online`/`matching`）
- `{"type":"error","reason":"..."}`：错误消息

说明：
- `type` 为数值消息时，按 `1/2/4/5/6` 固定语义处理；
- `type=="error"` 是唯一字符串类型；
- 未在上述约定中的字段/事件名（如 `event`、`payload`、`queue_size` 等）不作为正式协议输入。

## 项目结构（简要）

```text
GoServer/
├── main.go              # 入口
├── protocol/            # 协议与文档注释
├── matchmaker/          # 匹配队列、房间、专服进程、状态广播
├── server/              # WebSocket HTTP 服务
├── go.mod
└── README.md
```

## 常见问题

**Q：匹配成功但专服没起来？**  
A：确认 `DedicatedServer.exe` 能被解析到：设置 `DEDICATED_SERVER`，或与 `matchserver.exe` 同目录；不要用仅文件名的相对路径；专服须支持 `--port`。

**Q：报错 `cannot run executable found relative to current directory`？**  
A：Go 1.19+ Windows 限制；请更新到当前仓库代码（已用绝对路径），或设置 `DEDICATED_SERVER` 指向完整路径。

**Q：房间解散后端口会怎样？**  
A：匹配服会结束专服进程，并将端口放回内部池子供下次匹配复用。

**Q：明明只匹配了 2 人，游戏里却像有 3 人、其中一个「未连接」？**  
A：多半是 **Godot 专服**里把 **peer id = 1（服务端权威）** 和两名客户端一起画进玩家列表了。专服日志里的 `ready={ 1: false, 28372514: false, ... }` 里 **`1` 不是第三个真人**。  
- UI / 玩家槽位请只统计 **`multiplayer.get_peers()`**（或等价「远程玩家」），不要把 **1** 当成未连上的客户端。  
- 匹配成功 JSON 里已有 **`player_count`**（2~4），应用此值限制本局显示的真人数量。

**Q：DedicatedServer 日志里 `[DS][FireReq]` 太吵，想关掉？**  
A：该日志在 **Godot 工程**里打印的，不在本 Go 仓库。请在 Godot 项目里搜索 `FireReq` / `[DS][FireReq]`，注释或删掉对应 `print` / `push_error`。

---

专服路径也可通过环境变量 **`DEDICATED_SERVER`** 指定（无需与匹配服同目录）。
