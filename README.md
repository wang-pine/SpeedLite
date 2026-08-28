# ⚡ speedTest · 带宽测速（Go）

用 Go 编写的带宽测速工具，用于验证 **TCP / UDP** 模式下服务器的**上行 / 下行**带宽。
单二进制交付：内嵌 Web 前端 + 测速引擎 + 原生 socket 客户端（`speedctl`）。

> 一句话：浏览器开网页就能测，命令行 `speedctl` 测极限值；**每个结果都带服务端独立统计做双重对账**，可信度一目了然。

## 特性

- **双通道测速**
  - **Web 页面**（任何现代浏览器，含安卓手机）：TCP 走 WebSocket，UDP 走 WebRTC DataChannel（真实 UDP 封装）
  - **CLI `speedctl`**：原生 TCP/UDP socket，贴近线速（iperf 风格）
- **四条链路**：TCP 上行 / TCP 下行 / UDP 上行 / UDP 下行，可单独、组合、双向测
- **iperf 风格参数**：并行流数（`-P`）、测试时长（`-t`）、包大小（`-l`）
- **双重对账**：浏览器本地统计 + 服务端独立统计（服务端每链路另测一份），结果表直接展示两端差值
- **弱网共同边界**：TCP 用有序 `start/stop/result` 对齐双端统计，正常完成偏差应 <1%
- **UDP 交付对账**：区分 DataChannel 提交、排空、接收与队列残留，不把缓冲截断冒充 IP 层丢包
- **UDP 质量统计（CLI）**：丢包率、抖动（RFC3550 Jitter 平滑）
- **实时曲线**：前端 canvas 绘制上下行速率曲线；历史曲线缓存，可对比多次测试
- **零填充满包**：预分配全零 payload，最大化有效载荷比
- **峰值抗虚高**：上行用「累计发送 − 缓冲滞留」校正、峰值取 1s 滑动窗口平均，杜绝"内存拷贝速度"假象
- **单二进制**：`go:embed` 内嵌前端，一个文件部署；支持 `CGO_ENABLED=0` 静态编译，老系统（CentOS 7 等）也能跑

## 快速开始

### 1. 构建（推荐：makeVersion 一键多平台打包）

```bash
# 读取 config/version.ini，构建所有平台到 dest/<平台名>/
./makeVersion.sh

# 指定版本覆盖（临时）
./makeVersion.sh 1.2.3

# 查看用法
./makeVersion.sh -h
```

产物结构（每个平台独立目录 + 发行包 + 校验和）：

```
dest/
├── linux_x64/
│   ├── speedtest-server          # 服务端（静态、无 glibc 依赖）
│   ├── speedctl                  # 命令行客户端
│   ├── SHA256SUMS                # 校验和
│   └── speedtest-1.1.0-linux_x64.tar.gz   # 发行包
├── linux_arm64/ …   （armx86/arm/mips/windows_x64/darwin_x64/darwin_arm64 同构）
└── …
```

目标平台矩阵在 `config/version.ini` 的 `[platforms]` 中声明：

```ini
[version]
major = 1
minor = 1
patch = 0

[platforms]
linux_x64    = linux/amd64
linux_arm64  = linux/arm64
windows_x64  = windows/amd64 .exe
…
```

### 2. 仅构建单个平台（开发调试）

```bash
cd src
go build -o speedtest-server ./cmd/speedtest-server   # 服务端
go build -o speedctl ./cmd/speedctl                   # 命令行客户端
```

### 2. 运行服务器

```bash
./speedtest-server -http :8080 -tcp :5001 -udp :5201
```

| 参数 | 默认 | 说明 |
|---|---|---|
| `-http` | `:8080` | Web 页面 + WebSocket（TCP 测速）+ WebRTC 信令 |
| `-tcp` | `:5001` | 原生 TCP 测速端口（CLI 使用） |
| `-udp` | `:5201` | 原生 UDP 测速端口（CLI 使用） |

浏览器打开 `http://服务器IP:8080` 即可测速。

### 3. 命令行测速（speedctl）

```bash
# TCP 下行 / 上行（各 4 流，10 秒）
./speedctl -s 服务器IP tcp down -P 4 -t 10
./speedctl -s 服务器IP tcp up   -P 4 -t 10

# UDP 下行 / 上行（单流大包，丢包率低）
./speedctl -s 服务器IP udp down -P 1 -l 60000 -t 10
./speedctl -s 服务器IP udp up   -P 1 -l 60000 -t 10

# 自定义端口
./speedctl -s 服务器IP -tcp-port 5001 -udp-port 5201 tcp down -P 8 -t 5
```

flags 可放在任意位置（含位置参数之后）。

## 使用教程

详见 **[docs/用户手册.md](docs/用户手册.md)**。要点速览：

| 页面配置项 | 说明 |
|---|---|
| 模式 | TCP（WebSocket）/ UDP（WebRTC）/ TCP+UDP 连测 |
| 方向 | 下行 / 上行 / 双向 |
| 并行流数 | 1–16，默认 4（多流可榨干多核/多队列） |
| 时长（秒） | 1–120，默认 10 |
| 包大小（字节） | 1KiB–1MiB，默认 128KiB |
| 服务器地址 | 留空 = 当前页面所在服务器；可填局域网另一台 |

**结果表**：每一行是一条链路，明细行给出该链路服务端独立统计（对账）：

```
TCP 下行 ▼ 948.7 Mbit/s │ 峰值 1072.2 │ 传输 1.11 GiB │ 丢包 0%
4 条并行流 · 下行 1.11 GiB · 服务器对账: 传输 1.11 GiB · 平均 909 Mbit/s · 峰值 1450 Mbit/s · 对账正常
UDP 下行 ▼ 5.9 Mbit/s │ 峰值 27.6 │ 传输 7.25 MiB │ 丢包 87.9%（红）
4 条并行流 · 下行 7.25 MiB · 服务器对账: 传输 59.75 MiB · 平均 42 Mbit/s · 峰值 42 Mbit/s · 严重损耗 ⚠
```

**按钮**：`▶ 开始测速` / `■ 停止` / `⟳ 刷新清空`（清掉缓存的曲线与结论，历史记录保留）/ `清空历史`。

## 架构

```
┌─────────────── 浏览器（含安卓） ───────────────┐        ┌──── 有终端的设备 ────┐
│  Web 控制台（单页，go:embed）                   │        │  speedctl CLI        │
│  ├─ TCP 上下行 ←─ WebSocket(=TCP) ──→ │ HTTP   │        │  ├─ 原生 TCP 多流    │
│  ├─ UDP 上下行 ←─ WebRTC DC(unreliable) → │ :8080  │        │  └─ 原生 UDP 多流 ──→│ UDP
│  └─ WebRTC 信令 ←─ WebSocket SDP ────→         │        │                       │ :5201
└────────────────────────────────────────────────┘        └───────────────────────┘
                   Go 单二进制服务端（engine 统计核心共用）
```

### 双重对账原理

每个流的测速结束时，**服务端**独立回传一份统计（`result` 消息）：

| 链路 | 浏览器侧统计 | 服务端侧统计（对账方） |
|---|---|---|
| TCP 下行 | 收到字节数 | WS 发送字节数 |
| TCP 上行 | 有序 `stop` 前的发送字节数 | 收到 `stop` 前的 WS 数据字节数 |
| UDP 下行 | DataChannel 收到字节数 | DC 提交量 − 队列残留（已排空量） |
| UDP 上行 | DC 提交量 − 队列残留（已排空量） | DC 接收字节数 |

- TCP 的 result 与数据共用有序 WebSocket；正常完成时偏差 <1%，否则视为异常断链或协议边界错误。
- UDP 明细分别显示提交、排空、接收和残留；残留大于 0 标记为“队列截断”，不计入交付损失。
- 网页 UDP 的“交付损失”是 WebRTC 应用层差额，不是 IP 层包丢失率；精确丢包和抖动用 `speedctl udp`。

## 部署

完整部署教程（含 systemd 开机自启、Windows、防火墙、老系统静态编译）见 **[docs/部署指南.md](docs/部署指南.md)**。

最小部署：

```bash
# 直接用 makeVersion 产物：dest/linux_x64/speedtest-server
# （已静态编译，兼容 CentOS 7 / Ubuntu 18 等老系统）

# 服务器上启动（防火墙放行对应端口）
./speedtest-server -http :8080 -tcp :5001 -udp :5201

# 后台常驻
nohup ./speedtest-server -http :8080 -tcp :5001 -udp :5201 > speedtest.log 2>&1 &
```

## 技术说明

### Web 侧测速原理

- **TCP**：浏览器无法直接建裸 TCP socket，但 WebSocket 运行于 TCP 之上，吞吐与原生 TCP 基本一致（仅少量帧头开销），网页 WS 打流即等价于 TCP 测速。
- **UDP**：浏览器无法直接发裸 UDP 包。网页 UDP 测速走 **WebRTC DataChannel**，配置 `ordered:false, maxRetransmits:0`（不可靠模式），底层 SCTP-over-DTLS-over-UDP——流量真实经过 UDP 路径。信令（SDP 交换）经 `/ws/signal`，局域网用 host candidate 直连，无需 STUN。
- **前端本地采样**：每 100ms 聚合所有并行流，平均值由总字节/统一用时计算，峰值取同步聚合曲线的 1s 窗口；上行用 `bufferedAmount` 排除队列残留。

### CLI 侧（原生 socket）

- **TCP**：`internal/tcpnative`，JSON 控制帧 + 二进制打流。
- **UDP**：`internal/udpnative`，iperf 式 24B 包头（magic + session + seq + ts），接收端按序号连续性统计丢包、按时间戳算抖动。
- UDP 高 pps 小包时丢包率上升属真实现象（接收端处理上限），iperf 同样如此；大包单流丢包率极低。

### WebRTC 背压

pion 的 `DataChannel.Send` 只表示负载已提交。服务器下行使用 `bufferedAmount` 背压，停止提交后最多等待 3 秒排空，并将未排空量作为 `queued_bytes` 单独返回。

## 目录结构

```
speedTest/
├── makeVersion.sh               # 版本化多平台构建脚本（读 config/version.ini）
├── config/
│   └── version.ini              # 版本号 + 产物名 + 目标平台矩阵
├── src/                         # Go 源码（module 根）
│   ├── cmd/
│   │   ├── speedtest-server/    # 服务端入口 + web/(go:embed 前端)
│   │   │   └── web/             # index.html / app.js / style.css
│   │   └── speedctl/            # CLI 客户端
│   └── internal/
│       ├── engine/              # 采样/统计核心（纯逻辑，有单元测试）
│       ├── version/             # 构建期注入的版本号
│       ├── wsstream/            # WebSocket TCP 打流（服务端统计+result）
│       ├── tcpnative/           # 原生 TCP 测速（CLI）
│       ├── udpnative/           # 原生 UDP 测速（CLI）+ iperf式统计
│       └── rtcbridge/           # WebRTC 信令 + DataChannel 打流（服务端统计+result）
├── dest/                        # 构建产物（gitignored，makeVersion.sh 生成）
├── docs/
│   ├── 用户手册.md              # 网页与 CLI 使用教程
│   └── 部署指南.md              # 部署/常驻/防火墙/静态编译
├── .gitignore
└── go.mod                       # （位于 src/ 内）
```

## 测试

```bash
cd src
go test ./...          # 单元测试（engine、udpnative）
```

前端计数与渲染自测（Node 内置测试，无 npm 依赖）：

```bash
cd src && node --test cmd/speedtest-server/web/app.test.mjs
```

## 限制与精度说明

- 网页侧 WS/WebRTC 受浏览器协议栈开销影响，数值为"浏览器可达吞吐"；极限/精确数值请用 `speedctl`（原生 socket，iperf 风格）。
- 网页 UDP 只报告 WebRTC 应用层**交付损失**与队列残留，抖动显示不可用；IP 层意义上的精确丢包/抖动统计见 `speedctl udp` 输出。
- 测速结果为**尽力而为**的带宽指示：TCP 拥塞控制、UDP 丢包重传策略、对端 NAT 等都会影响读数；同一链路多次测试取峰值/均值更接近真实上限。
