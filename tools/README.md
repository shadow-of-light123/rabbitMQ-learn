# RabbitMQ 便携版运行环境

本目录是一套**免管理员、免安装、不写注册表**的 RabbitMQ 运行环境。
不想要了，直接删掉整个 `tools` 目录即可，系统里不留任何痕迹。

## 为什么不用 Docker？

本机装了 Docker Desktop，但 **WSL2 完全没安装**：

| 检查项 | 结果 |
|---|---|
| `wsl --version` | 只输出帮助文本（不是真的 WSL） |
| `C:\Windows\System32\lxss\tools` | 不存在（没有 WSL 内核） |
| `sc query WslService` | 错误 1060：服务未安装 |
| 注册表 `HKCU\...\Lxss` | 不存在（没有任何发行版） |

Docker Desktop 的 Linux 引擎依赖 WSL2 或 Hyper-V，而启用这两者需要
**管理员权限 + 重启电脑**。所以 Docker 引擎会一直返回 HTTP 500，不是等待能解决的。

本便携版提供了 Docker 的核心好处（隔离、好清理），且不需要任何特权。

## 版本组合（重要）

| 组件 | 版本 | 说明 |
|---|---|---|
| RabbitMQ | **4.3.6** | 当前最新正式版 |
| Erlang/OTP | **28.2** | 4.3.6 只支持 Erlang **27~28.x** |

> ⚠️ **不能用 Erlang 29**。虽然 OTP 29 已经发布，但 RabbitMQ 要 4.3.7 才开始支持它，
> 而 4.3.7 目前还不存在。装错了 RabbitMQ 会直接启动失败。
> 查证：<https://www.rabbitmq.com/docs/which-erlang>

## 使用步骤

### 1. 解压（只需一次）

双击 **`setup.bat`**

它会用系统自带的 `tar` 解压两个 zip，得到 `erlang\` 和 `rabbitmq\` 两个目录。

### 2. 启动服务

双击 **`start-rabbitmq.bat`**

- 这个窗口**要保持开着**，关掉就等于停止 RabbitMQ
- 数据目录会建在 `tools\data\` 下（而不是默认的 `%APPDATA%`），便于清理
- 首次启动会初始化数据库，稍等十几秒

### 3. 开启管理台（只需一次）

**另开一个窗口**，双击 **`enable-management.bat`**

开启后配置会持久化，以后启动自动生效。

### 4. 访问管理台

浏览器打开 <http://localhost:15672>，账号密码都是 `guest`。

## 端口

| 端口 | 用途 |
|---|---|
| 5672 | AMQP 协议（应用连接用） |
| 15672 | 管理台 Web UI |

## 验证是否启动成功

```bash
# 看端口是否在监听
netstat -ano | findstr "5672 15672"

# 看节点状态
tools\rabbitmq\sbin\rabbitmqctl.bat status
```

## 常用运维命令

在 `tools\rabbitmq\sbin\` 下（或先按 `start-rabbitmq.bat` 的方式配好 PATH）：

```bat
rabbitmqctl.bat status              :: 节点状态
rabbitmqctl.bat list_queues name messages   :: 各队列消息数
rabbitmqctl.bat list_exchanges      :: 所有交换机
rabbitmqctl.bat list_bindings       :: 所有绑定关系
rabbitmqctl.bat list_consumers      :: 在线消费者
```

## 彻底重置（清空所有数据）

```bat
:: 先停止 RabbitMQ（关掉 start-rabbitmq.bat 的窗口）
rmdir /s /q tools\data
```

下次启动会重新初始化，相当于全新安装。

## 如果启动失败

**报 "unable to connect to node" / 节点起不来**
→ 检查 Erlang 版本是不是 28.x（不能用 29）。运行 `tools\erlang\bin\erl.exe -version` 查看。

**报端口被占用**
→ `netstat -ano | findstr "5672"` 找到占用进程，`taskkill /PID <pid> /F`

**Erlang Cookie 问题（集群相关）**
→ 便携版会自动在 `%USERPROFILE%\.erlang.cookie` 生成。删掉它重启即可重新生成。
