# RabbitMQ 面试复习手册

> 配套工程：Spring Boot 4.1.1 + Java 21 + Spring AMQP
> 每个考点都有可运行的代码，见 `src/main/java/com/example/rabbitmqlearn/`

---

## 环境启动（先看这里）

RabbitMQ 服务端在 `tools/` 目录，是一套**免管理员、免安装**的便携环境。

```bat
tools\setup.bat              :: 一键解压（只需一次）
tools\start-rabbitmq.bat     :: 启动服务（窗口保持开着）
tools\enable-management.bat  :: 另开窗口，开启管理台（只需一次）
```

管理台：<http://localhost:15672>　账号：`guest` / `guest`

启动应用：

```bash
./mvnw spring-boot:run
```

> 详细说明（含为什么不走 Docker、版本约束、排错）见 **`tools/README.md`**
> Maven 构建踩坑见 **`docs/maven-踩坑记录.md`**
> **开发踩坑记录（面试加分素材）见 `docs/开发踩坑记录.md`**
> **动手练习清单见 `docs/动手练习清单.md`**

---

## 目录

1. [核心概念速查](#1-核心概念速查)
2. [消息丢失的三种情况（⭐⭐⭐ 必考）](#2-消息丢失的三种情况必考)
3. [重复消费与幂等（⭐⭐⭐ 必考）](#3-重复消费与幂等必考)
4. [Exchange 四种类型（⭐⭐⭐ 必考）](#4-exchange-四种类型必考)
5. [消息积压怎么处理（⭐⭐）](#5-消息积压怎么处理)
6. [死信队列与延迟队列（⭐⭐）](#6-死信队列与延迟队列)
7. [如何保证消息顺序性（⭐⭐）](#7-如何保证消息顺序性)
8. [消费者限流 prefetch（⭐）](#8-消费者限流-prefetch)
9. [集群与高可用（⭐）](#9-集群与高可用)
10. [面试速答卡](#10-面试速答卡)

---

## 1. 核心概念速查

消息的完整链路：

```
生产者 ──> Exchange ──(Binding + RoutingKey)──> Queue ──> 消费者
```

| 概念 | 作用 | 关键点 |
|---|---|---|
| **Exchange** | 路由中枢 | **不存储**消息。路由不到队列时默认**静默丢弃** |
| **Queue** | 存储消息 | 真正存消息的地方，有容量、TTL、DLX 等属性 |
| **Binding** | 路由规则 | 连接 Exchange 和 Queue，可带 RoutingKey 或 header 条件 |
| **RoutingKey** | 消息的"地址" | 生产者发送时指定 |
| **vhost** | 虚拟主机 | 逻辑隔离，不同 vhost 的 Exchange/Queue 互不可见 |
| **Connection / Channel** | 连接 / 信道 | Connection 是 TCP 连接，Channel 是其上的轻量复用通道。**不要多线程共享 Channel** |

> **面试陷阱**：有人会答"消息存在 Exchange 里"。错。Exchange 只做路由，收到消息后立即转发；**没有匹配队列就丢弃**（除非开 mandatory）。

---

## 2. 消息丢失的三种情况（⭐⭐⭐ 必考）

**这是 RabbitMQ 面试第一题。** 答题框架是"分段回答"：

```
生产者 ──①──> Exchange ──②──> Queue ──③──> 消费者
```

### ① 生产者 → Broker 之间丢

**原因**：网络抖动、Broker 宕机、RoutingKey 写错导致路由不到队列。

**解决**：
- **Confirm 机制**：确认消息是否到达 Exchange。`publisher-confirm-type: correlated`
- **Return 机制**：消息到了 Exchange 但路由不到 Queue 时回调。需同时开 `publisher-returns: true` + `mandatory: true`
- **本地消息表 / 事务消息**：Confirm 是异步的，强一致场景要落库 + 定时补偿重发

```java
// 见 OrderProducer.java
rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> { ... });
rabbitTemplate.setReturnsCallback(returned -> { ... });
```

> **代码演示**：`POST /demo/send-nowhere` 观察 Return 回调

### ② Broker 内部丢

**原因**：消息只在内存，Broker 重启就没了；或队列非持久化。

**解决**（三个都要满足，缺一不可）：
1. **Exchange 持久化** —— `new DirectExchange(name, true, false)`
2. **Queue 持久化** —— `QueueBuilder.durable(name)`
3. **消息持久化** —— `MessageProperties.setDeliveryMode(PERSISTENT)`

```java
// 见 RabbitConfig.java
QueueBuilder.durable(MqConstants.ORDER_QUEUE).build()  // durable = true
```

> **面试加分**：即使三者都开了，消息在**写入磁盘前**（还在 page cache）Broker 宕机仍会丢。要彻底解决需用 **Quorum Queue**（Raft 多数派确认）。

### ③ 消费者 → 处理完成之间丢

**原因**：自动 ACK 模式下，消息一投出就认为消费成功；消费者崩了消息就没了。

**解决**：**手动 ACK**，业务处理完成后再确认。

```yaml
spring.rabbitmq.listener.simple.acknowledge-mode: manual
```

```java
// 见 OrderConsumer.java
doBusiness(order);                      // 先做业务
channel.basicAck(deliveryTag, false);   // 再做 ACK ← 顺序不能反
```

> **典型错误**：把 `basicAck` 写在业务逻辑**之前** —— 那和自动 ACK 没区别，等于没做。

### 一句话总结

> 生产者用 **Confirm + Return** 保到达，Broker 用 **持久化（Exchange + Queue + 消息）** 保存储，
> 消费者用 **手动 ACK** 保处理。三者齐备才能做到"不丢"。

---

## 3. 重复消费与幂等（⭐⭐⭐ 必考）

### 为什么一定会重复？

只要用"至少一次"（at-least-once）投递，重复就**不可避免**：

1. 消费者处理成功，但 **ACK 在网络中丢失** → Broker 以为没消费，重新投递
2. 消费者处理超时，**连接断开** → 消息 requeue 后重新投递
3. 生产者 **Confirm 超时后重发**，但第一条其实已成功
4. Broker 主从切换，消费位点回退

> **面试关键句**：不要试图让 MQ 不重复 —— 那要么牺牲性能（两阶段提交），要么牺牲可用性。
> 正确做法是**让消费方幂等**。

### 幂等的四种实现

| 方式 | 做法 | 优缺点 |
|---|---|---|
| **数据库唯一索引** | 用 messageId / 业务唯一键建唯一索引，重复插入抛 `DuplicateKeyException` 后忽略 | 最常用，可靠 |
| **Redis SETNX** | `setIfAbsent(messageId, ttl)` | 快；但 Redis 与 DB 非同事务，极端情况仍重复 |
| **状态机**（推荐） | `UPDATE order SET status='PAID' WHERE id=? AND status='UNPAID'`，看影响行数 | **天然幂等**，无需额外表 |
| **去重表** | 单独表记录已处理 messageId，与业务同事务提交 | 可靠，多一张表 |

```java
// 见 IdempotentService.java（教学演示用内存 Set，生产须换 Redis/DB）
if (!idempotentService.tryMarkProcessed(messageId)) {
    channel.basicAck(deliveryTag, false);  // 重复消息直接 ACK 丢弃
    return;
}
```

> **代码演示**：`POST /demo/send-duplicate` 用同一 messageId 发 3 条，只有 1 条被处理

---

## 4. Exchange 四种类型（⭐⭐⭐ 必考）

| 类型 | 路由规则 | 适用场景 | RoutingKey 是否生效 |
|---|---|---|---|
| **Direct** | RoutingKey **精确匹配** | 点对点、明确知道投给谁 | ✅ 精确 |
| **Topic** | RoutingKey **通配符匹配** | 按业务维度灵活路由 | ✅ 模式匹配 |
| **Fanout** | **广播到所有**绑定队列 | 一个事件通知多个下游 | ❌ 忽略 |
| **Headers** | 按 **消息 header** 匹配 | 路由条件无法用字符串表达 | ❌ 忽略 |

### Topic 通配符（必考细节）

- `*` 匹配**恰好一个**单词
- `#` 匹配**零个或多个**单词
- 单词之间用 `.` 分隔

```
绑定 order.*   → ✅ order.created      ❌ order.created.vip
绑定 order.#   → ✅ order.created      ✅ order.created.vip   ✅ order
绑定 pay.#     → ✅ pay               ✅ pay.wechat.success
```

```java
// 见 RabbitConfig.java
return BindingBuilder.bind(topicOrderQueue()).to(topicExchange()).with("order.*");
```

> **代码演示**：`POST /demo/exchanges` 一次发完四种类型，去管理台对比各队列 Ready 数量

---

## 5. 消息积压怎么处理（⭐⭐）

### 答题框架（按顺序答，体现排查思路）

**第一步：先定位原因**

看管理台的 **publish rate** vs **deliver rate**：
- **消费者变慢** → 下游 DB 慢查询、外部接口超时、消费逻辑有锁
- **生产者突增** → 秒杀、批量导入、上游异常重试风暴

**第二步：紧急止血**

1. **扩容消费者**：增加实例数，或提高 `concurrency` / `max-concurrency`
2. **提高 prefetch**：批量拉取，减少与 Broker 的往返
3. **降级非核心消息**：先丢弃或转存，保主链路

**第三步：如果消费者实在处理不过来**

把积压消息**快速转存**到临时队列或 DB（用轻量消费者专门搬运），
再用另一批消费者慢慢处理 —— 避免堵塞主队列导致新消息也进不来。

**第四步：根治**

优化消费逻辑（加缓存、异步化、批处理）、给下游加限流保护、压测确定容量水位。

> ⚠️ **不要答"直接清空队列"** —— 那等于丢数据，面试官会立刻扣分。

> **代码演示**：`POST /demo/flood/1000` 制造积压，然后调大 prefetch 观察变化

---

## 6. 死信队列与延迟队列（⭐⭐）

### 消息变成死信的三种情况（必背）

1. 消费者 `basicNack` / `basicReject` 且 **`requeue=false`**
2. 消息 **TTL 过期**
3. 队列达到**最大长度**，最老的消息被挤出

### 配置方式

```java
// 见 RabbitConfig.java
QueueBuilder.durable(MqConstants.ORDER_QUEUE)
        .deadLetterExchange(MqConstants.DLX_EXCHANGE)      // 死信发到哪
        .deadLetterRoutingKey(MqConstants.ORDER_FAILED_KEY) // 用什么 key
        .build();
```

### 延迟队列的三种实现（必考对比）

| 方案 | 原理 | 优点 | 缺点 |
|---|---|---|---|
| **TTL + DLX** | 消息在队列待够 TTL → 过期变死信 → 转投目标队列 | 不装插件，原生支持 | **精度差**（见下方缺陷） |
| **延迟插件** | `rabbitmq-delayed-message-exchange` | 精度高、支持任意延迟 | 需装插件，非官方默认 |
| **定时任务扫表** | 定时扫 DB 找超时订单 | 简单、可靠、可追溯 | 精度受扫描间隔限制，大表有压力 |

> **TTL 方案的致命缺陷（面试加分点）**：
> RabbitMQ **只检查队首消息**是否过期。如果先发 TTL=30min 的消息、再发 TTL=1s 的消息，
> 后者会排在后面，必须等前面那条 30min 的消息过期后才会被检查到 —— **造成延迟严重不准**。
> 解决办法：按 TTL 拆分到不同队列（如 1s 队列、5s 队列、30min 队列），或改用延迟插件。

> **代码演示**：`POST /demo/send-delay/123` → 30 秒后观察 `⏰[延迟队列]` 日志

### 消费失败的正确处理姿势

```
失败 → requeue 重试 N 次（间隔递增 1s/5s/30s）→ 仍失败 → nack(requeue=false) → 死信队列 → 告警 + 人工介入
```

> **反例**：`basicNack(requeue=true)` 无脑重试 → 失败原因不消失 → **无限重试风暴**，打满 CPU。

> **代码演示**：`POST /demo/send-fail` 观察"首次 requeue 重试 → 二次进死信"的完整过程

---

## 7. 如何保证消息顺序性（⭐⭐）

### 先说结论：RabbitMQ **不保证**全局顺序

**破坏顺序的原因**：
1. 一个队列有**多个消费者** → 消息被并发消费，完成顺序不确定
2. 消费失败 **requeue** → 消息回到队尾，顺序被打乱
3. 集群模式下多个节点间同步有延迟

### 解决思路

| 场景 | 方案 |
|---|---|
| **全局有序** | 单队列 + 单消费者（`concurrency=1`）。吞吐极低，慎用 |
| **局部有序**（推荐） | 按业务 key（如 orderId）**hash 到不同队列**，同一 key 的消息进同一队列，每队列单消费者 |
| **消费端排序** | 消息带序号，消费端用滑动窗口/缓冲重排序 |

> **面试加分**：真正高性能的做法是**业务上规避** —— 比如订单状态变更用"状态机 + 版本号"，
> 旧版本的消息直接丢弃，而不是费力保证顺序。

---

## 7.5 两个实战踩坑（面试讲出来很加分）

这两个坑是本项目**实际开发时踩到的**，都是生产环境真实的事故类型。
完整记录见 `docs/开发踩坑记录.md`。

### 坑 A：`isRedelivered()` 不能用来判断重试次数

**现象**：消息消费失败后无限重试，**永远进不了死信队列**。

```
order.business.queue   累计发布=59  累计投递=68   ← 多出 9 次重试
order.failed.queue     累计发布=0   累计投递=0    ← 死信队列一条没收到
```

**原因**：`redelivered` 只表示"这条消息之前投递过"，**不能表示重试了几次**。
在 `requeue=true` 场景下语义边界模糊，导致「失败 → requeue → 再失败」无限循环。

**正确做法**：显式计数（Redis `INCR`），或用 **`x-death`** 头
（消息经过 DLX 时 Broker 自动累加 `count`），或改用延迟重试队列。

### 坑 B：幂等标记的写入时机（最隐蔽，会静默丢消息）

**现象**：失败的消息在重试时被**当作重复消息直接 ACK 丢弃**，凭空消失且无任何告警。

**原因**：把「检查 + 标记」合在业务逻辑**之前**：

```
第 1 次投递：标记为已处理 → 业务失败 → 消息 requeue
第 2 次投递：发现已被标记 → 判定"重复消息" → 直接 ACK 丢弃 ❌
                                    ↑ 重试被误当成重复，消息永久消失
```

**正确做法**：拆成两步，**只在业务成功后才写入标记**：

```java
if (idempotentService.isProcessed(messageId)) { ack; return; }  // ① 只读检查
doBusiness(order);                                               // ② 可能失败
idempotentService.markProcessed(messageId);                      // ③ 成功了才标记
channel.basicAck(deliveryTag, false);
```

> **核心认知**：**幂等和重试是相互作用的**。幂等做错了会把重试机制一起毁掉 ——
> 消息既不重试也不进死信，直接消失。这类 bug 最危险，因为**没有任何报错**。

---

## 8. 消费者限流 prefetch（⭐）

```yaml
spring.rabbitmq.listener.simple.prefetch: 1
```

含义：每个消费者最多同时持有 **1 条**未 ACK 的消息。

**作用**：
- 防止消费者被一次性推来的大量消息**打垮**（内存溢出、线程耗尽）
- 让多个消费者之间**公平分配**负载 —— 否则消息会全堆在启动早的消费者那里
- 代价：prefetch 太小会降低吞吐（每条都要往返 Broker 一次）

**经验值**：CPU 密集 1~10；IO 密集可放大到几十。

> 注意：prefetch 是**按 channel** 生效的，不是按队列。

---

## 9. 集群与高可用（⭐）

| 模式 | 原理 | 特点 |
|---|---|---|
| **普通集群** | 元数据同步，**消息只存一个节点** | 省空间；但节点挂了，其上的消息**不可用**，需等恢复 |
| **镜像队列**（经典） | 消息同步到多个节点，主从复制 | 高可用；但**同步复制**有性能损耗，脑裂处理复杂（已逐步淘汰） |
| **Quorum Queue**（4.x 推荐） | 基于 **Raft** 共识算法 | 官方推荐；强一致、自动选主；但**吞吐低于镜像队列**，且不支持部分特性 |

> **面试加分**：RabbitMQ 4.x 已**移除经典镜像队列**，新项目应直接用 Quorum Queue。
> 声明方式：`QueueBuilder.durable(name).withArgument("x-queue-type", "quorum")`

**其他高可用点**：
- **仲裁队列 / 客户端重连**：Spring AMQP 自动重连，但重连期间消息可能丢，需配合 Confirm
- **负载均衡**：多个 Broker 前挂 HAProxy / LVS，客户端连 VIP

---

## 10. 面试速答卡

### Q：RabbitMQ 如何保证消息不丢失？
> 分三段答：**生产者**用 Confirm（到达 Exchange）+ Return（路由到 Queue）+ 本地消息表补偿；
> **Broker** 用 Exchange/Queue/消息三者都持久化，更强可用 Quorum Queue；
> **消费者**用手动 ACK，业务处理成功后再确认。三处缺一不可。

### Q：消息重复了怎么办？
> 至少一次投递下重复不可避免（ACK 丢失、超时重投、主从切换）。因此要靠**消费端幂等**：
> 数据库唯一索引、Redis SETNX、**状态机条件更新**（推荐，天然幂等）或去重表。

### Q：怎么实现延迟消息？
> 三种方案：**TTL + DLX**（原生但精度差，因为只检查队首）、**延迟插件**（精度高需装插件）、
> **定时扫表**（简单可靠但精度受限于扫描间隔）。生产常用插件或扫表。

### Q：消息积压怎么处理？
> 先定位是消费慢还是生产快，再紧急扩容消费者 / 提高 prefetch，
> 处理不过来就把消息转存到临时队列用另一批消费者消费，最后根治（优化逻辑 + 限流 + 压测）。

### Q：怎么保证顺序？
> RabbitMQ 不保证全局顺序。要么单队列单消费者，要么按业务 key hash 分队列，
> 更好的是业务上规避（状态机 + 版本号，旧的丢弃）。

### Q：Direct / Topic / Fanout / Headers 区别？
> Direct 精确匹配 RoutingKey；Topic 通配符匹配（`*` 一个词，`#` 零或多个）；
> Fanout 广播忽略 RoutingKey；Headers 按消息头匹配、忽略 RoutingKey，性能较差。

### Q：prefetch 有什么用？
> 限制每个消费者未 ACK 的消息数，防止被打垮并实现负载均衡。CPU 密集 1~10，IO 密集可更大。

---

## 附：演示接口清单

| 接口 | 考点 |
|---|---|
| `POST /demo/send/{orderId}` | Confirm 可靠性投递 |
| `POST /demo/send-nowhere` | Return 路由失败回调 |
| `POST /demo/send-fail` | 重试 + 死信队列 |
| `POST /demo/send-delay/{orderId}` | 延迟队列（TTL+DLX） |
| `POST /demo/send-duplicate` | 重复消费 + 幂等 |
| `POST /demo/exchanges` | 四种 Exchange 路由差异 |
| `POST /demo/flood/{count}` | 消息积压 |
| `GET  /demo/stats` | 队列深度监控 |

管理台：http://localhost:15672 （guest / guest）
