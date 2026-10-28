package com.example.rabbitmqlearn.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 重试计数服务 —— 精确控制消息重试次数。
 *
 * <h3>为什么不能依赖 {@code isRedelivered()}？</h3>
 * RabbitMQ 的 <code>redelivered</code> 标志只表示"这条消息之前投递过"，
 * 它<b>不能表示重试了几次</b>。而且配合 <code>requeue=true</code> 时，
 * 消息可能被同一消费者反复取到，字段语义边界模糊，无法可靠判断"该放弃了"。
 *
 * <p>实际踩到的现象：只靠 <code>isRedelivered()</code> 判断时，
 * 消息会在「失败 → requeue → 再失败」之间循环，永远进不了死信队列 ——
 * 也就是经典的<b>无限重试风暴</b>。
 *
 * <h3>生产环境的正确做法</h3>
 * <ol>
 *   <li><b>显式计数</b>（本类）：用 Redis 的 INCR + EXPIRE 记录每个 messageId 的重试次数，
 *       超过阈值就 nack(requeue=false) 送死信。多实例部署也能共享计数。</li>
 *   <li><b>延迟重试队列</b>（推荐）：失败后把消息发到 TTL 递增的重试队列
 *       （retry.1s / retry.5s / retry.30s），到期后回到主队列。
 *       好处：不占用消费者、重试间隔可控、次数天然由队列层级决定。</li>
 *   <li><b>利用 x-death 头</b>：消息经过 DLX 时 Broker 会自动加
 *       <code>x-death</code> 头记录流转历史，其中 <code>count</code> 字段就是次数。
 *       这是 Broker 原生的计数方式，无需额外存储。</li>
 * </ol>
 *
 * <h3>⚠️ 本类仍是教学演示</h3>
 * 用内存 Map 计数，<b>重启即失效</b>，多实例也不共享。
 * 生产必须换 Redis 或依赖 x-death 头。
 */
@Slf4j
@Service
public class RetryCounterService {

    /** 最大重试次数，超过就送死信 */
    public static final int MAX_RETRY = 2;

    /** messageId -> 已重试次数。生产环境请替换为 Redis INCR */
    private final Map<String, AtomicInteger> retryCounts = new ConcurrentHashMap<>();

    /**
     * 记录一次重试，并返回当前是否已超出上限。
     *
     * @return true  = 还可以重试（本次失败后应 requeue）
     *         false = 重试次数已用完，应 nack(requeue=false) 送死信
     */
    public boolean canRetry(String messageId) {
        return getAndIncrement(messageId) <= MAX_RETRY;
    }

    /**
     * 计数 +1 并返回<b>本次</b>的序号（从 1 开始）。
     * <p>
     * 单独暴露出来是为了让日志能打准确的"第 N 次"，而不是全局跟踪数。
     */
    public int getAndIncrement(String messageId) {
        if (messageId == null || messageId.isBlank()) {
            // 没有 messageId 就无法计数，保守起见直接放弃重试，避免无限循环
            log.warn("消息缺少 messageId，无法计数，直接送死信以避免无限重试");
            return Integer.MAX_VALUE;
        }
        int count = retryCounts
                .computeIfAbsent(messageId, k -> new AtomicInteger(0))
                .incrementAndGet();
        log.debug("消息重试计数: messageId={}, 第 {} 次", messageId, count);
        return count;
    }

    /** 查询某条消息当前已重试的次数（0 表示还没重试过） */
    public int getCount(String messageId) {
        AtomicInteger c = messageId == null ? null : retryCounts.get(messageId);
        return c == null ? 0 : c.get();
    }

    /** 消费成功后清掉计数，释放内存 */
    public void clear(String messageId) {
        if (messageId != null) {
            retryCounts.remove(messageId);
        }
    }

    /** 当前正在跟踪的消息数（演示用） */
    public int trackedCount() {
        return retryCounts.size();
    }
}
