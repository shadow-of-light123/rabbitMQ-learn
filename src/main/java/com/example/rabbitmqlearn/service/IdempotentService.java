package com.example.rabbitmqlearn.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 幂等消费服务 —— 演示<b>如何解决重复消费</b>。
 *
 * <h3>面试必考：消息为什么会重复？</h3>
 * 只要用到"至少一次"投递语义，重复就<b>不可避免</b>：
 * <ol>
 *   <li>消费者处理成功，但 <b>ACK 在网络中丢失</b> → Broker 以为没消费，重新投递</li>
 *   <li>消费者处理耗时过长，<b>连接超时断开</b> → 消息 requeue 后被重新投递</li>
 *   <li>生产者 <b>Confirm 超时后重发</b>，但第一条其实已经成功了</li>
 *   <li>Broker 主从切换，消费位点回退</li>
 * </ol>
 *
 * <b>结论</b>：不要试图让 MQ 不重复（做不到，会牺牲可用性），
 * 而是让<b>消费方幂等</b> —— 同一条消息处理多少次，结果都一样。
 *
 * <h3>面试必答：幂等的实现方式</h3>
 * <ol>
 *   <li><b>数据库唯一索引</b>（最常用）：用 messageId 或业务唯一键建唯一索引，
 *       重复插入直接抛 DuplicateKeyException，捕获后忽略。</li>
 *   <li><b>Redis SETNX</b>：setIfAbsent(messageId)，设置合理过期时间。
 *       注意：Redis 与 DB 不是同一事务，极端情况仍可能重复，需兜底。</li>
 *   <li><b>状态机</b>：更新时带上前置状态条件，
 *       <code>UPDATE order SET status='PAID' WHERE id=? AND status='UNPAID'</code>，
 *       靠影响行数判断是否重复（天然幂等，推荐）。</li>
 *   <li><b>去重表</b>：单独一张表记录已处理的消息 ID，与业务操作同事务提交。</li>
 * </ol>
 *
 * <h3>⚠️⚠️ 关键陷阱：幂等标记的写入时机</h3>
 * 本类最初把「检查 + 标记」合成一个 {@code tryMarkProcessed()} 方法，
 * 在业务逻辑<b>之前</b>调用 —— 结果踩了大坑：
 *
 * <pre>
 *   第 1 次投递：标记为已处理 → 业务失败 → 消息 requeue
 *   第 2 次投递：发现已被标记 → 判定为"重复消息" → 直接 ACK 丢弃 ❌
 *                                        ↑ 重试被误当成重复，消息被永久吞掉
 * </pre>
 *
 * 一次<b>失败</b>的尝试就把这个 messageId 永久"毒化"了，后续重试全部被丢弃，
 * 既无法重试成功，也永远进不了死信队列 —— 消息就此消失。
 *
 * <p><b>正确做法（本类现在的设计）</b>：把「检查」和「标记」拆成两步，
 * <b>只在业务处理成功后才写入标记</b>：
 * <pre>
 *   if (isProcessed(id)) { ack; return; }   // ① 只读检查，不写入
 *   doBusiness();                            // ② 业务，可能失败
 *   markProcessed(id);                       // ③ 成功了才标记
 *   ack();
 * </pre>
 * 这样失败的消息不会留下标记，重试时能正常执行业务。
 *
 * <p>面试时如果能把这一点讲出来，说明你真正在业务里踩过坑，
 * 比背"用 Redis 做幂等"有说服力得多。
 *
 * <h3>⚠️ 本类仍是<b>教学演示</b></h3>
 * 用本地内存 Set 做去重，<b>重启即失效</b>，且单机有效、集群无效。
 * 生产环境必须换成 Redis 或数据库唯一索引。
 */
@Slf4j
@Service
public class IdempotentService {

    /** 生产环境请替换为 Redis 或数据库唯一索引 */
    private final Set<String> processedMessageIds = ConcurrentHashMap.newKeySet();

    /**
     * ① 只读检查：这条消息之前是否<b>成功处理过</b>？
     * <p>
     * 注意：本方法<b>不写入</b>任何状态。写入动作交给 {@link #markProcessed}，
     * 且必须在业务成功之后调用。
     *
     * @return true = 已成功处理过，应直接 ACK 丢弃
     */
    public boolean isProcessed(String messageId) {
        if (messageId == null || messageId.isBlank()) {
            // 没有 messageId 就无法去重，只能放行（并记警告）
            log.warn("消息没有 messageId，无法做幂等控制");
            return false;
        }
        return processedMessageIds.contains(messageId);
    }

    /**
     * ③ 标记为已处理。<b>必须在业务逻辑成功执行之后调用。</b>
     * <p>
     * 提前调用会导致失败的消息把 messageId 毒化，重试被误判为重复而丢弃。
     */
    public void markProcessed(String messageId) {
        if (messageId == null || messageId.isBlank()) {
            return;
        }
        processedMessageIds.add(messageId);
    }

    /** 仅用于演示：查看已处理的消息数量 */
    public int processedCount() {
        return processedMessageIds.size();
    }
}
