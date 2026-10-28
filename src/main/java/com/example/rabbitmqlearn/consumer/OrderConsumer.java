package com.example.rabbitmqlearn.consumer;

import com.example.rabbitmqlearn.constant.MqConstants;
import com.example.rabbitmqlearn.message.OrderMessage;
import com.example.rabbitmqlearn.service.IdempotentService;
import com.example.rabbitmqlearn.service.RetryCounterService;
import com.rabbitmq.client.Channel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 订单消费者 —— 演示<b>手动 ACK</b>、<b>消费者限流</b>、<b>死信</b>、<b>幂等消费</b>。
 *
 * <h3>面试必考：消费者怎么保证消息不丢？</h3>
 * 关键在于 <b>ACK 时机</b>。三种模式：
 * <table border="1">
 *   <tr><th>模式</th><th>行为</th><th>风险</th></tr>
 *   <tr><td>none（自动 ACK）</td><td>消息一投出就认为消费成功</td>
 *       <td><b>最危险</b>：消费者拿到消息后宕机 → 消息丢失</td></tr>
 *   <tr><td>auto（自动 ACK，Spring 的默认）</td>
 *       <td>方法正常返回才 ACK，抛异常则 NACK</td>
 *       <td>较安全；但 catch 住异常不抛出 = 变相丢消息</td></tr>
 *   <tr><td><b>manual（手动 ACK）</b></td><td>由业务代码决定何时 basicAck</td>
 *       <td><b>推荐</b>：业务处理完成才 ACK</td></tr>
 * </table>
 *
 * <h3>面试必考：prefetch 限流</h3>
 * {@code spring.rabbitmq.listener.simple.prefetch=1} 表示每个消费者最多同时持有
 * 1 条未 ACK 的消息。作用：
 * <ul>
 *   <li>防止消费者被一次性推来的大量消息打垮（内存溢出、线程耗尽）</li>
 *   <li>让多个消费者之间更"公平"地分配负载 —— 否则消息会堆在启动早的消费者那里</li>
 *   <li>代价：prefetch 太小会降低吞吐（每次都要往返一次 Broker）</li>
 * </ul>
 * 经验值：CPU 密集型 1~10，IO 密集型可放大到几十。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderConsumer {

    private final IdempotentService idempotentService;
    private final RetryCounterService retryCounterService;

    /** 统计消费成功数，供 /demo 接口查询 */
    private final AtomicInteger successCount = new AtomicInteger();
    private final AtomicInteger failCount = new AtomicInteger();

    /**
     * 消费订单创建消息。
     *
     * <h3>手动 ACK 的三个 API</h3>
     * <ul>
     *   <li>{@code basicAck(deliveryTag, false)} —— 确认单条。multiple=true 表示
     *       确认所有 ≤ 该 tag 的消息（批量 ACK）</li>
     *   <li>{@code basicNack(deliveryTag, requeue=false, ...)} —— 拒绝，且<b>重新入队</b>。
     *       ⚠️ 危险：如果失败原因不消失（如数据本身有问题），会无限循环重试</li>
     *   <li>{@code basicReject(deliveryTag, requeue=false)} —— 拒绝单条，
     *       与 basicNack(...,false) 等效（basicNack 支持批量，basicReject 不支持）</li>
     * </ul>
     *
     * <h3>面试必考：消费失败怎么办？</h3>
     * 直接 nack + requeue 会造成<b>无限重试风暴</b>。生产环境的正确做法：
     * <ol>
     *   <li><b>重试 N 次（本地重试或延迟重试队列）</b>，每次间隔递增（1s/5s/30s）</li>
     *   <li>超过次数后 <b>nack 且 requeue=false</b> → 进死信队列</li>
     *   <li>死信队列由人工或定时任务处理，并<b>告警</b></li>
     * </ol>
     * 本方法演示的是"直接进死信"的简化版，用 deliveryTag 判断是否重试过。
     */
    @RabbitListener(queues = MqConstants.ORDER_QUEUE)
    public void handleOrderCreated(OrderMessage order, Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String messageId = order.getMessageId();

        log.info("📥 收到订单消息: orderId={}, messageId={}, deliveryTag={}, redelivered={}",
                order.getOrderId(), messageId, deliveryTag,
                message.getMessageProperties().isRedelivered());

        try {
            // ===== 第 0 步：幂等校验（只读检查，不写入标记）=====
            // ⚠️ 关键：这里只"检查"不"标记"。
            // 如果在这里就把消息标记为已处理，那么一旦业务失败、消息 requeue 重试，
            // 第二次投递会被误判为"重复消息"直接 ACK 丢弃 —— 消息永久消失。
            // 标记必须等业务成功后再写，见下面第 2 步。
            if (idempotentService.isProcessed(messageId)) {
                channel.basicAck(deliveryTag, false);
                log.warn("🔁 重复消息已 ACK 丢弃: messageId={}", messageId);
                return;
            }

            // ===== 第 1 步：真正的业务处理 =====
            doBusiness(order);

            // ===== 第 2 步：业务成功后才 ACK，并写入幂等标记 =====
            // 顺序很重要：先确认业务成功，再标记 + ACK。
            // 反过来就会把失败的消息毒化。
            idempotentService.markProcessed(messageId);
            channel.basicAck(deliveryTag, false);
            successCount.incrementAndGet();
            retryCounterService.clear(messageId);   // 成功后清掉重试计数
            log.info("✅ 订单处理成功并已 ACK: orderId={}, messageId={}", order.getOrderId(), messageId);

        } catch (Exception e) {
            failCount.incrementAndGet();
            log.error("❌ 订单处理失败: orderId={}, 原因={}", order.getOrderId(), e.getMessage());

            // ===== 第 3 步：失败处理 =====
            // ⚠️ 这里<b>不能</b>依赖 message.getMessageProperties().isRedelivered()！
            // 实测踩坑：redelivered 只表示"投递过"，不能表示"重试了几次"。
            // 只用它判断会导致消息在「失败 → requeue → 再失败」之间循环，
            // 永远进不了死信队列 —— 就是无限重试风暴。
            // 正确做法是用显式计数（生产环境用 Redis INCR 或 x-death 头）。
            int attempt = retryCounterService.getAndIncrement(messageId);

            if (attempt <= RetryCounterService.MAX_RETRY) {
                // requeue=true → 消息重新入队，等下一次投递
                channel.basicNack(deliveryTag, false, true);
                log.warn("🔁 第 {} 次失败，消息重新入队等待重试: orderId={} (上限 {} 次)",
                        attempt, order.getOrderId(), RetryCounterService.MAX_RETRY);
            } else {
                // 重试次数用完 → requeue=false
                // 消息按队列配置的 DLX 进入 order.dlx.exchange → order.failed.queue
                channel.basicNack(deliveryTag, false, false);
                log.error("💀 重试次数已用完({} 次)，送入死信队列: orderId={}, messageId={}",
                        RetryCounterService.MAX_RETRY, order.getOrderId(), messageId);
            }
        }
    }

    /**
     * 真实业务逻辑。演示用：orderId 能被 10 整除的订单模拟处理失败。
     */
    private void doBusiness(OrderMessage order) {
        if (order.getOrderId() != null && order.getOrderId() % 10 == 0) {
            throw new IllegalStateException("模拟业务失败: orderId=" + order.getOrderId());
        }
        // 真实场景：扣库存、写订单表、调用其他服务...
    }

    /**
     * 消费死信队列 —— 处理那些重试后仍然失败的消息。
     * <p>
     * 生产环境这里通常是：记录到数据库 + 发告警 + 等人工干预，
     * 而不是继续自动重试（否则死信队列也会被堵满）。
     */
    @RabbitListener(queues = MqConstants.ORDER_FAILED_QUEUE)
    public void handleFailedOrder(OrderMessage order, Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        log.error("💀 [死信队列] 收到处理失败的订单，需人工介入: orderId={}, messageId={}",
                order.getOrderId(), order.getMessageId());

        // 死信也手动 ACK —— 落库/告警完成后确认，避免死信队列堆积
        // 实践中这里通常写入一张 order_failed 表，由运营后台重推
        channel.basicAck(deliveryTag, false);
    }

    /**
     * 消费延迟队列的死信 —— 订单超时未支付，执行自动取消。
     * <p>
     * 这条消息原本在 order.delay.queue 里待了 30 秒（TTL），
     * 过期后作为死信被转投到 order.cancel.queue。
     */
    @RabbitListener(queues = MqConstants.ORDER_CANCEL_QUEUE)
    public void handleOrderCancel(OrderMessage order, Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        log.warn("⏰ [延迟队列] 订单超时未支付，执行自动取消: orderId={}, 下单时间={}",
                order.getOrderId(), order.getTimestamp());

        // 真实场景：查库确认订单仍是"待支付"状态再取消（避免用户已经付了）
        //   if (orderService.isStillUnpaid(order.getOrderId())) {
        //       orderService.cancel(order.getOrderId());
        //   }
        channel.basicAck(deliveryTag, false);
    }

    public int getSuccessCount() {
        return successCount.get();
    }

    public int getFailCount() {
        return failCount.get();
    }
}
