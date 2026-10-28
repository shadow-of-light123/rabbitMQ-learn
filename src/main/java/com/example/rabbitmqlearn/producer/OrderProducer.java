package com.example.rabbitmqlearn.producer;

import com.example.rabbitmqlearn.constant.MqConstants;
import com.example.rabbitmqlearn.message.OrderMessage;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

/**
 * 订单消息生产者 —— 演示<b>消息可靠性投递</b>。
 *
 * <h3>面试必考：生产者怎么保证消息不丢？</h3>
 * 消息从生产者到 Broker 要过两关，每关都可能丢：
 *
 * <pre>
 *   生产者 ──①──> Exchange ──②──> Queue
 *            Confirm        Return
 * </pre>
 *
 * <ul>
 *   <li><b>① Confirm 机制</b>：消息是否成功到达 <b>Exchange</b>。
 *       网络抖动、Broker 宕机都会导致失败。开启方式：publisher-confirm-type=correlated。</li>
 *   <li><b>② Return 机制</b>：消息到了 Exchange，但<b>路由不到任何 Queue</b>
 *       （RoutingKey 写错、队列被删）。必须同时开启 publisher-returns=true
 *       和 mandatory=true 才会回调。</li>
 * </ul>
 *
 * <b>注意</b>：Confirm 和 Return 是<b>异步</b>回调，不是同步阻塞的。
 * 如果业务要求强可靠，需要配合本地消息表 / 事务消息 + 定时补偿。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderProducer {

    private final RabbitTemplate rabbitTemplate;

    @PostConstruct
    public void initCallbacks() {
        // ========== ① Confirm 回调：消息是否到达 Exchange ==========
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            String id = correlationData != null ? correlationData.getId() : "null";
            if (ack) {
                log.info("✅ [Confirm] 消息成功到达 Exchange, id={}", id);
            } else {
                // 到达失败：需要重发 or 落库补偿
                log.error("❌ [Confirm] 消息未到达 Exchange, id={}, 原因={}", id, cause);
            }
        });

        // ========== ② Return 回调：到达 Exchange 但路由不到 Queue ==========
        rabbitTemplate.setReturnsCallback(returned -> {
            log.error("⚠️ [Return] 消息路由失败(未进任何队列)! exchange={}, routingKey={}, replyCode={}, replyText={}, body={}",
                    returned.getExchange(),
                    returned.getRoutingKey(),
                    returned.getReplyCode(),
                    returned.getReplyText(),
                    new String(returned.getMessage().getBody()));
        });
    }

    /**
     * 发送订单创建消息到业务队列。
     * <p>
     * 注意这里传了 CorrelationData —— 这是 Confirm 回调能对应到具体消息的<b>唯一凭据</b>。
     * 不传的话 correlationData 为 null，回调里无法区分是哪条消息。
     */
    public void sendOrderCreated(OrderMessage message) {
        CorrelationData correlationData = new CorrelationData(message.getMessageId());

        rabbitTemplate.convertAndSend(
                MqConstants.ORDER_EXCHANGE,
                MqConstants.ORDER_CREATE_KEY,
                message,
                msg -> {
                    // 把 messageId 也放进消息属性，方便在管理台按 ID 查消息
                    msg.getMessageProperties().setMessageId(message.getMessageId());
                    return msg;
                },
                correlationData);

        log.info("📤 已发送订单消息: orderId={}, messageId={}", message.getOrderId(), message.getMessageId());
    }

    /**
     * 发送延迟消息：先进入 order.delay.queue（TTL=30s），
     * 到期后变成死信，被路由到 order.cancel.queue 触发取消逻辑。
     */
    public void sendDelayOrder(OrderMessage message) {
        CorrelationData correlationData = new CorrelationData(message.getMessageId());

        rabbitTemplate.convertAndSend(
                MqConstants.DLX_EXCHANGE,
                MqConstants.ORDER_DELAY_KEY,
                message,
                msg -> {
                    msg.getMessageProperties().setMessageId(message.getMessageId());
                    return msg;
                },
                correlationData);

        log.info("⏰ 已发送延迟订单消息(30s后触发取消): orderId={}", message.getOrderId());
    }

    /**
     * 故意发送一条"路由不到队列"的消息，用来观察 Return 回调。
     * <p>
     * 因为 mandatory=true，这条消息会触发 ReturnsCallback 而<b>不是被静默丢弃</b>。
     */
    public void sendToNowhere(OrderMessage message) {
        CorrelationData correlationData = new CorrelationData(message.getMessageId());
        rabbitTemplate.convertAndSend(
                MqConstants.ORDER_EXCHANGE,
                "this.routing.key.does.not.exist", // 没有任何队列绑定这个 key
                message,
                correlationData);
        log.warn("🧪 已发送路由失败的消息(用于演示 Return 回调), routingKey=this.routing.key.does.not.exist");
    }
}
