package com.example.rabbitmqlearn.controller;

import com.example.rabbitmqlearn.constant.MqConstants;
import com.example.rabbitmqlearn.consumer.OrderConsumer;
import com.example.rabbitmqlearn.message.OrderMessage;
import com.example.rabbitmqlearn.producer.OrderProducer;
import com.example.rabbitmqlearn.service.IdempotentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * 演示接口 —— 每个端点对应一个面试考点，方便逐个验证。
 * <p>
 * 打开管理台 http://localhost:15672 (guest/guest) 可以实时观察消息流动。
 */
@Slf4j
@RestController
@RequestMapping("/demo")
@RequiredArgsConstructor
public class DemoController {

    private final OrderProducer orderProducer;
    private final OrderConsumer orderConsumer;
    private final IdempotentService idempotentService;
    private final RabbitTemplate rabbitTemplate;

    private OrderMessage buildOrder(long orderId) {
        return new OrderMessage(
                UUID.randomUUID().toString(),   // messageId：幂等的关键
                orderId,
                1001L,
                "机械键盘",
                399,
                System.currentTimeMillis());
    }

    /* ================================================================
     * 考点：可靠性投递（Confirm + Return）
     * ================================================================ */

    /** 正常发送 —— 观察日志里的 ✅ [Confirm] */
    @PostMapping("/send/{orderId}")
    public Map<String, Object> send(@PathVariable long orderId) {
        OrderMessage msg = buildOrder(orderId);
        orderProducer.sendOrderCreated(msg);
        return Map.of("status", "sent", "messageId", msg.getMessageId(),
                "hint", "看应用日志的 [Confirm] 回调；再到管理台看 order.business.queue");
    }

    /**
     * 发送一条路由不到任何队列的消息 —— 观察 ⚠️ [Return] 回调。
     * <p>
     * 这是面试常问的"消息到了 Exchange 但没进队列"场景。
     * 如果没开 mandatory=true，这条消息会被<b>静默丢弃</b>，你根本不知道丢了。
     */
    @PostMapping("/send-nowhere")
    public Map<String, Object> sendNowhere() {
        orderProducer.sendToNowhere(buildOrder(9999L));
        return Map.of("status", "sent", "hint", "看应用日志的 ⚠️ [Return] 回调");
    }

    /* ================================================================
     * 考点：死信队列（DLX）
     * ================================================================ */

    /**
     * 发送一条必然失败的消息（orderId 能被 10 整除即失败）。
     * 第一次失败 → requeue 重试；第二次仍失败 → 进死信队列 order.failed.queue。
     */
    @PostMapping("/send-fail")
    public Map<String, Object> sendFail() {
        long orderId = 100L;  // 100 % 10 == 0 → 一定失败
        orderProducer.sendOrderCreated(buildOrder(orderId));
        return Map.of("status", "sent", "orderId", orderId,
                "hint", "日志会先出现 🔁重试，再出现 💀送入死信；管理台看 order.failed.queue");
    }

    /* ================================================================
     * 考点：延迟队列（TTL + DLX）
     * ================================================================ */

    /**
     * 发送延迟消息，30 秒后自动触发取消逻辑。
     * <p>
     * 消息先进入 order.delay.queue 等待 TTL 过期，
     * 过期后变死信被路由到 order.cancel.queue。
     */
    @PostMapping("/send-delay/{orderId}")
    public Map<String, Object> sendDelay(@PathVariable long orderId) {
        orderProducer.sendDelayOrder(buildOrder(orderId));
        return Map.of("status", "sent", "orderId", orderId,
                "hint", "30 秒后观察日志的 ⏰[延迟队列]；期间消息停在 order.delay.queue");
    }

    /* ================================================================
     * 考点：重复消费（幂等）
     * ================================================================ */

    /**
     * 模拟 MQ <b>重复投递同一条消息</b>：用同一个 messageId、同一个 orderId 连发 3 条。
     * <p>
     * 这才是真实场景 —— Broker 因为 ACK 丢失而把<b>同一条</b>消息重新投递，
     * 而不是"不同订单共用 messageId"（那样建模是错的，不同订单本来就该有不同 messageId）。
     * <p>
     * 预期：只有第 1 条执行业务，后 2 条被幂等拦截直接 ACK。
     */
    @PostMapping("/send-duplicate")
    public Map<String, Object> sendDuplicate() {
        String fixedMessageId = "dup-" + UUID.randomUUID();
        long fixedOrderId = 201L;   // 201 % 10 != 0，不会触发模拟失败，演示更清晰

        for (int i = 0; i < 3; i++) {
            OrderMessage msg = new OrderMessage(fixedMessageId, fixedOrderId, 1001L, "重复测试商品", 1, System.currentTimeMillis());
            rabbitTemplate.convertAndSend(MqConstants.ORDER_EXCHANGE, MqConstants.ORDER_CREATE_KEY, msg,
                    m -> { m.getMessageProperties().setMessageId(msg.getMessageId()); return m; });
        }
        return Map.of("status", "sent 3 copies of the SAME message",
                "messageId", fixedMessageId,
                "orderId", fixedOrderId,
                "hint", "预期只有 1 条被处理，另 2 条被幂等拦截（日志显示 🔁 重复消息已 ACK 丢弃）");
    }

    /* ================================================================
     * 考点：四种 Exchange 的路由差异
     * ================================================================ */

    /**
     * 对比四种 Exchange 的路由行为，一次发完看管理台各队列的收信情况。
     */
    @PostMapping("/exchanges")
    public Map<String, Object> demoExchanges() {
        String payload = "hello-" + System.currentTimeMillis();

        // 1. Direct：精确匹配 order.create
        rabbitTemplate.convertAndSend(MqConstants.DIRECT_EXCHANGE, MqConstants.DIRECT_KEY, payload);
        //    RoutingKey 不匹配 → 进不了 direct.queue
        rabbitTemplate.convertAndSend(MqConstants.DIRECT_EXCHANGE, "order.other", payload);

        // 2. Topic：order.* 匹配 order.created；pay.# 匹配 pay.wechat.success
        rabbitTemplate.convertAndSend(MqConstants.TOPIC_EXCHANGE, MqConstants.TOPIC_ORDER_KEY, payload);
        rabbitTemplate.convertAndSend(MqConstants.TOPIC_EXCHANGE, MqConstants.TOPIC_PAY_KEY, payload);

        // 3. Fanout：忽略 RoutingKey，两个队列都收到
        rabbitTemplate.convertAndSend(MqConstants.FANOUT_EXCHANGE, "", payload);

        // 4. Headers：需要 header 满足 type=order 且 region=cn
        rabbitTemplate.convertAndSend(MqConstants.HEADERS_EXCHANGE, "", payload, m -> {
            m.getMessageProperties().setHeader("type", "order");
            m.getMessageProperties().setHeader("region", "cn");
            return m;
        });

        return Map.of("status", "sent to all exchange types",
                "hint", "去管理台 Queues 页面对比各队列的 Ready 数量变化");
    }

    /* ================================================================
     * 考点：消息积压的处理
     * ================================================================ */

    /**
     * 批量发消息制造积压，用来练习"积压怎么处理"。
     * <p>
     * 面试答题框架：
     * <ol>
     *   <li><b>先定位</b>：是消费变慢（下游 DB/接口慢）还是生产变快（突发流量）？
     *       看管理台的 publish rate vs deliver rate。</li>
     *   <li><b>紧急止血</b>：临时扩容消费者（增加实例数 / 提高 concurrency）、
     *       提高 prefetch 批量拉取。</li>
     *   <li><b>如果消费者处理太慢</b>：先丢弃/降级非核心消息，或者把消息转存到
     *       临时队列/DB，用另一批消费者专门处理，避免堵塞主链路。</li>
     *   <li><b>根治</b>：优化消费逻辑、加缓存、异步化、限流保护下游。</li>
     * </ol>
     * ⚠️ 不要用"直接清空队列"来解决，那等于丢数据。
     */
    @PostMapping("/flood/{count}")
    public Map<String, Object> flood(@PathVariable int count) {
        int actual = Math.min(count, 5000); // 上限保护
        for (int i = 0; i < actual; i++) {
            rabbitTemplate.convertAndSend(MqConstants.ORDER_EXCHANGE, MqConstants.ORDER_CREATE_KEY,
                    buildOrder(3000L + i));
        }
        return Map.of("status", "flooded", "count", actual,
                "hint", "管理台看 order.business.queue 的 Ready 堆积；调大 prefetch/concurrency 观察变化");
    }

    /* ================================================================
     * 状态查询
     * ================================================================ */

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("消费成功", orderConsumer.getSuccessCount());
        result.put("消费失败", orderConsumer.getFailCount());
        result.put("幂等已记录消息数", idempotentService.processedCount());

        // 实时读取各队列深度（面试常问：怎么监控积压）
        Map<String, Object> queues = new LinkedHashMap<>();
        for (String q : new String[]{
                MqConstants.ORDER_QUEUE, MqConstants.ORDER_FAILED_QUEUE,
                MqConstants.ORDER_DELAY_QUEUE, MqConstants.ORDER_CANCEL_QUEUE}) {
            try {
                Integer depth = rabbitTemplate.execute(channel -> {
                    com.rabbitmq.client.AMQP.Queue.DeclareOk ok = channel.queueDeclarePassive(q);
                    return ok.getMessageCount();
                });
                queues.put(q, depth);
            } catch (Exception e) {
                queues.put(q, "查询失败: " + e.getMessage());
            }
        }
        result.put("队列深度", queues);
        return result;
    }
}
