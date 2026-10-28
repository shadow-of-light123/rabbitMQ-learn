package com.example.rabbitmqlearn.constant;

/**
 * 所有 Exchange / Queue / RoutingKey 的名字集中管理。
 * <p>
 * 面试点：Exchange 是消息的"路由中枢"，本身不存消息（绑定队列后才生效），
 * 真正存消息的是 Queue。
 */
public final class MqConstants {

    private MqConstants() {
    }

    /* ========== 1. Direct Exchange：精确匹配 RoutingKey ========== */
    public static final String DIRECT_EXCHANGE = "learn.direct.exchange";
    public static final String DIRECT_QUEUE = "learn.direct.queue";
    public static final String DIRECT_KEY = "order.create";

    /* ========== 2. Topic Exchange：按通配符匹配 RoutingKey ========== */
    // * 匹配一个单词，# 匹配零个或多个单词，单词之间用 . 分隔
    public static final String TOPIC_EXCHANGE = "learn.topic.exchange";
    public static final String TOPIC_ORDER_QUEUE = "learn.topic.order.queue"; // 绑定 order.*
    public static final String TOPIC_PAY_QUEUE = "learn.topic.pay.queue";     // 绑定 pay.#
    public static final String TOPIC_ORDER_KEY = "order.created";
    public static final String TOPIC_PAY_KEY = "pay.wechat.success";

    /* ========== 3. Fanout Exchange：广播，忽略 RoutingKey ========== */
    public static final String FANOUT_EXCHANGE = "learn.fanout.exchange";
    public static final String FANOUT_SMS_QUEUE = "learn.fanout.sms.queue";
    public static final String FANOUT_EMAIL_QUEUE = "learn.fanout.email.queue";

    /* ========== 4. Headers Exchange：按消息头匹配，忽略 RoutingKey ========== */
    public static final String HEADERS_EXCHANGE = "learn.headers.exchange";
    public static final String HEADERS_QUEUE = "learn.headers.queue";

    /* ========== 5. 业务交换机：订单消息走这里 ========== */
    public static final String ORDER_EXCHANGE = "order.exchange";
    public static final String ORDER_CREATE_KEY = "order.create";

    /* ========== 6. 死信交换机（DLX）与死信队列（DLQ） ========== */
    // 触发死信的三种情况：消费者 basicNack/reject 且 requeue=false、
    // 消息 TTL 过期、队列达到最大长度
    public static final String DLX_EXCHANGE = "order.dlx.exchange";
    public static final String ORDER_FAILED_QUEUE = "order.failed.queue";
    public static final String ORDER_FAILED_KEY = "order.failed";

    /* ========== 7. 业务主队列：消费失败后进 DLQ ========== */
    public static final String ORDER_QUEUE = "order.business.queue";

    /* ========== 8. 延迟队列：TTL 到期后死信到取消队列 ========== */
    public static final String ORDER_DELAY_QUEUE = "order.delay.queue";
    public static final String ORDER_DELAY_KEY = "order.delay";
    public static final String ORDER_CANCEL_QUEUE = "order.cancel.queue";
    public static final String ORDER_CANCEL_KEY = "order.cancel";

    /** 订单未支付超时时间：30 秒（真实业务一般 30 分钟） */
    public static final long ORDER_TIMEOUT_MILLIS = 30_000L;
}
