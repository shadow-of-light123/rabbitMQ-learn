package com.example.rabbitmqlearn.config;

import com.example.rabbitmqlearn.constant.MqConstants;
import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ 拓扑声明：所有 Exchange / Queue / Binding 都在这里定义。
 * <p>
 * 应用启动时 Spring AMQP 会自动向 Broker 声明这些组件（幂等操作，已存在则跳过）。
 *
 * <h3>面试核心概念</h3>
 * <ul>
 *   <li><b>Exchange</b>：路由中枢，本身<b>不存储</b>消息。收到消息后按类型 + Binding 决定投递到哪些 Queue。
 *       没有匹配队列时：mandatory=true 会 Return 给生产者，否则<b>静默丢弃</b>。</li>
 *   <li><b>Queue</b>：真正存储消息的地方。</li>
 *   <li><b>Binding</b>：Exchange 和 Queue 之间的"路由规则"。</li>
 *   <li><b>RoutingKey</b>：生产者发消息时带的"地址"。</li>
 * </ul>
 */
@Configuration
public class RabbitConfig {

    /**
     * 用 JSON 序列化消息体，而不是 JDK 默认序列化。
     * <p>
     * 面试点：JDK 序列化要求生产者和消费者有完全相同的类路径 + serialVersionUID，
     * 跨语言/跨版本升级极易失败，所以生产环境一律用 JSON。
     * <p>
     * ⚠️ <b>版本陷阱</b>：Spring Boot 4 / Spring AMQP 4 已经迁移到 <b>Jackson 3</b>
     * （包名 <code>tools.jackson.*</code>）。Jackson 2 的
     * <code>Jackson2JsonMessageConverter</code> 在这里会抛
     * <code>NoClassDefFoundError: com/fasterxml/jackson/databind/json/JsonMapper</code>，
     * 因为这个类根本不在 classpath 上。要用<b>不带 2</b> 的
     * {@link JacksonJsonMessageConverter}。
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    /* ==================================================================
     * 1. Direct Exchange —— 精确匹配 RoutingKey
     * 场景：点对点、明确知道要投给谁。最常用。
     * ================================================================== */

    @Bean
    public DirectExchange directExchange() {
        // durable=true：Broker 重启后 Exchange 仍然存在（注意：这只保证 Exchange 不丢，
        // 不保证 Queue 和消息不丢）
        return new DirectExchange(MqConstants.DIRECT_EXCHANGE, true, false);
    }

    @Bean
    public Queue directQueue() {
        return QueueBuilder.durable(MqConstants.DIRECT_QUEUE).build();
    }

    @Bean
    public Binding directBinding() {
        return BindingBuilder.bind(directQueue())
                .to(directExchange())
                .with(MqConstants.DIRECT_KEY);
    }

    /* ==================================================================
     * 2. Topic Exchange —— 通配符匹配 RoutingKey
     * 场景：按业务维度灵活路由，如 order.* / pay.#
     * 通配符：* 匹配<b>恰好一个</b>单词，# 匹配<b>零个或多个</b>单词
     * ================================================================== */

    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange(MqConstants.TOPIC_EXCHANGE, true, false);
    }

    @Bean
    public Queue topicOrderQueue() {
        return QueueBuilder.durable(MqConstants.TOPIC_ORDER_QUEUE).build();
    }

    @Bean
    public Queue topicPayQueue() {
        return QueueBuilder.durable(MqConstants.TOPIC_PAY_QUEUE).build();
    }

    /** order.* 能匹配 order.created，但匹配不到 order.created.vip（那是 order.# 的活） */
    @Bean
    public Binding topicOrderBinding() {
        return BindingBuilder.bind(topicOrderQueue())
                .to(topicExchange())
                .with("order.*");
    }

    /** pay.# 能匹配 pay.wechat、pay.wechat.success、pay.* */
    @Bean
    public Binding topicPayBinding() {
        return BindingBuilder.bind(topicPayQueue())
                .to(topicExchange())
                .with("pay.#");
    }

    /* ==================================================================
     * 3. Fanout Exchange —— 广播，<b>完全忽略</b> RoutingKey
     * 场景：一个事件需要通知多个下游（发短信 + 发邮件 + 加积分）
     * ================================================================== */

    @Bean
    public FanoutExchange fanoutExchange() {
        return new FanoutExchange(MqConstants.FANOUT_EXCHANGE, true, false);
    }

    @Bean
    public Queue fanoutSmsQueue() {
        return QueueBuilder.durable(MqConstants.FANOUT_SMS_QUEUE).build();
    }

    @Bean
    public Queue fanoutEmailQueue() {
        return QueueBuilder.durable(MqConstants.FANOUT_EMAIL_QUEUE).build();
    }

    @Bean
    public Binding fanoutSmsBinding() {
        // fanout 的 routingKey 传空字符串即可，因为根本不用它
        return BindingBuilder.bind(fanoutSmsQueue()).to(fanoutExchange());
    }

    @Bean
    public Binding fanoutEmailBinding() {
        return BindingBuilder.bind(fanoutEmailQueue()).to(fanoutExchange());
    }

    /* ==================================================================
     * 4. Headers Exchange —— 按消息 header 匹配，忽略 RoutingKey
     * 场景：路由条件无法用字符串表达（如按消息里的地区、类型组合过滤）
     * 缺点：性能比 direct/topic 差，实际用得少
     * ================================================================== */

    @Bean
    public HeadersExchange headersExchange() {
        return new HeadersExchange(MqConstants.HEADERS_EXCHANGE, true, false);
    }

    @Bean
    public Queue headersQueue() {
        return QueueBuilder.durable(MqConstants.HEADERS_QUEUE).build();
    }

    @Bean
    public Binding headersBinding() {
        Map<String, Object> args = new HashMap<>();
        // x-match=all：所有条件都要满足；x-match=any：满足任意一个即可
        args.put("x-match", "all");
        args.put("type", "order");
        args.put("region", "cn");
        return BindingBuilder.bind(headersQueue())
                .to(headersExchange())
                .whereAll(args)   // whereAll 等价于 x-match=all
                .match();
    }

    /* ==================================================================
     * 5. 死信交换机 DLX + 死信队列 DLQ
     * 面试必考：消息变成"死信"的三种情况
     *   (1) 消费者 basicNack / basicReject 且 requeue=false
     *   (2) 消息 TTL 过期
     *   (3) 队列达到最大长度，最老的消息被挤出去
     * ================================================================== */

    @Bean
    public DirectExchange dlxExchange() {
        return new DirectExchange(MqConstants.DLX_EXCHANGE, true, false);
    }

    @Bean
    public Queue orderFailedQueue() {
        return QueueBuilder.durable(MqConstants.ORDER_FAILED_QUEUE).build();
    }

    /** 延迟队列到期后也走这个 DLX，用 order.cancel 这个 key 路由到取消队列 */
    @Bean
    public Queue orderCancelQueue() {
        return QueueBuilder.durable(MqConstants.ORDER_CANCEL_QUEUE).build();
    }

    @Bean
    public Binding orderFailedBinding() {
        return BindingBuilder.bind(orderFailedQueue())
                .to(dlxExchange())
                .with(MqConstants.ORDER_FAILED_KEY);
    }

    @Bean
    public Binding orderCancelBinding() {
        return BindingBuilder.bind(orderCancelQueue())
                .to(dlxExchange())
                .with(MqConstants.ORDER_CANCEL_KEY);
    }

    /* ==================================================================
     * 6. 业务交换机 + 业务主队列
     * 订单创建后发到 order.exchange，路由到 order.business.queue
     * ================================================================== */

    @Bean
    public DirectExchange orderExchange() {
        return new DirectExchange(MqConstants.ORDER_EXCHANGE, true, false);
    }

    /**
     * 重点看 deadLetterExchange / deadLetterRoutingKey：
     * 当这个队列里的消息被 nack（requeue=false）或过期时，
     * Broker 会自动把它转发到 order.dlx.exchange，并用 order.failed 作为 routingKey。
     */
    @Bean
    public Queue orderQueue() {
        return QueueBuilder.durable(MqConstants.ORDER_QUEUE)
                .deadLetterExchange(MqConstants.DLX_EXCHANGE)
                .deadLetterRoutingKey(MqConstants.ORDER_FAILED_KEY)
                .build();
    }

    @Bean
    public Binding orderBinding() {
        return BindingBuilder.bind(orderQueue())
                .to(orderExchange())
                .with(MqConstants.ORDER_CREATE_KEY);
    }

    /* ==================================================================
     * 7. 延迟队列（TTL + DLX 方案）
     * 面试必考：如何实现延迟消息？
     *   方案A（本项目）：TTL + DLX —— 消息在 delay 队列待够 TTL，过期后死信到 cancel 队列
     *   方案B：rabbitmq-delayed-message-exchange 插件（更精确，但需装插件）
     *   方案C：定时任务扫表（简单可靠，但有延迟精度和扫表压力问题）
     *
     * TTL 方案的<b>致命缺陷</b>：RabbitMQ 只检查队首消息是否过期。
     * 如果先发 TTL=30min 的消息、再发 TTL=1s 的消息，后者会排在后面，
     * 必须等前面那条 30min 的消息过期后才会被检查到 —— 造成延迟不准。
     * 解决办法：按 TTL 分不同队列，或改用延迟插件。
     * ================================================================== */

    @Bean
    public Queue orderDelayQueue() {
        return QueueBuilder.durable(MqConstants.ORDER_DELAY_QUEUE)
                // 队列级别的 TTL：进入本队列的消息 30 秒后过期
                .ttl((int) MqConstants.ORDER_TIMEOUT_MILLIS)
                // 过期后不是丢弃，而是变成死信投递到 DLX
                .deadLetterExchange(MqConstants.DLX_EXCHANGE)
                .deadLetterRoutingKey(MqConstants.ORDER_CANCEL_KEY)
                .build();
    }

    @Bean
    public Binding orderDelayBinding() {
        return BindingBuilder.bind(orderDelayQueue())
                .to(dlxExchange())
                .with(MqConstants.ORDER_DELAY_KEY);
    }
}
