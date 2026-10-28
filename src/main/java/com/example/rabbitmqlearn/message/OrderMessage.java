package com.example.rabbitmqlearn.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 订单消息体。
 * <p>
 * 面试点：消息体要尽量<b>小</b>且<b>自描述</b>。常见错误是把整个数据库实体塞进消息，
 * 结果消费方需要的字段变了、消息体积爆炸。推荐只放 ID + 必要上下文，
 * 消费方按 ID 回查（但要注意"回查时数据已变"的时序问题）。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderMessage implements Serializable {

    /** 消息唯一 ID —— 幂等消费的关键 */
    private String messageId;

    private Long orderId;

    private Long userId;

    private String productName;

    private Integer amount;

    /** 业务发生时间戳 */
    private Long timestamp;
}
