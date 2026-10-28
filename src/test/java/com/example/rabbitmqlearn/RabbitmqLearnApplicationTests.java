package com.example.rabbitmqlearn;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * 上下文加载测试。
 * <p>
 * 默认测试不会去连 RabbitMQ —— Spring AMQP 的连接是延迟建立的，
 * 只有真正收发消息或主动声明队列时才会连。所以即使 RabbitMQ 没启动，
 * 这个测试也能通过。
 * <p>
 * 如果你写了会连 MQ 的测试而本地没启动 RabbitMQ，可以用：
 * <pre>./mvnw test -Dtest=RabbitmqLearnApplicationTests</pre>
 * 只跑这一个不连 MQ 的测试。
 */
@SpringBootTest
@TestPropertySource(properties = {
        // 关掉 RabbitMQ 健康检查，避免没有 MQ 时 actuator/健康检查报 DOWN
        "management.health.rabbit.enabled=false"
})
class RabbitmqLearnApplicationTests {

    @Test
    void contextLoads() {
        // 只要 Spring 容器能正常启动、所有 Bean 能装配，就算通过
    }
}
