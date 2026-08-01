package org.mmmq.broker;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * 실제 스프링 컨텍스트를 닫아 {@code SubscriptionContainer}와 {@code TopicQueueContainer}의 종료 순서를
 * 실측한다 — "{@code ContextClosedEvent}가 빈 소멸보다 먼저 발행된다"는 스프링 문서상의 주장을
 * 이 프로젝트의 실제 의존성 버전·와이어링으로 직접 증명하기 위한 테스트다. 추측이 아니라 로그 순서로 확인한다.
 *
 * <p>{@code @SpringBootTest} 대신 직접 만든 {@link ConfigurableApplicationContext}를 쓰는 이유는,
 * 컨텍스트를 테스트 안에서 명시적으로 닫아야 하는데 {@code @SpringBootTest}가 관리하는 컨텍스트를
 * 그렇게 닫으면 스프링 테스트 프레임워크 자신의 {@code afterTestExecution} 콜백이 이미 닫힌 컨텍스트에
 * 이벤트를 발행하려다 실패하기 때문이다. 이 테스트가 관심 있는 건 애플리케이션 종료 순서지 테스트
 * 프레임워크의 생명주기가 아니라, 프레임워크가 관리하지 않는 컨텍스트를 직접 만든다.
 */
@ExtendWith(OutputCaptureExtension.class)
class ShutdownOrderTest {

    @Test
    @DisplayName("컨텍스트를 닫으면 구독 워커 정지 로그가 TopicQueue 닫힘 로그보다 먼저 남는다")
    void subscriptionWorkersStopBeforeTopicQueuesClose(@TempDir Path tempDir, CapturedOutput output) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(TestConfiguration.class)
                .web(WebApplicationType.NONE)
                .properties("mmmq.broker.persistence.root-dir=" + tempDir.toAbsolutePath())
                .run();

        context.close();

        String log = output.getOut();
        int subscriptionStoppedAt = log.indexOf("Subscription workers stopped");
        int topicQueuesClosedAt = log.indexOf("All topic queues closed");

        assertThat(subscriptionStoppedAt).isPositive();
        assertThat(topicQueuesClosedAt).isPositive();
        assertThat(subscriptionStoppedAt).isLessThan(topicQueuesClosedAt);
    }

    @Configuration
    @EnableAutoConfiguration
    static class TestConfiguration {

    }
}
