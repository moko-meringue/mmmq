package org.mmmq.consumer.handler.execution.method;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.consumer.config.ConsumerConfiguration;
import org.mmmq.consumer.handler.FrontHandler;
import org.mmmq.consumer.handler.FrontHandlerUtil;
import org.mmmq.consumer.handler.execution.HandlerExecutions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    classes = {
        MethodExecutionRegistrationTest.MethodExecutionRegistrationConfiguration.class,
        ConsumerConfiguration.class
    }
)
class MethodExecutionRegistrationTest {

    @Autowired
    FrontHandler frontHandler;

    @Test
    @DisplayName("메서드 실행 등록이 정상적으로 수행된다.")
    void methodExecutionRegistrationTest() {
        HandlerExecutions handlerExecutions = FrontHandlerUtil.getHandlerExecutions(frontHandler);
        assertThat(handlerExecutions.size()).isEqualTo(3);
    }

    @Configuration
    static class MethodExecutionRegistrationConfiguration {

        @Bean
        public Bean1 bean1() {
            return new Bean1();
        }

        @Bean
        public Bean2 bean2() {
            return new Bean2();
        }

        @Bean
        public Bean3 bean3() {
            return new Bean3();
        }
    }

    static class Bean1 {

        @MMMQListener(topic = "topic1")
        void handle1(String content) {
        }
    }

    static class Bean2 {

        @MMMQListener(topic = "topic2")
        void handle2(String content) {
        }
    }

    static class Bean3 {

        @MMMQListener(topic = "topic3")
        void handle3(String content) {
        }
    }
}
