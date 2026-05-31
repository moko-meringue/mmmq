package org.mmmq.consumer.handler.execution.method;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.consumer.handler.execution.HandlerExecutionContainer;
import org.mmmq.core.identifier.ConsumerId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        classes = MethodExecutorRegistrationTest.MethodExecutionRegistrationConfiguration.class
)
class MethodExecutorRegistrationTest {

    @Autowired
    HandlerExecutionContainer handlerExecutionContainer;

    @Test
    @DisplayName("메서드 실행 등록이 정상적으로 수행된다.")
    void methodExecutionRegistrationTest() {
        assertThat(handlerExecutionContainer.find(new ConsumerId("topic1"))).isNotNull();
        assertThat(handlerExecutionContainer.find(new ConsumerId("topic2"))).isNotNull();
        assertThat(handlerExecutionContainer.find(new ConsumerId("topic3"))).isNotNull();
    }

    @Test
    @DisplayName("등록된 핸들러는 id로 조회된다.")
    void findsRegisteredHandlerById() {
        HandlerExecution execution = handlerExecutionContainer.find(new ConsumerId("topic1"));

        assertThat(execution).isNotNull();
        assertThat(execution.id()).isEqualTo(new ConsumerId("topic1"));
    }

    @Configuration
    static class MethodExecutionRegistrationConfiguration {

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        HandlerExecutionContainer handlerExecutionContainer() {
            return new HandlerExecutionContainer();
        }

        @Bean
        MethodExecutionRegistration methodExecutionRegistration(
                HandlerExecutionContainer handlerExecutionContainer,
                ObjectMapper objectMapper
        ) {
            return new MethodExecutionRegistration(handlerExecutionContainer, objectMapper);
        }

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

        @MMMQListener(id = "topic1")
        void handle1(String content) {
        }
    }

    static class Bean2 {

        @MMMQListener(id = "topic2")
        void handle2(String content) {
        }
    }

    static class Bean3 {

        @MMMQListener(id = "topic3")
        void handle3(String content) {
        }
    }
}
