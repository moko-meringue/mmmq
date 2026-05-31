package org.mmmq.consumer.handler.execution.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.consumer.handler.execution.HandlerExecutions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        classes = InterfaceExecutionRegistrationTest.InterfaceExecutionRegistrationConfiguration.class
)
class InterfaceExecutionRegistrationTest {

    @Autowired
    HandlerExecutions handlerExecutions;

    @Test
    @DisplayName("MMMQListener 인터페이스를 구현한 빈이 있으면 자동으로 HandlerExecutions에 등록되어야 한다")
    void interfaceExecutionRegistrationTest() {
        HandlerExecution execution = handlerExecutions.find("order-new");

        assertThat(execution).isNotNull();
        assertThat(execution.id()).isEqualTo("order-new");
    }

    @Configuration
    static class InterfaceExecutionRegistrationConfiguration {

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        HandlerExecutions handlerExecutions() {
            return new HandlerExecutions();
        }

        @Bean
        InterfaceExecutionRegistration interfaceExecutionRegistration(
                HandlerExecutions handlerExecutions,
                ObjectMapper objectMapper
        ) {
            return new InterfaceExecutionRegistration(handlerExecutions, objectMapper);
        }

        @Bean
        SampleListener sampleExecutor() {
            return new SampleListener();
        }
    }

    static class SampleDto {

        String name;
    }

    static class SampleListener implements MMMQListener<SampleDto> {

        @Override
        public String id() {
            return "order-new";
        }

        @Override
        public void handle(SampleDto content) {
        }
    }
}
