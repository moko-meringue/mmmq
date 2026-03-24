package org.mmmq.consumer.handler.execution.type;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.consumer.handler.FrontHandler;
import org.mmmq.consumer.handler.FrontHandlerUtil;
import org.mmmq.consumer.handler.execution.HandlerExecution;
import org.mmmq.consumer.handler.execution.HandlerExecutions;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Pattern;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        classes = InterfaceExecutionRegistrationTest.InterfaceExecutionRegistrationConfiguration.class
)
class InterfaceExecutionRegistrationTest {

    @Autowired
    FrontHandler frontHandler;

    @Test
    @DisplayName("MMMQListener 인터페이스를 구현한 빈이 있으면 자동으로 FrontHandler에 등록되어야 한다")
    void interfaceExecutionRegistrationTest() {
        HandlerExecutions handlerExecutions = FrontHandlerUtil.getHandlerExecutions(frontHandler);

        List<HandlerExecution> executions = handlerExecutions.getExecutions(
                new Message(new Topic("test-topic"), java.util.Map.of())
        );
        assertThat(executions.size()).isEqualTo(1);
        HandlerExecution execution = executions.get(0);
        assertThat(execution.getPattern().value()).isEqualTo("test-topic");
    }

    @Configuration
    static class InterfaceExecutionRegistrationConfiguration {

        @Bean
        FrontHandler frontHandler() {
            return new FrontHandler();
        }

        @Bean
        InterfaceExecutionRegistration interfaceExecutionRegistration(
                ObjectProvider<FrontHandler> frontHandlerObjectProvider
        ) {
            return new InterfaceExecutionRegistration(frontHandlerObjectProvider);
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
        public Pattern listens() {
            return new Pattern("test-topic");
        }

        @Override
        public void handle(SampleDto content) {
        }
    }
}
