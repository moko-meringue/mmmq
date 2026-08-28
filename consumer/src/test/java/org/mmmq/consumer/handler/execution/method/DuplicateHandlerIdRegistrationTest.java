package org.mmmq.consumer.handler.execution.method;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.consumer.handler.execution.HandlerExecutionContainer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DuplicateHandlerIdRegistrationTest {

    @Test
    @DisplayName("중복된 핸들러 id가 있으면 컨텍스트 초기화가 실패한다.")
    void duplicateHandlerIdFailsContextStartup() {
        assertThatThrownBy(() -> new AnnotationConfigApplicationContext(DuplicateIdConfiguration.class))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .rootCause()
                .hasMessageContaining("Duplicate HandlerExecution id");
    }

    @Configuration
    static class DuplicateIdConfiguration {

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
                ObjectProvider<HandlerExecutionContainer> handlerExecutionContainerProvider,
                ObjectProvider<ObjectMapper> objectMapperProvider
        ) {
            return new MethodExecutionRegistration(handlerExecutionContainerProvider, objectMapperProvider);
        }

        @Bean
        DuplicatedBean1 duplicatedBean1() {
            return new DuplicatedBean1();
        }

        @Bean
        DuplicatedBean2 duplicatedBean2() {
            return new DuplicatedBean2();
        }
    }

    static class DuplicatedBean1 {

        @MMMQListener(id = "duplicated")
        void handle(String content) {
        }
    }

    static class DuplicatedBean2 {

        @MMMQListener(id = "duplicated")
        void handle(String content) {
        }
    }
}
