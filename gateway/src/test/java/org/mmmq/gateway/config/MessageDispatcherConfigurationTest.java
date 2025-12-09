package org.mmmq.gateway.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.gateway.dispatcher.MessageDispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        classes = MessageDispatcherConfigurationTest.MessageDispatcherConfiguration.class
)
public class MessageDispatcherConfigurationTest {

    @Autowired
    ApplicationContext applicationContext;

    @Test
    @DisplayName("MessageDispatcher 빈이 정상적으로 생성된다.")
    void dispatcherBeanCreationTest() {
        Map<String, MessageDispatcher> beans = applicationContext.getBeansOfType(MessageDispatcher.class);

        assertThat(beans.keySet()).containsExactly("messageDispatcher");
    }

    @Configuration
    static class MessageDispatcherConfiguration {

        @Bean("messageDispatcher")
        public MessageDispatcher messageDispatcher() {
            return new MessageDispatcher.Builder("name", "http", "localhost", 8080)
                    .withTopics("topic1", "topic2")
                    .build();
        }
    }
}
