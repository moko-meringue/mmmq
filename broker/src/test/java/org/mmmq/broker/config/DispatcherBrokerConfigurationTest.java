package org.mmmq.broker.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        classes = DispatcherBrokerConfigurationTest.MessageDispatcherConfiguration.class
)
public class DispatcherBrokerConfigurationTest {

    @Autowired
    ApplicationContext applicationContext;

    @Test
    @DisplayName("MessageDispatcher 빈이 정상적으로 생성된다.")
    void dispatcherBeanCreationTest() {
        Map<String, Dispatcher> beans = applicationContext.getBeansOfType(Dispatcher.class);

        assertThat(beans.keySet()).containsExactly("messageDispatcher");
    }

    @Configuration
    static class MessageDispatcherConfiguration {

        @Bean("messageDispatcher")
        public Dispatcher messageDispatcher() {
            return new Dispatcher.Builder("name", "http", "localhost", 8080)
                    .withTopics("topic1", "topic2")
                    .build();
        }
    }
}
