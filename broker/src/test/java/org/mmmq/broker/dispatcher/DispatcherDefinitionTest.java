package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.identifier.ConsumerId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DispatcherDefinitionTest {

    @Test
    @DisplayName("정의는 consumerId를 가진 Dispatcher로 변환된다")
    void convertsToDispatcher() {
        DispatcherDefinition definition = new DispatcherDefinition(
                "order-created",
                new HostDefinition("HTTP", "127.0.0.1", 8080),
                "order.created"
        );

        Dispatcher dispatcher = definition.toDispatcher();

        assertThat(dispatcher.consumerId()).isEqualTo(new ConsumerId("order-created"));
    }

    @Test
    @DisplayName("consumerId가 regex에 어긋나면 예외를 던진다")
    void rejectsInvalidConsumerId() {
        DispatcherDefinition definition = new DispatcherDefinition(
                "invalid id!",
                new HostDefinition("HTTP", "127.0.0.1", 8080),
                "order.created"
        );

        assertThatThrownBy(definition::toDispatcher)
                .isInstanceOf(IllegalArgumentException.class);
    }
}
