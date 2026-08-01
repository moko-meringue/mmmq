package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;

class DispatcherTest {

    Host host = new Host(WebProtocol.HTTP, "localhost", 8080);
    Dispatcher dispatcher;

    @BeforeEach
    void setUp() {
        dispatcher = new Dispatcher(host, new ConsumerId("test-dispatcher"), new TopicPattern("**"));
    }

    @Test
    @DisplayName("consumer id가 regex에 부합하지 않으면 예외를 던진다")
    void rejectInvalidConsumerId() {
        assertThatThrownBy(() -> new ConsumerId("invalid handler!"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("consumer id getter가 생성자 인자를 그대로 반환한다")
    void consumerIdGetter() {
        Dispatcher dispatcher = new Dispatcher(host, new ConsumerId("order-dispatcher"), new TopicPattern("order.*"));

        assertThat(dispatcher.consumerId()).isEqualTo(new ConsumerId("order-dispatcher"));
    }

    @Test
    @DisplayName("패턴 매칭이 true이면 canDispatch()가 true를 반환한다")
    void canDispatchReturnsTrueWhenPatternMatches() {
        Dispatcher dispatcher = new Dispatcher(host, new ConsumerId("order-dispatcher"), new TopicPattern("order.*"));

        assertThat(dispatcher.canDispatch(new Topic("order.new"))).isTrue();
        assertThat(dispatcher.canDispatch(new Topic("payment.kakao"))).isFalse();
    }

    @Test
    @DisplayName("send에는 dispatcher의 consumerId가 전달된다")
    void deliversConsumerIdToSender() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerId[] receivedConsumerId = new ConsumerId[1];
        Dispatcher dispatcher = new Dispatcher(
                host,
                new ConsumerId("my-handler"),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId consumerId, int retryCount) {
                        receivedConsumerId[0] = consumerId;
                        latch.countDown();
                        return true;
                    }
                }
        );

        dispatcher.send(new Message(new Topic("test"), Map.of("k", "v")));

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(receivedConsumerId[0]).isEqualTo(new ConsumerId("my-handler"));
    }

    @Test
    @DisplayName("NACK이 소진되면 예외 없이 조용히 반환한다")
    void sendReturnsQuietlyWhenNackExhausted() {
        Dispatcher dispatcher = new Dispatcher(
                host,
                new ConsumerId("test-dispatcher"),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId consumerId, int retryCount) {
                        return false;
                    }
                }
        );

        assertThatCode(() -> dispatcher.send(new Message(new Topic("test"), Map.of("k", "v"))))
                .doesNotThrowAnyException();
    }
}
