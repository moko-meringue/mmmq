package org.mmmq.broker.dispatcher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.core.Host;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Pattern;
import org.mmmq.core.message.Topic;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class DispatcherTest {

    Host host = new Host("http", "localhost", 8080);
    Dispatcher dispatcher;

    static Stream<Arguments> isSubscribingTestSource() {
        return Stream.of(
                Arguments.of(new Pattern("sports.*"), "sports.football", true),
                Arguments.of(new Pattern("sports.*"), "sports.basketball", true),
                Arguments.of(new Pattern("sports.*"), "news.politics", false),
                Arguments.of(new Pattern("news.**"), "news", true),
                Arguments.of(new Pattern("news.**"), "news.world.europe", true),
                Arguments.of(new Pattern("news.**"), "sports.football", false)
        );
    }

    @BeforeEach
    void setUp() {
        dispatcher = new Dispatcher("name", host, new ArrayList<>(), null);
    }

    @Test
    @DisplayName("push 테스트")
    void dispatchTest() {
        Message message = new Message(new Topic("test"), Map.of("key", "value"));
        dispatcher.dispatch(message);

        assertThat(dispatcher.messageQueue).contains(message);
    }

    @Test
    @DisplayName("push 동시성 보장 테스트")
    void dispatchConcurrencyTest() throws InterruptedException {
        int threadCount = 100;
        int messagesPerThread = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < messagesPerThread; j++) {
                        Message message = new Message(new Topic("topic"), Map.of("id", threadId, "msg", j));
                        dispatcher.dispatch(message);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        endLatch.await();
        executor.shutdown();

        int expectedCount = threadCount * messagesPerThread;
        int actualCount = dispatcher.messageQueue.size();

        assertThat(actualCount).isEqualTo(expectedCount);
    }

    @Test
    @DisplayName("메시지 전송 테스트")
    void executeTest() {
        CountDownLatch latch = new CountDownLatch(1);
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message) {
                latch.countDown();
                return true;
            }
        };
        dispatcher.start();
        Message message = new Message(new Topic("test"), Map.of("key", "value"));
        dispatcher.dispatch(message);
        assertThatCode(latch::await).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @DisplayName("isSubscribing 테스트")
    @MethodSource("isSubscribingTestSource")
    void isSubscribingTest(Pattern pattern, String topicName, boolean expected) {
        dispatcher.patterns.add(pattern);
        Topic topic = new Topic(topicName);
        assertThat(dispatcher.isSubscribing(topic)).isEqualTo(expected);
    }

    @Nested
    @DisplayName("Binding 캐싱 테스트")
    class BindingCacheTest {

        @BeforeEach
        void setUp() {
            dispatcher.patternCache.clear();
        }

        @Test
        @DisplayName("캐시에 데이터를 삽입할 수 있다")
        void putTest() {
            var topic = new Topic("test");
            dispatcher.patternCache.add(topic);
            assertThat(dispatcher.patternCache.contains(topic)).isTrue();
        }

        @Test
        @DisplayName("캐시에 없는 데이터는 매칭되지 않는다")
        void matchesTest() {
            var topic = new Topic("test");
            assertThat(dispatcher.patternCache.contains(topic)).isFalse();
        }
    }
}
