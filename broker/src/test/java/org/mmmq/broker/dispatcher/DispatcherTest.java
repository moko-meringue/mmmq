package org.mmmq.broker.dispatcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.storage.CheckpointRegistry;
import org.mmmq.broker.topicqueue.storage.SegmentChain;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.TopicPattern;
import org.mmmq.core.message.Topic;

class DispatcherTest {

    private static final long SEGMENT_MAX_BYTES = 64L * 1024 * 1024; // 테스트 중 회전이 발생하지 않도록 충분히 큰 값

    Host host = new Host(WebProtocol.HTTP, "localhost", 8080); // 테스트용 Consumer 호스트 (실제 연결 없음)
    Dispatcher dispatcher; // 각 테스트에서 공유하는 Dispatcher 인스턴스

    @TempDir
    Path tempDir; // 각 테스트마다 격리된 임시 디렉토리. TopicQueue가 세그먼트 파일을 여기에 저장

    @BeforeEach
    void setUp() {
        dispatcher = new Dispatcher("test-dispatcher", host, List.of(new TopicPattern("**"))); // 모든 토픽을 매칭하는 와일드카드 패턴으로 초기화
    }

    @Test
    @DisplayName("Dispatcher 이름이 regex에 부합하지 않으면 예외를 던진다")
    void rejectInvalidDispatcherName() {
        assertThatThrownBy(() -> new Dispatcher("invalid name!", host, List.of(new TopicPattern("**")))) // 공백과 느낌표는 [A-Za-z0-9._-]+ 패턴에 불일치
                .isInstanceOf(IllegalArgumentException.class); // 파일명으로 사용할 수 없는 문자를 생성자에서 즉시 거부
    }

    @Test
    @DisplayName("토픽 큐에서 메시지를 읽어 전송한다")
    void consumeFromTopicQueueTest() {
        final CountDownLatch latch = new CountDownLatch(1); // 비동기 worker thread가 전송을 완료할 때까지 메인 스레드 대기
        dispatcher.sender = new Sender(null) { // HTTP 클라이언트 없이 실제 send 로직을 오버라이드해 테스트 격리
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown(); // 전송 완료 시 latch 감소

                return true; // ACK 시뮬레이션
            }
        };

        final TopicQueue topicQueue = createTopicQueue(new Topic("test")); // @TempDir 기반 TopicQueue 생성
        dispatcher.subscribe(topicQueue); // 패턴("**")이 토픽("test")에 매칭되어 Subscription 생성
        topicQueue.offer(new Message(new Topic("test"), Map.of("key", "value"))); // offset=0에 메시지 저장
        dispatcher.onMessageArrived(new MessageArrivedEvent(topicQueue)); // drain 작업을 worker thread에 제출

        assertThatCode(latch::await).doesNotThrowAnyException(); // worker thread가 send()를 호출할 때까지 대기
        dispatcher.stop(); // worker thread 종료
    }

    @Test
    @DisplayName("여러 토픽을 동시에 구독한다")
    void consumeMultipleTopicsTest() {
        final CountDownLatch latch = new CountDownLatch(2); // 두 토픽 각 1개씩 총 2개 전송 완료 대기
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown(); // 각 전송마다 1씩 감소

                return true;
            }
        };

        final TopicQueue orderQueue = createTopicQueue(new Topic("order.new")); // 첫 번째 토픽 큐
        final TopicQueue paymentQueue = createTopicQueue(new Topic("payment.kakao")); // 두 번째 토픽 큐
        dispatcher.subscribe(orderQueue);   // 각 토픽별로 독립적인 Subscription 생성
        dispatcher.subscribe(paymentQueue);

        orderQueue.offer(new Message(new Topic("order.new"), Map.of("id", 1)));     // offset=0
        paymentQueue.offer(new Message(new Topic("payment.kakao"), Map.of("id", 2))); // offset=0
        dispatcher.onMessageArrived(new MessageArrivedEvent(orderQueue));   // order 토픽 drain 트리거
        dispatcher.onMessageArrived(new MessageArrivedEvent(paymentQueue)); // payment 토픽 drain 트리거

        assertThatCode(latch::await).doesNotThrowAnyException(); // 두 전송 모두 완료될 때까지 대기
        dispatcher.stop();
    }

    @Test
    @DisplayName("토픽별 메시지를 독립적으로 소비한다")
    void consumePerTopicIndependentlyTest() {
        final CountDownLatch latch = new CountDownLatch(3); // topicA 2개 + topicB 1개 = 총 3개 전송 완료 대기
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                latch.countDown();

                return true;
            }
        };

        final TopicQueue topicA = createTopicQueue(new Topic("topicA"));
        final TopicQueue topicB = createTopicQueue(new Topic("topicB"));
        dispatcher.subscribe(topicA);
        dispatcher.subscribe(topicB);

        topicA.offer(new Message(new Topic("topicA"), Map.of("seq", 1))); // topicA offset=0
        topicA.offer(new Message(new Topic("topicA"), Map.of("seq", 2))); // topicA offset=1
        topicB.offer(new Message(new Topic("topicB"), Map.of("seq", 1))); // topicB offset=0
        dispatcher.onMessageArrived(new MessageArrivedEvent(topicA)); // topicA drain 트리거: 2개 전송
        dispatcher.onMessageArrived(new MessageArrivedEvent(topicB)); // topicB drain 트리거: 1개 전송

        assertThatCode(latch::await).doesNotThrowAnyException(); // 3개 모두 전송될 때까지 대기
        dispatcher.stop();
    }

    @Test
    @DisplayName("패턴 미매칭 토픽은 구독하지 않는다")
    void doesNotSubscribeUnmatchedTopicTest() {
        dispatcher = new Dispatcher("test-dispatcher", host, List.of(new TopicPattern("order.*"))); // "order.*"만 매칭. "payment.kakao"는 제외
        dispatcher.sender = new Sender(null) {
            @Override
            public boolean send(Message message, int retryCount) {
                return true;
            }
        };

        final TopicQueue paymentQueue = createTopicQueue(new Topic("payment.kakao")); // 패턴 불일치 토픽
        dispatcher.subscribe(paymentQueue); // "order.*"가 "payment.kakao"와 매칭 안되므로 subscribe 내부에서 조기 반환
        paymentQueue.offer(new Message(new Topic("payment.kakao"), Map.of("id", 1)));
        dispatcher.onMessageArrived(new MessageArrivedEvent(paymentQueue)); // 구독 없으므로 drain 발생 안함

        assertThat(dispatcher.subscriptions).doesNotContainKey(paymentQueue); // Subscription이 생성되지 않았음을 확인
    }

    private TopicQueue createTopicQueue(Topic topic) { // @TempDir 내에 토픽 전용 서브디렉토리를 만들어 TopicQueue 생성
        final Path topicDir = tempDir.resolve(topic.name()); // 토픽마다 격리된 디렉토리
        try {
            Files.createDirectories(topicDir); // 토픽 디렉토리는 토픽 레이어 책임
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        final SegmentChain segmentChain = SegmentChain.open(topicDir, SEGMENT_MAX_BYTES);
        final CheckpointRegistry checkpointRegistry = CheckpointRegistry.open(topicDir);

        return new TopicQueue(topic, segmentChain, checkpointRegistry);
    }
}
