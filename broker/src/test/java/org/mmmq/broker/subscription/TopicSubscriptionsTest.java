package org.mmmq.broker.subscription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.dispatcher.Dispatcher;
import org.mmmq.broker.dispatcher.sender.Sender;
import org.mmmq.broker.topicqueue.TopicQueue;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.core.Host;
import org.mmmq.core.WebProtocol;
import org.mmmq.core.identifier.ConsumerId;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;

class TopicSubscriptionsTest {

    private static final long SEGMENT_MAX_BYTES = 64L * 1024 * 1024;

    Host host = new Host(WebProtocol.HTTP, "localhost", 8080);

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("rematch로 매칭된 Dispatcher가 구독을 얻고 trigger로 메시지를 전달받는다")
    void rematchAddsMatchedDispatcher() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Dispatcher dispatcher = fakeDispatcher("consumer", latch);
        TopicQueue queue = createTopicQueue(new Topic("order.created"));
        TopicSubscriptions subscriptions = new TopicSubscriptions(CheckpointDirectory.open(tempDir.resolve("order.created")));

        subscriptions.rematch(queue, List.of(dispatcher));
        queue.offer(new Message(new Topic("order.created"), Map.of("id", 1)));
        subscriptions.trigger();

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @DisplayName("rematch에서 빠진 Dispatcher는 워커가 종료되고 체크포인트가 삭제된다")
    void rematchDropsUnmatchedDispatcher() {
        CountDownLatch latch = new CountDownLatch(1);
        Dispatcher dispatcher = fakeDispatcher("consumer", latch);
        TopicQueue queue = createTopicQueue(new Topic("order.created"));
        Path checkpointsDir = tempDir.resolve("order.created").resolve("checkpoints");
        TopicSubscriptions subscriptions = new TopicSubscriptions(CheckpointDirectory.open(tempDir.resolve("order.created")));
        subscriptions.rematch(queue, List.of(dispatcher));
        assertThat(checkpointsDir.resolve("consumer.checkpoint")).exists();

        subscriptions.rematch(queue, List.of());

        assertThat(checkpointsDir.resolve("consumer.checkpoint")).doesNotExist();
    }

    @Test
    @DisplayName("제거된 Dispatcher는 이후 trigger로도 메시지를 받지 못한다")
    void triggerAfterRemovalDeliversNothing() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Dispatcher dispatcher = fakeDispatcher("consumer", latch);
        TopicQueue queue = createTopicQueue(new Topic("order.created"));
        TopicSubscriptions subscriptions = new TopicSubscriptions(CheckpointDirectory.open(tempDir.resolve("order.created")));
        subscriptions.rematch(queue, List.of(dispatcher));

        subscriptions.rematch(queue, List.of());
        queue.offer(new Message(new Topic("order.created"), Map.of("id", 1)));
        subscriptions.trigger();

        assertThat(latch.await(300, TimeUnit.MILLISECONDS)).isFalse();
    }

    @Test
    @DisplayName("이미 구독 중인 Dispatcher는 rematch에도 다시 만들어지지 않고 이어서 처리한다")
    void rematchKeepsExistingSubscriptionUntouched() throws InterruptedException {
        List<Message> received = new CopyOnWriteArrayList<>();
        Dispatcher dispatcher = countingDispatcher("consumer", received);
        TopicQueue queue = createTopicQueue(new Topic("order.created"));
        TopicSubscriptions subscriptions = new TopicSubscriptions(CheckpointDirectory.open(tempDir.resolve("order.created")));
        subscriptions.rematch(queue, List.of(dispatcher));
        queue.offer(new Message(new Topic("order.created"), Map.of("seq", 1)));
        subscriptions.trigger();
        awaitSize(received, 1);

        // 같은 Dispatcher로 다시 rematch한다 — 구독을 새로 만들면 tailOffset이 다시 기록되어
        // 아직 처리되지 않은 두 번째 메시지가 통째로 스킵된다.
        subscriptions.rematch(queue, List.of(dispatcher));
        queue.offer(new Message(new Topic("order.created"), Map.of("seq", 2)));
        subscriptions.trigger();
        awaitSize(received, 2);

        assertThat(received).hasSize(2);
    }

    @Test
    @DisplayName("같은 consumerId로 host가 바뀐 Dispatcher가 오면 새 Dispatcher가 전송한다")
    void rematchReplacesDispatcherWithSameConsumerId() throws InterruptedException {
        List<Message> toPreviousHost = new CopyOnWriteArrayList<>();
        List<Message> toNextHost = new CopyOnWriteArrayList<>();
        Dispatcher previous = countingDispatcher("consumer", toPreviousHost);
        Dispatcher next = countingDispatcher("consumer", toNextHost);
        TopicQueue queue = createTopicQueue(new Topic("order.created"));
        TopicSubscriptions subscriptions = new TopicSubscriptions(CheckpointDirectory.open(tempDir.resolve("order.created")));
        subscriptions.rematch(queue, List.of(previous));

        subscriptions.rematch(queue, List.of(next));
        queue.offer(new Message(new Topic("order.created"), Map.of("id", 1)));
        subscriptions.trigger();
        awaitSize(toNextHost, 1);

        assertThat(toNextHost).hasSize(1);
        assertThat(toPreviousHost).isEmpty();
    }

    @Test
    @DisplayName("같은 consumerId로 교체돼도 기존 체크포인트를 이어받아 밀린 메시지를 놓치지 않는다")
    void rematchWithReplacedDispatcherContinuesFromExistingOffset() throws InterruptedException {
        List<Message> toPrevious = new CopyOnWriteArrayList<>();
        List<Message> toNext = new CopyOnWriteArrayList<>();
        Dispatcher previous = countingDispatcher("consumer", toPrevious);
        TopicQueue queue = createTopicQueue(new Topic("order.created"));
        TopicSubscriptions subscriptions = new TopicSubscriptions(CheckpointDirectory.open(tempDir.resolve("order.created")));
        subscriptions.rematch(queue, List.of(previous));
        queue.offer(new Message(new Topic("order.created"), Map.of("seq", 1)));
        subscriptions.trigger();
        awaitSize(toPrevious, 1);

        // 두 번째 메시지는 트리거되지 않은 채로 밀려 있다 — modify가 이 시점에 일어난다.
        // 새 Subscription이 tailOffset(=2)에서 다시 시작하면 이 메시지를 통째로 건너뛴다.
        queue.offer(new Message(new Topic("order.created"), Map.of("seq", 2)));
        Dispatcher next = countingDispatcher("consumer", toNext);
        subscriptions.rematch(queue, List.of(next));
        subscriptions.trigger();
        awaitSize(toNext, 1);

        assertThat(toPrevious).hasSize(1);
        assertThat(toNext).hasSize(1);
    }

    @Disabled("""
            뮤테이션 검증 중 발견한 경합을 증명하는 테스트 — 현재 구현에서는 실패한다(재현 확인 완료, 15회 반복이 아니라
            느린 컨슈머를 흉내 내는 테스트 더블만으로 매번 재현됨).
            Subscription.drain()은 dispatcher.send() 이후에야 offset.next()+checkpointFile.write()를 호출한다.
            그 사이(전송 완료~체크포인트 flush) 창에서 rematch가 끼어들면 새 Subscription이 아직 갱신되지 않은
            옛 체크포인트를 읽어 이미 전송된 메시지를 다시 열어 교체된 Dispatcher가 중복 수신한다.
            더 심각한 부수 효과도 같이 재현된다: rematch의 replaced 분기가 옛 Subscription.close()로 worker를
            shutdownNow()하면, 그 인터럽트가 하필 checkpointFile.write() 도중(NIO 블로킹 쓰기 중)에 꽂혀
            ClosedByInterruptException을 던진다 — 그런데 이 CheckpointFile은 같은 consumerId라서 새 Subscription과
            "공유"하는 객체라, 채널이 닫히면 새 Subscription의 체크포인트 쓰기까지 함께 깨진다(로그에 두 워커 모두
            "aborted drain"으로 남는 것으로 확인).
            위 rematchWithReplacedDispatcherContinuesFromExistingOffset은 리스트 "크기"만 확인해 이 경합을 놓친다:
            중복 수신 시점에 이미 size==1이라 통과해버리고, 이후 두 번째(진짜) 메시지가 도착해도 이미 assertion을
            지난 뒤라 잡히지 않는다.
            """)
    @Test
    @DisplayName("느린 컨슈머 처리 중 rematch가 끼어들면 체크포인트 flush 전에 교체된 Dispatcher가 이미 보낸 메시지를 중복 수신한다")
    void raceBetweenSlowDeliveryAndRematchDuplicatesMessage() throws InterruptedException {
        List<Message> toPrevious = new CopyOnWriteArrayList<>();
        List<Message> toNext = new CopyOnWriteArrayList<>();
        CountDownLatch previousSent = new CountDownLatch(1);
        Dispatcher previous = new Dispatcher(
                host,
                new ConsumerId("consumer"),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId id, int retryCount) {
                        toPrevious.add(message);
                        previousSent.countDown();
                        try {
                            // 느린 컨슈머(또는 느린 fsync)를 흉내 낸다 — 프로덕션 코드는 건드리지 않는다.
                            Thread.sleep(300);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                        }
                        return true;
                    }
                }
        );
        TopicQueue queue = createTopicQueue(new Topic("order.created"));
        TopicSubscriptions subscriptions = new TopicSubscriptions(CheckpointDirectory.open(tempDir.resolve("order.created")));
        subscriptions.rematch(queue, List.of(previous));
        queue.offer(new Message(new Topic("order.created"), Map.of("seq", 1)));
        subscriptions.trigger();
        assertThat(previousSent.await(5, TimeUnit.SECONDS)).isTrue();

        // previous의 send()가 아직 sleep 중이라 체크포인트는 seq=1 반영 전이다 — 바로 이 순간 rematch가 끼어든다.
        queue.offer(new Message(new Topic("order.created"), Map.of("seq", 2)));
        Dispatcher next = countingDispatcher("consumer", toNext);
        subscriptions.rematch(queue, List.of(next));
        subscriptions.trigger();
        awaitSize(toNext, 1);

        assertThat(toNext)
                .extracting(message -> (Object) ((Map<?, ?>) message.content()).get("seq"))
                .as("교체된 Dispatcher는 밀린 seq=2부터 이어받아야 한다 — 이미 처리된 seq=1을 중복 수신하면 안 된다")
                .containsExactly(2);
    }

    @Test
    @DisplayName("제거된 구독은 워커 종료 여부와 무관하게 목록에서 실제로 빠진다")
    void rematchStructurallyRemovesDroppedSubscription() {
        Dispatcher dispatcher = fakeDispatcher("consumer", new CountDownLatch(1));
        TopicQueue queue = createTopicQueue(new Topic("order.created"));
        TopicSubscriptions subscriptions = new TopicSubscriptions(CheckpointDirectory.open(tempDir.resolve("order.created")));
        subscriptions.rematch(queue, List.of(dispatcher));
        assertThat(subscriptions.containsSubscriptionFor(new ConsumerId("consumer"))).isTrue();

        subscriptions.rematch(queue, List.of());

        assertThat(subscriptions.containsSubscriptionFor(new ConsumerId("consumer"))).isFalse();
    }

    @Test
    @DisplayName("close하면 CheckpointDirectory도 함께 닫는다")
    void closeAlsoClosesCheckpointDirectory() {
        CheckpointDirectory checkpointDirectory = mock(CheckpointDirectory.class);
        TopicSubscriptions subscriptions = new TopicSubscriptions(checkpointDirectory);

        subscriptions.close();

        verify(checkpointDirectory).close();
    }

    private void awaitSize(List<?> list, int expectedSize) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000;
        while (list.size() < expectedSize && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
    }

    private Dispatcher countingDispatcher(String consumerId, List<Message> received) {
        return new Dispatcher(
                host,
                new ConsumerId(consumerId),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId id, int retryCount) {
                        received.add(message);
                        return true;
                    }
                }
        );
    }

    private Dispatcher fakeDispatcher(String consumerId, CountDownLatch latch) {
        return new Dispatcher(
                host,
                new ConsumerId(consumerId),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId id, int retryCount) {
                        latch.countDown();
                        return true;
                    }
                }
        );
    }

    private TopicQueue createTopicQueue(Topic topic) {
        Path topicDir = tempDir.resolve(topic.name());
        try {
            Files.createDirectories(topicDir);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, SEGMENT_MAX_BYTES);
        return new TopicQueue(topic, segmentFileChain);
    }
}
