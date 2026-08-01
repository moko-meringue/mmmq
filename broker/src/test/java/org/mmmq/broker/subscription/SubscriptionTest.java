package org.mmmq.broker.subscription;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
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

class SubscriptionTest {

    private static final long SEGMENT_MAX_BYTES = 64L * 1024 * 1024;

    Host host = new Host(WebProtocol.HTTP, "localhost", 8080);

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("체크포인트가 없으면 tailOffset을 기록한다")
    void opensAtTailWhenNoCheckpoint() {
        TopicQueue queue = createTopicQueue(new Topic("test"));
        queue.offer(new Message(new Topic("test"), Map.of("seq", 1)));
        queue.offer(new Message(new Topic("test"), Map.of("seq", 2)));
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(tempDir.resolve("test"));
        Dispatcher dispatcher = new Dispatcher(host, new ConsumerId("test-dispatcher"), new TopicPattern("**"));

        Subscription.open(queue, dispatcher, checkpointDirectory);

        assertThat(checkpointDirectory.get("test-dispatcher").read()).isEqualTo(2L);
    }

    @Test
    @DisplayName("이미 체크포인트가 있으면 손대지 않는다")
    void keepsExistingCheckpoint() {
        TopicQueue queue = createTopicQueue(new Topic("test"));
        queue.offer(new Message(new Topic("test"), Map.of("seq", 1)));
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(tempDir.resolve("test"));
        checkpointDirectory.register("test-dispatcher").write(5L);
        Dispatcher dispatcher = new Dispatcher(host, new ConsumerId("test-dispatcher"), new TopicPattern("**"));

        Subscription.open(queue, dispatcher, checkpointDirectory);

        assertThat(checkpointDirectory.get("test-dispatcher").read()).isEqualTo(5L);
    }

    @Test
    @DisplayName("trigger하면 큐의 메시지를 읽어 전송한다")
    void triggerDeliversQueuedMessage() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Dispatcher dispatcher = new Dispatcher(
                host,
                new ConsumerId("test-dispatcher"),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId consumerId, int retryCount) {
                        latch.countDown();
                        return true;
                    }
                }
        );
        TopicQueue queue = createTopicQueue(new Topic("test"));
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(tempDir.resolve("test"));
        Subscription subscription = Subscription.open(queue, dispatcher, checkpointDirectory);
        queue.offer(new Message(new Topic("test"), Map.of("key", "value")));

        subscription.trigger();

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        subscription.close();
    }

    @Test
    @DisplayName("여러 메시지를 순서대로 처리하며 체크포인트를 진전시킨다")
    void triggerAdvancesCheckpointPerMessage() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Dispatcher dispatcher = new Dispatcher(
                host,
                new ConsumerId("test-dispatcher"),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId consumerId, int retryCount) {
                        latch.countDown();
                        return true;
                    }
                }
        );
        TopicQueue queue = createTopicQueue(new Topic("test"));
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(tempDir.resolve("test"));
        Subscription subscription = Subscription.open(queue, dispatcher, checkpointDirectory);
        queue.offer(new Message(new Topic("test"), Map.of("seq", 1)));
        queue.offer(new Message(new Topic("test"), Map.of("seq", 2)));

        subscription.trigger();

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(checkpointDirectory.get("test-dispatcher").read()).isEqualTo(2L);
        subscription.close();
    }

    @Test
    @DisplayName("close 이후에는 trigger해도 메시지가 전송되지 않는다")
    void closeStopsFurtherDelivery() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Dispatcher dispatcher = new Dispatcher(
                host,
                new ConsumerId("test-dispatcher"),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId consumerId, int retryCount) {
                        latch.countDown();
                        return true;
                    }
                }
        );
        TopicQueue queue = createTopicQueue(new Topic("test"));
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(tempDir.resolve("test"));
        Subscription subscription = Subscription.open(queue, dispatcher, checkpointDirectory);

        subscription.close();
        queue.offer(new Message(new Topic("test"), Map.of("key", "value")));
        subscription.trigger();

        assertThat(latch.await(300, TimeUnit.MILLISECONDS)).isFalse();
    }

    @Test
    @DisplayName("재시작 후에도 마지막 커밋 위치부터 재개하고 이미 보낸 메시지를 다시 보내지 않는다")
    void resumesFromCommittedOffsetAfterRestart() throws InterruptedException {
        CountDownLatch firstDelivery = new CountDownLatch(1);
        Dispatcher dispatcher = new Dispatcher(
                host,
                new ConsumerId("test-dispatcher"),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId consumerId, int retryCount) {
                        firstDelivery.countDown();
                        return true;
                    }
                }
        );
        TopicQueue queue = createTopicQueue(new Topic("test"));
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(tempDir.resolve("test"));
        Subscription subscription = Subscription.open(queue, dispatcher, checkpointDirectory);
        queue.offer(new Message(new Topic("test"), Map.of("seq", 1)));
        subscription.trigger();
        assertThat(firstDelivery.await(5, TimeUnit.SECONDS)).isTrue();
        subscription.close();
        checkpointDirectory.close();

        // 재시작: 체크포인트 디렉터리와 Subscription을 새로 연다.
        List<Message> redeliveredAfterRestart = new CopyOnWriteArrayList<>();
        Dispatcher restartedDispatcher = new Dispatcher(
                host,
                new ConsumerId("test-dispatcher"),
                new TopicPattern("**"),
                new Sender(null) {
                    @Override
                    public boolean send(Message message, ConsumerId consumerId, int retryCount) {
                        redeliveredAfterRestart.add(message);
                        return true;
                    }
                }
        );
        CheckpointDirectory reopenedCheckpointDirectory = CheckpointDirectory.open(tempDir.resolve("test"));
        Subscription restarted = Subscription.open(queue, restartedDispatcher, reopenedCheckpointDirectory);
        queue.offer(new Message(new Topic("test"), Map.of("seq", 2)));
        restarted.trigger();

        long deadline = System.currentTimeMillis() + 5000;
        while (redeliveredAfterRestart.size() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(redeliveredAfterRestart).hasSize(1);
        assertThat(((Map<?, ?>) redeliveredAfterRestart.get(0).content()).get("seq")).isEqualTo(2);
        restarted.close();
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
