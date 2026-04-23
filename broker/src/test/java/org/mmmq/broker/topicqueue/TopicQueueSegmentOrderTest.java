package org.mmmq.broker.topicqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.fixture.NoOpTopicWal;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class TopicQueueSegmentOrderTest {

    private static final int SEGMENT_CAPACITY = 1000;
    private static final Topic TOPIC = new Topic("order");

    @Test
    @DisplayName("restore 삽입 순서와 무관하게 segmentIndex 오름차순으로 poll한다")
    void pollTraversesSegmentsInAscendingIndexRegardlessOfInsertionOrder() {
        TopicQueue topicQueue = new TopicQueue(TOPIC, new NoOpTopicWal());

        // segmentIndex=4, 3, 5 순서로 역순 삽입
        topicQueue.restore(new Message(TOPIC, Map.of("id", 5)), 4);
        topicQueue.restore(new Message(TOPIC, Map.of("id", 6)), 4);
        topicQueue.restore(new Message(TOPIC, Map.of("id", 0)), 3);
        topicQueue.restore(new Message(TOPIC, Map.of("id", 1)), 3);
        topicQueue.restore(new Message(TOPIC, Map.of("id", 2)), 3);
        topicQueue.restore(new Message(TOPIC, Map.of("id", 7)), 5);
        topicQueue.restore(new Message(TOPIC, Map.of("id", 3)), 3);
        topicQueue.restore(new Message(TOPIC, Map.of("id", 4)), 3);

        Offset offset = topicQueue.getOffsetAtHead();

        IntStream.range(0, 8).forEach(expected -> {
            Message polled = topicQueue.poll(offset);
            assertThat(polled).isNotNull();
            assertThat(((Map<?, ?>) polled.content()).get("id")).isEqualTo(expected);
        });
        assertThat(topicQueue.poll(offset)).isNull();
    }

    @Test
    @DisplayName("가장 낮은 segmentIndex 세그먼트가 evict되면 다음 segmentIndex가 head가 된다")
    void afterEvictionNextSegmentIndexBecomesHead() {
        TopicQueue topicQueue = new TopicQueue(TOPIC, new NoOpTopicWal());

        // segmentIndex=3: 꽉 찬 세그먼트 (evict 조건 충족)
        IntStream.range(0, SEGMENT_CAPACITY).forEach(id ->
                topicQueue.restore(new Message(TOPIC, Map.of("id", id)), 3)
        );
        // segmentIndex=4: tail 세그먼트
        IntStream.range(0, 5).forEach(id ->
                topicQueue.restore(new Message(TOPIC, Map.of("id", SEGMENT_CAPACITY + id)), 4)
        );

        // consumer가 segmentIndex=3을 전부 소비 → evict 발생
        Offset consumer = topicQueue.getOffsetAtHead();
        IntStream.range(0, SEGMENT_CAPACITY).forEach(ignored -> topicQueue.poll(consumer));

        // evict 후 head는 segmentIndex=4이므로 첫 메시지는 id=1000이어야 한다
        Offset newHead = topicQueue.getOffsetAtHead();
        Message first = topicQueue.poll(newHead);
        assertThat(first).isNotNull();
        assertThat(((Map<?, ?>) first.content()).get("id")).isEqualTo(SEGMENT_CAPACITY);
    }
}
