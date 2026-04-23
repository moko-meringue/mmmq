package org.mmmq.broker.topicqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.fixture.NoOpTopicWal;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class TopicQueueSegmentHoleTest {

    private static final int SEGMENT_CAPACITY = 1000;
    private static final int MESSAGES_IN_SEGMENT_ZERO_AFTER_HOLE = 999;
    private static final int MESSAGES_IN_SEGMENT_ONE = 1000;

    @Test
    @DisplayName("비-tail 세그먼트에 구멍이 있어도 poll이 멈추지 않고 다음 세그먼트로 진행한다")
    void pollContinuesAcrossHoleInNonTailSegment() {
        TopicQueue topicQueue = new TopicQueue(new Topic("order"), new NoOpTopicWal());

        IntStream.range(0, MESSAGES_IN_SEGMENT_ZERO_AFTER_HOLE).forEach(id ->
                topicQueue.restore(new Message(new Topic("order"), Map.of("id", id)), 0)
        );
        IntStream.range(0, MESSAGES_IN_SEGMENT_ONE).forEach(id ->
                topicQueue.restore(
                        new Message(new Topic("order"), Map.of("id", MESSAGES_IN_SEGMENT_ZERO_AFTER_HOLE + id)),
                        1
                )
        );

        Offset offset = topicQueue.getOffsetAtHead();
        IntStream.range(0, MESSAGES_IN_SEGMENT_ZERO_AFTER_HOLE).forEach(expected -> {
            Message polled = topicQueue.poll(offset);
            assertThat(polled).isNotNull();
            assertThat(((Map<?, ?>) polled.content()).get("id")).isEqualTo(expected);
        });

        Message firstOfSegmentOne = topicQueue.poll(offset);
        assertThat(firstOfSegmentOne).isNotNull();
        assertThat(((Map<?, ?>) firstOfSegmentOne.content()).get("id"))
                .isEqualTo(MESSAGES_IN_SEGMENT_ZERO_AFTER_HOLE);

        IntStream.range(1, MESSAGES_IN_SEGMENT_ONE).forEach(indexInSegmentOne -> {
            Message polled = topicQueue.poll(offset);
            assertThat(polled).isNotNull();
            assertThat(((Map<?, ?>) polled.content()).get("id"))
                    .isEqualTo(MESSAGES_IN_SEGMENT_ZERO_AFTER_HOLE + indexInSegmentOne);
        });

        assertThat(topicQueue.poll(offset)).isNull();
    }

    @Test
    @DisplayName("구멍이 있는 세그먼트가 evict되어도 새 offset은 head 세그먼트의 첫 메시지부터 읽는다")
    void newOffsetStartsAtHeadAfterEviction() {
        TopicQueue topicQueue = new TopicQueue(new Topic("order"), new NoOpTopicWal());

        IntStream.range(0, MESSAGES_IN_SEGMENT_ZERO_AFTER_HOLE).forEach(id ->
                topicQueue.restore(new Message(new Topic("order"), Map.of("id", id)), 0)
        );
        IntStream.range(0, MESSAGES_IN_SEGMENT_ONE).forEach(id ->
                topicQueue.restore(
                        new Message(new Topic("order"), Map.of("id", MESSAGES_IN_SEGMENT_ZERO_AFTER_HOLE + id)),
                        1
                )
        );

        Offset first = topicQueue.getOffsetAtHead();
        IntStream.range(0, MESSAGES_IN_SEGMENT_ZERO_AFTER_HOLE).forEach(ignored ->
                topicQueue.poll(first)
        );
        topicQueue.poll(first);

        Offset late = topicQueue.getOffsetAtHead();
        Message polled = topicQueue.poll(late);
        assertThat(polled).isNotNull();
        assertThat(((Map<?, ?>) polled.content()).get("id")).isEqualTo(MESSAGES_IN_SEGMENT_ZERO_AFTER_HOLE);
    }
}
