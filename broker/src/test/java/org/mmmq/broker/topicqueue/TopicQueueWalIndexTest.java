package org.mmmq.broker.topicqueue;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class TopicQueueWalIndexTest {

    private static class CapturingPersistence implements MessagePersistence {

        private final List<Integer> persistedIndexes = new ArrayList<>();

        @Override
        public void persist(Message message, int segmentIndex) {
            persistedIndexes.add(segmentIndex);
        }

        @Override
        public void evict(int segmentIndex) {
        }

        List<Integer> persistedIndexes() {
            return persistedIndexes;
        }
    }

    @Test
    @DisplayName("세그먼트 회전 시 persist에 전달되는 WAL 파일 인덱스가 실제 적재 세그먼트와 일치한다")
    void walFileIndexMatchesSegmentOnRotation() {
        CapturingPersistence persistence = new CapturingPersistence();
        TopicQueue topicQueue = new TopicQueue(new Topic("order"), persistence);

        IntStream.range(0, 1001).forEach(id ->
                topicQueue.offer(new Message(new Topic("order"), Map.of("id", id)))
        );

        List<Integer> indexes = persistence.persistedIndexes();
        assertThat(indexes).hasSize(1001);
        assertThat(indexes.subList(0, 1000)).containsOnly(0);
        assertThat(indexes.get(1000)).isEqualTo(1);
    }
}
