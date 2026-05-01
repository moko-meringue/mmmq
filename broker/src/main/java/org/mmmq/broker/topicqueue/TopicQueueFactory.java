package org.mmmq.broker.topicqueue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.mmmq.broker.config.SegmentProperties;
import org.mmmq.broker.config.StorageProperties;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.core.message.Topic;
import org.springframework.stereotype.Component;

@Component
public class TopicQueueFactory { // 디스크 레이아웃을 알고 TopicQueue를 조립하는 책임. 캐시/lifecycle/이벤트 발행은 모름

    private final Path root; // 세그먼트 파일 루트 디렉토리
    private final long segmentMaxBytes; // 세그먼트 파일 최대 크기 (바이트). 초과 시 새 세그먼트로 회전

    public TopicQueueFactory(StorageProperties storage, SegmentProperties segment) {
        this.root = Path.of(storage.rootDir());
        this.segmentMaxBytes = segment.maxBytes();
    }

    public TopicQueue create(Topic topic) { // 주어진 토픽으로 새 TopicQueue를 조립. 토픽 디렉토리를 만들고 storage 컴포넌트를 연결
        Path topicDir = root.resolve(topic.name()); // data/{topic}/ 경로
        try {
            Files.createDirectories(topicDir); // 토픽 디렉토리는 토픽 레이어 책임. storage 클래스들은 base 존재를 가정
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to create topic directory: " + topicDir, exception);
        }
        SegmentFileChain segmentFileChain = SegmentFileChain.open(topicDir, segmentMaxBytes);
        CheckpointDirectory checkpointDirectory = CheckpointDirectory.open(topicDir);
        return new TopicQueue(topic, segmentFileChain, checkpointDirectory);
    }
}
