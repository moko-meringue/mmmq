package org.mmmq.broker.topicqueue.storage;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import org.mmmq.core.message.Message;

public final class SegmentChain implements Closeable { // 한 토픽의 세그먼트 파일 목록을 관리. 세그먼트 회전, offset 기반 조회, 부팅 복구를 담당

    private static final String OFFSETS_DIR_NAME = "offsets"; // dispatcher offset 파일을 저장하는 서브디렉토리 이름

    private final Path path; // data/{topic}/ 디렉토리 경로
    private final long segmentMaxBytes; // 세그먼트 회전 임계값. active 세그먼트 크기가 이 값 이상이면 새 세그먼트 생성
    private final ConcurrentSkipListMap<Long, Segment> segmentsByStartOffset = new ConcurrentSkipListMap<>(); // startOffset → Segment 맵. floorEntry로 임의 offset의 세그먼트를 O(log n)에 조회
    private Segment active; // 현재 쓰기 중인 세그먼트. 항상 segmentsByStartOffset의 가장 큰 startOffset을 가진 segment

    private SegmentChain(Path path, long segmentMaxBytes) { // 외부에서 직접 생성하지 않도록 private
        this.path = path;
        this.segmentMaxBytes = segmentMaxBytes;
    }

    public static SegmentChain open(Path topicDir, long segmentMaxBytes) { // 디렉토리를 생성/열고 기존 세그먼트를 스캔해 복원
        try {
            Files.createDirectories(topicDir); // data/{topic}/ 생성. 이미 존재하면 noop
            Files.createDirectories(topicDir.resolve(OFFSETS_DIR_NAME)); // data/{topic}/offsets/ 생성
        } catch (IOException exception) {
            throw new StorageException("Failed to create topic directory: " + topicDir, exception);
        }
        SegmentChain chain = new SegmentChain(topicDir, segmentMaxBytes);
        chain.bootstrap(); // 기존 세그먼트 파일 스캔 후 active 설정. 각 segment는 자기 생성자에서 recover 수행

        return chain;
    }

    public void append(Message message) { // 메시지를 active 세그먼트에 추가. 필요하면 먼저 회전
        if (active.reaches(segmentMaxBytes)) {
            long nextOffset = active.startOffset() + active.count();
            Segment next = Segment.open(path, nextOffset); // 새 .mmm/.idx 파일 생성
            segmentsByStartOffset.put(nextOffset, next); // 맵에 등록해 readAt에서 조회 가능하도록
            active = next; // 이후 쓰기는 새 세그먼트로
        }
        active.append(message); // .mmm 쓰기 + fsync, .idx 쓰기 + fsync
    }

    @Nullable
    public Message readAt(long absoluteOffset) { // absoluteOffset에 해당하는 세그먼트를 찾아 메시지를 읽어 반환
        Map.Entry<Long, Segment> entry = segmentsByStartOffset.floorEntry(
                absoluteOffset); // startOffset <= absoluteOffset인 가장 큰 세그먼트를 O(log n)에 조회
        if (entry == null) { // 모든 세그먼트의 startOffset보다 작으면 해당 메시지는 없음
            return null;
        }

        long relativeOffset = absoluteOffset - entry.getKey();

        return entry.getValue().readAt(relativeOffset);
    }

    public Path offsetsDir() { // data/{topic}/offsets/ 경로 반환. TopicQueue가 OffsetStore를 만들 때 사용
        return path.resolve(OFFSETS_DIR_NAME);
    }

    private void bootstrap() { // 디렉토리에서 기존 세그먼트를 스캔해 맵에 등록하고 active 설정
        Segment.openAll(path).forEach( // ConcurrentSkipListMap이 키 기준으로 자동 정렬하므로 삽입 순서 무관
                segment -> segmentsByStartOffset.put(segment.startOffset(), segment));
        if (segmentsByStartOffset.isEmpty()) { // 기존 세그먼트가 없으면 startOffset=0으로 첫 세그먼트를 새로 생성
            Segment first = Segment.open(path, 0L);
            segmentsByStartOffset.put(0L, first);
        }
        active = segmentsByStartOffset.lastEntry().getValue(); // 가장 큰 startOffset을 가진 세그먼트가 active
    }

    @Override
    public void close() { // 모든 세그먼트의 .mmm/.idx 채널을 닫아 fd 누수 방지
        segmentsByStartOffset.values()
                .forEach(Segment::close);
    }
}
