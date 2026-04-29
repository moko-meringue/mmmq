package org.mmmq.broker.topicqueue.storage;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListMap;
import org.mmmq.core.message.Message;

public final class SegmentChain implements Closeable { // 한 디렉토리 내 세그먼트 파일 목록을 관리. 세그먼트 회전, offset 기반 조회, 부팅 복구를 담당

    private final Path path; // segment 파일이 저장되는 디렉토리
    private final long rotationThreshold; // tail segment 크기가 이 값 이상이 되면 회전 (새 segment 생성)
    private final ConcurrentSkipListMap<Long, Segment> segmentsByStartOffset = new ConcurrentSkipListMap<>(); // startOffset → Segment 맵. floorEntry로 임의 offset의 세그먼트를 O(log n)에 조회

    private SegmentChain(Path path, long rotationThreshold) { // 외부에서 직접 생성하지 않도록 private
        this.path = path;
        this.rotationThreshold = rotationThreshold;
    }

    public static SegmentChain open(Path base, long rotationThreshold) { // base는 caller가 존재 보장. 기존 세그먼트를 스캔해 복원
        SegmentChain chain = new SegmentChain(base, rotationThreshold);
        chain.bootstrap(); // 기존 세그먼트 파일 스캔. 각 segment는 자기 생성자에서 recover 수행
        return chain;
    }

    private void bootstrap() { // 디렉토리에서 기존 세그먼트를 스캔해 맵에 등록. 이후 tail은 항상 lastEntry로 도출
        Segment.openAll(path)
                .forEach(segment -> segmentsByStartOffset.put(segment.startOffset(), segment));
        if (segmentsByStartOffset.isEmpty()) { // 기존 세그먼트가 없으면 startOffset=0으로 첫 세그먼트를 새로 생성
            segmentsByStartOffset.put(0L, Segment.open(path, 0L));
        }
    }

    public void append(Message message) { // 메시지를 tailSegment 세그먼트에 추가. 필요하면 먼저 회전
        Segment tailSegment = segmentsByStartOffset.lastEntry().getValue();
        if (tailSegment.reaches(rotationThreshold)) {
            long nextOffset = tailSegment.startOffset() + tailSegment.count();
            tailSegment = Segment.open(path, nextOffset); // 새 .mmm/.idx 파일 생성
            segmentsByStartOffset.put(nextOffset, tailSegment); // 맵에 등록해 readAt에서 조회 가능하도록
        }
        tailSegment.append(message); // .mmm 쓰기 + fsync, .idx 쓰기 + fsync
    }

    @Nullable
    public Message readAt(long absoluteOffset) { // absoluteOffset에 해당하는 세그먼트를 찾아 메시지를 읽어 반환
        Long startOffset = segmentsByStartOffset.floorKey(absoluteOffset);
        if (startOffset == null) { // 모든 세그먼트의 startOffset보다 작으면 해당 메시지는 없음
            return null;
        }
        Segment segment = segmentsByStartOffset.get(startOffset);
        return segment.readAt(absoluteOffset - segment.startOffset());
    }

    @Override
    public void close() { // 모든 세그먼트의 .mmm/.idx 채널을 닫아 fd 누수 방지
        segmentsByStartOffset.values()
                .forEach(Segment::close);
    }
}
