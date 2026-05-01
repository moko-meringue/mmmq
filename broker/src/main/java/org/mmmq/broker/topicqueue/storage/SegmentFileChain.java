package org.mmmq.broker.topicqueue.storage;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListMap;
import org.mmmq.core.message.Message;

public final class SegmentFileChain implements Closeable {

    private final Path path; // segment 파일이 저장되는 디렉토리
    private final long rotationThreshold; // tail segment 크기가 이 값 이상이 되면 회전 (새 segment 생성)
    private final ConcurrentSkipListMap<Long, SegmentFile> segmentsByStartOffset = new ConcurrentSkipListMap<>(); // startOffset → Segment 맵. floorEntry로 임의 offset의 세그먼트를 O(log n)에 조회

    private SegmentFileChain(Path path, long rotationThreshold) { // 외부에서 직접 생성하지 않도록 private
        this.path = path;
        this.rotationThreshold = rotationThreshold;
    }

    public static SegmentFileChain open(Path base, long rotationThreshold) { // base는 caller가 존재 보장. 기존 세그먼트를 스캔해 복원
        SegmentFileChain chain = new SegmentFileChain(base, rotationThreshold);
        chain.bootstrap(); // 기존 세그먼트 파일 스캔. 각 segment는 자기 생성자에서 recover 수행
        return chain;
    }

    private void bootstrap() { // 디렉토리에서 기존 세그먼트를 스캔해 맵에 등록. 이후 tail은 항상 lastEntry로 도출
        SegmentFile.openAll(path)
                .forEach(segmentFile -> segmentsByStartOffset.put(segmentFile.startOffset(), segmentFile));
        if (segmentsByStartOffset.isEmpty()) { // 기존 세그먼트가 없으면 startOffset=0으로 첫 세그먼트를 새로 생성
            segmentsByStartOffset.put(0L, SegmentFile.open(path, 0L));
        }
    }

    public void append(Message message) { // 메시지를 tailSegment 세그먼트에 추가. 필요하면 먼저 회전
        SegmentFile tailSegmentFile = segmentsByStartOffset.lastEntry().getValue();
        if (tailSegmentFile.reaches(rotationThreshold)) {
            long nextOffset = tailSegmentFile.startOffset() + tailSegmentFile.count();
            tailSegmentFile = SegmentFile.open(path, nextOffset); // 새 .mmm/.idx 파일 생성
            segmentsByStartOffset.put(nextOffset, tailSegmentFile); // 맵에 등록해 readAt에서 조회 가능하도록
        }
        tailSegmentFile.append(message); // .mmm 쓰기 + fsync, .idx 쓰기 + fsync
    }

    @Nullable
    public Message readAt(long absoluteOffset) { // absoluteOffset에 해당하는 세그먼트를 찾아 메시지를 읽어 반환
        Long startOffset = segmentsByStartOffset.floorKey(absoluteOffset);
        if (startOffset == null) { // 모든 세그먼트의 startOffset보다 작으면 해당 메시지는 없음
            return null;
        }
        SegmentFile segmentFile = segmentsByStartOffset.get(startOffset);
        return segmentFile.readAt(absoluteOffset - segmentFile.startOffset());
    }

    @Override
    public void close() { // 모든 세그먼트의 .mmm/.idx 채널을 닫아 fd 누수 방지
        segmentsByStartOffset.values()
                .forEach(SegmentFile::close);
    }
}
