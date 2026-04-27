package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.mmmq.core.message.Message;

public final class SegmentDirectory implements Closeable { // 한 토픽의 세그먼트 파일 목록을 관리. 세그먼트 회전, offset 기반 조회, 부팅 복구를 담당

    private static final Pattern SEGMENT_FILE_PATTERN = Pattern.compile(
            "segment-(\\d{20})\\.mmm"); // 파일명에서 20자리 startOffset을 파싱하기 위한 정규식
    private static final String OFFSETS_DIR_NAME = "offsets"; // dispatcher offset 파일을 저장하는 서브디렉토리 이름

    private final Path topicDir; // data/{topic}/ 디렉토리 경로
    private final long segmentMaxBytes; // 세그먼트 회전 임계값. active 세그먼트 크기가 이 값 이상이면 새 세그먼트 생성
    private final ConcurrentSkipListMap<Long, Segment> segmentsByStartOffset = new ConcurrentSkipListMap<>(); // startOffset → Segment 맵. floorEntry로 임의 offset의 세그먼트를 O(log n)에 조회
    private Segment active; // 현재 쓰기 중인 세그먼트. 항상 segmentsByStartOffset의 마지막 엔트리

    private SegmentDirectory(Path topicDir, long segmentMaxBytes) { // 외부에서 직접 생성하지 않도록 private
        this.topicDir = topicDir;
        this.segmentMaxBytes = segmentMaxBytes;
    }

    public static SegmentDirectory openOrCreate(Path topicDir, long segmentMaxBytes) { // 디렉토리를 생성/열고 기존 세그먼트를 스캔해 복원
        try {
            Files.createDirectories(topicDir); // data/{topic}/ 생성. 이미 존재하면 noop
            Files.createDirectories(topicDir.resolve(OFFSETS_DIR_NAME)); // data/{topic}/offsets/ 생성
        } catch (IOException exception) {
            throw new StorageException("Failed to create topic directory: " + topicDir, exception);
        }
        SegmentDirectory directory = new SegmentDirectory(topicDir, segmentMaxBytes);
        directory.bootstrap(); // 기존 세그먼트 파일 스캔 및 마지막 세그먼트 정합성 복구

        return directory;
    }

    public void append(Message message) { // 메시지를 active 세그먼트에 추가. 필요하면 먼저 회전
        if (active.size() > 0 && active.size() >= segmentMaxBytes) { // 빈 세그먼트는 메시지 크기와 무관하게 항상 수용 (무한 rotate 방지)
            rotate();
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
        return topicDir.resolve(OFFSETS_DIR_NAME);
    }

    // MERINGUE: 모든 파일 정합성 검사 하도록
    private void bootstrap() { // 디렉토리에서 기존 세그먼트를 스캔해 맵에 등록하고 active 설정
        List<Long> startOffsets = scanStartOffsets(); // 파일명 기준으로 정렬된 startOffset 목록
        if (startOffsets.isEmpty()) { // 기존 세그먼트가 없으면 startOffset=0으로 첫 세그먼트를 새로 생성
            Segment first = Segment.openOrCreate(topicDir, 0L);
            segmentsByStartOffset.put(0L, first);
            active = first;

            return;
        }
        for (Long startOffset : startOffsets) { // 스캔된 세그먼트를 순서대로 맵에 등록
            Segment segment = Segment.openOrCreate(topicDir, startOffset);
            segmentsByStartOffset.put(startOffset, segment);
        }
        active = segmentsByStartOffset.lastEntry().getValue(); // 가장 큰 startOffset을 가진 세그먼트가 active
        active.recoverActiveSegment(); // 마지막 세그먼트의 미커밋 trailing bytes를 제거하고 정합성 복구
    }

    private List<Long> scanStartOffsets() { // 토픽 디렉토리의 .mmm 파일을 스캔해 startOffset 목록을 정렬된 순서로 반환
        try (Stream<Path> entries = Files.list(topicDir)) {
            return entries.filter(Files::isRegularFile) // 디렉토리(offsets/)는 제외
                    .map(path -> path.getFileName().toString()) // 파일 경로 → 파일명만 추출
                    .map(SEGMENT_FILE_PATTERN::matcher) // 파일명 → Matcher 생성
                    .filter(Matcher::matches) // "segment-{20자리}.mmm" 형식에 맞는 파일만 남김
                    .map(matcher -> Long.parseLong(matcher.group(1))) // 정규식 첫 번째 그룹(20자리 숫자) → long startOffset
                    .sorted() // 오름차순 정렬: 오래된 세그먼트 → 최신 세그먼트 순
                    .toList();
        } catch (IOException exception) {
            throw new StorageException("Failed to scan topic directory: " + topicDir, exception);
        }
    }

    private void rotate() { // 새 세그먼트를 생성해 active로 교체
        long nextStart = segmentsByStartOffset.lastKey() + active.count();
        Segment next = Segment.openOrCreate(topicDir, nextStart); // 새 .mmm/.idx 파일 생성
        segmentsByStartOffset.put(nextStart, next); // 맵에 등록해 readAt에서 조회 가능하도록
        active = next; // 이후 쓰기는 새 세그먼트로
    }

    @Override
    public void close() { // 모든 세그먼트의 .mmm/.idx 채널을 닫아 fd 누수 방지
        segmentsByStartOffset.values()
                .forEach(Segment::close);
    }
}
