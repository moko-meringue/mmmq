package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.Optional;
import org.mmmq.core.message.Message;

final class SegmentFile implements Closeable { // .mmm 파일과 .idx 파일 한 쌍을 묶어 하나의 세그먼트를 표현

    private static final String SEGMENT_SUFFIX = ".mmm"; // 세그먼트 파일 확장자
    private static final String INDEX_SUFFIX = ".idx"; // 인덱스 파일 확장자
    private static final String FILE_NAME_FORMAT = "segment-%020d"; // 20자리 zero-padding: lexicographic 정렬 = numeric 정렬 보장

    private final long startOffset; // 이 세그먼트의 첫 메시지가 갖는 절대 offset. 파일명에서 파싱됨
    private final Segment segment; // .mmm 파일 핸들
    private final SegmentIndex index; // .idx 파일 핸들

    private SegmentFile(long startOffset, Segment segment, SegmentIndex index) { // 외부에서 직접 생성하지 않도록 private
        this.startOffset = startOffset;
        this.segment = segment;
        this.index = index;
    }

    static SegmentFile openOrCreate(Path dir, long startOffset) { // startOffset으로 파일명을 결정하고 .mmm/.idx를 각각 열거나 생성
        final String baseName = String.format(FILE_NAME_FORMAT, startOffset);
        final Segment segment = Segment.openOrCreate(dir.resolve(baseName + SEGMENT_SUFFIX));
        final SegmentIndex index = SegmentIndex.openOrCreate(dir.resolve(baseName + INDEX_SUFFIX));

        return new SegmentFile(startOffset, segment, index);
    }

    static String segmentFileName(long startOffset) { // SegmentDirectory에서 파일명으로 startOffset을 파싱할 때 포맷 재사용 목적
        return String.format(FILE_NAME_FORMAT, startOffset) + SEGMENT_SUFFIX;
    }

    long startOffset() { // 이 세그먼트가 시작하는 절대 offset. floorEntry 키로 사용됨
        return startOffset;
    }

    long nextAbsoluteOffset() { // 다음 메시지가 받을 절대 offset = 이 세그먼트 startOffset + 현재 엔트리 수
        return startOffset + index.count();
    }

    long size() { // .mmm 파일의 현재 크기. SegmentDirectory가 rotate 여부를 판단할 때 사용
        return segment.size();
    }

    void append(Message message) { // 메시지를 인코딩해 .mmm에 쓰고, 그 위치를 .idx에 기록
        try {
            append(MessageCodec.encode(message)); // Message → JSON bytes + '\n' → byte[] 버전으로 위임
        } catch (MessageCodecException exception) {
            throw new StorageException("Failed to encode message: " + message, exception);
        }
    }

    void append(byte[] payload) { // 인코딩된 payload를 .mmm에 쓰고 .idx에 위치를 기록. fsync 순서: segment → index
        final long position = segment.appendAndForce(payload); // .mmm 쓰기 + fsync #1. 반환값은 .idx에 저장될 위치
        index.appendAndForce(position); // .idx 쓰기 + fsync #2. segment fsync 완료 후 실행해야 불변식 유지
    }

    Optional<Message> readAt(long absoluteOffset) { // absoluteOffset 위치의 메시지를 읽어 반환. 범위 밖이면 empty
        final long relativeLong = absoluteOffset - startOffset; // 절대 offset → 이 세그먼트 내 상대 offset으로 변환
        if (relativeLong < 0 || relativeLong >= index.count()) { // 이 세그먼트에 해당 offset이 없으면 empty
            return Optional.empty();
        }
        final long position = index.readPositionAt(relativeLong); // .idx에서 .mmm 내 실제 위치를 조회
        try {
            return Optional.of(MessageCodec.decode(segment.readLineAt(position))); // .mmm에서 해당 위치의 라인을 읽어 Message로 역직렬화
        } catch (MessageCodecException exception) {
            throw new StorageException("Failed to decode message at offset: " + absoluteOffset, exception);
        }
    }

    void recoverActiveSegment() { // 부팅 시 마지막 세그먼트의 정합성을 검사하고 미커밋 trailing bytes를 제거
        final long entryCount = index.count(); // .idx 크기 / 8 = 커밋이 완료된 메시지 수
        final long actualSegmentSize = segment.size(); // 현재 .mmm 파일의 실제 크기
        if (entryCount == 0) { // 인덱스가 비어있으면 커밋된 메시지가 없으므로 .mmm도 비워야 함
            // MERINGUE: 애플리케이션이 마음대로 파일 내용을 조작하면 안됨.
            // 조작 없이 종료하도록 변경해야 함.
            // ~가 잘못되었으니 ~ 파일 내용 비워주세요 정도로 로그 남기고 종료하도록 변경해야 함.
            if (actualSegmentSize > 0) { // .mmm에 데이터가 있으면 이전 비정상 종료로 쓰여진 partial write
                segment.truncate(0); // .mmm을 완전히 비워 일관성 복원
            }
            return;
        }
        final long lastIdxEntry = index.readPositionAt(entryCount - 1); // .idx 마지막 엔트리 = 마지막 커밋 메시지의 .mmm 위치
        if (lastIdxEntry >= actualSegmentSize) { // .idx가 .mmm 끝을 넘어서 가리키면 심각한 디스크 손상
            throw new StorageException(
                    "Index points beyond segment end. segmentSize=" + actualSegmentSize + ", lastEntry=" + lastIdxEntry
            );
        }
        final byte[] lastLine = segment.readLineAt(lastIdxEntry); // 마지막 커밋 엔트리를 읽어 길이를 측정
        if (lastLine[lastLine.length - 1] != '\n') { // '\n' 미발견: .idx는 있는데 .mmm의 해당 라인이 incomplete
            throw new StorageException(
                    "Index points to incomplete line. segmentSize=" + actualSegmentSize + ", lastEntry=" + lastIdxEntry
            );
        }
        final long expectedSegmentEnd = lastIdxEntry + lastLine.length; // 마지막 커밋 엔트리가 끝나는 위치 = 정상 .mmm의 기대 크기
        if (actualSegmentSize > expectedSegmentEnd) { // 기대 크기보다 크면 .idx에 반영되지 않은 trailing bytes가 존재
            // MERINGUE: 애플리케이션이 마음대로 파일 내용을 조작하면 안됨.
            // 조작 없이 종료하도록 변경해야 함.
            // ~가 잘못되었으니 ~ 파일 내용 고쳐주세요 정도로 로그 남기고 종료하도록 변경해야 함.
            segment.truncate(expectedSegmentEnd); // trailing bytes 제거: 이 위치 이후는 미커밋 partial write
        }
    }

    @Override
    public void close() { // .mmm와 .idx 채널을 모두 닫아 fd 누수 방지
        segment.close();
        index.close();
    }
}
