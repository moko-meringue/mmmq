package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import org.mmmq.core.message.Message;

final class SegmentFile implements Closeable { // .mmm 파일과 .idx 파일 한 쌍을 묶어 하나의 세그먼트를 표현. 레코드 구획(framing) 책임을 가짐

    private static final String SEGMENT_SUFFIX = ".mmm"; // 세그먼트 파일 확장자
    private static final String INDEX_SUFFIX = ".idx"; // 인덱스 파일 확장자
    private static final String FILE_NAME_FORMAT = "segment-%020d"; // 20자리 zero-padding: lexicographic 정렬 = numeric 정렬 보장
    private static final int LENGTH_HEADER_SIZE = Integer.BYTES; // 레코드 앞에 붙는 길이 헤더 크기 (4 bytes)

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

    long nextAbsoluteOffset() { // 다음 메시지가 받을 절대 offset = 이 세그먼트 startOffset + 현재 엔트리 수
        return startOffset + index.count();
    }

    long size() { // .mmm 파일의 현재 크기. SegmentDirectory가 rotate 여부를 판단할 때 사용
        return segment.size();
    }

    void append(Message message) { // 메시지를 인코딩해 .mmm에 쓰고, 그 위치를 .idx에 기록
        try {
            append(MessageCodec.encode(message)); // Message → JSON bytes → byte[] 버전으로 위임
        } catch (MessageCodecException exception) {
            throw new StorageException("Failed to encode message: " + message, exception);
        }
    }

    void append(byte[] payload) { // payload 앞에 4 bytes 길이 헤더를 붙여 .mmm에 쓰고 .idx에 record 시작 위치를 기록
        final ByteBuffer record = ByteBuffer.allocate(LENGTH_HEADER_SIZE + payload.length); // 헤더 + payload 한 번에 쓰기 위해 합쳐 둠
        record.putInt(payload.length); // 4 bytes BIG_ENDIAN 길이 헤더 (ByteBuffer 기본 endian)
        record.put(payload); // 헤더 뒤에 실제 payload 붙임
        final long position = segment.appendAndForce(record.array()); // .mmm 쓰기 + fsync #1. 반환값은 record 시작 위치
        index.appendAndForce(position); // .idx 쓰기 + fsync #2. segment fsync 완료 후 실행해야 불변식 유지
    }

    Optional<Message> readAt(long absoluteOffset) { // absoluteOffset 위치의 메시지를 읽어 반환. 범위 밖이면 empty
        final long relativeLong = absoluteOffset - startOffset; // 절대 offset → 이 세그먼트 내 상대 offset으로 변환
        if (relativeLong < 0 || relativeLong >= index.count()) { // 이 세그먼트에 해당 offset이 없으면 empty
            return Optional.empty();
        }
        final long position = index.readPositionAt(relativeLong); // .idx에서 .mmm 내 record 시작 위치 조회
        final int payloadLength = ByteBuffer.wrap(segment.readAt(position, LENGTH_HEADER_SIZE)).getInt(); // 헤더 4 bytes를 읽어 payload 길이 파악
        final byte[] payload = segment.readAt(position + LENGTH_HEADER_SIZE, payloadLength); // 헤더 다음부터 정확히 payloadLength bytes만 읽기
        try {
            return Optional.of(MessageCodec.decode(payload));
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
        final long lastIdxEntry = index.readPositionAt(entryCount - 1); // .idx 마지막 엔트리 = 마지막 커밋 record의 시작 위치
        if (lastIdxEntry + LENGTH_HEADER_SIZE > actualSegmentSize) { // 헤더조차 다 들어있지 않으면 .idx가 .mmm 끝을 넘어 가리키는 손상
            throw new StorageException(
                    "Index points beyond segment end. segmentSize=" + actualSegmentSize + ", lastEntry=" + lastIdxEntry
            );
        }
        final byte[] header = segment.readAt(lastIdxEntry, LENGTH_HEADER_SIZE); // 마지막 record의 길이 헤더 읽기
        final int payloadLength = ByteBuffer.wrap(header).getInt(); // 헤더에서 payload 길이 추출
        final long expectedSegmentEnd = lastIdxEntry + LENGTH_HEADER_SIZE + payloadLength; // 마지막 record가 끝나는 위치 = 정상 .mmm의 기대 크기
        if (expectedSegmentEnd > actualSegmentSize) { // payload가 다 들어있지 않으면 incomplete record
            throw new StorageException(
                    "Index points to incomplete record. segmentSize=" + actualSegmentSize
                            + ", lastEntry=" + lastIdxEntry + ", expectedEnd=" + expectedSegmentEnd
            );
        }
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
