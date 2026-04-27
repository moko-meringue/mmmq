package org.mmmq.broker.topicqueue.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.mmmq.core.message.Message;

final class MessageCodec { // 외부에서 직접 인스턴스화할 필요가 없는 정적 유틸 클래스

    private static final ObjectMapper MAPPER = new ObjectMapper(); // ObjectMapper는 thread-safe이므로 공유 인스턴스 하나로 재사용
    static final byte RECORD_TERMINATOR = (byte) '\n'; // 직렬화 포맷의 레코드 구분자. 포맷이 바뀌면 이 값만 변경

    private MessageCodec() { // 인스턴스화 방지: 모든 메서드가 static이므로 객체를 만들 이유가 없음
    }

    static byte[] encode(Message message)
            throws MessageCodecException { // Message를 JSON bytes로 직렬화한 뒤 마지막에 '\n'을 추가해 반환
        try {
            final byte[] body = MAPPER.writeValueAsBytes(message); // Message를 JSON bytes로 직렬화
            final byte[] line = new byte[body.length + 1]; // '\n' 1바이트를 추가할 공간을 확보한 새 배열 생성
            System.arraycopy(body, 0, line, 0, body.length); // JSON bytes를 새 배열 앞부분에 복사
            line[body.length] = RECORD_TERMINATOR; // 배열 마지막 바이트에 레코드 구분자 추가
            return line;
        } catch (Exception exception) { // 직렬화 중 발생하는 모든 예외를 캐치해 MessageCodecException으로 던짐
            throw new MessageCodecException("Failed to encode message: " + message, exception);
        }
    }

    static boolean isComplete(byte[] record) { // 레코드의 마지막 바이트가 구분자이면 완전한 레코드. 포맷 변경 시 이 로직만 수정
        return record.length > 0 && record[record.length - 1] == RECORD_TERMINATOR;
    }

    static Message decode(byte[] line) throws MessageCodecException { // RECORD_TERMINATOR가 포함된 bytes를 읽어 Message 객체로 역직렬화
        try {
            return MAPPER.readValue(
                    new String(line, 0, line.length - 1, StandardCharsets.UTF_8),
                    Message.class
            );
        } catch (Exception exception) { // 역직렬화 중 발생하는 모든 예외를 캐치해 MessageCodecException으로 던짐
            throw new MessageCodecException("Failed to decode message line", exception);
        }
    }
}
