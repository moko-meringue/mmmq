package org.mmmq.broker.topicqueue.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.core.message.Message;

final class MessageCodec { // Message ↔ JSON bytes 변환만 담당. 레코드 구획(framing)은 SegmentFile의 책임

    private static final ObjectMapper MAPPER = new ObjectMapper(); // ObjectMapper는 thread-safe이므로 공유 인스턴스 하나로 재사용

    private MessageCodec() { // 인스턴스화 방지: 모든 메서드가 static이므로 객체를 만들 이유가 없음
    }

    static byte[] encode(Message message) throws MessageCodecException { // Message를 순수 JSON bytes로 직렬화
        try {
            return MAPPER.writeValueAsBytes(message);
        } catch (Exception exception) { // 직렬화 중 발생하는 모든 예외를 캐치해 MessageCodecException으로 던짐
            throw new MessageCodecException("Failed to encode message: " + message, exception);
        }
    }

    static Message decode(byte[] payload) throws MessageCodecException { // JSON bytes를 Message 객체로 역직렬화
        try {
            return MAPPER.readValue(payload, Message.class);
        } catch (Exception exception) { // 역직렬화 중 발생하는 모든 예외를 캐치해 MessageCodecException으로 던짐
            throw new MessageCodecException("Failed to decode message", exception);
        }
    }
}
