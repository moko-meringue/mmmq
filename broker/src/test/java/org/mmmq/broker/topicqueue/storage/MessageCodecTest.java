package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class MessageCodecTest {

    @Test
    @DisplayName("encode 결과는 순수 JSON bytes이며 trailing 구분자가 붙지 않는다")
    void encodeReturnsPlainJsonBytes() throws MessageCodecException {
        final Message message = new Message(new Topic("topic"), Map.of("key", "value")); // 임의 메시지 생성

        final byte[] encoded = MessageCodec.encode(message); // 인코딩 실행

        assertThat(encoded[encoded.length - 1]).isNotEqualTo((byte) '\n'); // 직렬화 결과에 record terminator가 붙지 않아야 함 (framing은 SegmentFile 책임)
        assertThat(new String(encoded)).startsWith("{"); // JSON 객체 시작 문자
        assertThat(new String(encoded)).endsWith("}"); // JSON 객체 종료 문자 (trailing bytes 없음)
    }

    @Test
    @DisplayName("encode/decode round-trip 시 원본 메시지와 동일하다")
    void roundTripPreservesMessage() throws MessageCodecException {
        final Message original = new Message(new Topic("orders"), Map.of("id", 42, "name", "abc")); // 여러 필드를 가진 메시지로 직렬화 충실도 검증

        final byte[] encoded = MessageCodec.encode(original); // JSON bytes 생성
        final Message decoded = MessageCodec.decode(encoded); // 그대로 역직렬화

        assertThat(decoded).isEqualTo(original); // round-trip 후 모든 필드가 동일해야 함
    }
}
