package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class MessageCodecTest {

    @Test
    @DisplayName("encode 결과 마지막 byte는 \\n 이다")
    void encodeAppendsLineTerminator() throws MessageCodecException {
        final Message message = new Message(new Topic("topic"), Map.of("key", "value")); // 임의 메시지 생성

        final byte[] encoded = MessageCodec.encode(message); // 인코딩 실행

        assertThat(encoded[encoded.length - 1]).isEqualTo((byte) '\n'); // 마지막 바이트가 '\n'(0x0A)인지 확인. readLineAt의 경계 탐지 조건과 일치해야 함
    }

    @Test
    @DisplayName("encode/decode round-trip 시 원본 메시지와 동일하다")
    void roundTripPreservesMessage() throws MessageCodecException {
        final Message original = new Message(new Topic("orders"), Map.of("id", 42, "name", "abc")); // 여러 필드를 가진 메시지로 직렬화 충실도 검증

        final byte[] encoded = MessageCodec.encode(original); // JSON bytes + '\n' 생성
        final Message decoded = MessageCodec.decode(encoded); // '\n' 제거 후 역직렬화

        assertThat(decoded).isEqualTo(original); // round-trip 후 모든 필드가 동일해야 함
    }

}
