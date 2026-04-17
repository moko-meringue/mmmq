package org.mmmq.broker.wal.codec;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.broker.wal.WalEntry;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

class JsonWalCodecTest {

    private final JsonWalCodec codec = new JsonWalCodec();

    @Test
    @DisplayName("encode한 결과를 decode하면 원본 엔트리와 동일하다")
    void encodeDecodeTripReturnsOriginalEntry() {
        final WalEntry original = new WalEntry(3, new Message(new Topic("order"), Map.of("id", 42)));

        final byte[] encoded = codec.encode(original);
        final WalEntry decoded = codec.decode(new String(encoded));

        assertThat(decoded).isNotNull();
        assertThat(decoded.segmentIndex()).isEqualTo(3);
        assertThat(decoded.message().topic().name()).isEqualTo("order");
    }

    @Test
    @DisplayName("encode 결과에 segmentIndex 필드가 포함된다")
    void encodedJsonContainsSegmentIndex() {
        final WalEntry entry = new WalEntry(5, new Message(new Topic("payment"), Map.of()));

        final String json = new String(codec.encode(entry));

        assertThat(json).contains("\"segmentIndex\":5");
    }

    @Test
    @DisplayName("손상된 JSON 라인을 decode하면 null을 반환한다")
    void returnsNullForCorruptLine() {
        final WalEntry result = codec.decode("not-valid-json");

        assertThat(result).isNull();
    }

    @Test
    @DisplayName("빈 라인을 decode하면 null을 반환한다")
    void returnsNullForBlankLine() {
        final WalEntry result = codec.decode("   ");

        assertThat(result).isNull();
    }
}
