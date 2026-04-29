package org.mmmq.broker.topicqueue.storage;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OffsetCheckpointTest {

    @Test
    @DisplayName("새로 생성한 store의 read는 0L을 반환한다")
    void newStoreReturnsZero(@TempDir Path tempDir) {
        try (OffsetCheckpoint store = OffsetCheckpoint.open(tempDir, "dispatcher-a")) { // 파일 없음 → 0으로 초기화
            assertThat(store.read()).isZero(); // 신규 dispatcher는 offset 0부터 시작해야 함
        }
    }

    @Test
    @DisplayName("write 후 read는 동일한 값을 반환한다")
    void writeAndRead(@TempDir Path tempDir) {
        try (OffsetCheckpoint store = OffsetCheckpoint.open(tempDir, "dispatcher-a")) {
            store.write(123L); // 123을 디스크에 fsync

            assertThat(store.read()).isEqualTo(123L); // 같은 인스턴스에서 다시 읽어도 123이 반환됨
        }
    }

    @Test
    @DisplayName("write 후 새로 open한 store도 동일한 값을 읽는다")
    void persistsAcrossInstances(@TempDir Path tempDir) {
        try (OffsetCheckpoint store = OffsetCheckpoint.open(tempDir, "dispatcher-a")) {
            store.write(42L); // 42를 디스크에 fsync 완료
        } // 채널 닫기: 브로커 종료 시뮬레이션

        try (OffsetCheckpoint reopened = OffsetCheckpoint.open(tempDir, "dispatcher-a")) { // 새 인스턴스로 같은 파일 열기: 재시작 시뮬레이션
            assertThat(reopened.read()).isEqualTo(42L); // fsync가 실제로 완료됐으므로 재시작 후에도 42를 읽어야 함
        }
    }
}
