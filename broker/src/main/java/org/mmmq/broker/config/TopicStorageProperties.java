package org.mmmq.broker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mmmq.broker.storage") // application.yml에서 mmmq.broker.storage.* 키를 이 레코드에 바인딩
public record TopicStorageProperties(
        String rootDir,       // 세그먼트 파일을 저장할 루트 디렉토리 경로
        long segmentMaxBytes  // 세그먼트 파일 최대 크기 (바이트). 초과 시 새 세그먼트로 회전
) {

    private static final String DEFAULT_DATA_DIR = "./data"; // rootDir 미설정 시 사용하는 기본 경로
    private static final long DEFAULT_SEGMENT_MAX_BYTES = 64L * 1024 * 1024; // 기본 최대 세그먼트 크기: 64MB

    public TopicStorageProperties { // compact 생성자: 바인딩 직후 실행되어 누락된 값에 기본값을 채움
        if (rootDir == null || rootDir.isBlank()) { // dataDir가 null이거나 공백 문자열이면 기본값으로 대체
            rootDir = DEFAULT_DATA_DIR;
        }
        if (segmentMaxBytes <= 0) { // 0 이하의 값은 설정 오류로 간주하고 기본값(64MB)으로 대체
            segmentMaxBytes = DEFAULT_SEGMENT_MAX_BYTES;
        }
    }
}
