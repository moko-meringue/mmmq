package org.mmmq.broker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mmmq.broker.segment") // application.yml에서 mmmq.broker.segment.* 키를 이 레코드에 바인딩
public record SegmentProperties(
        long maxBytes // 세그먼트 파일 최대 크기 (바이트). 초과 시 새 세그먼트로 회전
) {

    private static final long DEFAULT_MAX_BYTES = 64L * 1024 * 1024; // 기본 최대 세그먼트 크기: 64MB

    public SegmentProperties { // compact 생성자: 바인딩 직후 누락된 값에 기본값을 채움
        if (maxBytes <= 0) { // 0 이하의 값은 설정 오류로 간주하고 기본값(64MB)으로 대체
            maxBytes = DEFAULT_MAX_BYTES;
        }
    }
}