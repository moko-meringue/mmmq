package org.mmmq.broker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("mmmq.broker.storage") // application.yml에서 mmmq.broker.storage.* 키를 이 레코드에 바인딩
public record StorageProperties(
        String rootDir // 세그먼트/인덱스/체크포인트 등 모든 영속 자료의 루트 디렉토리 경로
) {

    private static final String DEFAULT_ROOT_DIR = "./data"; // rootDir 미설정 시 사용하는 기본 경로

    public StorageProperties { // compact 생성자: 바인딩 직후 누락된 값에 기본값을 채움
        if (rootDir == null || rootDir.isBlank()) {
            rootDir = DEFAULT_ROOT_DIR;
        }
    }
}