package org.mmmq.broker.topicqueue.storage;

public class StorageException extends RuntimeException { // IOException 등 checked 예외를 unchecked로 래핑해 호출부 코드를 단순하게 유지

    public StorageException(String message) { // 메시지만 있는 경우: 원인 예외 없이 문자열 메시지만 전달
        super(message);
    }

    public StorageException(String message, Throwable cause) { // IOException을 래핑할 때 사용: 원본 예외를 cause로 보존해 스택 트레이스가 유지됨
        super(message, cause);
    }
}
