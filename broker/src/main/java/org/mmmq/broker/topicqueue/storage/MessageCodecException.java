package org.mmmq.broker.topicqueue.storage;

public class MessageCodecException extends Exception { // 직/역직렬화 실패를 나타내는 checked 예외. checked로 선언해 호출부가 반드시 처리하도록 강제

    public MessageCodecException(String message, Throwable cause) { // 원인 예외를 보존해 스택 트레이스 유지
        super(message, cause);
    }
}
