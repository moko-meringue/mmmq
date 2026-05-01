package org.mmmq.broker.topicqueue;

public record TopicQueueInitializedEvent( // Registry에 새 TopicQueue가 등록되면 발행 (lazy 생성/부팅 복원 모두). listener가 패턴 매칭해 자체 구독
        TopicQueue topicQueue
) {

}