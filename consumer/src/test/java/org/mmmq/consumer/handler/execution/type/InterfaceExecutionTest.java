package org.mmmq.consumer.handler.execution.type;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.mmmq.core.message.TopicPattern;

class InterfaceExecutionTest {

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("content가 null이면 인터페이스 핸들러에 null을 전달한다")
    void executeWithNullContent() {
        SampleListener sampleListener = new SampleListener();
        InterfaceExecution interfaceExecution = new InterfaceExecution(sampleListener, objectMapper);

        interfaceExecution.execute(new Message(new Topic("order.new"), null));

        assertThat(sampleListener.calledCount).isEqualTo(1);
        assertThat(sampleListener.receivedDto).isNull();
    }

    static class SampleDto {
        String name;
    }

    static class SampleListener implements MMMQListener<SampleDto> {

        int calledCount = 0;
        SampleDto receivedDto;

        @Override
        public TopicPattern listens() {
            return new TopicPattern("order.new");
        }

        @Override
        public void handle(SampleDto content) {
            calledCount++;
            receivedDto = content;
        }
    }
}
