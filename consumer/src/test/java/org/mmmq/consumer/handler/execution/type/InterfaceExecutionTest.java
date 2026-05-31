package org.mmmq.consumer.handler.execution.type;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

import static org.assertj.core.api.Assertions.assertThat;

class InterfaceExecutionTest {

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("id는 MMMQListener의 id() 반환값과 같다.")
    void idReturnsListenerId() {
        SampleListener sampleListener = new SampleListener();
        InterfaceExecution interfaceExecution = new InterfaceExecution(sampleListener, objectMapper);

        assertThat(interfaceExecution.id()).isEqualTo("order-new");
    }

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
        public String id() {
            return "order-new";
        }

        @Override
        public void handle(SampleDto content) {
            calledCount++;
            receivedDto = content;
        }
    }
}
