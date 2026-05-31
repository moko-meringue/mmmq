package org.mmmq.consumer.handler.execution.method;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class MethodExecutionTest {

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("메시지를 핸들링한다.")
    void executeTest() throws NoSuchMethodException {
        Target target = new Target();
        Method targetMethod = Target.class.getMethod("method", Dto.class);
        MethodExecution methodExecution = new MethodExecution("handler-1", target, targetMethod, objectMapper);

        methodExecution.execute(
                new Message(
                        new Topic("topic"),
                        new Dto("value")
                ));

        assertThat(target.calledCount).isEqualTo(1);
    }

    @Test
    @DisplayName("id는 생성자 인자로 받은 값을 그대로 반환한다.")
    void idReturnsConstructorArgument() throws NoSuchMethodException {
        Target target = new Target();
        Method targetMethod = Target.class.getMethod("method", Dto.class);
        MethodExecution methodExecution = new MethodExecution("handler-1", target, targetMethod, objectMapper);

        assertThat(methodExecution.id()).isEqualTo("handler-1");
    }

    @Test
    @DisplayName("content가 null이면 핸들러에 null을 전달한다")
    void executeWithNullContent() throws NoSuchMethodException {
        Target target = new Target();
        Method targetMethod = Target.class.getMethod("method", Dto.class);
        MethodExecution methodExecution = new MethodExecution("handler-1", target, targetMethod, objectMapper);

        methodExecution.execute(new Message(new Topic("topic"), null));

        assertThat(target.calledCount).isEqualTo(1);
        assertThat(target.receivedDto).isNull();
    }

    static class Target {

        int calledCount = 0;
        Dto receivedDto;

        public void method(Dto dto) {
            calledCount++;
            receivedDto = dto;
        }
    }

    record Dto(
            String field
    ) {
    }
}
