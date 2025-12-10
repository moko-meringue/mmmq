package org.mmmq.consumer.handler.execution.method;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;

import com.fasterxml.jackson.databind.ObjectMapper;

class MethodExecutionTest {

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("메시지를 핸들링한다.")
    void executeTest() throws NoSuchMethodException {
        Topic topic = new Topic("topic");
        Target target = new Target();
        Method targetMethod = Target.class.getMethod("method", Dto.class);
        MethodExecution methodExecution = new MethodExecution(topic, target, targetMethod, objectMapper);
        methodExecution.execute(
            new Message(
                new Topic("topic"),
                Map.of("field", "value")
            ));

        assertThat(target.calledCount).isEqualTo(1);
    }

    static class Target {
        int calledCount = 0;

        public void method(Dto dto) {
            calledCount++;
        }
    }

    record Dto(
        String field
    ) {
    }
}
