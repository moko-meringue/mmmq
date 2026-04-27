package org.mmmq.broker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mmmq.broker.dispatcher.FrontDispatcher;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, // 포트 충돌 방지를 위해 랜덤 포트 사용
        classes = BrokerTest.TestConfiguration.class
)
class BrokerTest {

    @TempDir
    static Path tempDataDir; // 테스트 격리를 위한 임시 데이터 디렉토리. @DynamicPropertySource에서 dataDir로 등록

    @LocalServerPort
    int port; // 랜덤 포트 번호를 RestAssured에 주입하기 위해 필드 바인딩

    @Autowired
    ObjectMapper objectMapper; // JSON 직렬화를 위해 Spring 컨텍스트의 ObjectMapper 재사용

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("mmmq.broker.storage.dataDir", () -> tempDataDir.toAbsolutePath().toString()); // 테스트마다 격리된 임시 디렉토리를 data/ 루트로 설정해 다른 테스트와 파일 공유 방지
    }

    @BeforeEach
    void setup() {
        RestAssured.port = port; // 랜덤 포트를 RestAssured 기본 포트로 설정
    }

    @Test
    @DisplayName("Manager는 외부로부터 메시지를 받을 수 있다.")
    void receiveMessageTest() throws JsonProcessingException {
        final Message message = new Message(new Topic("topic"), Map.of("key", "value"));
        RestAssured.given().log().all()
                .body(objectMapper.writeValueAsString(message)) // Message를 JSON 문자열로 직렬화
                .contentType("application/json")
                .when().log().all()
                .post("/messages") // POST /messages 호출
                .then().log().all()
                .statusCode(200); // 메시지 저장 성공 시 200 응답 검증
    }

    @Test
    @DisplayName("Manager는 전달받은 메시지를 브로커에게 전달할 수 있다.")
    void forwardToBrokerTest() {
        final Message message = new Message(new Topic("topic"), Map.of("key", "value"));
        final FrontDispatcher frontDispatcher = mock(FrontDispatcher.class); // 실제 디스크 I/O 없이 dispatch 호출 여부만 검증
        final Broker broker = new Broker(frontDispatcher);

        broker.postMessage(message); // HTTP 계층 없이 직접 postMessage 호출

        verify(frontDispatcher).dispatch(message); // postMessage가 frontDispatcher.dispatch를 올바르게 위임하는지 확인
    }

    @Configuration
    @EnableAutoConfiguration // broker 자동 설정을 활성화해 실제 컨텍스트와 동일한 환경 구성
    public static class TestConfiguration {

    }
}
