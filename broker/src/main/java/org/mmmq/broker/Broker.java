package org.mmmq.broker;

import org.mmmq.broker.dispatcher.FrontDispatcher;
import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.BrokerAcknowledgement;
import org.mmmq.core.message.Message;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Broker { // POST /messages 엔드포인트. 메시지를 수신해 FrontDispatcher에 전달하고 결과에 따라 ACK/NACK를 반환

    final FrontDispatcher frontDispatcher; // 메시지를 TopicQueue에 저장하고 Dispatcher에 이벤트를 전파하는 진입점

    public Broker(FrontDispatcher frontDispatcher) {
        this.frontDispatcher = frontDispatcher;
    }

    @PostMapping("/messages")
    public ResponseEntity<BrokerAcknowledgement> postMessage(@RequestBody Message message) {
        final boolean persisted = frontDispatcher.dispatch(message); // 디스크 fsync 완료 시 true, IOException 발생 시 false
        final Acknowledgement acknowledgement = persisted ? Acknowledgement.ACK : Acknowledgement.NACK; // true → ACK(Producer가 재시도 불필요), false → NACK(Producer가 재시도)

        return ResponseEntity.ok(new BrokerAcknowledgement(acknowledgement));
    }
}
