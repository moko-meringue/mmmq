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
public class Broker {

    final FrontDispatcher frontDispatcher;

    public Broker(FrontDispatcher frontDispatcher) {
        this.frontDispatcher = frontDispatcher;
    }

    @PostMapping("/messages")
    public ResponseEntity<BrokerAcknowledgement> postMessage(@RequestBody Message message) {
        if (frontDispatcher.dispatch(message)) {
            return ResponseEntity.ok(new BrokerAcknowledgement(Acknowledgement.ACK));
        }
        return ResponseEntity.ok(new BrokerAcknowledgement(Acknowledgement.NACK));
    }
}
