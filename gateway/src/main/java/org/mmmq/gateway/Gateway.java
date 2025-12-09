package org.mmmq.gateway;

import org.mmmq.core.acknowledgement.Acknowledgement;
import org.mmmq.core.acknowledgement.GatewayAcknowledgement;
import org.mmmq.core.message.Message;
import org.mmmq.gateway.broker.Broker;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Gateway {

    final Broker broker;

    public Gateway(Broker broker) {
        this.broker = broker;
    }

    @PostMapping("/messages")
    public ResponseEntity<GatewayAcknowledgement> postMessage(@RequestBody Message message) {
        broker.push(message);
        return ResponseEntity.ok(new GatewayAcknowledgement(Acknowledgement.ACK));
    }
}
