package org.mmmq.core.acknowledgement;

public record BrokerAcknowledgement(
        Acknowledgement acknowledgement
) {

    public boolean isAck() {
        return this.acknowledgement == Acknowledgement.ACK;
    }
}
