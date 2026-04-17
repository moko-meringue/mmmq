package org.mmmq.broker.wal;

import org.mmmq.core.message.Message;

public record WalEntry(
        int segmentIndex,
        Message message
) {

}
