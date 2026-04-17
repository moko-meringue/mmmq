package org.mmmq.broker.wal.codec;

import jakarta.annotation.Nullable;
import org.mmmq.broker.wal.WalEntry;

public interface WalCodec {

    byte[] encode(WalEntry entry);

    @Nullable
    WalEntry decode(String line);
}
