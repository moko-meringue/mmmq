package org.mmmq.broker.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Stream;
import org.mmmq.broker.wal.codec.WalCodec;

class WalReader {

    private final WalCodec walCodec;

    WalReader(WalCodec walCodec) {
        this.walCodec = walCodec;
    }

    Stream<WalEntry> stream(Path walFile) {
        if (!Files.exists(walFile)) {
            return Stream.empty();
        }
        try {
            Stream<String> lines = Files.lines(walFile);
            return lines
                    .map(walCodec::decode)
                    .filter(Objects::nonNull)
                    .onClose(lines::close);
        } catch (IOException exception) {
            throw new RuntimeException("Failed to open WAL file: " + walFile, exception);
        }
    }
}
