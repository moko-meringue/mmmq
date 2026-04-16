package org.mmmq.broker.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WalReader {

    private static final Logger log = LoggerFactory.getLogger(WalReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Stream을 반환하여 호출자가 필요한 만큼만 읽도록 한다.
    // onClose로 파일 채널을 등록하므로, 호출자가 try-with-resources 또는
    // flatMap(자동 close)으로 소비하면 파일 핸들이 누수 없이 닫힌다.
    public Stream<WalEntry> stream(Path walFile) {
        if (!Files.exists(walFile)) {
            return Stream.empty();
        }
        try {
            Stream<String> lines = Files.lines(walFile);
            return lines
                    .filter(line -> !line.isBlank())
                    .flatMap(this::deserialize)
                    .onClose(lines::close);
        } catch (IOException exception) {
            throw new RuntimeException("Failed to open WAL file: " + walFile, exception);
        }
    }

    private Stream<WalEntry> deserialize(String line) {
        try {
            return Stream.of(MAPPER.readValue(line, WalEntry.class));
        } catch (IOException exception) {
            log.warn("Skipping unreadable WAL line: {}", exception.getMessage());
            return Stream.empty();
        }
    }
}
