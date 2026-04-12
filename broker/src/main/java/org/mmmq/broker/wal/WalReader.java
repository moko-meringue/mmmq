package org.mmmq.broker.wal;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WalReader {

    private static final Logger log = LoggerFactory.getLogger(WalReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public List<WalEntry> read(Path walFile) {
        if (!Files.exists(walFile)) {
            return Collections.emptyList();
        }
        try (Stream<String> lines = Files.lines(walFile)) {
            return lines
                    .filter(line -> !line.isBlank())
                    .flatMap(this::deserialize)
                    .toList();
        } catch (IOException exception) {
            throw new RuntimeException("Failed to read WAL file: " + walFile, exception);
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
