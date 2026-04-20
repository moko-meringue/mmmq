package org.mmmq.broker.wal.codec;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.mmmq.broker.wal.WalEntry;

@Component
public class JsonWalCodec implements WalCodec {

    private static final Logger log = LoggerFactory.getLogger(JsonWalCodec.class);

    private final ObjectMapper mapper;

    public JsonWalCodec() {
        this.mapper = new ObjectMapper();
    }

    @Override
    public byte[] encode(WalEntry entry) {
        try {
            return mapper.writeValueAsBytes(entry);
        } catch (JsonProcessingException exception) {
            throw new RuntimeException(
                    "Failed to encode WAL entry for topic: " + entry.message().topic().name(),
                    exception
            );
        }
    }

    @Nullable
    @Override
    public WalEntry decode(String line) {
        if (line.isBlank()) {
            return null;
        }
        try {
            return mapper.readValue(line, WalEntry.class);
        } catch (IOException exception) {
            log.warn("Skipping unreadable WAL line: {}", exception.getMessage());
            return null;
        }
    }
}
