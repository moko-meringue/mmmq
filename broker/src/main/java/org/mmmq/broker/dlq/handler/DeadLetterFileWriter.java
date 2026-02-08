package org.mmmq.broker.dlq.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mmmq.broker.dlq.DeadLetter;
import org.mmmq.broker.dlq.exception.DLQInitializeException;
import org.mmmq.broker.dlq.handler.exception.DeadLetterHandlingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.stream.Collectors;

public class DeadLetterFileWriter implements DeadLetterHandler {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterFileWriter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    final Path target;
    final String name;

    public DeadLetterFileWriter(Path base, String name) {
        try {
            Files.createDirectories(base);
            this.target = base.resolve(String.format("dead-letters-%s.json", name));
            this.name = name;
        } catch (IOException e) {
            throw new DLQInitializeException("Failed to initialize DeadLetterFileWriter", e);
        }
    }

    @Override
    public void handle(Collection<DeadLetter> deadLetters) {
        try {
            if (!deadLetters.isEmpty()) {
                write(serialize(deadLetters));
            }
        } catch (Exception e) {
            log.error("Failed to write dead letters to file", e);
        }
    }

    protected String serialize(Collection<DeadLetter> deadLetters) {
        return deadLetters.stream()
                .map(this::writeValueAsJson)
                .collect(Collectors.joining(System.lineSeparator()));
    }

    private String writeValueAsJson(Object object) {
        try {
            return MAPPER.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new DeadLetterHandlingException("Failed to serialize object to JSON", e);
        }
    }

    private void write(String content) throws IOException {
        Files.writeString(
                target,
                content + System.lineSeparator(),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE
        );
    }
}
