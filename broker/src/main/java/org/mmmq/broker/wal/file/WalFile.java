package org.mmmq.broker.wal.file;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.mmmq.broker.wal.WalEntry;
import org.mmmq.broker.wal.codec.WalCodec;

public class WalFile {

    private final Path path;
    private final int index;

    private WalFile(Path path, int index) {
        this.path = path;
        this.index = index;
    }

    static WalFile of(Path walDirectory, String topicName, int index) {
        return new WalFile(walDirectory.resolve(Name.of(topicName, index)), index);
    }

    static Path pathOf(String topicName, int index) {
        return Path.of(Name.of(topicName, index));
    }

    @Nullable
    static WalFile parse(Path filePath) {
        Integer index = Name.parseIndex(filePath.getFileName());
        if (index == null) {
            return null;
        }

        return new WalFile(filePath, index);
    }

    public int index() {
        return index;
    }

    public Stream<WalEntry> read(WalCodec codec) {
        try {
            Stream<String> lines = Files.lines(path);
            return lines
                    .map(codec::decode)
                    .filter(Objects::nonNull)
                    .onClose(lines::close);
        } catch (NoSuchFileException ignored) {
            return Stream.empty();
        } catch (IOException exception) {
            throw new RuntimeException("Failed to read WAL segment file: " + path, exception);
        }
    }

    public FileChannel openAppendChannel() {
        try {
            return FileChannel.open(
                    path,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.CREATE
            );
        } catch (IOException exception) {
            throw new RuntimeException("Failed to open WAL segment channel: " + path, exception);
        }
    }

    private static class Name {

        private static final String SUFFIX = ".wal";
        private static final String SEPARATOR = "-";
        private static final Pattern PATTERN =
                Pattern.compile("^.+" + SEPARATOR + "(\\d+)" + Pattern.quote(SUFFIX) + "$");

        static String of(String topicName, int index) {
            return topicName + SEPARATOR + index + SUFFIX;
        }

        @Nullable
        static Integer parseIndex(Path fileName) {
            Matcher matcher = PATTERN.matcher(fileName.toString());
            if (!matcher.matches()) {
                return null;
            }

            return Integer.parseInt(matcher.group(1));
        }
    }
}
