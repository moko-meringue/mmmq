package org.mmmq.broker.wal;

import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

class TopicWalReader {

    private static final Pattern WAL_FILE_PATTERN = Pattern.compile("^(.+)-(\\d+)\\.wal$");

    private final WalReader walReader;

    TopicWalReader(WalReader walReader) {
        this.walReader = walReader;
    }

    Stream<WalEntry> stream(Path walDir, String topicName) {
        if (!Files.isDirectory(walDir)) {
            return Stream.empty();
        }
        List<Path> sortedPaths = collectSortedPaths(walDir, topicName);
        List<Stream<WalEntry>> openedStreams = new ArrayList<>();

        Stream<WalEntry> merged = sortedPaths.stream()
                .flatMap(path -> {
                    Stream<WalEntry> inner = walReader.stream(path);
                    openedStreams.add(inner);
                    return inner;
                });

        return merged.onClose(() -> openedStreams.forEach(Stream::close));
    }

    Stream<String> topicNames(Path walDir) {
        try (Stream<Path> files = Files.list(walDir)) {
            return files
                    .map(path -> path.getFileName().toString())
                    .map(WAL_FILE_PATTERN::matcher)
                    .filter(Matcher::matches)
                    .map(matcher -> matcher.group(1))
                    .distinct()
                    .toList()
                    .stream();
        } catch (IOException exception) {
            throw new RuntimeException("Failed to list WAL directory: " + walDir, exception);
        }
    }

    private List<Path> collectSortedPaths(Path walDir, String topicName) {
        try (Stream<Path> files = Files.list(walDir)) {
            return files
                    .map(path -> parse(path, topicName))
                    .filter(Objects::nonNull)
                    .sorted(Comparator.comparingInt(ParsedWalFile::segmentIndex))
                    .map(ParsedWalFile::path)
                    .toList();
        } catch (IOException exception) {
            throw new RuntimeException("Failed to list WAL directory: " + walDir, exception);
        }
    }

    @Nullable
    private ParsedWalFile parse(Path path, String topicName) {
        Matcher matcher = WAL_FILE_PATTERN.matcher(path.getFileName().toString());
        if (!matcher.matches() || !matcher.group(1).equals(topicName)) {
            return null;
        }

        return new ParsedWalFile(path, Integer.parseInt(matcher.group(2)));
    }

    private record ParsedWalFile(
            Path path,
            int segmentIndex
    ) {
    }
}
