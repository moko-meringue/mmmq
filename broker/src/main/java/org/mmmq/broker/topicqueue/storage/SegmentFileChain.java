package org.mmmq.broker.topicqueue.storage;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentSkipListMap;
import org.mmmq.core.annotation.Nullable;
import org.mmmq.core.message.Message;

public class SegmentFileChain implements Closeable {

    private final Path path;
    private final long rotationThreshold;
    private final ConcurrentSkipListMap<Long, SegmentFile> segmentsByStartOffset = new ConcurrentSkipListMap<>();

    private SegmentFileChain(Path path, long rotationThreshold) {
        this.path = path;
        this.rotationThreshold = rotationThreshold;
    }

    public static SegmentFileChain open(Path base, long rotationThreshold) {
        SegmentFileChain chain = new SegmentFileChain(base, rotationThreshold);
        chain.bootstrap();
        return chain;
    }

    private void bootstrap() {
        SegmentFile.openAll(path)
                .forEach(segmentFile -> segmentsByStartOffset.put(segmentFile.startOffset(), segmentFile));
        if (segmentsByStartOffset.isEmpty()) {
            segmentsByStartOffset.put(0L, SegmentFile.open(path, 0L));
        }
    }

    public void append(Message message) {
        SegmentFile tailSegmentFile = segmentsByStartOffset.lastEntry().getValue();
        if (tailSegmentFile.reaches(rotationThreshold)) {
            long nextOffset = tailSegmentFile.startOffset() + tailSegmentFile.count();
            tailSegmentFile = SegmentFile.open(path, nextOffset);
            segmentsByStartOffset.put(nextOffset, tailSegmentFile);
        }
        tailSegmentFile.append(message);
    }

    @Nullable
    public Message readAt(long absoluteOffset) throws CorruptionException {
        Long startOffset = segmentsByStartOffset.floorKey(absoluteOffset);
        if (startOffset == null) {
            return null;
        }
        SegmentFile segmentFile = segmentsByStartOffset.get(startOffset);
        return segmentFile.readAt(absoluteOffset - segmentFile.startOffset());
    }

    @Override
    public void close() {
        segmentsByStartOffset.values().forEach(SegmentFile::close);
    }
}
