package org.mmmq.broker.topicqueue;

import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.util.concurrent.locks.ReentrantLock;
import org.mmmq.broker.topicqueue.storage.CheckpointDirectory;
import org.mmmq.broker.topicqueue.storage.CheckpointFile;
import org.mmmq.broker.topicqueue.storage.CorruptionException;
import org.mmmq.broker.topicqueue.storage.SegmentFileChain;
import org.mmmq.broker.topicqueue.storage.StorageException;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicQueue implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TopicQueue.class);

    private final Topic topic;
    private final SegmentFileChain segmentFileChain;
    private final CheckpointDirectory checkpointDirectory;
    private final ReentrantLock writeLock = new ReentrantLock();

    public TopicQueue(Topic topic, SegmentFileChain segmentFileChain, CheckpointDirectory checkpointDirectory) {
        this.topic = topic;
        this.segmentFileChain = segmentFileChain;
        this.checkpointDirectory = checkpointDirectory;
    }

    public Offset subscribe(String name) {
        return new Offset(checkpointDirectory.register(name).read());
    }

    public boolean offer(Message message) {
        writeLock.lock();
        try {
            segmentFileChain.append(message);
            return true;
        } catch (StorageException exception) {
            log.error("Failed to persist message for topic {}", topic, exception);
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    @Nullable
    public Message peek(Offset offset) throws CorruptionException {
        return segmentFileChain.readAt(offset.value());
    }

    public Offset commit(String name, Offset offset) {
        CheckpointFile checkpointFile = checkpointDirectory.get(name);
        if (checkpointFile == null) {
            throw new IllegalStateException(
                    "Cannot commit: '" + name + "' has not subscribed topic '" + topic.name() + "'"
            );
        }
        Offset next = offset.next();
        checkpointFile.write(next.value());
        return next;
    }

    public Topic getTopic() {
        return topic;
    }

    @Override
    public void close() {
        try {
            segmentFileChain.close();
        } finally {
            checkpointDirectory.close();
        }
    }
}
