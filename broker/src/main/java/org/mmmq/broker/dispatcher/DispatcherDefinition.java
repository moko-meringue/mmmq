package org.mmmq.broker.dispatcher;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.mmmq.broker.dispatcher.dlq.CounterDeadLetterQueue;
import org.mmmq.core.Host;
import org.mmmq.core.message.Topic;

public class DispatcherDefinition {

    final String name;
    final Host host;
    final Set<Topic> topics;
    final ThreadPoolExecutor threadPool;
    final int capacity;
    final Path deadLetterPath;

    private DispatcherDefinition(
        String name,
        Host host,
        Set<Topic> topics,
        ThreadPoolExecutor threadPool,
        int capacity,
        Path deadLetterPath
    ) {
        this.name = name;
        this.host = host;
        this.topics = topics;
        this.threadPool = threadPool;
        this.capacity = capacity;
        this.deadLetterPath = deadLetterPath;
    }

    public static Builder builder(
        String name,
        String webProtocol,
        String hostName,
        int port
    ) {
        return new Builder(name, webProtocol, hostName, port);
    }

    public Dispatcher toDispatcher() {
        return new Dispatcher(
            name,
            host,
            topics,
            threadPool,
            new CounterDeadLetterQueue(name, capacity, deadLetterPath)
        );
    }

    public static class Builder {

        private static final int DEFAULT_CAPACITY = 10;
        private static final ThreadPoolExecutor DEFAULT_THREAD_POOL = new ThreadPoolExecutor(
            2,
            5,
            40L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()
        );

        private final String name;
        private final Host host;
        private final Set<Topic> subscribed = new HashSet<>();
        private ThreadPoolExecutor threadPool = DEFAULT_THREAD_POOL;
        private int capacity = DEFAULT_CAPACITY;
        private Path deadLetterPath;

        private Builder(String name, String webProtocol, String hostName, int port) {
            this.name = name;
            this.host = new Host(webProtocol, hostName, port);
        }

        public Builder withTopics(String... topics) {
            for (String topic : topics) {
                subscribed.add(new Topic(topic));
            }
            return this;
        }

        public Builder threadPool(ThreadPoolExecutor threadPool) {
            this.threadPool = threadPool;
            return this;
        }

        public Builder capacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder deadLetterPath(Path deadLetterPath) {
            this.deadLetterPath = deadLetterPath;
            return this;
        }

        public DispatcherDefinition build() {
            return new DispatcherDefinition(
                name,
                host,
                subscribed,
                threadPool,
                capacity,
                deadLetterPath
            );
        }
    }
}
