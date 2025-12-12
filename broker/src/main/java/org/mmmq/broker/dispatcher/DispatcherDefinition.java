package org.mmmq.broker.dispatcher;

import org.mmmq.broker.dispatcher.dlq.DeadLetterQueue;
import org.mmmq.core.Host;
import org.mmmq.core.message.Topic;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DispatcherDefinition {

    final String name;
    final Host host;
    final Set<Topic> topics;
    final ThreadPoolExecutor threadPool;

    private DispatcherDefinition(
            String name,
            Host host,
            Set<Topic> topics,
            ThreadPoolExecutor threadPool
    ) {
        this.name = name;
        this.host = host;
        this.topics = topics;
        this.threadPool = threadPool;
    }

    public static Builder builder(String name, String webProtocol, String hostName, int port) {
        return new Builder(name, webProtocol, hostName, port);
    }

    public Dispatcher toDispatcher(DeadLetterQueue deadLetterQueue) {
        return new Dispatcher(name, host, topics, threadPool, deadLetterQueue);
    }

    public static class Builder {

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

        public Builder threadPool(ThreadPoolExecutor executor) {
            threadPool = executor;
            return this;
        }

        public DispatcherDefinition build() {
            return new DispatcherDefinition(
                    name,
                    host,
                    subscribed,
                    threadPool
            );
        }
    }
}
