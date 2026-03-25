package org.mmmq.broker.dispatcher;

import java.util.List;
import org.mmmq.core.message.Message;
import org.mmmq.core.message.Topic;
import org.springframework.context.ApplicationEventPublisher;

public class TopicQueue {

    private final Topic topic;
    private final SegmentChain segmentChain;
    private final ApplicationEventPublisher publisher;

    public TopicQueue(Topic topic, ApplicationEventPublisher publisher) {
        this.topic = topic;
        this.segmentChain = new SegmentChain();
        this.publisher = publisher;
    }

    void assignWorkers(List<Dispatcher> dispatchers) {
        dispatchers.forEach(dispatcher -> dispatcher.startWorkerFor(this));
    }

    public void add(Message message) {
        segmentChain.add(message);
        publisher.publishEvent(new MessageArrivedEvent(this));
    }

    public Cursor subscribe() {
        return segmentChain.createAndRegisterCursor();
    }

    public boolean hasNext(Cursor cursor) {
        return segmentChain.hasNext(cursor);
    }

    public Message poll(Cursor cursor) {
        return segmentChain.getMessageAt(cursor);
    }

    public Topic getTopic() {
        return this.topic;
    }
}
