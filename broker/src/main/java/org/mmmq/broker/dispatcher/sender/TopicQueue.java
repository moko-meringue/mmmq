//package org.mmmq.broker.dispatcher;
//
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.locks.Condition;
//import java.util.concurrent.locks.ReentrantLock;
//import org.mmmq.core.message.Message;
//import org.mmmq.core.message.Topic;
//
/// **
// * TopicQueue는 특정 토픽에 대한 메시지를 저장하는 큐입니다.
// * <p>
// * 이 클래스는 고성능 및 동시성 환경에 맞춰 설계되었습니다. 내부적으로 메시지를 'Segment'라는 청크 단위로 관리하여, 배열 복사를 최소화하고 동적으로 큐를 확장합니다.
// * <p>
// * 주요 동시성 제어 전략: 1. ReentrantLock: 메시지 추가(생산) 및 세그먼트 구조 변경 시 큐의 상태를 일관성 있게 유지하기 위해 사용됩니다. 2. Condition: 소비자가 새 메시지를
// * 효율적으로 기다릴 수 있도록 'guarded suspension' 패턴을 구현합니다. 3. AtomicInteger: 여러 구독자가 동시에 각 세그먼트를 읽고 있을 때, 락 경합 없이 '읽는 중인 구독자 수'를
// * 안전하게 추적하기 위해 사용됩니다. 4. volatile: 'head', 'tail', 'next', 'size' 필드에 사용하여 멀티 스레드 환경에서 항상 최신 값을 볼 수 있도록 가시성을 보장합니다.
// */
//public class TopicQueue {
//
//    // 각 세그먼트가 가질 수 있는 메시지의 최대 용량입니다.
//    // 이 값을 통해 메모리 사용량과 세그먼트 생성 빈도 사이의 균형을 조절할 수 있습니다.
//    static final int SEGMENT_CAPACITY = 1024;
//
//    // 이 큐가 담당하는 토픽을 나타냅니다. final로 선언되어 불변성을 보장합니다.
//    private final Topic topic;
//
//    // 큐의 전역적인 상태 변경(메시지 추가, 세그먼트 확장 등)을 보호하기 위한 락입니다.
//    private final ReentrantLock lock = new ReentrantLock();
//
//    // 큐에 새 메시지가 추가되었음을 소비자(Subscription)에게 알리고, 소비자가 메시지를 기다릴 때 사용하는 Condition 객체입니다.
//    private final Condition newMessage = lock.newCondition();
//
//    // 세그먼트 체인의 첫 번째 노드를 가리킵니다. volatile을 통해 멀티 스레드 가시성을 보장합니다.
//    // 이 포인터는 소비자가 읽고 지나간 세그먼트를 메모리에서 해제할 때 앞으로 이동합니다.
//    private volatile Segment head;
//
//    // 세그먼트 체인의 마지막 노드를 가리킵니다. 모든 새 메시지는 여기에 추가됩니다.
//    // volatile을 통해 멀티 스레드 가시성을 보장합니다.
//    private volatile Segment tail;
//
//    /**
//     * 지정된 토픽에 대한 TopicQueue를 생성합니다.
//     *
//     * @param topic 이 큐가 관리할 토픽
//     */
//    public TopicQueue(Topic topic) {
//        // 토픽을 설정합니다.
//        this.topic = topic;
//        // 초기 세그먼트를 생성합니다. head와 tail이 동일한 초기 세그먼트를 가리킵니다.
//        this.head = new Segment(SEGMENT_CAPACITY);
//        this.tail = head;
//    }
//
//    /**
//     * 이 큐의 토픽을 반환합니다.
//     *
//     * @return 큐의 토픽
//     */
//    public Topic topic() {
//        return topic;
//    }
//
//    /**
//     * 큐의 끝에 메시지를 추가합니다. (생산자 역할)
//     *
//     * @param message 추가할 메시지
//     */
//    public void add(Message message) {
//        // lock을 획득하여 다른 스레드가 큐의 구조를 변경하지 못하도록 합니다.
//        lock.lock();
//        try {
//            // 현재 tail 세그먼트가 가득 찼는지 확인합니다.
//            if (tail.isFull()) {
//                // 가득 찼다면 새로운 세그먼트를 생성합니다.
//                Segment newTail = new Segment(SEGMENT_CAPACITY);
//                // 현재 tail의 다음 노드로 새 세그먼트를 연결합니다.
//                tail.setNext(newTail);
//                // 큐의 tail 포인터를 새 세그먼트로 업데이트합니다.
//                tail = newTail;
//            }
//            // tail 세그먼트에 메시지를 추가합니다.
//            tail.put(message);
//            // 큐에 새 메시지가 추가되었음을 기다리고 있는 모든 소비자(Subscription)에게 알립니다.
//            newMessage.signalAll();
//        } finally {
//            // try-finally 블록을 사용하여 어떤 상황에서든 lock이 반드시 해제되도록 보장합니다.
//            lock.unlock();
//        }
//    }
//
//    /**
//     * 이 큐에 대한 새로운 구독(Subscription)을 생성하여 반환합니다. 각 구독은 큐의 메시지를 독립적으로 읽어가는 소비자 역할을 합니다.
//     *
//     * @return 생성된 Subscription 인스턴스
//     */
//    Subscription subscribe() {
//        return new Subscription();
//    }
//
//    /**
//     * 특정 세그먼트를 큐의 head에서 제거할지 시도합니다. (메모리 회수) 이 메서드는 해당 세그먼트를 더 이상 읽고 있는 구독자가 없을 때만 안전하게 head를 다음 세그먼트로 이동시킵니다.
//     *
//     * @param segmentToRemove 제거를 시도할 세그먼트 (일반적으로 구독자가 읽기를 마친 이전 세그먼트)
//     */
//    void tryAdvanceHead(Segment segmentToRemove) {
//        // 전역 lock을 획득하여 head 포인터 변경의 원자성을 보장합니다.
//        lock.lock();
//        try {
//            // 제거하려는 세그먼트가 현재 head이고, 해당 세그먼트를 읽는 구독자가 더 이상 없는지 확인합니다.
//            // 이 두 조건이 모두 충족되어야만 안전하게 head를 이동시킬 수 있습니다.
//            if (segmentToRemove == head && !segmentToRemove.hasActiveReaders()) {
//                // head를 다음 세그먼트로 이동시킵니다.
//                // 만약 다음 세그먼트가 없다면 head는 null이 되어 큐가 비어있음을 나타냅니다.
//                this.head = segmentToRemove.getNext();
//            }
//        } finally {
//            // lock을 반드시 해제합니다.
//            lock.unlock();
//        }
//    }
//
//    /**
//     * Segment는 메시지를 담는 고정 크기 배열 기반의 내부 저장소입니다. TopicQueue는 이러한 Segment들을 연결 리스트 형태로 관리합니다.
//     */
//    static class Segment {
//
//        // 메시지를 실제로 저장하는 배열입니다. final이므로 재할당될 수 없습니다.
//        private final Message[] data;
//
//        // 이 세그먼트를 현재 읽고 있는 구독자(Subscription)의 수를 추적하는 원자적 카운터입니다.
//        // lock 없이도 스레드-세이프하게 카운트를 조작하기 위해 AtomicInteger를 사용합니다.
//        private final AtomicInteger activeReaderCount = new AtomicInteger(0);
//
//        // 연결 리스트의 다음 세그먼트를 가리키는 포인터입니다.
//        // volatile을 사용하여 한 스레드에서 next 필드가 변경될 때 다른 스레드에서 즉시 볼 수 있도록 보장합니다.
//        private volatile Segment next = null;
//
//        // 이 세그먼트에 저장된 메시지의 현재 개수입니다.
//        // 이 값은 TopicQueue의 전역 lock 하에서만 변경되어야 하지만, 여러 스레드에서의 가시성을 위해 volatile로 선언됩니다.
//        private volatile int size = 0;
//
//        /**
//         * 지정된 용량으로 Segment를 생성합니다.
//         *
//         * @param capacity 이 세그먼트가 저장할 수 있는 메시지의 최대 개수
//         */
//        Segment(int capacity) {
//            this.data = new Message[capacity];
//        }
//
//        /**
//         * 이 세그먼트에 메시지를 추가합니다. 이 메서드는 TopicQueue.add() 내부에서 전역 lock을 획득한 상태에서만 호출되어야 합니다.
//         *
//         * @param message 추가할 메시지
//         */
//        void put(Message message) {
//            // 현재 size 위치에 메시지를 저장하고, 그 후에 size를 1 증가시킵니다.
//            this.data[size++] = message;
//        }
//
//        /**
//         * 세그먼트가 가득 찼는지 확인합니다.
//         *
//         * @return 가득 찼으면 true, 아니면 false
//         */
//        boolean isFull() {
//            return size >= data.length;
//        }
//
//        /**
//         * 지정된 인덱스의 메시지를 반환합니다.
//         *
//         * @param index 메시지의 인덱스
//         * @return 해당 인덱스의 메시지
//         */
//        Message getMessageAt(int index) {
//            return data[index];
//        }
//
//        /**
//         * 현재 세그먼트에 저장된 메시지의 개수를 반환합니다.
//         *
//         * @return 메시지 개수
//         */
//        int getSize() {
//            return size;
//        }
//
//        /**
//         * 다음 세그먼트를 반환합니다.
//         *
//         * @return 다음 Segment 객체
//         */
//        Segment getNext() {
//            return next;
//        }
//
//        /**
//         * 다음 세그먼트를 설정합니다. 이 메서드는 TopicQueue.add()에서 전역 lock 하에 호출됩니다.
//         *
//         * @param nextSegment 다음 세그먼트로 설정할 객체
//         */
//        void setNext(Segment nextSegment) {
//            this.next = nextSegment;
//        }
//
//        /**
//         * 현재 이 세그먼트를 읽고 있는 구독자가 있는지 확인합니다.
//         *
//         * @return 활성 구독자가 있으면 true, 없으면 false
//         */
//        boolean hasActiveReaders() {
//            return activeReaderCount.get() > 0;
//        }
//
//        /**
//         * 활성 구독자 수를 1 증가시킵니다. 구독자가 이 세그먼트에서 메시지 읽기를 시작할 때 호출됩니다.
//         */
//        void addActiveReader() {
//            activeReaderCount.incrementAndGet();
//        }
//
//        /**
//         * 활성 구독자 수를 1 감소시킵니다. 구독자가 이 세그먼트의 모든 메시지를 읽고 다음 세그먼트로 넘어갈 때 호출됩니다.
//         */
//        void removeActiveReader() {
//            activeReaderCount.decrementAndGet();
//        }
//    }
//
//    /**
//     * Subscription은 각 소비자가 TopicQueue로부터 메시지를 독립적으로 읽어갈 수 있도록 하는 '커서' 또는 '이터레이터'입니다. 각 Subscription은 자신이 어디까지 읽었는지
//     * (세그먼트와 오프셋)를 내부적으로 추적합니다.
//     */
//    class Subscription {
//
//        // 현재 읽고 있는 세그먼트 내에서의 위치(인덱스)를 나타내는 오프셋입니다.
//        private int offset;
//        // 현재 읽고 있는 세그먼트를 가리키는 참조입니다.
//        private Segment segment;
//
//        /**
//         * 새로운 Subscription을 생성합니다.
//         */
//        private Subscription() {
//            // 구독을 시작할 때의 큐의 head 세그먼트부터 읽기 시작합니다.
//            this.segment = head;
//            // 현재 세그먼트를 읽기 시작했음을 알리기 위해 활성 리더 카운트를 증가시킵니다.
//            this.segment.addActiveReader();
//        }
//
//        /**
//         * 큐에서 다음 메시지를 가져옵니다. 만약 메시지가 없다면 새로 추가될 때까지 대기(blocking)합니다.
//         *
//         * @return 다음 메시지
//         * @throws InterruptedException 대기 중에 스레드가 중단될 경우 발생
//         */
//        Message take() throws InterruptedException {
//            // 루프를 돌며 메시지를 성공적으로 가져올 때까지 시도합니다.
//            while (true) {
//                // TopicQueue의 전역 lock을 획득합니다. hasNext()와 await()의 원자성을 보장하기 위함입니다.
//                lock.lock();
//                try {
//                    // 읽을 메시지가 없을 경우, 새 메시지가 도착할 때까지 대기합니다.
//                    // while 루프를 사용하여 'spurious wakeup'(허위 각성)에 대응합니다.
//                    while (!hasNext()) {
//                        // newMessage Condition에서 대기 상태로 들어갑니다.
//                        // await()은 내부적으로 lock을 해제하고, signal을 받으면 다시 lock을 획득합니다.
//                        newMessage.await();
//                    }
//                } finally {
//                    // await() 도중 InterruptedException이 발생하더라도 lock은 해제되어야 합니다.
//                    lock.unlock();
//                }
//
//                // 대기 상태에서 깨어났거나 원래부터 메시지가 있었다면, 다음 메시지를 가져옵니다.
//                // 이 시점에는 lock이 해제된 상태이므로, 다른 구독자들이 동시에 next()를 호출할 수 있습니다.
//                Message msg = next();
//
//                // next() 메서드가 드물게 null을 반환할 수 있는 경쟁 상태(race condition)가 있을 수 있습니다.
//                // (예: hasNext()가 true를 반환한 직후 다른 스레드가 메시지를 소비하여 상태가 바뀐 경우)
//                // 메시지를 성공적으로 가져온 경우에만 루프를 종료하고 반환합니다.
//                if (msg != null) {
//                    return msg;
//                }
//            }
//        }
//
//        /**
//         * 현재 Subscription이 읽을 수 있는 메시지가 더 있는지 확인합니다. 이 메서드는 TopicQueue의 전역 lock을 획득한 상태에서 호출되어야 합니다.
//         *
//         * @return 읽을 메시지가 있으면 true, 없으면 false
//         */
//        private boolean hasNext() {
//            // 현재 세그먼트에서 아직 읽지 않은 메시지가 있는지 확인합니다.
//            // offset은 다음 읽을 위치, segment.getSize()는 현재까지 저장된 메시지 수입니다.
//            if (offset < segment.getSize()) {
//                return true;
//            }
//
//            // 현재 세그먼트를 다 읽었다면, 다음 세그먼트가 있는지 확인합니다.
//            Segment nextSegment = segment.getNext();
//            if (nextSegment != null) {
//                // 다음 세그먼트에 읽을 메시지가 있는지 확인합니다.
//                return nextSegment.getSize() > 0;
//            }
//
//            // 다음 세그먼트도 없고, 현재 세그먼트도 다 읽었으면 읽을 메시지가 없습니다.
//            return false;
//        }
//
//        /**
//         * 다음 메시지를 반환하고 내부 오프셋을 증가시킵니다. 필요하다면 다음 세그먼트로 자동으로 이동합니다. 이 메서드는 여러 구독자 스레드에 의해 동시에 호출될 수 있으며, 스레드로부터 안전해야
//         * 합니다.
//         *
//         * @return 다음 메시지. 경쟁 상태로 인해 메시지를 못 찾으면 null을 반환할 수 있습니다.
//         */
//        private Message next() {
//            // 현재 세그먼트의 끝에 도달했는지 확인합니다.
//            if (offset >= segment.getSize()) {
//                // 다음 세그먼트로 이동을 시도합니다.
//                // 성공적으로 이동했다면 true, 이동할 다음 세그먼트가 없다면 false를 반환합니다.
//                if (!advanceToNextSegment()) {
//                    // 이동 실패 시, 읽을 메시지가 없음을 의미합니다.
//                    return null;
//                }
//            }
//            // 현재 세그먼트의 현재 오프셋 위치에 있는 메시지를 가져옵니다.
//            // 그런 다음, 오프셋을 1 증가시켜 다음 호출을 준비합니다. (후위 증가)
//            return segment.getMessageAt(offset++);
//        }
//
//        /**
//         * 현재 세그먼트에서 다음 세그먼트로 구독의 포인터를 이동시킵니다.
//         *
//         * @return 이동에 성공하면 true, 이동할 다음 세그먼트가 없으면 false
//         */
//        private boolean advanceToNextSegment() {
//            // 현재 세그먼트의 다음 세그먼트를 가져옵니다.
//            Segment nextSegment = segment.getNext();
//
//            // 다음 세그먼트가 존재하지 않으면 이동할 수 없습니다.
//            if (nextSegment == null) {
//                return false;
//            }
//
//            // 이전 세그먼트를 참조하기 위해 현재 세그먼트를 oldSegment에 저장합니다.
//            Segment oldSegment = this.segment;
//
//            // 구독의 현재 세그먼트를 다음 세그먼트로 업데이트합니다.
//            this.segment = nextSegment;
//            // 새 세그먼트에서 읽기를 시작하므로, 오프셋을 0으로 초기화합니다.
//            this.offset = 0;
//            // 새 세그먼트에 대한 활성 리더 카운트를 증가시킵니다.
//            this.segment.addActiveReader();
//
//            // 이전 세그먼트에 대한 읽기를 마쳤으므로, 활성 리더 카운트를 감소시킵니다.
//            oldSegment.removeActiveReader();
//
//            // 이전 세그먼트를 더 이상 읽는 구독자가 없는지 확인합니다.
//            if (!oldSegment.hasActiveReaders()) {
//                // 만약 없다면, 큐의 head 포인터를 앞으로 이동시켜 메모리를 회수할 수 있는지 시도합니다.
//                // non-static inner class이므로 외부 클래스(TopicQueue)의 인스턴스 메서드를 호출할 수 있습니다.
//                TopicQueue.this.tryAdvanceHead(oldSegment);
//            }
//
//            // 세그먼트 이동에 성공했음을 알립니다.
//            return true;
//        }
//    }
//}
