package com.gs.photo.common.workflow.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.TimeMeasurement;

public class KafkaUtils {

    protected static Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

    public static interface PreCommitAction extends java.util.function.Consumer<Integer> {}

    public static interface BeforeProcessBatchAction<T> extends java.util.function.BiConsumer<Integer, T> {}

    static public interface GetPartition<T> extends Function<T, Integer> {}

    static public interface GetTopic<T> extends Function<T, String> {}

    static public interface GetKafkaOffset<T> extends Function<T, Long> {}

    public static class ConsumerRecordsSpliteratorPerTopicData<K, V> {
        private List<ConsumerRecord<K, V>> consumerRecordList;
        private int                        index;
        private int                        first;
        private int                        last;
        private long                       size;
        private int                        characteristics;
        private int                        batchSize;
        private TopicPartition             currentTopicPartition;

        public ConsumerRecordsSpliteratorPerTopicData() {}

        public List<ConsumerRecord<K, V>> getConsumerRecordList() { return this.consumerRecordList; }

        public void setConsumerRecordList(List<ConsumerRecord<K, V>> consumerRecordList) {
            this.consumerRecordList = consumerRecordList;
        }

        public int getIndex() { return this.index; }

        public void setIndex(int index) { this.index = index; }

        public int getFirst() { return this.first; }

        public void setFirst(int first) { this.first = first; }

        public int getLast() { return this.last; }

        public void setLast(int last) { this.last = last; }

        public long getSize() { return this.size; }

        public void setSize(long size) { this.size = size; }

        public int getCharacteristics() { return this.characteristics; }

        public void setCharacteristics(int characteristics) { this.characteristics = characteristics; }

        public int getBatchSize() { return this.batchSize; }

        public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

        public TopicPartition getCurrentTopicPartition() { return this.currentTopicPartition; }

        public void setCurrentTopicPartition(TopicPartition currentTopicPartition) {
            this.currentTopicPartition = currentTopicPartition;
        }
    }

    protected static class ConsumerRecordsSpliteratorPerTopic<K, V> implements Spliterator<ConsumerRecord<K, V>> {
        protected ConsumerRecordsSpliteratorPerTopicData<K, V> data = new ConsumerRecordsSpliteratorPerTopicData<>();

        @Override
        public String toString() {
            return "ConsumerRecordsSpliteratorPerTopic [index=" + this.data.getIndex() + ", first="
                + this.data.getFirst() + ", last=" + this.data.getLast() + "]";
        }

        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super ConsumerRecord<K, V>> action) {
            return Optional.ofNullable(action)
                .map(a -> {
                    if ((this.data.getIndex() >= this.data.getFirst())
                        && (this.data.getIndex() < this.data.getLast())) {
                        final ConsumerRecord<K, V> currentRecord = this.data.getConsumerRecordList()
                            .get(this.data.getIndex());
                        action.accept(currentRecord);
                        this.data.setIndex(this.data.getIndex() + 1);
                        return true;
                    } else {
                        return false;
                    }
                })
                .orElseThrow();
        }

        @Override
        public Spliterator<ConsumerRecord<K, V>> trySplit() {
            synchronized (this) {

                if ((this.data.getSize() == Long.MAX_VALUE) && (this.data.getLast() < this.data.getConsumerRecordList()
                    .size())) {
                    final ConsumerRecordsSpliteratorPerTopic<K, V> consumerRecordsSpliteratorPerTopic = new ConsumerRecordsSpliteratorPerTopic<>(
                        this.data.getConsumerRecordList(),
                        this.data.getFirst(),
                        this.data.getBatchSize(),
                        this.data.getBatchSize(),
                        this.data.getCurrentTopicPartition());

                    this.data.setFirst(this.data.getFirst() + this.data.getBatchSize());
                    this.data.setLast(
                        Math.min(
                            this.data.getFirst() + this.data.getBatchSize(),
                            this.data.getConsumerRecordList()
                                .size()));
                    this.data.setIndex(this.data.getFirst());
                    return consumerRecordsSpliteratorPerTopic;
                }
                return null;
            }
        }

        @Override
        public void forEachRemaining(java.util.function.Consumer<? super ConsumerRecord<K, V>> action) {
            if (action == null) { throw new NullPointerException(); }
            while (this.data.getIndex() < this.data.getLast()) {
                final ConsumerRecord<K, V> currentRecord = this.data.getConsumerRecordList()
                    .get(this.data.getIndex());
                action.accept(currentRecord);
                this.data.setIndex(this.data.getIndex() + 1);
            }
        }

        @Override
        public long estimateSize() { return this.data.getSize(); }

        @Override
        public int characteristics() { return this.data.getCharacteristics(); }

        public ConsumerRecordsSpliteratorPerTopic(
            List<ConsumerRecord<K, V>> consumerRecordList,
            int first,
            long size,
            int batchSize,
            TopicPartition currentTopicPartition
        ) {
            super();
            this.data.setConsumerRecordList(consumerRecordList);
            this.data.setIndex(first);
            this.data.setFirst(first);
            this.data.setLast(Math.min(first + batchSize, consumerRecordList.size()));
            this.data.setCharacteristics(Spliterator.SIZED);
            this.data.setSize(size);
            this.data.setBatchSize(batchSize);
            this.data.setCurrentTopicPartition(currentTopicPartition);
        }
    }

    protected static class ConsumerAllRecordsSpliterator<K, V> implements Spliterator<ConsumerRecord<K, V>> {
        protected final ConsumerRecords<K, V> nextRecords;
        protected final List<TopicPartition>  topics;
        protected int                         currentTopicPartition;
        private final int                     characteristics;
        private final int                     batchSize;

        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super ConsumerRecord<K, V>> action) { return false; }

        @Override
        public Spliterator<ConsumerRecord<K, V>> trySplit() {
            synchronized (this) {
                if (this.currentTopicPartition < this.topics.size()) {
                    final List<ConsumerRecord<K, V>> records = this.nextRecords
                        .records(this.topics.get(this.currentTopicPartition));
                    TopicPartition currentTopicPartition = this.topics.get(this.currentTopicPartition);
                    this.currentTopicPartition++;
                    return new ConsumerRecordsSpliteratorPerTopic<>(new ArrayList<>(
                        records), 0, Long.MAX_VALUE, this.batchSize, currentTopicPartition);
                }
            }
            return null;
        }

        @Override
        public long estimateSize() { return Long.MAX_VALUE; }

        @Override
        public int characteristics() { return this.characteristics; }

        public ConsumerAllRecordsSpliterator(
            ConsumerRecords<K, V> nextRecords,
            int batchSize
        ) {
            super();
            this.nextRecords = nextRecords;
            this.topics = new ArrayList<>(nextRecords.partitions());
            this.characteristics = Spliterator.IMMUTABLE;
            this.batchSize = batchSize;
        }

    }

    protected static class ConsumerRecordsSpliteratorPerBatch<K, V>
        implements Spliterator<Collection<ConsumerRecord<K, V>>> {
        static Object                                          mutex = new Object();

        protected ConsumerRecordsSpliteratorPerTopicData<K, V> data  = new ConsumerRecordsSpliteratorPerTopicData<>();

        @Override
        public String toString() {
            return super.toString() + " [index=" + this.data.getIndex() + ", first=" + this.data.getFirst() + ", last="
                + this.data.getLast() + ", batchSize=" + this.data.getBatchSize() + "]";
        }

        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super Collection<ConsumerRecord<K, V>>> action) {
            return Optional.ofNullable(action)
                .map(a -> {
                    if (this.data.getIndex() < this.data.getConsumerRecordList()
                        .size()) {
                        System.out.println(
                            this + " size is " + this.data.getConsumerRecordList()
                                .size());
                        final Collection<ConsumerRecord<K, V>> currentRecords = this.data.getConsumerRecordList()
                            .subList(
                                this.data.getIndex(),
                                Math.min(this.data.getIndex() + this.data.getBatchSize(), this.data.getLast()));
                        action.accept(currentRecords);
                        this.data.setIndex(this.data.getIndex() + this.data.getBatchSize());
                        System.out.println(this + " tryAdvance - index is " + this.data.getIndex());
                        return true;

                    } else {
                        return false;
                    }
                })
                .orElseThrow();
        }

        @Override
        public Spliterator<Collection<ConsumerRecord<K, V>>> trySplit() { return null; }

        @Override
        public void forEachRemaining(java.util.function.Consumer<? super Collection<ConsumerRecord<K, V>>> action) {
            if (action == null) { throw new NullPointerException(); }
            while (this.data.getFirst() < this.data.getLast()) {
                final Collection<ConsumerRecord<K, V>> currentRecords = this.data.getConsumerRecordList()
                    .subList(
                        this.data.getFirst(),
                        Math.min(this.data.getFirst() + this.data.getBatchSize(), this.data.getLast()));
                action.accept(currentRecords);
                this.data.setFirst(this.data.getFirst() + this.data.getBatchSize());
            }
        }

        @Override
        public long estimateSize() { return Long.MAX_VALUE; }

        @Override
        public int characteristics() { return this.data.getCharacteristics(); }

        public ConsumerRecordsSpliteratorPerBatch(
            List<ConsumerRecord<K, V>> consumerRecordList,
            int first,
            int batchSize
        ) {
            super();
            this.data.setConsumerRecordList(consumerRecordList);
            this.data.setFirst(first);
            this.data.setLast(Math.min(first + batchSize, consumerRecordList.size()));
            this.data.setCharacteristics(Spliterator.SIZED);
            this.data.setBatchSize(batchSize);
        }
    }

    protected static class ConsumerAllRecordsByBatchSpliterator<K, V>
        implements Spliterator<Collection<ConsumerRecord<K, V>>> {
        protected final ConsumerRecords<K, V>      nextRecords;
        protected final List<ConsumerRecord<K, V>> actualList;
        private final int                          characteristics;
        private final int                          batchSize;
        private int                                currentIndex;

        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super Collection<ConsumerRecord<K, V>>> action) {
            return false;
        }

        @Override
        public Spliterator<Collection<ConsumerRecord<K, V>>> trySplit() {
            synchronized (this) {
                if (this.currentIndex < this.actualList.size()) {
                    var returnValue = new ConsumerRecordsSpliteratorPerBatch<>(this.actualList,
                        this.currentIndex,
                        this.batchSize);
                    this.currentIndex = this.currentIndex + this.batchSize;
                    return returnValue;
                }
                return null;
            }
        }

        @Override
        public long estimateSize() { return Long.MAX_VALUE; }

        @Override
        public int characteristics() { return this.characteristics; }

        public ConsumerAllRecordsByBatchSpliterator(
            ConsumerRecords<K, V> nextRecords,
            int batchSize
        ) {
            super();
            this.nextRecords = nextRecords;
            this.characteristics = Spliterator.IMMUTABLE;
            this.batchSize = batchSize;
            this.actualList = StreamSupport.stream(this.nextRecords.spliterator(), false)
                .collect(Collectors.toList());
        }

    }

    protected static class ConsumerRecordsSpliterator<K, V, T> implements Spliterator<ConsumerRecord<K, V>> {

        protected final Consumer<K, V>              consumer;
        protected final T                           context;
        protected final int                         pollDurationInMs;
        private final int                           characteristics;
        private final int                           batchSize;
        private boolean                             firstSplit = true;
        private int                                 nbOfProcessedRecords;
        final protected BeforeProcessBatchAction<T> beforeProcessAction;
        protected Optional<TimeMeasurement>         timeMeasurement;

        @Override
        public Spliterator<ConsumerRecord<K, V>> trySplit() {
            Optional<ConsumerRecords<K, V>> nextRecords = Optional.empty();
            if (this.firstSplit) {
                do {
                    nextRecords = Optional.ofNullable(this.consumer.poll(Duration.ofMillis(this.pollDurationInMs)));
                    if (nextRecords.isEmpty()) {
                        break;
                    }
                } while (!nextRecords.isEmpty() && nextRecords.get()
                    .isEmpty());
                nextRecords.map(t -> t.count())
                    .ifPresent(t -> {
                        this.firstSplit = false;
                        this.beforeProcessAction.accept(t, this.context);
                        this.nbOfProcessedRecords = t;
                        this.timeMeasurement.ifPresent(tm -> tm.addStep("Start of processing records " + t));
                    });
            }
            return nextRecords.map(t -> new ConsumerAllRecordsSpliterator<>(t, this.batchSize))
                .orElseGet(() -> null);
        }

        @Override
        public long estimateSize() { return Long.MAX_VALUE; }

        @Override
        public int characteristics() { return this.characteristics; }

        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super ConsumerRecord<K, V>> action) { return false; }

        public ConsumerRecordsSpliterator(
            T context,
            Consumer<K, V> consumer,
            int pollDurationInMs,
            int batchSize,
            BeforeProcessBatchAction<T> beforeProcessAction,
            TimeMeasurement timeMeasurement
        ) {
            this.context = context;
            this.pollDurationInMs = pollDurationInMs;
            this.consumer = consumer;
            this.characteristics = Spliterator.IMMUTABLE;
            this.batchSize = batchSize;
            this.beforeProcessAction = beforeProcessAction;
            this.timeMeasurement = Optional.ofNullable(timeMeasurement);
        }

    }

    protected static class ConsumerBatchRecordsSpliterator<K, V, T>
        implements Spliterator<Collection<ConsumerRecord<K, V>>> {

        protected final Consumer<K, V>              consumer;
        protected final T                           context;
        protected final int                         pollDurationInMs;
        private final int                           characteristics;
        private final int                           batchSize;
        private boolean                             firstSplit = true;
        private int                                 nbOfProcessedRecords;
        final protected BeforeProcessBatchAction<T> beforeProcessAction;

        @Override
        public Spliterator<Collection<ConsumerRecord<K, V>>> trySplit() {
            Optional<ConsumerRecords<K, V>> nextRecords = Optional.empty();
            if (this.firstSplit) {
                do {
                    nextRecords = Optional.ofNullable(this.consumer.poll(Duration.ofMillis(this.pollDurationInMs)));
                    if (nextRecords.isEmpty()) {
                        break;
                    }
                } while (!nextRecords.isEmpty() && nextRecords.get()
                    .isEmpty());
                nextRecords.map(t -> t.count())
                    .ifPresent(t -> {
                        this.firstSplit = false;
                        this.beforeProcessAction.accept(t, this.context);
                        this.nbOfProcessedRecords = t;
                    });
            }
            return nextRecords.map(t -> new ConsumerAllRecordsByBatchSpliterator<>(t, this.batchSize))
                .orElseGet(() -> null);
        }

        @Override
        public long estimateSize() { return Long.MAX_VALUE; }

        @Override
        public int characteristics() { return this.characteristics; }

        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super Collection<ConsumerRecord<K, V>>> action) {
            return false;
        }

        public ConsumerBatchRecordsSpliterator(
            T context,
            Consumer<K, V> consumer,
            int pollDurationInMs,
            int batchSize,
            BeforeProcessBatchAction<T> beforeProcessAction
        ) {
            this.context = context;
            this.pollDurationInMs = pollDurationInMs;
            this.consumer = consumer;
            this.characteristics = Spliterator.IMMUTABLE;
            this.batchSize = batchSize;
            this.beforeProcessAction = beforeProcessAction;
        }

    }

    public static <K, V, T> Stream<ConsumerRecord<K, V>> buildParallelKafkaBatchStreamPerTopicAndPartition(
        T context,
        Consumer<K, V> consumer,
        int pollDurationInMs,
        int batchSize,
        boolean parallelStream,
        BeforeProcessBatchAction<T> beforeProcessAction
    ) {
        return StreamSupport.stream(
            new ConsumerRecordsSpliterator<>(context, consumer, pollDurationInMs, batchSize, beforeProcessAction, null),
            parallelStream);

    }

    public static <K, V, T> Stream<Collection<ConsumerRecord<K, V>>> buildParallelKafkaBatchStream(
        T context,
        Consumer<K, V> consumer,
        int pollDurationInMs,
        int batchSize,
        boolean parallelStream,
        BeforeProcessBatchAction<T> beforeProcessAction
    ) {
        return StreamSupport.stream(
            new ConsumerBatchRecordsSpliterator<>(context, consumer, pollDurationInMs, batchSize, beforeProcessAction),
            parallelStream);

    }

    static public <T> OffsetAndMetadata updateMapOfOffset(
        Map<TopicPartition, OffsetAndMetadata> mapOfOffset,
        T dataToRead,
        GetPartition<T> getPartition,
        GetTopic<T> getTopic,
        GetKafkaOffset<T> getKafkaOffset
    ) {
        final TopicPartition key = new TopicPartition(getTopic.apply(dataToRead), getPartition.apply(dataToRead));
        OffsetAndMetadata retValue = mapOfOffset.compute(
            key,
            (k, v) -> v == null ? new OffsetAndMetadata(getKafkaOffset.apply(dataToRead) + 1)
                : new OffsetAndMetadata(Math.max(getKafkaOffset.apply(dataToRead) + 1, v.offset())));
        return retValue;
    }

    static public void merge(Map<TopicPartition, OffsetAndMetadata> r, Map<TopicPartition, OffsetAndMetadata> t) {
        t.entrySet()
            .stream()
            .forEach((entry) -> KafkaUtils.updateMapOfOffset(r, entry));
    }

    static public void updateMapOfOffset(
        Map<TopicPartition, OffsetAndMetadata> mapOfOffset,
        Entry<TopicPartition, OffsetAndMetadata> entry
    ) {
        mapOfOffset.compute(
            entry.getKey(),
            (k, v) -> v == null ? entry.getValue()
                : new OffsetAndMetadata(Math.max(
                    entry.getValue()
                        .offset(),
                    v.offset())));

    }

}
