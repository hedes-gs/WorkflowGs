package com.gs.photo.common.workflow.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.concurrent.locks.ReentrantLock;
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

    public static <K, V> Stream<ConsumerRecord<K, V>> toStream(Consumer<K, V> consumer) {
        Iterable<ConsumerRecord<K, V>> iterable = () -> new Iterator<ConsumerRecord<K, V>>() {
            Iterator<ConsumerRecord<K, V>> records;

            @Override
            public boolean hasNext() {
                if ((this.records == null) || !this.records.hasNext()) {
                    ConsumerRecords<K, V> nextRecords;
                    if (this.records != null) {
                        consumer.commitSync();
                    }
                    do {
                        nextRecords = consumer.poll(Duration.ofMillis(250));
                        if (nextRecords == null) {
                            break;
                        }
                    } while (nextRecords.isEmpty());
                    this.records = nextRecords != null ? nextRecords.iterator() : null;
                }
                return this.records != null;
            }

            @Override
            public ConsumerRecord<K, V> next() { return this.records.next(); }
        };
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public static interface PreCommitAction { void apply(int nbOfRecord); }

    public static interface BeforeProcessAction { void apply(int nbOfRecord); }

    protected static class ConsumerRecordsSpliteratorPerTopic<K, V> implements Spliterator<ConsumerRecord<K, V>> {

        protected static final int                 BATCH_SIZE = 15;
        final protected List<ConsumerRecord<K, V>> consumerRecordList;
        protected int                              index;
        protected int                              first;
        protected int                              last;
        final long                                 size;
        private final int                          characteristics;
        private final int                          batchSize;
        private TopicPartition                     currentTopicPartition;

        @Override
        public String toString() {
            return "ConsumerRecordsSpliteratorPerTopic [index=" + this.index + ", first=" + this.first + ", last="
                + this.last + "]";
        }

        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super ConsumerRecord<K, V>> action) {
            if (action == null) { throw new NullPointerException(); }
            if ((this.index >= this.first) && (this.index < this.last)) {
                final ConsumerRecord<K, V> currentRecord = this.consumerRecordList.get(this.index);
                KafkaUtils.LOGGER.debug(
                    "tryAdvance - process index {} partition is {} , offset is {}, first is {}, last is {} ",
                    this.index,
                    currentRecord.partition(),
                    currentRecord.offset(),
                    this.first,
                    this.last);
                action.accept(currentRecord);
                this.index++;
                return true;
            }
            return false;
        }

        @Override
        public Spliterator<ConsumerRecord<K, V>> trySplit() {
            synchronized (this) {

                if ((this.size == Long.MAX_VALUE) && (this.last < this.consumerRecordList.size())) {
                    final ConsumerRecordsSpliteratorPerTopic<K, V> consumerRecordsSpliteratorPerTopic = new ConsumerRecordsSpliteratorPerTopic<>(
                        this.consumerRecordList,
                        this.first,
                        this.batchSize,
                        this.batchSize,
                        this.currentTopicPartition);

                    this.first = this.first + this.batchSize;
                    this.last = Math.min(this.first + this.batchSize, this.consumerRecordList.size());
                    this.index = this.first;
                    KafkaUtils.LOGGER.debug(
                        "trySplit  - topic is {} partition is {}  new first is {}, new last is {}, size is {}",
                        this.currentTopicPartition.topic(),
                        this.currentTopicPartition.partition(),
                        this.first,
                        this.last,
                        this.consumerRecordList.size());

                    return consumerRecordsSpliteratorPerTopic;
                }
                return null;
            }
        }

        @Override
        public void forEachRemaining(java.util.function.Consumer<? super ConsumerRecord<K, V>> action) {
            if (action == null) { throw new NullPointerException(); }
            while (this.index < this.last) {
                final ConsumerRecord<K, V> currentRecord = this.consumerRecordList.get(this.index);
                KafkaUtils.LOGGER.debug(
                    "forEachRemaining - process index {} partition is {} , offset is {}, first is {}, last is {} ",
                    this.index,
                    currentRecord.partition(),
                    currentRecord.offset(),
                    this.first,
                    this.last);
                action.accept(currentRecord);
                this.index++;
            }
        }

        @Override
        public long estimateSize() { return this.size; }

        @Override
        public int characteristics() { return this.characteristics; }

        public ConsumerRecordsSpliteratorPerTopic(
            List<ConsumerRecord<K, V>> consumerRecordList,
            int first,
            long size,
            int batchSize,
            TopicPartition currentTopicPartition
        ) {
            super();
            this.consumerRecordList = consumerRecordList;
            this.index = first;
            this.first = first;
            this.last = Math.min(first + batchSize, consumerRecordList.size());
            this.characteristics = Spliterator.SIZED;
            this.size = size;
            this.batchSize = batchSize;
            this.currentTopicPartition = currentTopicPartition;
        }

    }

    protected static class ConsumerAllRecordsSpliterator<K, V> implements Spliterator<ConsumerRecord<K, V>> {
        final protected ConsumerRecords<K, V> nextRecords;
        final List<TopicPartition>            topics;
        int                                   currentTopicPartition;
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

    protected static class ConsumerRecordsSpliterator<K, V> implements Spliterator<ConsumerRecord<K, V>> {

        protected final Consumer<K, V>      consumer;
        protected final int                 pollDurationInMs;
        private final int                   characteristics;
        private final int                   batchSize;
        private boolean                     firstSplit = true;
        final protected BeforeProcessAction beforeProcessAction;
        protected TimeMeasurement           timeMeasurement;

        @Override
        public Spliterator<ConsumerRecord<K, V>> trySplit() {
            ConsumerRecords<K, V> nextRecords = null;
            if (this.firstSplit) {
                do {
                    nextRecords = this.consumer.poll(Duration.ofMillis(this.pollDurationInMs));

                    if (nextRecords == null) {
                        break;
                    }
                    final ConsumerRecords<K, V> records = nextRecords;
                    nextRecords.partitions()
                        .forEach((p) -> {
                            KafkaUtils.LOGGER.info(
                                "pollDurationInMs is {}, batchSize is {} , fetched nbOfRecords {} -  Partition {} has max offset {}, min offset {} ",
                                this.pollDurationInMs,
                                this.batchSize,
                                records.count(),
                                p,
                                records.records(p)
                                    .stream()
                                    .mapToLong((r) -> r.offset())
                                    .max()
                                    .orElse(Long.MIN_VALUE),
                                records.records(p)
                                    .stream()
                                    .mapToLong((r) -> r.offset())
                                    .min()
                                    .orElse(Long.MIN_VALUE));
                        });
                } while (nextRecords.isEmpty());
                if (nextRecords != null) {
                    if (this.timeMeasurement != null) {
                        this.timeMeasurement.addStep("Start of processing records " + nextRecords.count());
                    }
                    this.firstSplit = false;
                    this.beforeProcessAction.apply(nextRecords.count());
                }
            }

            return nextRecords == null ? null : new ConsumerAllRecordsSpliterator<>(nextRecords, this.batchSize);
        }

        @Override
        public long estimateSize() { return Long.MAX_VALUE; }

        @Override
        public int characteristics() { return this.characteristics; }

        public ConsumerRecordsSpliterator(
            int pollDurationInMs,
            Consumer<K, V> consumer,
            int batchSize,
            BeforeProcessAction beforeProcessAction,
            TimeMeasurement timeMeasurement
        ) {
            this.pollDurationInMs = pollDurationInMs;
            this.consumer = consumer;
            this.characteristics = Spliterator.IMMUTABLE;
            this.batchSize = batchSize;
            this.beforeProcessAction = beforeProcessAction;
            this.timeMeasurement = timeMeasurement;
        }

        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super ConsumerRecord<K, V>> action) { return false; }

    }

    public static <K, V> Stream<ConsumerRecord<K, V>> toStreamV2(
        int pollDurationInMs,
        Consumer<K, V> consumer,
        int batchSize,
        boolean parallelStream,
        BeforeProcessAction beforeProcessAction,
        TimeMeasurement timeMeasurement
    ) {
        return StreamSupport.stream(
            new ConsumerRecordsSpliterator<>(pollDurationInMs,
                consumer,
                batchSize,
                beforeProcessAction,
                timeMeasurement),
            parallelStream);

    }

    public static <K, V, K1, V1> Stream<ConsumerRecord<K, V>> toStream(
        int pollDurationInMs,
        Consumer<K, V> consumer,
        PreCommitAction preCommitAction,
        BeforeProcessAction beforeProcessAction,
        boolean parallelStream
    ) {
        Iterable<ConsumerRecord<K, V>> iterable = () -> new Iterator<ConsumerRecord<K, V>>() {
            ReentrantLock                     lock      = new ReentrantLock();
            boolean                           first     = true;
            Iterator<ConsumerRecord<K, V>>    records;
            int                               currentNbOfRecords;
            ThreadLocal<ConsumerRecord<K, V>> nextValue = new ThreadLocal<>();

            @Override
            public boolean hasNext() {
                if (parallelStream) {
                    this.lock.lock();
                    try {
                        if (this.first) {
                            this.first = false;
                            ConsumerRecords<K, V> nextRecords;
                            do {
                                nextRecords = consumer.poll(Duration.ofMillis(pollDurationInMs));
                                if (nextRecords == null) {
                                    break;
                                }
                            } while (nextRecords.isEmpty());
                            if (nextRecords != null) {
                                this.records = nextRecords.iterator();
                                this.currentNbOfRecords = nextRecords.count();
                                beforeProcessAction.apply(this.currentNbOfRecords);
                            } else {
                                this.records = null;
                            }
                        }
                    } finally {
                        this.lock.unlock();
                    }
                    this.lock.lock();
                    try {
                        if (this.records.hasNext()) {
                            this.nextValue.set(this.records.next());
                            return true;
                        }
                        return false;
                    } finally {
                        this.lock.unlock();
                    }
                } else {
                    if ((this.records == null) || !this.records.hasNext()) {
                        ConsumerRecords<K, V> nextRecords;
                        if (this.records != null) {
                            preCommitAction.apply(this.currentNbOfRecords);
                            if (!parallelStream) {
                                consumer.commitSync();
                            }
                        }
                        do {
                            nextRecords = consumer.poll(Duration.ofMillis(pollDurationInMs));
                            if (nextRecords == null) {
                                break;
                            }
                        } while (nextRecords.isEmpty());
                        if (nextRecords != null) {
                            this.records = nextRecords.iterator();
                            this.currentNbOfRecords = nextRecords.count();
                            beforeProcessAction.apply(this.currentNbOfRecords);
                        } else {
                            this.records = null;
                        }
                    }
                }
                return this.records != null;
            }

            @Override
            public ConsumerRecord<K, V> next() { return this.nextValue.get(); }
        };
        return StreamSupport.stream(iterable.spliterator(), parallelStream);
    }

    public static <K, V, K1, V1> Stream<ConsumerRecord<K, V>> toTransactionalStream(
        int pollDurationInMs,
        Consumer<K, V> consumer,
        PreCommitAction preCommitAction,
        BeforeProcessAction beforeProcessAction
    ) {
        Iterable<ConsumerRecord<K, V>> iterable = () -> new Iterator<ConsumerRecord<K, V>>() {
            Iterator<ConsumerRecord<K, V>> records;
            int                            currentNbOfRecords;

            @Override
            public boolean hasNext() {
                if ((this.records == null) || !this.records.hasNext()) {
                    ConsumerRecords<K, V> nextRecords;
                    if (this.records != null) {
                        preCommitAction.apply(this.currentNbOfRecords);
                    }
                    do {
                        nextRecords = consumer.poll(Duration.ofMillis(pollDurationInMs));
                        if (nextRecords == null) {
                            break;
                        }
                    } while (nextRecords.isEmpty());
                    if (nextRecords != null) {
                        this.records = nextRecords.iterator();
                        this.currentNbOfRecords = nextRecords.count();
                        beforeProcessAction.apply(this.currentNbOfRecords);
                    } else {
                        this.records = null;
                    }
                }
                return this.records != null;
            }

            @Override
            public ConsumerRecord<K, V> next() { return this.records.next(); }
        };
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    static public interface GetPartition<T> { int get(T t); }

    static public interface GetTopic<T> { String get(T t); }

    static public interface GetKafkaOffset<T> { long get(T t); }

    static public <T> OffsetAndMetadata updateMapOfOffset(
        Map<TopicPartition, OffsetAndMetadata> mapOfOffset,
        T dataToRead,
        GetPartition<T> getPartition,
        GetTopic<T> getTopic,
        GetKafkaOffset<T> getKafkaOffset
    ) {
        final TopicPartition key = new TopicPartition(getTopic.get(dataToRead), getPartition.get(dataToRead));
        OffsetAndMetadata retValue = mapOfOffset.compute(
            key,
            (k, v) -> v == null ? new OffsetAndMetadata(getKafkaOffset.get(dataToRead) + 1)
                : new OffsetAndMetadata(Math.max(getKafkaOffset.get(dataToRead) + 1, v.offset())));
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
