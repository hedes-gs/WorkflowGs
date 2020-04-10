package com.gs.photo.workflow.consumers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.gs.photo.workflow.dao.GenericDAO;
import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEvents;

public abstract class AbstractConsumerForRecordHbase<T extends HbaseData> {

    private static final String          HBASE_RECORDER                   = "HBASE_RECORDER";
    private static Logger                LOGGER                           = LogManager
        .getLogger(AbstractConsumerForRecordHbase.class);
    public static final int              NB_OF_THREADS_TO_RECORD_IN_HBASE = 3;

    @Autowired
    protected GenericDAO<T>              genericDAO;

    @Autowired
    protected Producer<String, WfEvents> producerForPublishingInModeTransactionalOnLongTopic;

    @Value("${topic.topicRecordedCountObjects}")
    protected String                     topicRecordedCountObjects;

    protected ExecutorService[]          services;

    @Value("${group.id}")
    private String                       groupId;

    @PostConstruct
    protected void init() {
        this.services = new ExecutorService[AbstractConsumerForRecordHbase.NB_OF_THREADS_TO_RECORD_IN_HBASE];
        for (int k = 0; k < AbstractConsumerForRecordHbase.NB_OF_THREADS_TO_RECORD_IN_HBASE; k++) {
            this.services[k] = Executors.newFixedThreadPool(1);
        }
    }

    protected static final class DbTaskResult {
        protected final TopicPartition            partition;
        protected final OffsetAndMetadata         commitData;
        protected final Multimap<String, WfEvent> wfEvents;

        public TopicPartition getPartition() { return this.partition; }

        public OffsetAndMetadata getCommitData() { return this.commitData; }

        public Multimap<String, WfEvent> getWfEvents() { return this.wfEvents; }

        protected DbTaskResult(
            TopicPartition partition,
            OffsetAndMetadata commitData,
            Multimap<String, WfEvent> keysNumber
        ) {
            super();
            this.partition = partition;
            this.commitData = commitData;
            this.wfEvents = keysNumber;
        }

    }

    protected final class DbTask implements Callable<DbTaskResult> {
        protected ListMultimap<String, T> data;
        protected TopicPartition          partition;
        protected long                    lastOffset;

        public DbTask(
            ListMultimap<String, T> data,
            TopicPartition partition,
            long offsetToCommit
        ) {
            super();
            this.data = data;
            this.partition = partition;
            this.lastOffset = offsetToCommit + 1;
        }

        @Override
        public DbTaskResult call() throws Exception {
            Multimap<String, WfEvent> keysNumber = ArrayListMultimap.create();
            this.data.values()
                .stream()
                .collect(Collectors.groupingBy((k) -> k.getClass()))
                .forEach((k, v) -> {
                    AbstractConsumerForRecordHbase.this.genericDAO.put(v, (Class<T>) k);
                    AbstractConsumerForRecordHbase.this.postRecord(v, (Class<T>) k);
                });
            this.data.values()
                .forEach(
                    (x) -> keysNumber.put(
                        x.getDataId(),
                        AbstractConsumerForRecordHbase.this.buildEvent(x)
                            .orElseThrow(() -> new IllegalArgumentException(x + " not processed!"))));

            return new DbTaskResult(this.partition, new OffsetAndMetadata(this.lastOffset + 1), keysNumber);
        }
    }

    protected void processMessagesFromTopic(Consumer<String, T> consumer) {
        List<Future<DbTaskResult>> futuresList = new ArrayList<>();
        this.producerForPublishingInModeTransactionalOnLongTopic.initTransactions();
        while (true) {
            futuresList.clear();
            ConsumerRecords<String, T> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            this.producerForPublishingInModeTransactionalOnLongTopic.beginTransaction();
            AbstractConsumerForRecordHbase.LOGGER.info(" found {} records ", records.count());
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, T>> partitionRecords = records.records(partition);
                ListMultimap<String, T> multimapRecords = MultimapBuilder.treeKeys()
                    .arrayListValues()
                    .build();
                for (ConsumerRecord<String, T> record : partitionRecords) {
                    multimapRecords.put(record.key(), record.value());
                }
                Future<DbTaskResult> f = this.services[partition.partition()
                    % AbstractConsumerForRecordHbase.NB_OF_THREADS_TO_RECORD_IN_HBASE].submit(
                        new DbTask(multimapRecords,
                            partition,
                            partitionRecords.get(partitionRecords.size() - 1)
                                .offset()));
                futuresList.add(f);
            }
            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();

            futuresList.forEach((f) -> {
                try {
                    DbTaskResult task = f.get();
                    offsets.put(task.getPartition(), task.getCommitData());
                    Set<String> keys = task.wfEvents.keySet();
                    keys.forEach((k) -> {
                        WfEvents wfEvents = WfEvents.builder()
                            .withEvents(task.wfEvents.get(k))
                            .withProducer(AbstractConsumerForRecordHbase.HBASE_RECORDER)
                            .build();
                        ProducerRecord<String, WfEvents> producerRecord = new ProducerRecord<>(
                            this.topicRecordedCountObjects,
                            k,
                            wfEvents);
                        this.producerForPublishingInModeTransactionalOnLongTopic.send(producerRecord);

                    });

                } catch (
                    CommitFailedException |
                    InterruptedException |
                    ExecutionException e) {
                    AbstractConsumerForRecordHbase.LOGGER.warn("Error while commiting ", e);
                }
            });
            this.producerForPublishingInModeTransactionalOnLongTopic.sendOffsetsToTransaction(offsets, this.groupId);
            this.producerForPublishingInModeTransactionalOnLongTopic.commitTransaction();

        }
    }

    protected abstract Optional<WfEvent> buildEvent(T x);

    protected abstract void postRecord(List<T> v, Class<T> k);

}
