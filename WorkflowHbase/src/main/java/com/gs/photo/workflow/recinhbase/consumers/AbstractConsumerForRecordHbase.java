package com.gs.photo.workflow.recinhbase.consumers;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.common.workflow.TimeMeasurement;
import com.gs.photo.common.workflow.TimeMeasurement.Step;
import com.gs.photo.common.workflow.impl.KafkaUtils;
import com.gs.photo.common.workflow.internal.GenericKafkaManagedObject;
import com.gs.photo.common.workflow.internal.KafkaManagedHbaseData;
import com.gs.photo.common.workflow.internal.KafkaManagedWfEvent;
import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEvents;

public abstract class AbstractConsumerForRecordHbase<T extends HbaseData> {

    private static final String                                                     HBASE_RECORDER = "HBASE_RECORDER";
    private static Logger                                                           LOGGER         = LoggerFactory
        .getLogger(AbstractConsumerForRecordHbase.class);

    protected Producer<String, WfEvents>                                            producerForPublishingWfEvents;

    @Autowired
    @Qualifier("propertiesForPublishingWfEvents")
    protected Properties                                                            propertiesForPublishingWfEvents;

    @Value("${topic.topicEvent}")
    protected String                                                                topicEvent;

    @Value("${kafka.consumer.batchSizeForParallelProcessingIncomingRecords}")
    protected int                                                                   batchSizeForParallelProcessingIncomingRecords;

    @Value("${kafka.pollTimeInMillisecondes}")
    protected int                                                                   kafkaPollTimeInMillisecondes;

    @Autowired
    protected IBeanTaskExecutor                                                     beanTaskExecutor;

    protected static BlockingQueue<Map<String, List<GenericKafkaManagedObject<?>>>> eventsQueue    = new LinkedBlockingQueue<>();

    protected static class MetricsStatistics implements Runnable {
        protected final BlockingQueue<Map<String, List<GenericKafkaManagedObject<?>>>> queue;

        @Override
        public void run() {
            Map<String, Integer> metrics = new HashMap<>();
            long nextPrint = System.currentTimeMillis() + (15 * 1000);
            while (true) {
                try {
                    Map<String, List<GenericKafkaManagedObject<?>>> lastEvent = this.queue
                        .poll(1000, TimeUnit.MILLISECONDS);
                    if (lastEvent != null) {
                        lastEvent.entrySet()
                            .forEach((e) -> metrics.compute(e.getKey(), (k, v) -> this.compute(e, k, v)));
                        lastEvent.clear();
                    }
                    if (System.currentTimeMillis() > nextPrint) {
                        if (metrics.size() > 0) {
                            AbstractConsumerForRecordHbase.LOGGER.info(
                                "[STAT] nb of processed events during the last {} ms : {}  ",
                                (System.currentTimeMillis() - nextPrint) / 1000.0f,
                                metrics.size());
                            metrics.entrySet()
                                .forEach(
                                    (e) -> AbstractConsumerForRecordHbase.LOGGER.info(
                                        "[EVENT][{}] statistics nb of processed events : {} ",
                                        e.getKey(),
                                        e.getValue()));
                            metrics.clear();

                        }
                        nextPrint = System.currentTimeMillis() + (15 * 1000);
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
        }

        private Integer compute(Entry<String, List<GenericKafkaManagedObject<?>>> e, String k, Integer v) {
            if (v == null) { return e.getValue()
                .size(); }
            return v + e.getValue()
                .size();
        }

        public MetricsStatistics(BlockingQueue<Map<String, List<GenericKafkaManagedObject<?>>>> queue) {
            this.queue = queue;
        }
    }

    static {
        new Thread(new MetricsStatistics(AbstractConsumerForRecordHbase.eventsQueue), "events-statistics-thread")
            .start();

    }

    protected void processMessagesFromTopic(Consumer<String, T> consumer, String uniqueId, String groupId) {

        Properties propertiesForPublishingWfEvents = (Properties) this.propertiesForPublishingWfEvents.clone();
        propertiesForPublishingWfEvents.put(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            this.propertiesForPublishingWfEvents.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) + "-" + uniqueId);
        AbstractConsumerForRecordHbase.LOGGER.info(
            "[CONSUMER][{}] starting - ",
            this.getConsumer(),
            this.propertiesForPublishingWfEvents.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        this.producerForPublishingWfEvents = new KafkaProducer<>(propertiesForPublishingWfEvents);
        this.producerForPublishingWfEvents.initTransactions();
        AbstractConsumerForRecordHbase.LOGGER.info("Start job {}", this);
        while (true) {
            try (
                TimeMeasurement timeMeasurement = TimeMeasurement
                    .of("BATCH_PROCESS_FILES", (d) -> this.dummy(d), System.currentTimeMillis())) {
                Stream<ConsumerRecord<String, T>> recordsToProcess = KafkaUtils.toStreamV2(
                    this.kafkaPollTimeInMillisecondes,
                    consumer,
                    this.batchSizeForParallelProcessingIncomingRecords,
                    true,
                    (i) -> this.startTransactionForRecords(i),
                    timeMeasurement);
                List<ConsumerRecord<String, T>> processedRecords = recordsToProcess.map((rec) -> this.record(rec))
                    .collect(Collectors.toList());
                this.postRecord(
                    processedRecords.stream()
                        .map((rec) -> rec.value())
                        .collect(Collectors.toList()));
                Stream<KafkaManagedHbaseData> kafkaManagedDataToProcess = processedRecords.stream()
                    .map((rec) -> this.toKafkaManagedHbaseData(rec));

                Map<TopicPartition, OffsetAndMetadata> offsets = null;
                if (this.eventsShouldBeProduced()) {
                    Map<String, List<GenericKafkaManagedObject<?>>> eventsToSend = kafkaManagedDataToProcess
                        .map((kmo) -> this.buildEvent(kmo))
                        .collect(Collectors.groupingByConcurrent(GenericKafkaManagedObject::getImageKey));
                    long eventsNumber = eventsToSend.keySet()
                        .stream()
                        .map(
                            (img) -> WfEvents.builder()
                                .withDataId(img)
                                .withProducer(AbstractConsumerForRecordHbase.HBASE_RECORDER)
                                .withEvents(
                                    (Collection<WfEvent>) eventsToSend.get(img)
                                        .stream()
                                        .map(GenericKafkaManagedObject::getValue)
                                        .collect(Collectors.toList()))
                                .build())
                        .map((evts) -> this.send(evts))
                        .collect(Collectors.toList())
                        .stream()
                        .map((t) -> this.getRecordMetaData(t))
                        .count();

                    offsets = eventsToSend.values()
                        .stream()
                        .flatMap((c) -> c.stream())
                        .collect(
                            () -> new HashMap<TopicPartition, OffsetAndMetadata>(),
                            (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                            (r, t) -> this.merge(r, t));

                    AbstractConsumerForRecordHbase.eventsQueue.offer(eventsToSend);
                } else {
                    offsets = kafkaManagedDataToProcess.collect(
                        () -> new HashMap<TopicPartition, OffsetAndMetadata>(),
                        (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                        (r, t) -> this.merge(r, t));
                }
                boolean done = false;
                int nbOfTimes = 0;
                do {
                    try {
                        if (nbOfTimes > 0) {
                            AbstractConsumerForRecordHbase.LOGGER.warn("Retrying again after {} fails", nbOfTimes);
                        }
                        this.flushAllDAO();
                        if (nbOfTimes > 0) {
                            AbstractConsumerForRecordHbase.LOGGER
                                .warn("Retrying again after {}, finally succeed ", nbOfTimes);
                            // TO do :
                            // recordAgain().
                        }
                        done = true;
                    } catch (
                        RetriesExhaustedWithDetailsException |
                        NotServingRegionException e) {
                        AbstractConsumerForRecordHbase.LOGGER.warn("Retries error is detected", e);
                        nbOfTimes++;
                        Thread.sleep(1000);
                    }
                } while (!done && (nbOfTimes < 3));
                if (nbOfTimes < 3) {
                    AbstractConsumerForRecordHbase.LOGGER
                        .info("[CONSUMER][{}]  Offsets to commit {}", this.getConsumer(), offsets);
                    this.producerForPublishingWfEvents.sendOffsetsToTransaction(offsets, groupId);
                    this.producerForPublishingWfEvents.commitTransaction();
                } else {
                    AbstractConsumerForRecordHbase.LOGGER
                        .error("[CONSUMER][{}] Stopping process !!", this.getConsumer());
                    this.producerForPublishingWfEvents.abortTransaction();
                    break;
                }
            } catch (Throwable e) {
                AbstractConsumerForRecordHbase.LOGGER
                    .warn("[CONSUMER][{}] Unexpected error, stopping process ", this.getConsumer(), e);
                this.producerForPublishingWfEvents.abortTransaction();
                break;
            }
        }

    }

    protected abstract String getConsumer();

    private void dummy(List<Step> d) {
        d.forEach((s) -> {
            if (s.getDuration() > 0.5f) {
                AbstractConsumerForRecordHbase.LOGGER
                    .warn("[CONSUMER][{}] Long time proessing detected  {} , ", this.getConsumer(), s.getDuration());
            }
        });
    }

    protected abstract void flushAllDAO() throws IOException;

    private void merge(Map<TopicPartition, OffsetAndMetadata> r, Map<TopicPartition, OffsetAndMetadata> t) {
        KafkaUtils.merge(r, t);
    }

    private void updateMapOfOffset(Map<TopicPartition, OffsetAndMetadata> mapOfOffset, GenericKafkaManagedObject<?> t) {
        KafkaUtils
            .updateMapOfOffset(mapOfOffset, t, (f) -> f.getPartition(), (f) -> f.getTopic(), (f) -> f.getKafkaOffset());
    }

    private void updateMapOfOffset(Map<TopicPartition, OffsetAndMetadata> mapOfOffset, KafkaManagedHbaseData t) {
        KafkaUtils
            .updateMapOfOffset(mapOfOffset, t, (f) -> f.getPartition(), (f) -> f.getTopic(), (f) -> f.getKafkaOffset());
    }

    protected RecordMetadata getRecordMetaData(Future<RecordMetadata> t) {
        try {
            return t.get();
        } catch (
            InterruptedException |
            ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Future<RecordMetadata> send(WfEvents wfe) {
        ProducerRecord<String, WfEvents> pr = new ProducerRecord<>(this.topicEvent, wfe.getDataId(), wfe);
        return this.producerForPublishingWfEvents.send(pr);
    }

    private KafkaManagedWfEvent buildEvent(KafkaManagedHbaseData kmo) {
        return KafkaManagedWfEvent.builder()
            .withImageKey(kmo.getImageKey())
            .withKafkaOffset(kmo.getKafkaOffset())
            .withPartition(kmo.getPartition())
            .withTopic(kmo.getTopic())
            .withValue(
                this.buildEvent((T) kmo.getValue())
                    .orElseThrow(() -> new IllegalArgumentException()))
            .build();

    }

    protected ConsumerRecord<String, T> record(ConsumerRecord<String, T> rec) {
        this.doRecord(rec.key(), rec.value());
        return rec;
    }

    protected KafkaManagedHbaseData toKafkaManagedHbaseData(ConsumerRecord<String, T> rec) {
        return KafkaManagedHbaseData.builder()
            .withKafkaOffset(rec.offset())
            .withPartition(rec.partition())
            .withTopic(rec.topic())
            .withValue(rec.value())
            .withImageKey(rec.key())
            .build();
    }

    private void startTransactionForRecords(int i) {
        AbstractConsumerForRecordHbase.LOGGER
            .info("[CONSUMER][{}] Start transactions, nb of elements {}", this.getConsumer(), i);
        this.producerForPublishingWfEvents.beginTransaction();
    }

    @PostConstruct
    protected void init() { this.beanTaskExecutor.execute(() -> this.processIncomingMessages()); }

    protected AbstractConsumerForRecordHbase() {}

    protected abstract Optional<WfEvent> buildEvent(T x);

    protected abstract void postRecord(List<T> v);

    protected abstract void doRecord(String key, T k);

    protected boolean eventsShouldBeProduced() { return true; }

    public abstract void processIncomingMessages();

}
