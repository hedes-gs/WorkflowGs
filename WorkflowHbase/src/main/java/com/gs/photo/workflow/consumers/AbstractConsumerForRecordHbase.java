package com.gs.photo.workflow.consumers;

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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.TimeMeasurement;
import com.gs.photo.workflow.TimeMeasurement.Step;
import com.gs.photo.workflow.hbase.dao.GenericDAO;
import com.gs.photo.workflow.impl.KafkaUtils;
import com.gs.photo.workflow.internal.GenericKafkaManagedObject;
import com.gs.photo.workflow.internal.KafkaManagedHbaseData;
import com.gs.photo.workflow.internal.KafkaManagedWfEvent;
import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEvents;

public abstract class AbstractConsumerForRecordHbase<T extends HbaseData> {

    private static final String                                                     HBASE_RECORDER = "HBASE_RECORDER";
    private static Logger                                                           LOGGER         = LogManager
        .getLogger(AbstractConsumerForRecordHbase.class);

    protected Producer<String, WfEvents>                                            producerForPublishingWfEvents;

    @Autowired
    @Qualifier("propertiesForPublishingWfEvents")
    protected Properties                                                            propertiesForPublishingWfEvents;

    @Value("${topic.topicEvent}")
    protected String                                                                topicEvent;

    @Value("${group.id}")
    private String                                                                  groupId;

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

    protected void processMessagesFromTopic(Consumer<String, T> consumer, String uniqueId) {

        this.propertiesForPublishingWfEvents.put(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            this.propertiesForPublishingWfEvents.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) + "-" + uniqueId);

        this.producerForPublishingWfEvents = new KafkaProducer<>(this.propertiesForPublishingWfEvents);
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
                Stream<KafkaManagedHbaseData> kafkaManagedDataToProcess = recordsToProcess
                    .map((rec) -> this.record(rec));
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
                this.flushAllDAO();
                this.producerForPublishingWfEvents.sendOffsetsToTransaction(offsets, this.groupId);
                this.producerForPublishingWfEvents.commitTransaction();
            } catch (Exception e) {
                AbstractConsumerForRecordHbase.LOGGER.warn("Unexpected error ", e);
                break;
            }
        }

    }

    private void dummy(List<Step> d) {
        d.forEach((s) -> {
            if (s.getDuration() > 0.5f) {
                AbstractConsumerForRecordHbase.LOGGER.warn("Long time proessing detected  {} , ", s.getDuration());
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

    protected KafkaManagedHbaseData record(ConsumerRecord<String, T> rec) {
        try {
            this.getGenericDAO(
                (Class<T>) rec.value()
                    .getClass())
                .put(
                    rec.value(),
                    (Class<T>) rec.value()
                        .getClass());
            return KafkaManagedHbaseData.builder()
                .withKafkaOffset(rec.offset())
                .withPartition(rec.partition())
                .withTopic(rec.topic())
                .withValue(rec.value())
                .withImageKey(rec.key())
                .build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void startTransactionForRecords(int i) { this.producerForPublishingWfEvents.beginTransaction(); }

    @PostConstruct
    protected void init() { this.beanTaskExecutor.execute(() -> this.processIncomingMessages()); }

    protected AbstractConsumerForRecordHbase() {}

    protected abstract Optional<WfEvent> buildEvent(T x);

    protected abstract void postRecord(List<T> v, Class<T> k);

    protected abstract <X extends T> GenericDAO<X> getGenericDAO(Class<X> k);

    protected boolean eventsShouldBeProduced() { return true; }

    public abstract void processIncomingMessages();

}
