package com.gs.photo.workflow.consumers;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import com.gs.photo.workflow.dao.GenericDAO;
import com.gs.photo.workflow.impl.KafkaUtils;
import com.gs.photo.workflow.internal.GenericKafkaManagedObject;
import com.gs.photo.workflow.internal.KafkaManagedHbaseData;
import com.gs.photo.workflow.internal.KafkaManagedWfEvent;
import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEvents;

public abstract class AbstractConsumerForRecordHbase<T extends HbaseData> {

    private static final String          HBASE_RECORDER = "HBASE_RECORDER";
    private static Logger                LOGGER         = LogManager.getLogger(AbstractConsumerForRecordHbase.class);

    protected Producer<String, WfEvents> producerForPublishingWfEvents;

    @Autowired
    @Qualifier("propertiesForPublishingWfEvents")
    protected Properties                 propertiesForPublishingWfEvents;

    @Value("${topic.topicEvent}")
    protected String                     topicEvent;

    @Value("${group.id}")
    private String                       groupId;

    @Value("${kafka.consumer.batchSizeForParallelProcessingIncomingRecords}")
    protected int                        batchSizeForParallelProcessingIncomingRecords;

    @Value("${kafka.pollTimeInMillisecondes}")
    protected int                        kafkaPollTimeInMillisecondes;

    @Autowired
    protected IBeanTaskExecutor          beanTaskExecutor;

    protected void processMessagesFromTopic(Consumer<String, T> consumer, String uniqueId) {

        this.propertiesForPublishingWfEvents.put(
            ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            this.propertiesForPublishingWfEvents.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) + "-" + uniqueId);

        this.producerForPublishingWfEvents = new KafkaProducer<>(this.propertiesForPublishingWfEvents);
        this.producerForPublishingWfEvents.initTransactions();
        AbstractConsumerForRecordHbase.LOGGER.info("Start job {}", this);

        while (true) {
            try (
                TimeMeasurement timeMeasurement = TimeMeasurement.of(
                    "BATCH_PROCESS_FILES",
                    (d) -> AbstractConsumerForRecordHbase.LOGGER.info(" Perf. metrics {}", d),
                    System.currentTimeMillis())) {
                Stream<ConsumerRecord<String, T>> recordsToProcess = KafkaUtils.toStreamV2(
                    this.kafkaPollTimeInMillisecondes,
                    consumer,
                    this.batchSizeForParallelProcessingIncomingRecords,
                    true,
                    (i) -> this.startTransactionForRecords(i),
                    timeMeasurement);
                Map<String, List<GenericKafkaManagedObject<?>>> eventsToSend = recordsToProcess
                    .map((rec) -> this.record(rec))
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

                Map<TopicPartition, OffsetAndMetadata> offsets = eventsToSend.values()
                    .stream()
                    .flatMap((c) -> c.stream())
                    .collect(
                        () -> new HashMap<TopicPartition, OffsetAndMetadata>(),
                        (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                        (r, t) -> this.merge(r, t));

                AbstractConsumerForRecordHbase.LOGGER.info(" {} events are sent ", eventsNumber);
                AbstractConsumerForRecordHbase.LOGGER.info("Offset to commit {} ", offsets.toString());
                this.producerForPublishingWfEvents.sendOffsetsToTransaction(offsets, this.groupId);
                this.producerForPublishingWfEvents.commitTransaction();
            } catch (Exception e) {
                AbstractConsumerForRecordHbase.LOGGER.warn("Unexpected error ", e);
                if (e.getCause() instanceof InterruptedException) {
                    break;
                }
            }
        }

    }

    private void merge(Map<TopicPartition, OffsetAndMetadata> r, Map<TopicPartition, OffsetAndMetadata> t) {
        KafkaUtils.merge(r, t);
    }

    private void updateMapOfOffset(Map<TopicPartition, OffsetAndMetadata> mapOfOffset, GenericKafkaManagedObject<?> t) {
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
        AbstractConsumerForRecordHbase.LOGGER.info(
            "EVENT[{}] nb of events sent {}",
            wfe.getDataId(),
            wfe.getEvents()
                .size());
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

    private KafkaManagedHbaseData record(ConsumerRecord<String, T> rec) {
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

    private void startTransactionForRecords(int i) {
        this.producerForPublishingWfEvents.beginTransaction();
        AbstractConsumerForRecordHbase.LOGGER.info("Start processing {} file records ", i);
    }

    @PostConstruct
    protected void init() { this.beanTaskExecutor.execute(() -> this.processIncomingMessages()); }

    protected AbstractConsumerForRecordHbase() {}

    protected abstract Optional<WfEvent> buildEvent(T x);

    protected abstract void postRecord(List<T> v, Class<T> k);

    protected abstract <X extends T> GenericDAO<X> getGenericDAO(Class<X> k);

    public abstract void processIncomingMessages();

}
