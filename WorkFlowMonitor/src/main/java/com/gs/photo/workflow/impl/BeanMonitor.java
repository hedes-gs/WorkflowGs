package com.gs.photo.workflow.impl;

import java.util.List;
import java.util.Optional;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.common.workflow.TimeMeasurement.Step;
import com.gs.photo.common.workflow.internal.GenericKafkaManagedObject;
import com.gs.photo.common.workflow.internal.KafkaManagedWfEvents;
import com.gs.photo.workflow.IMonitor;
import com.gs.photo.workflow.daos.impl.CacheNodeDAOV2;
import com.workflow.model.events.WfEvents;

public class BeanMonitor implements IMonitor {

    protected static final Logger        LOGGER = LoggerFactory.getLogger(BeanMonitor.class);

    @Autowired
    protected Consumer<String, WfEvents> consumerOfWfEventsWithStringKey;
    @Autowired
    protected Producer<String, String>   producerForPublishingOnStringTopic;
    @Autowired
    protected IBeanTaskExecutor          beanTaskExecutor;
    @Value("${topic.topicEvent}")
    private String                       topicEvent;
    @Value("${topic.topicProcessedFile}")
    private String                       topicProcessedFile;
    @Value("${topic.topicFullyProcessedImage}")
    private String                       topicFullyProcessedImage;

    @Autowired
    protected CacheNodeDAOV2             cacheNodeDAO;
    @Value("${kafka.pollTimeInMillisecondes}")
    protected int                        kafkaPollTimeInMillisecondes;

    @Value("${kafka.consumer.batchSizeForParallelProcessingIncomingRecords}")
    protected int                        batchSizeForParallelProcessingIncomingRecords;

    @PostConstruct
    public void init() { this.beanTaskExecutor.execute(() -> this.processInputFile()); }

    protected void processInputFile() {
        /*
         * this.consumerOfWfEventsWithStringKey.subscribe(Collections.singleton(this.
         * topicEvent)); while (true) { try ( TimeMeasurement timeMeasurement =
         * TimeMeasurement .of("BATCH_PROCESS_FILES", (d) -> this.dummy(d),
         * System.currentTimeMillis())) { Stream<ConsumerRecord<String, WfEvents>>
         * eventStreams = KafkaUtils.toStreamV2( 200,
         * this.consumerOfWfEventsWithStringKey,
         * this.batchSizeForParallelProcessingIncomingRecords, true, (i) ->
         * this.startTransactionForRecords(i), timeMeasurement);
         * List<GenericKafkaManagedObject<?>> eventsToSend = eventStreams .peek( (rec)
         * -> BeanMonitor.LOGGER.info( "Starting to process evts {} for img id {} ",
         * rec.value() .getEvents() .size(), rec.key())) .map((rec) ->
         * this.processEvent(rec)) .filter((evt) ->
         * this.cacheNodeDAO.allEventsAreReceivedForAnImage(evt.getImageKey()))
         * .peek((evt) -> BeanMonitor.LOGGER.info("Img Id {} is complete ", evt))
         * .map((evts) -> this.send(evts)) .collect(Collectors.toList());
         *
         * Map<TopicPartition, OffsetAndMetadata> offsets = eventsToSend.stream()
         * .flatMap((c) -> c.stream()) .collect( () -> new
         * ConcurrentHashMap<TopicPartition, OffsetAndMetadata>(), (mapOfOffset, t) ->
         * this.updateMapOfOffset(mapOfOffset, t), (r, t) -> this.merge(r, t));
         *
         * BeanMonitor.LOGGER.info("Offset to commit {} ", offsets.toString());
         * this.producerForPublishingOnStringTopic.sendOffsetsToTransaction(offsets,
         * this.groupId); this.producerForPublishingOnStringTopic.commitTransaction();
         *
         * } catch (IOException e) { e.printStackTrace(); } }
         */

    }

    private GenericKafkaManagedObject<?> send(GenericKafkaManagedObject<?> evts) {
        this.producerForPublishingOnStringTopic
            .send(new ProducerRecord<>(this.topicFullyProcessedImage, evts.getImageKey()));
        return evts;
    }

    private GenericKafkaManagedObject<?> processEvent(ConsumerRecord<String, WfEvents> record) {
        this.cacheNodeDAO.addOrCreate(record.key(), record.value());
        return KafkaManagedWfEvents.builder()
            .withKafkaOffset(record.offset())
            .withPartition(record.partition())
            .withWfEvents(Optional.of(record.value()))
            .build();

    }

    private Object dummy(List<Step> d) { // TODO Auto-generated method stub
        return null;
    }

    private Object startTransactionForRecords(int i) { // TODO Auto-generated method stub
        return null;
    }

}
