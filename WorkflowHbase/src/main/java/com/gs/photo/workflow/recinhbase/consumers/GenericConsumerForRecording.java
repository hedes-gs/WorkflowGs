package com.gs.photo.workflow.recinhbase.consumers;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.gs.instrumentation.KafkaSpy;
import com.gs.instrumentation.TimedBean;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.common.workflow.KafkaConsumerProperties;
import com.gs.photo.common.workflow.impl.KafkaUtils;
import com.gs.photo.workflow.recinhbase.business.IProcessKafkaEvent;
import com.gs.photo.workflow.recinhbase.consumers.config.SpecificApplicationProperties;
import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEvents;

import io.micrometer.core.annotation.Timed;

@KafkaSpy
@TimedBean
public class GenericConsumerForRecording<R extends WfEvents, U extends WfEventRecorded, T extends HbaseData>
    implements IGenericConsumerForRecording<R, T> {
    protected static Logger                 LOGGER = LoggerFactory.getLogger(GenericConsumerForRecording.class);
    protected IProcessKafkaEvent<U, T>      processKafkaEvent;
    protected Supplier<Consumer<String, T>> kafkaConsumerSupplier;
    protected Supplier<Producer<String, R>> kafkaProducerSupplier;
    protected ThreadPoolTaskExecutor        threadPoolTaskExecutor;
    protected SpecificApplicationProperties specificApplicationProperties;
    protected KafkaConsumerProperties       kafkaConsumerProperties;
    protected IKafkaProperties              kafkaProperties;
    protected Class<T>                      objectClassToConsum;
    protected String                        topic;

    protected record EventsAndConsumerRecords<R, T>(
        Collection<R> events,
        Collection<ConsumerRecord<String, T>> kafkaRecords
    ) {}

    @Override
    public void start() {
        this.threadPoolTaskExecutor.execute(() -> this.processKafkaEvent(this.objectClassToConsum, this.topic));
    }

    protected void processKafkaEvent(Class<T> cl, String topic) {
        GenericConsumerForRecording.LOGGER.info("Starting an instance of GenericConsumerForRecording..");
        boolean end = false;
        boolean recover = true;
        while (recover) {
            try (
                Consumer<String, T> kafkaConsumer = this.kafkaConsumerSupplier.get();
                Producer<String, R> kafkaProducer = this.kafkaProducerSupplier.get()) {
                kafkaConsumer.subscribe(Collections.singleton(topic));
                kafkaProducer.initTransactions();
                while (!end) {
                    try {
                        this.processsRecords(cl, kafkaConsumer, kafkaProducer);
                    } catch (
                        ProducerFencedException |
                        OutOfOrderSequenceException |
                        AuthorizationException e) {
                        GenericConsumerForRecording.LOGGER.error(" Error - closing ", e);
                        recover = false;
                        end = true;
                    } catch (KafkaException e) {
                        // For all other exceptions, just abort the transaction and try again.
                        GenericConsumerForRecording.LOGGER.error(" Error - aborting, trying to recover", e);
                        kafkaProducer.abortTransaction();
                        end = true;
                        recover = true;
                    } catch (Exception e) {
                        if (!(e.getCause() instanceof InterruptedException)) {
                            GenericConsumerForRecording.LOGGER.error("Unexpected error - closing  ", e);
                        }

                        end = true;
                        recover = false;
                    }
                }
            }
        }

    }

    @KafkaSpy
    @Timed
    private void processsRecords(Class<T> cl, Consumer<String, T> kafkaConsumer, Producer<String, R> kafkaProducer) {
        Map<TopicPartition, OffsetAndMetadata> mapOfOffset = KafkaUtils
            .buildParallelKafkaBatchStream(
                kafkaProducer,
                kafkaConsumer,
                this.kafkaConsumerProperties.maxPollIntervallMs(),
                this.kafkaConsumerProperties.batchSizeForParallelProcessingIncomingRecords(),
                true,
                (i, p) -> this.startRecordsProcessing(i, p))
            .map(collection -> this.asyncProcess(cl, collection))
            .map(CompletableFuture::join)
            .map(t -> this.asyncSendEvent(kafkaProducer, t))
            .map(CompletableFuture::join)
            .flatMap((c) -> c.stream())
            .collect(
                () -> new HashMap<TopicPartition, OffsetAndMetadata>(),
                (o, t) -> this.updateMapOfOffset(o, t),
                (r, t) -> this.merge(r, t));
        kafkaProducer.sendOffsetsToTransaction(mapOfOffset, kafkaConsumer.groupMetadata());
        kafkaProducer.commitTransaction();
    }

    private Object startRecordsProcessing(Integer i, Producer<String, R> kafkaProducer) {
        GenericConsumerForRecording.LOGGER
            .info("[{}] Start processing {} records", this.objectClassToConsum.getSimpleName(), i);
        kafkaProducer.beginTransaction();
        return null;
    }

    private void merge(Map<TopicPartition, OffsetAndMetadata> r, Map<TopicPartition, OffsetAndMetadata> t) {
        KafkaUtils.merge(r, t);
    }

    private void updateMapOfOffset(Map<TopicPartition, OffsetAndMetadata> mapOfOffset, ConsumerRecord<String, T> t) {
        KafkaUtils.updateMapOfOffset(mapOfOffset, t, (f) -> f.partition(), (f) -> f.topic(), (f) -> f.offset());
    }

    private CompletableFuture<EventsAndConsumerRecords<U, T>> asyncProcess(
        Class<T> cl,
        Collection<ConsumerRecord<String, T>> collection
    ) {

        return this.processKafkaEvent.asyncProcess(
            cl,
            collection.stream()
                .map(t -> t.value())
                .toList())
            .thenApply(x -> this.createEventsAndConsumerRecords(collection, x));
    }

    private EventsAndConsumerRecords<U, T> createEventsAndConsumerRecords(
        Collection<ConsumerRecord<String, T>> collection,
        Collection<U> x
    ) {
        return new EventsAndConsumerRecords<>(x, collection);
    }

    private CompletableFuture<Collection<ConsumerRecord<String, T>>> asyncSendEvent(
        Producer<String, R> kafkaProducer,
        EventsAndConsumerRecords<U, T> eventsAndConsumerRecords
    ) {
        return CompletableFuture
            .supplyAsync(() -> { return this.doSendEvents(kafkaProducer, eventsAndConsumerRecords); });
    }

    @Timed
    private Collection<ConsumerRecord<String, T>> doSendEvents(
        Producer<String, R> kafkaProducer,
        EventsAndConsumerRecords<U, T> eventsAndConsumerRecords
    ) {
        eventsAndConsumerRecords.events.stream()
            .collect(Collectors.groupingBy(t -> t.getImgId()))
            .entrySet()
            .stream()
            .forEach(e -> {
                kafkaProducer.send(
                    new ProducerRecord<>(this.kafkaProperties.getTopics()
                        .topicEvent(), e.getKey(), this.toEvents(e.getKey(), e.getValue())));
            });
        return eventsAndConsumerRecords.kafkaRecords;
    }

    private R toEvents(String key, List<? extends WfEventRecorded> t) {
        return (R) WfEvents.builder()
            .withDataId(key)
            .withProducer(this.specificApplicationProperties.getProducerName())
            .withRecordedEvents(t)
            .build();
    }

    public GenericConsumerForRecording(
        Class<T> objectClassToConsum,
        String topic,
        IProcessKafkaEvent<U, T> processKafkaEvent,
        Supplier<Consumer<String, T>> kafkaConsumerSupplierForRecordingImage,
        Supplier<Producer<String, R>> kafkaProducerSupplier,
        ThreadPoolTaskExecutor threadPoolTaskExecutor,
        SpecificApplicationProperties specificApplicationProperties,
        KafkaConsumerProperties kafkaConsumerProperties,
        IKafkaProperties kafkaProperties
    ) {
        this.objectClassToConsum = objectClassToConsum;
        this.topic = topic;
        this.processKafkaEvent = processKafkaEvent;
        this.kafkaConsumerSupplier = kafkaConsumerSupplierForRecordingImage;
        this.kafkaProducerSupplier = kafkaProducerSupplier;
        this.threadPoolTaskExecutor = threadPoolTaskExecutor;
        this.specificApplicationProperties = specificApplicationProperties;
        this.kafkaConsumerProperties = kafkaConsumerProperties;
        this.kafkaProperties = kafkaProperties;
    }

    public static <R extends WfEvents, U extends WfEventRecorded, T extends HbaseData> GenericConsumerForRecording<R, U, T> of(
        Class<T> objectClassToConsum,
        String topic,
        IProcessKafkaEvent<U, T> processKafkaEvent,
        Supplier<Consumer<String, T>> kafkaConsumerSupplierForRecordingImage,
        Supplier<Producer<String, R>> kafkaProducerSupplierForSendingEvent,
        ThreadPoolTaskExecutor threadPoolTaskExecutor,
        SpecificApplicationProperties specificApplicationProperties,
        KafkaConsumerProperties kafkaConsumerProperties,
        IKafkaProperties kafkaProperties
    ) {
        return new GenericConsumerForRecording<>(objectClassToConsum,
            topic,
            processKafkaEvent,
            kafkaConsumerSupplierForRecordingImage,
            kafkaProducerSupplierForSendingEvent,
            threadPoolTaskExecutor,
            specificApplicationProperties,
            kafkaConsumerProperties,
            kafkaProperties);
    }

}
