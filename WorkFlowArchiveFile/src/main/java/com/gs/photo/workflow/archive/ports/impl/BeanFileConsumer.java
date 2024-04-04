package com.gs.photo.workflow.archive.ports.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.commons.lang3.exception.ExceptionUtils;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.gs.instrumentation.KafkaSpy;
import com.gs.instrumentation.TimedBean;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.common.workflow.impl.KafkaUtils;
import com.gs.photo.workflow.archive.ApplicationConfig;
import com.gs.photo.workflow.archive.business.IBeanArchive;
import com.gs.photo.workflow.archive.ports.IBeanFileConsumer;
import com.gs.photo.workflow.archive.ports.IFileSystem;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEventRecorded.RecordedEventType;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;

import io.micrometer.core.annotation.Timed;

@Component
@TimedBean
@KafkaSpy
public class BeanFileConsumer implements IBeanFileConsumer {

    private static final int                            BUFFER_SIZE = 2 * 1024 * 1024;

    private static Logger                               LOGGER      = LoggerFactory.getLogger(BeanFileConsumer.class);

    @Autowired
    protected ThreadPoolTaskExecutor                    beanTaskExecutor;

    @Autowired
    protected IBeanArchive<FileToProcess>               beanArchive;

    @Autowired
    protected Supplier<IFileSystem>                     supplierForHdfsFileSystem;

    @Autowired
    protected Supplier<Consumer<String, FileToProcess>> kafkaConsumerFactoryForFileToProcessValue;

    @Autowired
    protected Supplier<Producer<String, WfEvents>>      producerSupplierForTransactionPublishingOnExifTopic;

    @Autowired
    protected IKafkaProperties                          kafkaProperties;

    @Override
    public void start() {
        this.beanTaskExecutor.execute(() -> this.processInputFile(ApplicationConfig.CONSUMER_TYPE_NAME));
    }

    private void processInputFile(final String consumerTypeName) {
        boolean end = false;
        boolean recover = true;
        while (true) {
            try (
                IFileSystem hdfsFileSystem = this.supplierForHdfsFileSystem.get();
                Consumer<String, FileToProcess> consumerForTransactionalReadOfFileToProcess = this.kafkaConsumerFactoryForFileToProcessValue
                    .get();
                Producer<String, WfEvents> producerForPublishingWfEvents = this.producerSupplierForTransactionPublishingOnExifTopic
                    .get()) {
                while (!end) {
                    BeanFileConsumer.LOGGER.info("[ARCHIVE]Start processing file");
                    producerForPublishingWfEvents.initTransactions();
                    consumerForTransactionalReadOfFileToProcess.subscribe(
                        Collections.singleton(
                            this.kafkaProperties.getTopics()
                                .topicLocalFileCopy()));
                    try {
                        while (recover) {

                            try {
                                this.processRecords(
                                    consumerTypeName,
                                    hdfsFileSystem,
                                    consumerForTransactionalReadOfFileToProcess,
                                    producerForPublishingWfEvents);
                            } catch (
                                ProducerFencedException |
                                OutOfOrderSequenceException |
                                AuthorizationException e) {
                                BeanFileConsumer.LOGGER.error(" Error - closing ", e);
                                recover = false;
                                end = true;
                            } catch (KafkaException e) {
                                // For all other exceptions, just abort the transaction and try again.
                                BeanFileConsumer.LOGGER.error(" Error - aborting, trying to recover", e);
                                producerForPublishingWfEvents.abortTransaction();
                                end = true;
                                recover = true;
                            } catch (Exception e) {
                                if (!(e.getCause() instanceof InterruptedException)) {
                                    BeanFileConsumer.LOGGER.error("Unexpected error - closing  ", e);
                                }
                                end = true;
                                recover = false;
                            }
                        }
                    } catch (Throwable e) {
                        end = true;
                        recover = false;

                    }
                }

            } catch (Exception e) {
                BeanFileConsumer.LOGGER.error("[ARCHIVE]Error detected {}", ExceptionUtils.getStackTrace(e));
            }
        }
    }

    @Timed
    @KafkaSpy
    private void processRecords(
        String consumerTypeName,
        IFileSystem hdfsFileSystem,
        Consumer<String, FileToProcess> consumerForTransactionalReadOfFileToProcess,
        Producer<String, WfEvents> producerForPublishingWfEvents
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsets = KafkaUtils
            .buildParallelKafkaBatchStreamPerTopicAndPartition(
                producerForPublishingWfEvents,
                consumerForTransactionalReadOfFileToProcess,
                this.kafkaProperties.getConsumersType()
                    .get(consumerTypeName)
                    .maxPollIntervallMs(),
                this.kafkaProperties.getConsumersType()
                    .get(consumerTypeName)
                    .batchSizeForParallelProcessingIncomingRecords(),
                true,
                (i, p) -> this.startTransactionForRecords(i, p))
            .map((rec) -> this.asyncProcessRecord(rec, hdfsFileSystem))
            .map((rec) -> this.asyncSendEvent(producerForPublishingWfEvents, rec))
            .map(CompletableFuture::join)
            .collect(
                () -> new HashMap<TopicPartition, OffsetAndMetadata>(),
                (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                (r, t) -> this.merge(r, t));
        BeanFileConsumer.LOGGER.info("Offset to commit {} ", offsets.toString());
        producerForPublishingWfEvents
            .sendOffsetsToTransaction(offsets, consumerForTransactionalReadOfFileToProcess.groupMetadata());
        producerForPublishingWfEvents.commitTransaction();
    }

    private void startTransactionForRecords(int nbOfRecords, Producer<String, WfEvents> producer) {
        BeanFileConsumer.LOGGER.info("Start to process {} records", nbOfRecords);
        producer.beginTransaction();
    }

    private void merge(Map<TopicPartition, OffsetAndMetadata> r, Map<TopicPartition, OffsetAndMetadata> t) {
        KafkaUtils.merge(r, t);
    }

    private void updateMapOfOffset(
        Map<TopicPartition, OffsetAndMetadata> mapOfOffset,
        ConsumerRecord<String, FileToProcess> cr
    ) {
        KafkaUtils.updateMapOfOffset(
            mapOfOffset,
            cr,
            (f) -> f.partition(),
            (f) -> this.kafkaProperties.getTopics()
                .topicLocalFileCopy(),
            (f) -> f.offset());
    }

    private CompletableFuture<ConsumerRecord<String, FileToProcess>> asyncSendEvent(
        Producer<String, WfEvents> producerForPublishingWfEvents,
        CompletableFuture<ConsumerRecord<String, FileToProcess>> cf
    ) {
        return cf.thenApply(r -> { return this.doSendEvents(producerForPublishingWfEvents, r); });
    }

    @Timed
    private ConsumerRecord<String, FileToProcess> doSendEvents(
        Producer<String, WfEvents> producerForPublishingWfEvents,
        ConsumerRecord<String, FileToProcess> r
    ) {
        BeanFileConsumer.LOGGER.info(
            "[EVENT][{}] End of process file to record in HDFS",
            r.value()
                .getImageId());
        final WfEvents eventsToSend = WfEvents.builder()
            .withDataId(
                r.value()
                    .getImageId())
            .withProducer("ARCHIVE")
            .withEvents(
                Collections.singleton(
                    WfEventRecorded.builder()
                        .withRecordedEventType(RecordedEventType.ARCHIVE)
                        .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_ARCHIVED_IN_HDFS)
                        .build()))
            .build();
        producerForPublishingWfEvents.send(
            new ProducerRecord<String, WfEvents>(this.kafkaProperties.getTopics()
                .topicEvent(),
                r.value()
                    .getImageId(),
                eventsToSend));
        return r;
    }

    private CompletableFuture<ConsumerRecord<String, FileToProcess>> asyncProcessRecord(
        ConsumerRecord<String, FileToProcess> rec,
        IFileSystem hdfsFileSystem
    ) {
        return this.beanArchive.asyncArchiveFile(hdfsFileSystem, rec.value())
            .thenApply(cf -> rec);
    }

}