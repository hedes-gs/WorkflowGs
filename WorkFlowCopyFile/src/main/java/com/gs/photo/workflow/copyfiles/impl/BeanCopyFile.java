package com.gs.photo.workflow.copyfiles.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.common.workflow.TimeMeasurement;
import com.gs.photo.common.workflow.impl.FileUtils;
import com.gs.photo.common.workflow.impl.KafkaUtils;
import com.gs.photo.common.workflow.internal.KafkaManagedFileToProcess;
import com.gs.photo.common.workflow.internal.KafkaManagedObject;
import com.gs.photo.workflow.copyfiles.ApplicationConfig;
import com.gs.photo.workflow.copyfiles.ICopyFile;
import com.gs.photo.workflow.copyfiles.IServicesFile;
import com.gs.photo.workflow.copyfiles.config.SpecificApplicationProperties;
import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;

@Component
public class BeanCopyFile implements ICopyFile {

    protected static Logger                             LOGGER      = LoggerFactory.getLogger(ICopyFile.class);
    private static final int                            BUFFER_COPY = 4 * 1024 * 1024;

    @Autowired
    protected IBeanTaskExecutor                         beanTaskExecutor;

    @Autowired
    protected IKafkaProperties                          kafkaProperties;

    @Autowired
    protected SpecificApplicationProperties             specificApplicationProperties;

    @Autowired
    protected Supplier<Consumer<String, FileToProcess>> consumerSupplierForFileToProcessValue;

    @Autowired
    protected Supplier<Producer<String, HbaseData>>     producerSupplierForTransactionTopicWithFileToProcessOrEventValue;

    @Autowired
    protected IServicesFile                             beanServicesFile;

    @Override
    public void start() { this.beanTaskExecutor.execute(() -> this.processInputFile(ApplicationConfig.CONSUMER_NAME)); }

    protected void processInputFile(String consumerType) {
        BeanCopyFile.LOGGER.info(
            "Starting to process input messages from {} to {}",
            this.kafkaProperties.getTopics()
                .topicDupFilteredFile(),
            this.kafkaProperties.getTopics()
                .topicLocalFileCopy());
        boolean end = false;
        boolean recover = true;
        while (!end) {
            try (
                Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue = this.consumerSupplierForFileToProcessValue
                    .get();
                Producer<String, HbaseData> producerForTopicWithFileToProcessOrEventValue = this.producerSupplierForTransactionTopicWithFileToProcessOrEventValue
                    .get()) {
                producerForTopicWithFileToProcessOrEventValue.initTransactions();
                consumerForTopicWithFileToProcessValue.subscribe(
                    Collections.singleton(
                        this.kafkaProperties.getTopics()
                            .topicDupFilteredFile()));
                while (recover) {
                    try (
                        TimeMeasurement timeMeasurement = TimeMeasurement.of(
                            "BATCH_PROCESS_FILES",
                            (d) -> BeanCopyFile.LOGGER.info(" Perf. metrics {}", d),
                            System.currentTimeMillis())) {
                        producerForTopicWithFileToProcessOrEventValue.beginTransaction();

                        Map<TopicPartition, OffsetAndMetadata> offsets = KafkaUtils
                            .buildParallelKafkaBatchStreamPerTopicAndPartition(
                                producerForTopicWithFileToProcessOrEventValue,
                                consumerForTopicWithFileToProcessValue,
                                this.kafkaProperties.getConsumersType()
                                    .get(consumerType)
                                    .maxPollIntervallMs(),
                                this.kafkaProperties.getConsumersType()
                                    .get(consumerType)
                                    .batchSizeForParallelProcessingIncomingRecords(),
                                true,
                                (i, p) -> this.startTransactionForRecords(i, p))
                            .map((r) -> this.copyToLocal(r))
                            .map(CompletableFuture::join)
                            .collect(Collectors.groupingByConcurrent(k -> k.getObjectKey()))
                            .values()
                            .stream()
                            .map((r) -> this.sendToNext(r, producerForTopicWithFileToProcessOrEventValue))
                            .map((r) -> this.sendEvent(r, producerForTopicWithFileToProcessOrEventValue))
                            .map(CompletableFuture::join)
                            .flatMap(t -> t.stream())
                            .collect(
                                () -> new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>(),
                                (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                                (r, t) -> this.merge(r, t));
                        BeanCopyFile.LOGGER.info("Offset to commit {} ", offsets.toString());
                        producerForTopicWithFileToProcessOrEventValue.sendOffsetsToTransaction(
                            offsets,
                            new ConsumerGroupMetadata(this.kafkaProperties.getConsumersType()
                                .get(consumerType)
                                .groupId()));
                        producerForTopicWithFileToProcessOrEventValue.commitTransaction();
                    } catch (
                        ProducerFencedException |
                        OutOfOrderSequenceException |
                        AuthorizationException e) {
                        BeanCopyFile.LOGGER.error(" Error - closing ", e);
                        recover = false;
                        end = false;
                    } catch (KafkaException e) {
                        // For all other exceptions, just abort the transaction and try again.
                        BeanCopyFile.LOGGER.error(" Error - aborting, trying to recover", e);
                        producerForTopicWithFileToProcessOrEventValue.abortTransaction();
                        end = false;
                        recover = true;
                    } catch (Exception e) {
                        BeanCopyFile.LOGGER.error("Unexpected error - closing  ", e);
                        recover = false;
                        end = false;
                    }
                }

            }
        }

    }

    private void merge(Map<TopicPartition, OffsetAndMetadata> r, Map<TopicPartition, OffsetAndMetadata> t) {
        KafkaUtils.merge(r, t);
    }

    private void updateMapOfOffset(
        Map<TopicPartition, OffsetAndMetadata> mapOfOffset,
        KafkaManagedObject fileToProcess
    ) {
        KafkaUtils.updateMapOfOffset(
            mapOfOffset,
            fileToProcess,
            (f) -> f.getPartition(),
            (f) -> this.kafkaProperties.getTopics()
                .topicDupFilteredFile(),
            (f) -> f.getKafkaOffset());
    }

    private void startTransactionForRecords(
        int i,
        Producer<String, HbaseData> producerForTopicWithFileToProcessOrEventValue
    ) {
        producerForTopicWithFileToProcessOrEventValue.beginTransaction();
        BeanCopyFile.LOGGER.info("Start processing {} file records ", i);
    }

    private void endTransactionForRecords(Integer i) { BeanCopyFile.LOGGER.info("End processing {} file records ", i); }

    private CompletableFuture<Collection<KafkaManagedFileToProcess>> sendEvent(
        CompletableFuture<Collection<KafkaManagedFileToProcess>> cf,
        Producer<String, HbaseData> producerForTopicWithFileToProcessOrEventValue
    ) {
        return cf.thenApply(collectionOfKmft -> {
            collectionOfKmft.forEach(f -> {
                f.getValue()
                    .ifPresentOrElse((origin) -> {
                        Future<RecordMetadata> result = producerForTopicWithFileToProcessOrEventValue.send(
                            new ProducerRecord<String, HbaseData>(this.kafkaProperties.getTopics()
                                .topicEvent(),
                                f.getHashKey(),
                                WfEvents.builder()
                                    .withDataId(f.getImageKey())
                                    .withProducer("LOCAL_COPY")
                                    .withEvents(Collections.singleton(f.createWfEvent()))
                                    .build()));
                        RecordMetadata data;
                        try {
                            data = result.get();
                            BeanCopyFile.LOGGER.info(
                                "[EVENT][{}] Recorded file [{}] at [part={},offset={},topic={},time={}]",
                                origin.getDataId(),
                                f,
                                data.partition(),
                                data.offset(),
                                data.topic(),
                                data.timestamp());
                        } catch (
                            InterruptedException |
                            ExecutionException e) {
                            BeanCopyFile.LOGGER.error(" Interrupted... stopping process", e);
                            throw new RuntimeException(e);
                        }
                    },
                        () -> BeanCopyFile.LOGGER.error(
                            " Unable to process offset {} of partition {} of topic {} ",
                            f.getKafkaOffset(),
                            f.getPartition(),
                            this.kafkaProperties.getTopics()
                                .topicDupFilteredFile()));
            });
            return new ArrayList(collectionOfKmft);
        });
    }

    private CompletableFuture<Collection<KafkaManagedFileToProcess>> sendToNext(
        Collection<KafkaManagedFileToProcess> cf,
        Producer<String, HbaseData> producerForTopicWithFileToProcessOrEventValue
    ) {
        return CompletableFuture.supplyAsync(() -> {
            cf.forEach(f -> {
                f.getValue()
                    .ifPresentOrElse((origin) -> {
                        Future<RecordMetadata> result = producerForTopicWithFileToProcessOrEventValue.send(
                            new ProducerRecord<String, HbaseData>(this.kafkaProperties.getTopics()
                                .topicLocalFileCopy(), f.getHashKey(), origin));
                        RecordMetadata data;
                        try {
                            data = result.get();
                            BeanCopyFile.LOGGER.info(
                                "[EVENT][{}] Recorded file [{}] at [part={},offset={},topic={},time={}]",
                                origin.getDataId(),
                                f,
                                data.partition(),
                                data.offset(),
                                data.topic(),
                                data.timestamp());
                        } catch (
                            InterruptedException |
                            ExecutionException e) {
                            BeanCopyFile.LOGGER.error(" Interrupted... stopping process", e);
                            throw new RuntimeException(e);
                        }
                    },
                        () -> BeanCopyFile.LOGGER.error(
                            " Unable to process offset {} of partition {} of topic {} ",
                            f.getKafkaOffset(),
                            f.getPartition(),
                            this.kafkaProperties.getTopics()
                                .topicDupFilteredFile()));

            });
            return cf;
        });
    }

    private CompletableFuture<KafkaManagedFileToProcess> copyToLocal(ConsumerRecord<String, FileToProcess> rec) {
        return CompletableFuture.supplyAsync(() -> {

            try {
                InetAddress ip = InetAddress.getLocalHost();
                String hostname = ip.getHostName();
                FileToProcess origin = rec.value();
                Path currentFolder = this.beanServicesFile.getCurrentFolderInWhichCopyShouldBeDone(
                    Paths.get(this.specificApplicationProperties.getRepository()));
                Path destPath = Paths.get(this.specificApplicationProperties.getRepository())
                    .resolve(
                        Paths.get(
                            currentFolder.toString(),
                            rec.key() + "-" + rec.value()
                                .getName()));

                BeanCopyFile.LOGGER.info(
                    "[EVENT][{}] File {} copying to place {} ",
                    rec.key(),
                    rec.value(),
                    destPath.toAbsolutePath()
                        .toString());
                if (Files.exists(destPath)) {
                    BeanCopyFile.LOGGER.warn(
                        "[EVENT][{}] File already created when processing {}: {}, overwriting it...",
                        rec.key(),
                        rec.value(),
                        destPath.toAbsolutePath()
                            .toString());
                }

                this.beanServicesFile.copyRemoteToLocal(rec.value(), destPath.toFile());
                BeanCopyFile.LOGGER.info(
                    "[EVENT][{}] File {} has been copied in local at place {} ",
                    rec.key(),
                    rec.value(),
                    destPath.toAbsolutePath()
                        .toString());
                return KafkaManagedFileToProcess.builder()
                    .withHashKey(rec.key())
                    .withValue(
                        Optional.of(
                            FileToProcess.builder()
                                .withCompressedFile(origin.isCompressedFile())
                                .withImportEvent(origin.getImportEvent())
                                .withImageId(origin.getImageId())
                                .withDataId(origin.getDataId())
                                .withIsLocal(true)
                                .withUrl(
                                    "nfs://" + hostname + ":/"
                                        + Paths.get(this.specificApplicationProperties.getRepository()) + "/"
                                        + destPath.toAbsolutePath()
                                            .subpath(1, destPath.getNameCount())
                                            .toString())
                                .withName(
                                    rec.key() + "-" + FileUtils.getSimpleNameFromUrl(
                                        rec.value()
                                            .getUrl()))
                                .build()))
                    .withKafkaOffset(rec.offset())
                    .withPartition(rec.partition())
                    .build();
            } catch (IOException e) {
                BeanCopyFile.LOGGER.error(
                    "[EVENT][{}] Unexpected error when processing value {} : {} ",
                    rec.key(),
                    rec.value(),
                    ExceptionUtils.getStackTrace(e));
            }
            return KafkaManagedFileToProcess.builder()
                .withHashKey(rec.key())
                .withKafkaOffset(rec.offset())
                .withPartition(rec.partition())
                .withValue(Optional.of(rec.value()))
                .build();
        });

    }
}