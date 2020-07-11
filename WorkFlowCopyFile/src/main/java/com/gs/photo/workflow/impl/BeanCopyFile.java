package com.gs.photo.workflow.impl;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.ICopyFile;
import com.gs.photo.workflow.IServicesFile;
import com.gs.photo.workflow.TimeMeasurement;
import com.gs.photo.workflow.internal.KafkaManagedFileToProcess;
import com.gs.photo.workflow.internal.KafkaManagedObject;
import com.workflow.model.files.FileToProcess;

@Component
public class BeanCopyFile implements ICopyFile {

    protected static Logger                   LOGGER      = LoggerFactory.getLogger(ICopyFile.class);
    private static final int                  BUFFER_COPY = 4 * 1024 * 1024;

    @Autowired
    protected IBeanTaskExecutor               beanTaskExecutor;

    @Value("${copy.repository}")
    protected String                          repository;

    @Value("${group.id}")
    protected String                          groupId;

    @Value("${topic.topicDupFilteredFile}")
    protected String                          topicDupDilteredFile;

    @Value("${topic.topicLocalFileCopy}")
    protected String                          topicLocalFileCopy;

    protected Path                            repositoryPath;

    @Autowired
    @Qualifier("consumerForTopicWithFileToProcessValue")
    protected Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue;

    @Autowired
    @Qualifier("producerForTopicWithFileToProcessValue")
    protected Producer<String, FileToProcess> producerForTopicWithFileToProcessValue;

    @Autowired
    protected IServicesFile                   beanServicesFile;

    @Autowired
    protected FileUtils                       fileUtils;

    @Value("${kafka.consumer.batchSizeForParallelProcessingIncomingRecords}")
    protected int                             batchSizeForParallelProcessingIncomingRecords;
    private String                            hostname;

    @Value("${kafka.pollTimeInMillisecondes}")
    protected int                             kafkaPollTimeInMillisecondes;

    @PostConstruct
    public void init() {
        this.repositoryPath = Paths.get(this.repository);
        this.beanTaskExecutor.execute(() -> this.processInputFile());
        try {
            InetAddress ip = InetAddress.getLocalHost();
            this.hostname = ip.getHostName();
        } catch (UnknownHostException e) {
            BeanCopyFile.LOGGER.error("Error", e);
            throw new RuntimeException(e);
        }
    }

    private void processInputFile() {
        BeanCopyFile.LOGGER.info(
            "Starting to process input messages from {} to {}",
            this.topicDupDilteredFile,
            this.topicLocalFileCopy);
        this.consumerForTopicWithFileToProcessValue.subscribe(Collections.singleton(this.topicDupDilteredFile));
        this.producerForTopicWithFileToProcessValue.initTransactions();
        while (true) {
            try (
                TimeMeasurement timeMeasurement = TimeMeasurement.of(
                    "BATCH_PROCESS_FILES",
                    (d) -> BeanCopyFile.LOGGER.info(" Perf. metrics {}", d),
                    System.currentTimeMillis())) {
                Stream<ConsumerRecord<String, FileToProcess>> filesToCopyStream = KafkaUtils.toStreamV2(
                    this.kafkaPollTimeInMillisecondes,
                    this.consumerForTopicWithFileToProcessValue,
                    this.batchSizeForParallelProcessingIncomingRecords,
                    true,
                    (i) -> this.startTransactionForRecords(i),
                    timeMeasurement);
                Map<TopicPartition, OffsetAndMetadata> offsets = filesToCopyStream.map((r) -> this.copyToLocal(r))
                    .map((r) -> this.sendToNext(r))
                    .collect(
                        () -> new HashMap<TopicPartition, OffsetAndMetadata>(),
                        (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                        (r, t) -> this.merge(r, t));
                BeanCopyFile.LOGGER.info("Offset to commit {} ", offsets.toString());
                BeanCopyFile.this.producerForTopicWithFileToProcessValue
                    .sendOffsetsToTransaction(offsets, BeanCopyFile.this.groupId);
                BeanCopyFile.this.producerForTopicWithFileToProcessValue.commitTransaction();
            } catch (IOException e) {
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
            (f) -> this.topicDupDilteredFile,
            (f) -> f.getKafkaOffset());
    }

    private void startTransactionForRecords(int i) {
        BeanCopyFile.this.producerForTopicWithFileToProcessValue.beginTransaction();
        BeanCopyFile.LOGGER.info("Start processing {} file records ", i);
    }

    private KafkaManagedFileToProcess sendToNext(KafkaManagedFileToProcess f) {
        f.getOrigin()
            .ifPresentOrElse((origin) -> {
                Future<RecordMetadata> result = this.producerForTopicWithFileToProcessValue
                    .send(new ProducerRecord<String, FileToProcess>(this.topicLocalFileCopy, f.getHashKey(), origin));
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
                    this.topicDupDilteredFile));
        return f;
    }

    private KafkaManagedFileToProcess copyToLocal(ConsumerRecord<String, FileToProcess> rec) {
        FileToProcess origin = rec.value();
        try {
            String currentFolder = this.beanServicesFile.getCurrentFolderInWhichCopyShouldBeDone(this.repositoryPath);
            Path destPath = this.repositoryPath.resolve(
                Paths.get(
                    currentFolder,
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

            try (
                OutputStream os = new FileOutputStream(destPath.toFile())) {
                this.fileUtils.copyRemoteToLocal(rec.value(), os, BeanCopyFile.BUFFER_COPY);
                ;
            }
            BeanCopyFile.LOGGER.info(
                "[EVENT][{}] File {} has been copied in local at place {} ",
                rec.key(),
                rec.value(),
                destPath.toAbsolutePath()
                    .toString());
            return KafkaManagedFileToProcess.builder()
                .withHashKey(rec.key())
                .withOrigin(
                    Optional.of(
                        FileToProcess.builder()
                            .withCompressedFile(origin.isCompressedFile())
                            .withDataId(origin.getDataId())
                            .withHost(this.hostname)
                            .withPath(
                                destPath.toAbsolutePath()
                                    .toString())
                            .withName(
                                rec.key() + "-" + rec.value()
                                    .getName())
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
            .build();
    }

}
