package com.gs.photo.workflow.archive.impl;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.common.workflow.TimeMeasurement;
import com.gs.photo.common.workflow.impl.FileUtils;
import com.gs.photo.common.workflow.impl.KafkaUtils;
import com.gs.photo.common.workflow.internal.KafkaManagedFileToProcess;
import com.gs.photo.common.workflow.internal.KafkaManagedObject;
import com.gs.photo.workflow.archive.IBeanArchive;
import com.gs.photo.workflow.archive.IUserGroupInformationAction;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;

@Component
public class BeanArchive implements IBeanArchive {

    private static final int              BUFFER_SIZE = 2 * 1024 * 1024;

    private static Logger                 LOGGER      = LoggerFactory.getLogger(BeanArchive.class);

    @Autowired
    protected IBeanTaskExecutor           beanTaskExecutor;

    @Value("${topic.topicLocalFileCopy}")
    protected String                      topicLocalFileCopy;

    @Value("${wf.hdfs.rootPath}")
    protected String                      rootPath;

    @Value("${topic.topicEvent}")
    protected String                      topicEvent;

    @Autowired
    @Qualifier("producerForPublishingWfEvents")
    protected Producer<String, WfEvents>  producerForPublishingWfEvents;

    @Autowired
    protected ApplicationContext          applicationContext;

    @Autowired
    @Qualifier("userGroupInformationAction")
    protected IUserGroupInformationAction userGroupInformationAction;

    @Autowired
    protected FileUtils                   fileUtils;

    @Autowired
    protected FileSystem                  hdfsFileSystem;

    @Value("${kafka.consumer.batchSizeForParallelProcessingIncomingRecords}")
    protected int                         batchSizeForParallelProcessingIncomingRecords;

    @Value("${group.id}")
    protected String                      groupId;

    @Value("${kafka.pollTimeInMillisecondes}")
    protected int                         kafkaPollTimeInMillisecondes;

    @Override
    @PostConstruct
    public void init() { this.beanTaskExecutor.execute(() -> this.processInputFile()); }

    private void processInputFile() {
        boolean stop = false;
        boolean ready = true;
        try {
            do {
                BeanArchive.LOGGER.info("[ARCHIVE]Start processing file");
                this.producerForPublishingWfEvents.initTransactions();
                Consumer<String, FileToProcess> consumerForTransactionalReadOfFileToProcess = (Consumer<String, FileToProcess>) this.applicationContext
                    .getBean("consumerForTransactionalReadOfFileToProcess");
                consumerForTransactionalReadOfFileToProcess.subscribe(Collections.singleton(this.topicLocalFileCopy));
                try {
                    while (ready) {
                        try (
                            TimeMeasurement timeMeasurement = TimeMeasurement.of(
                                "BATCH_PROCESS_FILES",
                                (d) -> BeanArchive.LOGGER.info(" Perf. metrics {}", d),
                                System.currentTimeMillis())) {
                            Map<TopicPartition, OffsetAndMetadata> offsets = KafkaUtils
                                .toStreamV2(
                                    this.kafkaPollTimeInMillisecondes,
                                    consumerForTransactionalReadOfFileToProcess,
                                    this.batchSizeForParallelProcessingIncomingRecords,
                                    true,
                                    (i) -> this.startTransactionForRecords(i),
                                    timeMeasurement)
                                .map((rec) -> this.processRecord(rec))
                                .map((rec) -> this.sendAck(rec))
                                .collect(
                                    () -> new HashMap<TopicPartition, OffsetAndMetadata>(),
                                    (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                                    (r, t) -> this.merge(r, t));
                            BeanArchive.LOGGER.info("Offset to commit {} ", offsets.toString());
                            this.producerForPublishingWfEvents.sendOffsetsToTransaction(offsets, this.groupId);
                            this.producerForPublishingWfEvents.commitTransaction();
                        }
                    }
                } catch (Throwable e) {

                    if ((e instanceof InterruptedException) || (e.getCause() instanceof InterruptedException)) {
                        stop = true;
                        ready = false;
                        BeanArchive.LOGGER.info("[ARCHIVE]Stop is detected ", e);
                    } else {
                        BeanArchive.LOGGER
                            .error("[ARCHIVE]Error detected, trying to restart {}", ExceptionUtils.getStackTrace(e));
                        stop = false;
                        ready = true;
                    }
                    try {
                        this.producerForPublishingWfEvents.abortTransaction();
                    } catch (Exception e1) {
                        BeanArchive.LOGGER
                            .error("[ARCHIVE]Unable to abort transaction {}", ExceptionUtils.getStackTrace(e1));
                    }
                    try {
                        consumerForTransactionalReadOfFileToProcess.close();
                    } catch (Exception e1) {
                        BeanArchive.LOGGER
                            .error("[ARCHIVE]Unable to close consumer. {}", ExceptionUtils.getStackTrace(e1));
                    }

                }
            } while (!stop);

        } catch (Exception e) {
            BeanArchive.LOGGER.error("[ARCHIVE]Error detected {}", ExceptionUtils.getStackTrace(e));
        }
    }

    private void startTransactionForRecords(int nbOfRecords) {
        BeanArchive.LOGGER.info("Start to process {} records", nbOfRecords);
        this.producerForPublishingWfEvents.beginTransaction();
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
            (f) -> this.topicLocalFileCopy,
            (f) -> f.getKafkaOffset());
    }

    private KafkaManagedFileToProcess sendAck(KafkaManagedFileToProcess rec) {
        rec.getValue()
            .ifPresentOrElse((f) -> {
                final WfEvents eventsToSend = WfEvents.builder()
                    .withDataId(f.getImageId())
                    .withProducer("ARCHIVE")
                    .withEvents(
                        Collections.singleton(
                            rec.createWfEvent(
                                KeysBuilder.TopicCopyKeyBuilder.build(
                                    rec.getValue()
                                        .get()),
                                WfEventStep.WF_STEP_CREATED_FROM_STEP_ARCHIVED_IN_HDFS)))
                    .build();
                this.producerForPublishingWfEvents
                    .send(new ProducerRecord<String, WfEvents>(this.topicEvent, f.getImageId(), eventsToSend));
                BeanArchive.LOGGER.info("[EVENT][{}] End of process file to record in HDFS", f.getImageId());
            },
                () -> BeanArchive.LOGGER.warn(
                    "[EVENT {}] Offset {} of partition {}  is not processed ",
                    rec.getKafkaOffset(),
                    rec.getPartition()));
        return rec;
    }

    private KafkaManagedFileToProcess processRecord(ConsumerRecord<String, FileToProcess> rec) {
        String key = rec.key();
        FileToProcess value = rec.value();

        try
        // (
        // FileSystem hdfsFileSystem = (FileSystem)
        // this.applicationContext.getBean("hdfsFileSystem")

        // )

        {
            BeanArchive.LOGGER.info("[EVENT {}] Process file to record in HDFS {} ", key, value);
            final PrivilegedAction<KafkaManagedFileToProcess> action = () -> {
                try {
                    String importName = value.getImportEvent()
                        .getImportName();
                    importName = StringUtils.isEmpty(importName) ? "DEFAULT_IMPORT" : importName;
                    final Path folderWhereRecord = new Path(new Path(this.rootPath, importName), new Path(key));
                    boolean dirIsCreated = this.hdfsFileSystem.mkdirs(folderWhereRecord);
                    if (dirIsCreated) {
                        final Path hdfsFilePath = this.build(folderWhereRecord, "/" + value.getName());
                        try (
                            FSDataOutputStream fdsOs = this.hdfsFileSystem.create(hdfsFilePath, true)) {
                            try {
                                this.fileUtils.copyRemoteToLocal(value, fdsOs, BeanArchive.BUFFER_SIZE, "/localcache");
                                boolean isDeleted = this.fileUtils.deleteIfLocal(value, "/localcache");
                                if (!isDeleted) {
                                    BeanArchive.LOGGER.warn("[ARCHIVE]File {} is not deleted ", value);
                                }
                            } catch (MissingFileException e) {
                                if (this.hdfsFileSystem.exists(hdfsFilePath)) {
                                    BeanArchive.LOGGER.warn("[ARCHIVE]File {} already exist - {} ", value);
                                } else {
                                    BeanArchive.LOGGER
                                        .error("[ARCHIVE]File {} does not  exist in hdfs - {}", value, hdfsFilePath);
                                }
                            }
                            return KafkaManagedFileToProcess.builder()
                                .withKafkaOffset(rec.offset())
                                .withPartition(rec.partition())
                                .withValue(Optional.of(rec.value()))
                                .build();
                        }
                    } else {
                        BeanArchive.LOGGER.warn("Unable to create the HDFS folder {} ", folderWhereRecord);
                    }
                } catch (IOException e) {
                    BeanArchive.LOGGER
                        .warn("Exception while processing {} : {} ", value, ExceptionUtils.getStackTrace(e));
                    throw new RuntimeException(e);
                }
                BeanArchive.LOGGER.info("[EVENT][{}] End of record file in HDFS {} ", key, value);
                return KafkaManagedFileToProcess.builder()
                    .withKafkaOffset(rec.offset())
                    .withPartition(rec.partition())
                    .withValue(Optional.empty())
                    .build();
            };
            return this.userGroupInformationAction.run(action);
        } catch (IOException e) {
            BeanArchive.LOGGER.warn("Exception while processing {} : {} ", value, ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    private Path build(Path rootPath2, String key) { return Path.mergePaths(rootPath2, new Path(key)); }

}