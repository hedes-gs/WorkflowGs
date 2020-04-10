package com.gs.photo.workflow.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.StreamSupport;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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

@Component
public class BeanCopyFile implements ICopyFile {

    protected static Logger            LOGGER      = LoggerFactory.getLogger(ICopyFile.class);
    private static final int           BUFFER_COPY = 4 * 1024 * 1024;

    @Autowired
    protected IBeanTaskExecutor        beanTaskExecutor;

    @Value("${copy.repository}")
    protected String                   repository;

    @Value("${copy.group.id}")
    protected String                   copyGroupId;

    @Value("${topic.topicDupDilteredFile}")
    protected String                   topicDupDilteredFile;

    @Value("${topic.topicCopyOtherFile}")
    protected String                   topicFile;

    protected Path                     repositoryPath;

    @Autowired
    @Qualifier("consumerForTransactionalCopyForTopicWithStringKey")
    protected Consumer<String, String> consumerForTransactionalCopyForTopicWithStringKey;

    @Autowired
    @Qualifier("producerForPublishingInModeTransactionalOnStringTopic")
    protected Producer<String, String> producerForPublishingOnStringTopic;

    @Autowired
    protected IServicesFile            beanServicesFile;

    @PostConstruct
    public void init() {
        this.repositoryPath = Paths.get(this.repository);
        this.beanTaskExecutor.execute(() -> this.processInputFile());
    }

    private void processInputFile() {
        BeanCopyFile.LOGGER
            .info("Starting to process input messages from {} to {}", this.topicDupDilteredFile, this.topicFile);
        this.consumerForTransactionalCopyForTopicWithStringKey
            .subscribe(Collections.singleton(this.topicDupDilteredFile));
        this.producerForPublishingOnStringTopic.initTransactions();
        Iterable<ConsumerRecord<String, String>> iterable = () -> new Iterator<ConsumerRecord<String, String>>() {
            Iterator<ConsumerRecord<String, String>> records;
            Map<TopicPartition, OffsetAndMetadata>   currentOffset;

            @Override
            public boolean hasNext() {
                if ((this.records == null) || !this.records.hasNext()) {
                    if (this.records != null) {
                        BeanCopyFile.this.producerForPublishingOnStringTopic
                            .sendOffsetsToTransaction(this.currentOffset, BeanCopyFile.this.copyGroupId);
                        BeanCopyFile.this.producerForPublishingOnStringTopic.commitTransaction();
                    }
                    ConsumerRecords<String, String> nextRecords;
                    do {
                        nextRecords = BeanCopyFile.this.consumerForTransactionalCopyForTopicWithStringKey
                            .poll(Duration.ofMillis(250));
                    } while (nextRecords.isEmpty());
                    this.currentOffset = BeanCopyFile.this.currentOffsets(
                        BeanCopyFile.this.consumerForTransactionalCopyForTopicWithStringKey,
                        nextRecords);
                    BeanCopyFile.this.producerForPublishingOnStringTopic.beginTransaction();
                    this.records = nextRecords.iterator();
                }
                return true;
            }

            @Override
            public ConsumerRecord<String, String> next() { return this.records.next(); }
        };

        StreamSupport.stream(iterable.spliterator(), false)
            .peek((rec) -> this.copyToLocal(rec));
    }

    private void copyToLocal(ConsumerRecord<String, String> rec) {
        try {
            Path dest = this.copy(rec.key(), rec.value());
            Future<RecordMetadata> result = null;
            final String hostPathName = FileUtils.getHostPathName(dest);
            result = this.producerForPublishingOnStringTopic
                .send(new ProducerRecord<String, String>(this.topicFile, rec.key(), hostPathName));
            RecordMetadata data = result.get();
            BeanCopyFile.LOGGER.info(
                "[EVENT][{}] Recorded file [{}] at [part={},offset={},topic={},time={}]",
                rec.key(),
                hostPathName,
                data.partition(),
                data.offset(),
                data.topic(),
                data.timestamp());
        } catch (IOException e) {
            BeanCopyFile.LOGGER.error("[EVENT][{}] Unexpected error", rec.key(), e);
        } catch (InterruptedException e) {
            BeanCopyFile.LOGGER.error("[EVENT][{}] Unexpected error", rec.key(), e);
        } catch (ExecutionException e) {
            BeanCopyFile.LOGGER.error("[EVENT][{}] Unexpected error", rec.key(), e);
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets(
        Consumer<String, String> consumerTransactionalForTopicWithStringKey2,
        ConsumerRecords<String, String> records
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1)
                .offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }
        return offsetsToCommit;
    }

    private Path copy(String key, String value) throws IOException {
        String currentFolder = this.beanServicesFile.getCurrentFolderInWhichCopyShouldBeDone(this.repositoryPath);

        Path destPath = this.repositoryPath.resolve(Paths.get(currentFolder, key));

        try {

            if (!Files.exists(destPath)) {
                String nfsCoordinates = value.substring(0, value.lastIndexOf(File.pathSeparator));
                String fileName = value.substring(value.lastIndexOf(File.pathSeparator) + 1);
                key = key + "--" + fileName;
                BeanCopyFile.LOGGER.info("Copying input file from {} to {}", value, destPath.toAbsolutePath());
                FileUtils.copyRemoteToLocal(nfsCoordinates, fileName, destPath, BeanCopyFile.BUFFER_COPY);
                return destPath;
            }
            throw new IOException("Erreur : destination path " + destPath + " Already exists");
        } catch (IOException e) {
            BeanCopyFile.LOGGER.error("[EVENT][{}] Unexpected error when processing [{}]", key, value, e);
            throw e;
        }
    }

}
