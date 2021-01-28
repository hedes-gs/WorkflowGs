package com.gs.photo.workflow.cmphashkey.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
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
import org.springframework.stereotype.Service;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.common.workflow.IIgniteDAO;
import com.gs.photo.common.workflow.TimeMeasurement;
import com.gs.photo.common.workflow.impl.KafkaUtils;
import com.gs.photo.common.workflow.internal.KafkaManagedFileToProcess;
import com.gs.photo.workflow.cmphashkey.IBeanImageFileHelper;
import com.gs.photo.workflow.cmphashkey.IProcessInputForHashKeyCompute;
import com.workflow.model.files.FileToProcess;

@Service
public class BeanProcessInputForHashKeyCompute implements IProcessInputForHashKeyCompute {

    protected final Logger                    LOGGER = LoggerFactory.getLogger(IProcessInputForHashKeyCompute.class);
    @Value("${topic.topicScannedFiles}")
    protected String                          topicScanOutput;

    @Value("${topic.topicFileHashKey}")
    protected String                          topicHashKeyOutput;

    @Autowired
    @Qualifier("producerForTopicWithFileToProcessValue")
    protected Producer<String, FileToProcess> producerForTopicWithFileToProcessValue;

    @Value("${kafka.consumer.batchSizeForParallelProcessingIncomingRecords}")
    protected int                             batchSizeForParallelProcessingIncomingRecords;

    @Autowired
    protected IIgniteDAO                      igniteDAO;

    @Autowired
    protected IBeanTaskExecutor               beanTaskExecutor;

    @Autowired
    protected IBeanImageFileHelper            beanImageFileHelper;

    @Value("${kafka.pollTimeInMillisecondes}")
    protected int                             kafkaPollTimeInMillisecondes;

    @Autowired
    private ApplicationContext                context;

    @PostConstruct
    public void init() { this.beanTaskExecutor.execute(() -> this.processIncomingFile()); }

    protected void processIncomingFile() {
        boolean ready = true;
        boolean stop = false;
        do {
            do {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    ready = false;
                    break;
                }
            } while (!this.igniteDAO.isReady());
            this.LOGGER.info("Ignite is finally ready, let's go !!!");
            Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue = this.context
                .getBean("consumerForTopicWithFileToProcessValue", Consumer.class);
            consumerForTopicWithFileToProcessValue.subscribe(Collections.singleton((this.topicScanOutput)));
            this.producerForTopicWithFileToProcessValue = this.context
                .getBean("producerForTopicWithFileToProcessValue", Producer.class);
            while (ready) {
                try (
                    TimeMeasurement timeMeasurement = TimeMeasurement.of(
                        "BATCH_PROCESS_FILES",
                        (d) -> this.LOGGER.info(" Perf. metrics {}", d),
                        System.currentTimeMillis())) {
                    Stream<ConsumerRecord<String, FileToProcess>> stream = KafkaUtils.toStreamV2(
                        this.kafkaPollTimeInMillisecondes,
                        consumerForTopicWithFileToProcessValue,
                        this.batchSizeForParallelProcessingIncomingRecords,
                        true,
                        (i) -> this.loggerStart(i),
                        timeMeasurement);
                    Map<TopicPartition, OffsetAndMetadata> offsets = stream.map((r) -> this.create(r))
                        .map((r) -> this.saveInIgnite(r))
                        .map((r) -> this.sendToNext(r))
                        .collect(
                            () -> new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>(),
                            (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                            (r, t) -> this.merge(r, t));
                    this.LOGGER.info("Offset to commit {} ", offsets);
                    consumerForTopicWithFileToProcessValue.commitSync(offsets);
                    this.producerForTopicWithFileToProcessValue.flush();
                } catch (Throwable e) {
                    if ((e instanceof InterruptedException) || (e.getCause() instanceof InterruptedException)) {
                        this.LOGGER.info("Stopping process...");
                        stop = true;
                        ready = false;
                    } else {
                        this.LOGGER.error("Unexpected error {} ", ExceptionUtils.getStackTrace(e));
                        ready = false;
                    }
                    consumerForTopicWithFileToProcessValue.close();
                    try {
                        this.producerForTopicWithFileToProcessValue.close();
                    } catch (Exception e1) {
                        this.LOGGER.error("Unexpected error when aborting transaction {}", e1.getMessage());

                    }
                }
            }
        } while (!stop);
        this.LOGGER.info("!! END OF PROCESS !!");
    }

    private void merge(Map<TopicPartition, OffsetAndMetadata> r, Map<TopicPartition, OffsetAndMetadata> t) {
        KafkaUtils.merge(r, t);
    }

    private void updateMapOfOffset(
        Map<TopicPartition, OffsetAndMetadata> mapOfOffset,
        KafkaManagedFileToProcess fileToProcess
    ) {
        KafkaUtils.updateMapOfOffset(
            mapOfOffset,
            fileToProcess,
            (f) -> f.getPartition(),
            (f) -> this.topicScanOutput,
            (f) -> f.getKafkaOffset());
    }

    private void loggerStart(int nbOfRecords) { this.LOGGER.info("Starting to process {} records", nbOfRecords); }

    private KafkaManagedFileToProcess sendToNext(KafkaManagedFileToProcess fileToProcess) {
        fileToProcess.getValue()
            .ifPresentOrElse((o) -> {
                o.setImageId(fileToProcess.getHashKey());
                this.producerForTopicWithFileToProcessValue.send(
                    new ProducerRecord<String, FileToProcess>(this.topicHashKeyOutput, fileToProcess.getHashKey(), o));
            },
                () -> this.LOGGER.warn(
                    "Error : offset {} of partition {} of topic {} is not processed",
                    fileToProcess.getKafkaOffset(),
                    fileToProcess.getPartition(),
                    this.topicScanOutput));
        return fileToProcess;
    }

    private KafkaManagedFileToProcess saveInIgnite(KafkaManagedFileToProcess r) {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        boolean saved = this.igniteDAO.save(r.getHashKey(), r.getRawFile());
        r.setRawFile(null);
        stopWatch.stop();
        if (saved) {
            this.LOGGER.info(
                "[EVENT][{}] saved in ignite {} - duration is {}",
                r.getHashKey(),
                KafkaManagedFileToProcess.toString(r),
                stopWatch.formatTime());
        } else {
            this.LOGGER.warn(
                "[EVENT][{}] not saved in ignite {} : was already seen before... - duration is {} ",
                r.getHashKey(),
                KafkaManagedFileToProcess.toString(r),
                stopWatch.formatTime());
        }
        return r;
    }

    private KafkaManagedFileToProcess create(ConsumerRecord<String, FileToProcess> f) {
        try {
            byte[] rawFile = this.beanImageFileHelper.readFirstBytesOfFile(f.value());
            String key = this.beanImageFileHelper.computeHashKey(rawFile);

            this.LOGGER.debug("[EVENT][{}] getting bytes to compute hash key, length is {}", key, rawFile.length);
            return KafkaManagedFileToProcess.builder()
                .withHashKey(key)
                .withRawFile(rawFile)
                .withValue(Optional.of(f.value()))
                .withKafkaOffset(f.offset())
                .withPartition(f.partition())
                .build();
        } catch (Throwable e) {
            this.LOGGER.warn("[EVENT][{}] Error {}", f, ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }
}