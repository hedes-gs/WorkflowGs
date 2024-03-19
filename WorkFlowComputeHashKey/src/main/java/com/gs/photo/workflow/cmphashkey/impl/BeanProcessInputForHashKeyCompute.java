package com.gs.photo.workflow.cmphashkey.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.common.workflow.TimeMeasurement;
import com.gs.photo.common.workflow.impl.KafkaUtils;
import com.gs.photo.common.workflow.internal.KafkaManagedFileToProcess;
import com.gs.photo.common.workflow.ports.IIgniteDAO;
import com.gs.photo.workflow.cmphashkey.IBeanImageFileHelper;
import com.gs.photo.workflow.cmphashkey.IProcessInputForHashKeyCompute;
import com.gs.photo.workflow.cmphashkey.config.IApplicationProperties;
import com.workflow.model.files.FileToProcess;

@Service
public class BeanProcessInputForHashKeyCompute implements IProcessInputForHashKeyCompute {

    protected final Logger                              LOGGER = LoggerFactory
        .getLogger(BeanProcessInputForHashKeyCompute.class);

    @Autowired
    protected Supplier<Consumer<String, FileToProcess>> kafkaConsumerFactoryForFileToProcessValue;

    @Autowired
    protected Supplier<Producer<String, FileToProcess>> producerSupplierForTransactionPublishingOnExifTopic;

    @Autowired
    protected IKafkaProperties                          kafkaProperties;

    @Autowired
    protected IApplicationProperties                    applicationProperties;

    @Autowired
    protected IIgniteDAO                                igniteDAO;

    @Autowired
    protected ThreadPoolTaskExecutor                    beanTaskExecutor;

    @Autowired
    protected IBeanImageFileHelper                      beanImageFileHelper;

    @PostConstruct
    public void init() { this.beanTaskExecutor.execute(() -> this.processIncomingFile()); }

    protected void processIncomingFile() {
        boolean stop = false;
        do {
            boolean ready = true;
            do {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    ready = false;
                    break;
                }
            } while (!this.igniteDAO.isReady());
            if (ready) {
                this.LOGGER.info("Ignite is finally ready, let's go !!!");
                try (
                    Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue = this.kafkaConsumerFactoryForFileToProcessValue
                        .get();
                    Producer<String, FileToProcess> producerForTopicWithFileToProcessValue = this.producerSupplierForTransactionPublishingOnExifTopic
                        .get()) {
                    consumerForTopicWithFileToProcessValue.subscribe(
                        Collections.singleton(
                            (this.kafkaProperties.getTopics()
                                .topicScanFile())));
                    while (ready) {
                        try (
                            TimeMeasurement timeMeasurement = TimeMeasurement.of(
                                "BATCH_PROCESS_FILES",
                                (d) -> this.LOGGER.info(" Perf. metrics {}", d),
                                System.currentTimeMillis())) {
                            Stream<ConsumerRecord<String, FileToProcess>> stream = KafkaUtils.toStreamV2(
                                this.applicationProperties.kafkaPollTimeInMillisecondes(),
                                consumerForTopicWithFileToProcessValue,
                                this.applicationProperties.batchSizeForParallelProcessingIncomingRecords(),
                                true,
                                (i) -> this.loggerStart(i),
                                timeMeasurement);
                            Map<TopicPartition, OffsetAndMetadata> offsets = stream.map((r) -> this.create(r))
                                .map((r) -> this.saveInIgnite(r))
                                .map((r) -> this.sendToNext(producerForTopicWithFileToProcessValue, r))
                                .collect(
                                    () -> new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>(),
                                    (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                                    (r, t) -> this.merge(r, t));
                            this.LOGGER.info("Offset to commit {} ", offsets);
                            producerForTopicWithFileToProcessValue.flush();
                            consumerForTopicWithFileToProcessValue.commitSync(offsets);
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
                                producerForTopicWithFileToProcessValue.close();
                            } catch (Exception e1) {
                                this.LOGGER.error("Unexpected error when aborting transaction {}", e1.getMessage());

                            }
                        }
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
            (f) -> this.kafkaProperties.getTopics()
                .topicScanFile(),
            (f) -> f.getKafkaOffset());
    }

    private void loggerStart(int nbOfRecords) { this.LOGGER.info("Starting to process {} records", nbOfRecords); }

    private KafkaManagedFileToProcess sendToNext(
        Producer<String, FileToProcess> producerForTopicWithFileToProcessValue,
        KafkaManagedFileToProcess fileToProcess
    ) {
        if (fileToProcess.getHashKey()
            .length() > 0) {

            fileToProcess.getValue()
                .ifPresentOrElse((o) -> {
                    o.setImageId(fileToProcess.getHashKey());
                    producerForTopicWithFileToProcessValue.send(
                        new ProducerRecord<String, FileToProcess>(this.kafkaProperties.getTopics()
                            .topicHashKeyOutput(), fileToProcess.getHashKey(), o));
                },
                    () -> this.LOGGER.warn(
                        "Error : offset {} of partition {} of topic {} is not processed",
                        fileToProcess.getKafkaOffset(),
                        fileToProcess.getPartition(),
                        this.kafkaProperties.getTopics()
                            .topicScanFile()));
        }
        return fileToProcess;
    }

    private KafkaManagedFileToProcess saveInIgnite(KafkaManagedFileToProcess r) {
        if (r.getHashKey()
            .length() > 0) {
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
        }
        return r;
    }

    private KafkaManagedFileToProcess create(ConsumerRecord<String, FileToProcess> f) {
        try {
            byte[] rawFile = this.beanImageFileHelper.readFirstBytesOfFile(f.value());
            String key = "";
            if ((rawFile != null) && (rawFile.length > 0)) {
                key = this.beanImageFileHelper.computeHashKey(rawFile);
            }

            this.LOGGER.info(
                "[EVENT][{}] getting bytes to compute hash key, length is {} [kafka : offset {}, topic {}] ",
                key,
                rawFile.length,
                f.offset(),
                f.topic());
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