package com.gs.photo.workflow.cmphashkey.ports.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
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

import com.gs.instrumentation.KafkaSpy;
import com.gs.instrumentation.TimedBean;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.common.workflow.impl.KafkaUtils;
import com.gs.photo.common.workflow.internal.GenericKafkaManagedObject;
import com.gs.photo.common.workflow.internal.KafkaManagedFileToProcess;
import com.gs.photo.common.workflow.internal.KafkaManagedObject;
import com.gs.photo.common.workflow.ports.IIgniteDAO;
import com.gs.photo.workflow.cmphashkey.ApplicationConfig;
import com.gs.photo.workflow.cmphashkey.business.IBeanImageFileHelper;
import com.gs.photo.workflow.cmphashkey.ports.IFileUtils;
import com.gs.photo.workflow.cmphashkey.ports.IProcessInputForHashKeyCompute;
import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.files.FileToProcess;

import io.micrometer.core.annotation.Timed;

@Service
@KafkaSpy
@TimedBean
public class BeanFileConsumer implements IProcessInputForHashKeyCompute {

    protected final Logger                              LOGGER = LoggerFactory.getLogger(BeanFileConsumer.class);

    @Autowired
    protected Supplier<Consumer<String, FileToProcess>> kafkaConsumerFactoryForFileToProcessValue;

    @Autowired
    protected Supplier<Producer<String, HbaseData>>     producerSupplierForTransactionPublishingOnExifTopic;

    @Autowired
    protected IKafkaProperties                          kafkaProperties;

    @Autowired
    protected IIgniteDAO                                igniteDAO;

    @Autowired
    protected ThreadPoolTaskExecutor                    beanTaskExecutor;

    @Autowired
    protected IBeanImageFileHelper                      beanImageFileHelper;

    @Autowired
    protected IFileUtils                                fileUtils;

    @Override
    public void start() { this.beanTaskExecutor.execute(() -> this.processIncomingFile()); }

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
                    Producer<String, HbaseData> producerForTopicWithFileToProcessValue = this.producerSupplierForTransactionPublishingOnExifTopic
                        .get()) {
                    producerForTopicWithFileToProcessValue.initTransactions();
                    consumerForTopicWithFileToProcessValue.subscribe(
                        Collections.singleton(
                            (this.kafkaProperties.getTopics()
                                .topicScanFile())));
                    while (ready) {
                        try {
                            this.processRecords(
                                consumerForTopicWithFileToProcessValue,
                                producerForTopicWithFileToProcessValue);
                        } catch (Throwable e) {
                            if ((e instanceof InterruptedException) || (e.getCause() instanceof InterruptedException)) {
                                this.LOGGER.info("Stopping process...");
                                stop = true;
                                ready = false;
                            } else {
                                this.LOGGER.error("Unexpected error {} ", ExceptionUtils.getStackTrace(e));
                                ready = false;
                            }
                        }
                    }
                }
            }
        } while (!stop);
        this.LOGGER.info("!! END OF PROCESS !!");
    }

    @Timed
    @KafkaSpy
    private void processRecords(
        Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue,
        Producer<String, HbaseData> producerForTopicWithFileToProcessValue
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsets = null;

        KafkaUtils
            .buildParallelKafkaBatchStreamPerTopicAndPartition(
                producerForTopicWithFileToProcessValue,
                consumerForTopicWithFileToProcessValue,
                this.kafkaProperties.getConsumersType()
                    .get(ApplicationConfig.CONSUMER_NAME)
                    .maxPollIntervallMs(),
                this.kafkaProperties.getConsumersType()
                    .get(ApplicationConfig.CONSUMER_NAME)
                    .batchSizeForParallelProcessingIncomingRecords(),
                true,
                (i, p) -> this.startRecordsProcessing(i, p))
            .map((r) -> this.asyncCreateKafkaManagedFileToProcess(r))
            .map((r) -> this.asyncSaveInIgnite(r))
            .map(CompletableFuture::join)
            .collect(Collectors.groupingByConcurrent(t -> this.getKey(t)))
            .entrySet()
            .stream()
            .map(t -> t.getValue())
            .map((r) -> this.asyncSendFileToProcess(producerForTopicWithFileToProcessValue, r))
            .map((r) -> this.asyncSendEvent(producerForTopicWithFileToProcessValue, r))
            .map(CompletableFuture::join)
            .flatMap(t -> t.stream())
            .collect(
                () -> new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>(),
                (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                (r, t) -> this.merge(r, t));
        producerForTopicWithFileToProcessValue
            .sendOffsetsToTransaction(offsets, consumerForTopicWithFileToProcessValue.groupMetadata());
        producerForTopicWithFileToProcessValue.commitTransaction();
    }

    protected String getKey(KafkaManagedObject kmo) {
        if (kmo instanceof KafkaManagedFileToProcess fileToProcess) { return fileToProcess.getObjectKey(); }
        return "UNKNOWN";
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
                .topicScanFile(),
            (f) -> f.getKafkaOffset());
    }

    private void startRecordsProcessing(
        int nbOfRecords,
        Producer<String, HbaseData> producerForTopicWithFileToProcessValue
    ) {
        producerForTopicWithFileToProcessValue.beginTransaction();
    }

    private CompletableFuture<Collection<KafkaManagedObject>> asyncSendFileToProcess(
        Producer<String, HbaseData> producerForTopicWithFileToProcessValue,
        Collection<KafkaManagedObject> kmos
    ) {
        return CompletableFuture.supplyAsync(() -> {
            return this.doSendFileToProcess(producerForTopicWithFileToProcessValue, kmos);
        }, Executors.newVirtualThreadPerTaskExecutor());
    }

    @Timed
    private Collection<KafkaManagedObject> doSendFileToProcess(
        Producer<String, HbaseData> producerForTopicWithFileToProcessValue,
        Collection<KafkaManagedObject> kmos
    ) {
        kmos.forEach(kmo -> {
            if (kmo instanceof KafkaManagedFileToProcess fileToProcess) {
                fileToProcess.getValue()
                    .ifPresentOrElse((o) -> {
                        o.setImageId(fileToProcess.getHashKey());
                        producerForTopicWithFileToProcessValue.send(
                            new ProducerRecord<String, HbaseData>(this.kafkaProperties.getTopics()
                                .topicHashKeyOutput(), fileToProcess.getHashKey(), o));
                    },
                        () -> this.LOGGER.warn(
                            "Error : offset {} of partition {} of topic {} is not processed",
                            fileToProcess.getKafkaOffset(),
                            fileToProcess.getPartition(),
                            this.kafkaProperties.getTopics()
                                .topicScanFile()));
            }
        });
        return new ArrayList<>(kmos);
    }

    private CompletableFuture<KafkaManagedObject> asyncSaveInIgnite(
        CompletableFuture<KafkaManagedObject> kmoAsCompletableFuture
    ) {
        return kmoAsCompletableFuture.thenApply((t) -> this.doSaveInIgnite(t));
    }

    @Timed
    private KafkaManagedObject doSaveInIgnite(KafkaManagedObject kmo) {
        if (kmo instanceof KafkaManagedFileToProcess kafkaManagedFileToProcess) {
            boolean saved = this.igniteDAO
                .save(kafkaManagedFileToProcess.getHashKey(), kafkaManagedFileToProcess.getRawFile());
            kafkaManagedFileToProcess.setRawFile(null);
            if (saved) {
                this.LOGGER.info(
                    "[EVENT][{}] saved in ignite",
                    kafkaManagedFileToProcess.getHashKey(),
                    KafkaManagedFileToProcess.toString(kafkaManagedFileToProcess));
            } else {
                this.LOGGER.warn(
                    "[EVENT][{}] not saved in ignite {} : was already seen before... ",
                    kafkaManagedFileToProcess.getHashKey(),
                    KafkaManagedFileToProcess.toString(kafkaManagedFileToProcess));
            }
        }
        return kmo;
    }

    private CompletableFuture<Collection<KafkaManagedObject>> asyncSendEvent(
        Producer<String, HbaseData> producerForTopicWithFileToProcessValue,
        CompletableFuture<Collection<KafkaManagedObject>> kmoCf
    ) {
        return kmoCf.thenApply((kmos) -> this.doSendEvents(producerForTopicWithFileToProcessValue, kmos));
    }

    @Timed
    private Collection<KafkaManagedObject> doSendEvents(
        Producer<String, HbaseData> producerForTopicWithFileToProcessValue,
        Collection<KafkaManagedObject> kmos
    ) {
        kmos.forEach(kmo -> {
            if (kmo instanceof GenericKafkaManagedObject gkmo) {
                producerForTopicWithFileToProcessValue.send(
                    new ProducerRecord<String, HbaseData>(this.kafkaProperties.getTopics()
                        .topicEvent(), gkmo.getObjectKey(), gkmo.createWfEvent()));
            }
        });
        return new ArrayList<>(kmos);
    }

    private CompletableFuture<KafkaManagedObject> asyncCreateKafkaManagedFileToProcess(
        ConsumerRecord<String, FileToProcess> f
    ) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return this.doCreateKafkaManagedFileToProcess(f);
            } catch (Throwable e) {
                this.LOGGER.warn("[EVENT][{}] Error {}", f, ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        }, Executors.newVirtualThreadPerTaskExecutor());
    }

    @Timed
    private KafkaManagedObject doCreateKafkaManagedFileToProcess(ConsumerRecord<String, FileToProcess> f)
        throws IOException {
        byte[] rawFile = this.fileUtils.readFirstBytesOfFileRetry(f.value());
        String key = "";
        if ((rawFile != null) && (rawFile.length > 0)) {
            key = this.beanImageFileHelper.computeHashKey(rawFile);

            this.LOGGER.info(
                "[EVENT][{}] getting bytes to compute hash key, length is {} [kafka : offset {}, topic {}] ",
                key,
                rawFile.length,
                f.offset(),
                f.topic());
            return KafkaManagedFileToProcess.builder()
                .withObjectKey(key)
                .withHashKey(key)
                .withRawFile(rawFile)
                .withValue(Optional.of(f.value()))
                .withKafkaOffset(f.offset())
                .withPartition(f.partition())
                .withStep(WfEventStep.WF_CREATED_FROM_STEP_COMPUTE_HASH_KEY)
                .build();
        }
        return KafkaManagedObject.builderForKafkaManagedObject()
            .withKafkaOffset(f.offset())
            .withPartition(f.partition())
            .build();
    }
}