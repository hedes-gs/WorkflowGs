package com.gs.photo.workflow.scan.impl;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gs.instrumentation.KafkaSpy;
import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.common.workflow.impl.AbstractRemoteFile;
import com.gs.photo.workflow.scan.IScan;
import com.gs.photo.workflow.scan.business.IRetrieveFilesFromFolder;
import com.gs.photo.workflow.scan.config.SpecificApplicationProperties;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.ComponentEvent;
import com.workflow.model.events.ComponentEvent.ComponentStatus;
import com.workflow.model.events.ComponentEvent.ComponentType;
import com.workflow.model.events.ImportEvent;
import com.workflow.model.files.FileToProcess;

import io.micrometer.core.annotation.Timed;

@Component
public class BeanScan implements IScan {

    protected static final org.slf4j.Logger              LOGGER         = LoggerFactory.getLogger(BeanScan.class);

    @Autowired
    protected IKafkaProperties                           kafkaProperties;
    @Autowired
    protected SpecificApplicationProperties              applicationProperties;

    @Autowired
    protected Supplier<Producer<String, ComponentEvent>> producerFactoryForComponentEvent;

    @Autowired
    protected Supplier<Consumer<String, ImportEvent>>    kafkaConsumerFactoryForImportEvent;

    @Autowired
    protected Supplier<Producer<String, FileToProcess>>  producerFactoryForPublishingOnFileTopic;

    protected Map<String, String>                        mapOfFiles     = new HashMap<>();
    private static final String                          EXTENSTION_EIP = "EIP";
    @Autowired
    protected IBeanTaskExecutor                          beanTaskExecutor;

    @Autowired
    protected IRetrieveFilesFromFolder                   retrieveFilesFromFolder;

    @Autowired
    public String                                        createScanName;

    @Override
    public void start() {
        BeanScan.LOGGER.info("init and this.scannedFolder are {}", this.applicationProperties.getScannedFolder());
        this.beanTaskExecutor.execute(() -> this.scan());
        this.beanTaskExecutor.execute(() -> {
            try {
                this.sendHeartBeat();
            } catch (InterruptedException e) {
            }
        });

    }

    private void sendHeartBeat() throws InterruptedException {
        try (
            Producer<String, ComponentEvent> producerForComponentEvent = this.producerFactoryForComponentEvent.get()) {
            while (true) {
                producerForComponentEvent.send(
                    new ProducerRecord<>(this.kafkaProperties.getTopics()
                        .topicComponentStatus(),
                        ComponentEvent.builder()
                            .withMessage("Component started !")
                            .withScannedFolder(
                                this.applicationProperties.getScannedFolder()
                                    .toArray(new String[0]))
                            .withComponentName(this.createScanName)
                            .withComponentType(ComponentType.SCAN)
                            .withStatus(ComponentStatus.ALIVE)
                            .build()));
                TimeUnit.MILLISECONDS.sleep(2000);
            }
        }
    }

    private void scan() {
        try (
            Producer<String, FileToProcess> producerForPublishingOnFileTopic = this.producerFactoryForPublishingOnFileTopic
                .get();
            Consumer<String, ImportEvent> consumerForComponentEvent = this.kafkaConsumerFactoryForImportEvent.get()) {
            consumerForComponentEvent.subscribe(
                Collections.singleton(
                    this.kafkaProperties.getTopics()
                        .topicImportEvent()));
            while (true) {
                try {
                    this.processRecords(producerForPublishingOnFileTopic, consumerForComponentEvent);
                } catch (RuntimeException e) {
                    BeanScan.LOGGER.warn("Unexpected error ", e);
                } catch (Exception e) {
                    if (e instanceof InterruptedException) {
                        BeanScan.LOGGER.info("Interruption received : stopping..");
                        break;
                    }
                    BeanScan.LOGGER.warn("Unexpected error ", e);
                }

            }
        }
    }

    @KafkaSpy
    @Timed
    private void processRecords(
        Producer<String, FileToProcess> producerForPublishingOnFileTopic,
        Consumer<String, ImportEvent> consumerForComponentEvent
    ) {
        ConsumerRecords<String, ImportEvent> records = consumerForComponentEvent
            .poll(Duration.ofSeconds(this.applicationProperties.getHeartBeatTime()));
        Set<TopicPartition> partitions = consumerForComponentEvent.assignment();
        consumerForComponentEvent.commitSync();
        consumerForComponentEvent.pause(partitions);

        records.forEach(t -> {
            ImportEvent importEvent = t.value();
            BeanScan.LOGGER.info(
                "Start scan for {}, url is {} -  test mode is {} - nb max {}",
                this.createScanName,
                importEvent.getUrlScanFolder(),
                importEvent.isForTest(),
                importEvent.getNbMaxOfImages());
            if (importEvent.isForTest()) {
                this.retrieveFilesFromFolder.process(
                    importEvent.getUrlScanFolder(),
                    (f) -> this.publishMainFile(producerForPublishingOnFileTopic, importEvent, f),
                    (associatedFile, f) -> this
                        .publishSubFile(producerForPublishingOnFileTopic, importEvent, associatedFile, f),
                    importEvent.getNbMaxOfImages());

            } else {
                this.retrieveFilesFromFolder.process(
                    importEvent.getUrlScanFolder(),
                    (f) -> this.publishMainFile(producerForPublishingOnFileTopic, importEvent, f),
                    (associatedFile, f) -> this
                        .publishSubFile(producerForPublishingOnFileTopic, importEvent, associatedFile, f));
            }
            BeanScan.LOGGER.info("End of Scan ");
        });
        consumerForComponentEvent.resume(partitions);
    }

    private void publishMainFile(
        Producer<String, FileToProcess> producerForPublishingOnFileTopic,
        ImportEvent importEvent,
        AbstractRemoteFile mainFile
    ) {
        boolean isCompressed = mainFile.getName()
            .toUpperCase()
            .endsWith(BeanScan.EXTENSTION_EIP);
        this.publishFile(
            producerForPublishingOnFileTopic,
            this.buildFileToProcess(importEvent, mainFile, isCompressed),
            this.kafkaProperties.getTopics()
                .topicScanFile());
    }

    private void publishSubFile(
        Producer<String, FileToProcess> producerForPublishingOnFileTopic,
        ImportEvent importEvent,
        AbstractRemoteFile associatedFile,
        AbstractRemoteFile subFile

    ) {
        this.publishFile(
            producerForPublishingOnFileTopic,
            this.buildFileToProcess(importEvent, associatedFile, subFile),
            this.kafkaProperties.getTopics()
                .topicScanFile());
    }

    private void publishFile(
        Producer<String, FileToProcess> producerForPublishingOnFileTopic,
        FileToProcess fileToProcess,
        String topic
    ) {
        BeanScan.LOGGER.info("[EVENT][{}] publish file {} ", fileToProcess.getUrl(), fileToProcess.toString());
        producerForPublishingOnFileTopic.send(
            new ProducerRecord<String, FileToProcess>(topic,
                KeysBuilder.topicFileKeyBuilder()
                    .withUrl(fileToProcess.getUrl())
                    .build(),
                fileToProcess));
    }

    private FileToProcess buildFileToProcess(ImportEvent importEvent, AbstractRemoteFile file, boolean isCompressed) {
        return FileToProcess.builder()
            .withName(file.getName())
            .withUrl(
                file.toExternalURL()
                    .toString())
            .withDataId(file.getName())
            .withImageId("<UNSET>")
            .withCompressedFile(isCompressed)
            .withImportEvent(importEvent)
            .withIsLocal(false)
            .build();
    }

    private FileToProcess buildFileToProcess(
        ImportEvent importEvent,
        AbstractRemoteFile associatedFile,
        AbstractRemoteFile file
    ) {
        return FileToProcess.builder()
            .withName(file.getName())
            .withCompressedFile(false)
            .withDataId(file.getName())
            .withImportDate(System.currentTimeMillis())
            .withImportEvent(importEvent)
            .withIsLocal(false)
            .withUrl(
                file.getUrl()
                    .toString())
            .withParent(
                this.buildFileToProcess(
                    importEvent,
                    associatedFile,
                    BeanScan.EXTENSTION_EIP.equals(FilenameUtils.getExtension(associatedFile.getName()))))
            .build();
    }

}