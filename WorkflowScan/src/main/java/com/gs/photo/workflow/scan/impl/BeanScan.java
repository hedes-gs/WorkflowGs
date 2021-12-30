package com.gs.photo.workflow.scan.impl;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.common.workflow.Mailbox;
import com.gs.photo.common.workflow.impl.AbstractRemoteFile;
import com.gs.photo.common.workflow.impl.FileUtils;
import com.gs.photo.workflow.scan.IScan;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.ComponentEvent;
import com.workflow.model.events.ComponentEvent.ComponentStatus;
import com.workflow.model.events.ComponentEvent.ComponentType;
import com.workflow.model.events.ImportEvent;
import com.workflow.model.files.FileToProcess;

@Component
public class BeanScan implements IScan {

    private static final String                EXTENSTION_EIP         = "EIP";

    private static final String                EXTENSION_ARW_COMASK   = "COMASK";

    private static final String                EXTENSION_ARW_COS      = "COS";

    private static final String                EXTENSION_ARW_COP      = "COP";

    private static final String                EXTENSION_FILE_ARW_COF = "COF";

    private static final String                EXTENSION_FILE_ARW     = "ARW";

    protected static final org.slf4j.Logger    LOGGER                 = LoggerFactory.getLogger(BeanScan.class);

    protected ReentrantReadWriteLock           lock                   = new ReentrantReadWriteLock();

    @Value("${topic.topicScannedFiles}")
    protected String                           outputTopic;

    @Value("${topic.topicScannedFilesChild}")
    protected String                           outputParentTopic;

    @Value("${topic.topicComponentStatus}")
    protected String                           topicComponentStatus;

    @Value("${topic.topicImportEvent}")
    protected String                           topicImportEvent;

    @Value("${scan.folder}")
    protected String[]                         scannedFolder;

    @Value("${scan.heartBeatTimeInSeconds}")
    protected int                              heartBeatTime;

    protected Set<AbstractRemoteFile>          filesProcessed;
    protected Set<AbstractRemoteFile>          filesProcessedOfSession;

    @Autowired
    protected Producer<String, ComponentEvent> producerForComponentEvent;

    @Autowired
    protected Consumer<String, ImportEvent>    consumerForComponentEvent;

    @Autowired
    protected Producer<String, FileToProcess>  producerForPublishingOnFileTopic;

    protected Map<String, String>              mapOfFiles             = new HashMap<>();

    @Autowired
    protected IBeanTaskExecutor                beanTaskExecutor;

    @Autowired
    protected FileUtils                        fileUtils;

    protected Mailbox<ImportEvent>             importEventMailbox     = new Mailbox<>();

    @Autowired
    public String                              createScanName;

    @PostConstruct
    protected void init() {
        BeanScan.LOGGER.info("init and this.scannedFolder are {}", Arrays.asList(this.scannedFolder));
        for (String element : this.scannedFolder) {
            BeanScan.LOGGER.info("Starting a task exeuctor for ", element);
            this.beanTaskExecutor.execute(() -> this.scan());
        }
        this.filesProcessed = ConcurrentHashMap.newKeySet();
        this.filesProcessedOfSession = ConcurrentHashMap.newKeySet();
        this.beanTaskExecutor.execute(() -> this.waitForImportEvent());
    }

    private void scan() {
        while (true) {
            try {
                ImportEvent importEvent = this.waitForStarting();
                BeanScan.LOGGER.info(
                    "Start scan for {}, url is {} -  test mode is {} - nb max {}",
                    this.createScanName,
                    importEvent.getUrlScanFolder(),
                    importEvent.isForTest(),
                    importEvent.getNbMaxOfImages());
                this.filesProcessedOfSession.clear();
                try (
                    Stream<AbstractRemoteFile> stream = this.fileUtils.toStream(
                        new URL(importEvent.getUrlScanFolder()),
                        BeanScan.EXTENSTION_EIP,
                        BeanScan.EXTENSION_ARW_COMASK,
                        BeanScan.EXTENSION_ARW_COS,
                        BeanScan.EXTENSION_ARW_COP,
                        BeanScan.EXTENSION_FILE_ARW_COF,
                        BeanScan.EXTENSION_FILE_ARW)) {
                    stream.filter((f) -> this.isNotAlreadyProcessed(f))
                        .takeWhile((f) -> this.isNotTestModeOrMaxNbOfElementsReached(importEvent))
                        .parallel()
                        .forEach((f) -> this.processFoundFile(importEvent, f));
                }
                BeanScan.LOGGER.info("End of Scan ");
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

    private boolean isNotTestModeOrMaxNbOfElementsReached(ImportEvent importEvent) {
        this.lock.readLock()
            .lock();
        try {
            return !importEvent.isForTest() || (this.filesProcessedOfSession.size() < importEvent.getNbMaxOfImages());
        } finally {
            this.lock.readLock()
                .unlock();
        }
    }

    private ImportEvent waitForStarting() throws InterruptedException {
        try {
            return this.importEventMailbox.read();
        } catch (InterruptedException e) {
            BeanScan.LOGGER.info("Interruption received : stopping..");
            throw e;
        }
    }

    private void waitForImportEvent() {
        this.consumerForComponentEvent.subscribe(Collections.singleton(this.topicImportEvent));
        BeanScan.LOGGER.info("Starting heartbeat");
        while (true) {
            try {
                ConsumerRecords<String, ImportEvent> records = this.consumerForComponentEvent
                    .poll(Duration.ofSeconds(this.heartBeatTime));
                this.consumerForComponentEvent.commitSync();
                for (ConsumerRecord<String, ImportEvent> rec : records) {

                    if (Objects.equals(
                        rec.value()
                            .getScanners()
                            .get(0),
                        this.createScanName)) {
                        BeanScan.LOGGER.info(
                            "[COMPONENT][{}]Start import event : {}",
                            this.createScanName,
                            rec.value()
                                .toString());
                        this.importEventMailbox.post(rec.value());
                    }
                }
                this.producerForComponentEvent.send(
                    new ProducerRecord<>(this.topicComponentStatus,
                        ComponentEvent.builder()
                            .withMessage("Component started !")
                            .withScannedFolder(this.scannedFolder)
                            .withComponentName(this.createScanName)
                            .withComponentType(ComponentType.SCAN)
                            .withStatus(ComponentStatus.ALIVE)
                            .build()));
            } catch (Exception e) {
                BeanScan.LOGGER.warn("Error received {} - stopping", ExceptionUtils.getStackTrace(e));
                break;
            }
        }
    }

    private boolean isNotAlreadyProcessed(AbstractRemoteFile f) {
        this.lock.readLock()
            .lock();
        try {
            return !this.filesProcessed.contains(f);
        } finally {
            this.lock.readLock()
                .unlock();
        }
    }

    public void processFoundFile(ImportEvent importEvent, AbstractRemoteFile f) {
        this.lock.writeLock()
            .lock();
        try {
            this.filesProcessed.add(f);
            this.filesProcessedOfSession.add(f);
        } finally {
            this.lock.writeLock()
                .unlock();
        }
        try {
            BeanScan.LOGGER.info("[EVENT][{}] processFoundFile {} ", f.getName(), f.getUrl());
            final String currentFileName = f.getName();
            String extension = FilenameUtils.getExtension(currentFileName);
            switch (extension.toUpperCase()) {
                case BeanScan.EXTENSTION_EIP: {
                    // TODO : copy on local, and uncompress it
                    // FileUtils.copyRemoteToLocal(coordinates, filePath, os, bufferSize);
                    // FileUtils.copyRemoteToLocal(coordinates, filePath, os, bufferSize);
                    this.publishMainFile(importEvent, f, true);
                    break;
                }
                case BeanScan.EXTENSION_FILE_ARW: {
                    this.publishMainFile(importEvent, f, false);
                    break;
                }
                case BeanScan.EXTENSION_FILE_ARW_COF:
                case BeanScan.EXTENSION_ARW_COP:
                case BeanScan.EXTENSION_ARW_COS:
                case BeanScan.EXTENSION_ARW_COMASK: {
                    this.publishSubFile(importEvent, f);
                    break;
                }
            }
        } catch (Exception e) {
            BeanScan.LOGGER.error(
                "[EVENT][{}] Error processFoundFile {}, {} : {}",
                f.getName(),
                f.getUrl(),
                ExceptionUtils.getStackTrace(e));
            this.filesProcessed.remove(f);
        }
    }

    private void publishMainFile(ImportEvent importEvent, AbstractRemoteFile mainFile, boolean isCompressed) {
        this.publishFile(this.buildFileToProcess(importEvent, mainFile, isCompressed), this.outputTopic);
    }

    private void publishFile(FileToProcess fileToProcess, String topic) {
        BeanScan.LOGGER.info("[EVENT][{}] publish file {} ", fileToProcess.getUrl(), fileToProcess.toString());
        this.producerForPublishingOnFileTopic.send(
            new ProducerRecord<String, FileToProcess>(topic,
                KeysBuilder.topicFileKeyBuilder()
                    .withUrl(fileToProcess.getUrl())
                    .build(),
                fileToProcess));
        this.producerForPublishingOnFileTopic.flush();
    }

    private FileToProcess buildFileToProcess(ImportEvent importEvent, AbstractRemoteFile file, boolean isCompressed) {
        return FileToProcess.builder()
            .withName(file.getName())
            .withUrl(
                file.getUrl()
                    .toString())
            .withDataId(file.getName())
            .withImageId("<UNSET>")
            .withCompressedFile(isCompressed)
            .withImportEvent(importEvent)
            .build();
    }

    private FileToProcess buildFileToProcess(
        ImportEvent importEvent,
        AbstractRemoteFile file,
        AbstractRemoteFile associatedFile
    ) {
        return FileToProcess.builder()
            .withName(file.getName())
            .withCompressedFile(false)
            .withDataId(file.getName())
            .withImportDate(System.currentTimeMillis())
            .withImportEvent(importEvent)
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

    private void publishSubFile(ImportEvent importEvent, AbstractRemoteFile file) {
        try {
            AbstractRemoteFile parentFolder = file.getParentFile()
                .getParentFile();
            if (parentFolder.exists()) {
                AbstractRemoteFile associatedFile = parentFolder.getChild(
                    file.getName()
                        .substring(
                            0,
                            file.getName()
                                .indexOf("."))
                        + BeanScan.EXTENSION_FILE_ARW);
                if ((associatedFile != null) && associatedFile.exists()) {
                    this.publishFile(
                        this.buildFileToProcess(importEvent, file, associatedFile),
                        this.outputParentTopic);
                }
            }
        } catch (MalformedURLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
