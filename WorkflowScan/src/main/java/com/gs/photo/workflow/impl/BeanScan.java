package com.gs.photo.workflow.impl;

import java.io.File;
import java.net.InetAddress;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hbase.thirdparty.com.google.common.base.Objects;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.IScan;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.ComponentEvent;
import com.workflow.model.events.ComponentEvent.ComponentStatus;
import com.workflow.model.events.ComponentEvent.ComponentType;
import com.workflow.model.events.ImportEvent;
import com.workflow.model.files.FileToProcess;

@Component
public class BeanScan implements IScan {

    public static class Mailbox<T> {
        protected static final Logger LOGGER         = LoggerFactory.getLogger(Mailbox.class);
        T                             value;
        protected ReadWriteLock       lock           = new ReentrantReadWriteLock();
        protected CountDownLatch      countDownLatch = new CountDownLatch(1);

        public void post(T v) {
            this.lock.writeLock()
                .lock();
            try {
                this.value = v;
                Mailbox.LOGGER.info("Post event {} ", v);
                this.countDownLatch.countDown();
            } finally {
                this.lock.writeLock()
                    .unlock();
            }

        }

        public T read() throws InterruptedException {
            T retValue = null;
            do {
                try {
                    this.lock.readLock()
                        .lock();
                    retValue = this.value;
                } finally {
                    this.lock.readLock()
                        .unlock();
                }
                if (retValue == null) {
                    this.countDownLatch.await();
                }
                Mailbox.LOGGER.info("Read event {} ", this.value);
                this.lock.writeLock()
                    .lock();
                try {
                    retValue = this.value;
                    this.value = null;
                    this.countDownLatch = new CountDownLatch(1);
                } finally {
                    this.lock.writeLock()
                        .unlock();
                }
            } while (retValue == null);
            return retValue;
        }

    }

    private static final String                EXTENSTION_EIP         = "EIP";

    private static final String                EXTENSION_ARW_COMASK   = "COMASK";

    private static final String                EXTENSION_ARW_COS      = "COS";

    private static final String                EXTENSION_ARW_COP      = "COP";

    private static final String                EXTENSION_FILE_ARW_COF = "COF";

    private static final String                EXTENSION_FILE_ARW     = "ARW";

    protected static final org.slf4j.Logger    LOGGER                 = LoggerFactory.getLogger(IScan.class);

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

    protected Set<File>                        filesProcessed;

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
        this.beanTaskExecutor.execute(() -> this.waitForImportEvent());
    }

    private void scan() {
        boolean nfsIsUsed = false;
        String hostname;
        String rootFolderForNfs = "<not set>";
        while (true) {
            try {
                ImportEvent importEvent = this.waitForStarting();
                String[] splitFolder = importEvent.getScanFolder()
                    .split("\\:");
                if ((splitFolder != null) && (splitFolder.length == 2)) {
                    hostname = splitFolder[0];
                    rootFolderForNfs = splitFolder[1];
                    nfsIsUsed = true;
                } else {
                    InetAddress ip = InetAddress.getLocalHost();
                    hostname = ip.getHostName();
                    rootFolderForNfs = importEvent.getScanFolder();
                }
                BeanScan.LOGGER.info(
                    "Start scan for {}, hostname is {} , root folder for nfs {}, nfs is used {} -  test mode is {} - nb max {}",
                    this.createScanName,
                    hostname,
                    rootFolderForNfs,
                    nfsIsUsed,
                    importEvent.isForTest(),
                    importEvent.getNbMaxOfImages());
                final String folder = rootFolderForNfs;
                final String usedHostname = hostname;
                if (!nfsIsUsed) {

                    try (
                        Stream<File> stream = this.fileUtils.toStream(
                            Paths.get(rootFolderForNfs),
                            BeanScan.EXTENSTION_EIP,
                            BeanScan.EXTENSION_ARW_COMASK,
                            BeanScan.EXTENSION_ARW_COS,
                            BeanScan.EXTENSION_ARW_COP,
                            BeanScan.EXTENSION_FILE_ARW_COF,
                            BeanScan.EXTENSION_FILE_ARW)) {
                        stream.filter((f) -> this.isNotAlreadyProcessed(f))
                            .takeWhile((f) -> this.isNotTestModeOrMaxNbOfElementsReached(importEvent))
                            .parallel()
                            .forEach((f) -> this.processFoundFile(importEvent, f, folder, usedHostname));
                    }
                } else {
                    try (
                        Stream<File> stream = this.fileUtils.toStream(
                            hostname + ":" + rootFolderForNfs,
                            "/",
                            BeanScan.EXTENSTION_EIP,
                            BeanScan.EXTENSION_ARW_COMASK,
                            BeanScan.EXTENSION_ARW_COS,
                            BeanScan.EXTENSION_ARW_COP,
                            BeanScan.EXTENSION_FILE_ARW_COF,
                            BeanScan.EXTENSION_FILE_ARW)) {
                        stream.filter((f) -> this.isNotAlreadyProcessed(f))
                            .takeWhile((f) -> this.isNotTestModeOrMaxNbOfElementsReached(importEvent))
                            .forEach((f) -> this.processFoundFile(importEvent, f, folder, usedHostname));
                    }
                }
                BeanScan.LOGGER.info("End of Scan ");
            } catch (RuntimeException e) {
                if (e.getCause() instanceof NoSuchFileException) {
                    BeanScan.LOGGER.warn("Warning : {} is not available..", rootFolderForNfs);
                } else {
                    BeanScan.LOGGER.warn("Unexpected error ", e);
                }

            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    BeanScan.LOGGER.warn("Interruption received : stoping..");
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
            return !importEvent.isForTest() || (this.filesProcessed.size() <= importEvent.getNbMaxOfImages());
        } finally {
            this.lock.readLock()
                .unlock();
        }
    }

    private ImportEvent waitForStarting() throws InterruptedException {
        try {
            return this.importEventMailbox.read();

        } catch (InterruptedException e) {
            BeanScan.LOGGER.warn("Interruption received : stoping..");
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

                    if (Objects.equal(
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

    private boolean isNotAlreadyProcessed(File f) {
        this.lock.readLock()
            .lock();
        try {
            return !this.filesProcessed.contains(f);
        } finally {
            this.lock.readLock()
                .unlock();
        }
    }

    public void processFoundFile(ImportEvent importEvent, File f, String rootFolderForNfs, String hostname) {
        this.lock.writeLock()
            .lock();
        try {
            this.filesProcessed.add(f);
        } finally {
            this.lock.writeLock()
                .unlock();
        }
        try {
            BeanScan.LOGGER.info("[EVENT][{}] processFoundFile {} ", f.getName(), f.getAbsoluteFile());
            final String currentFileName = f.getName();
            String extension = FilenameUtils.getExtension(currentFileName);
            switch (extension.toUpperCase()) {
                case BeanScan.EXTENSTION_EIP: {
                    // TODO : copy on local, and uncompress it
                    // FileUtils.copyRemoteToLocal(coordinates, filePath, os, bufferSize);
                    // FileUtils.copyRemoteToLocal(coordinates, filePath, os, bufferSize);
                    this.publishMainFile(importEvent, f, true, rootFolderForNfs, hostname);
                    break;
                }
                case BeanScan.EXTENSION_FILE_ARW: {
                    this.publishMainFile(importEvent, f, false, rootFolderForNfs, hostname);
                    break;
                }
                case BeanScan.EXTENSION_FILE_ARW_COF:
                case BeanScan.EXTENSION_ARW_COP:
                case BeanScan.EXTENSION_ARW_COS:
                case BeanScan.EXTENSION_ARW_COMASK: {
                    this.publishSubFile(importEvent, f, rootFolderForNfs, hostname);
                    break;
                }
            }
        } catch (Exception e) {
            BeanScan.LOGGER.error(
                "[EVENT][{}] Error processFoundFile {} : {} ",
                f.getName(),
                f.getAbsoluteFile(),
                ExceptionUtils.getStackTrace(e));
            this.filesProcessed.remove(f);
        }
    }

    private void publishMainFile(
        ImportEvent importEvent,
        File mainFile,
        boolean isCompressed,
        String rootFolderForNfs,
        String hostname
    ) {
        this.publishFile(
            this.buildFileToProcess(importEvent, mainFile, isCompressed, rootFolderForNfs, hostname),
            this.outputTopic);
    }

    private void publishFile(FileToProcess fileToProcess, String topic) {
        BeanScan.LOGGER.info("[EVENT][{}] publish file {} ", fileToProcess.getName(), fileToProcess.toString());
        this.producerForPublishingOnFileTopic.send(
            new ProducerRecord<String, FileToProcess>(topic,
                KeysBuilder.topicFileKeyBuilder()
                    .withFileName(fileToProcess.getName())
                    .withFilePath(fileToProcess.getPath())
                    .build(),
                fileToProcess));
        this.producerForPublishingOnFileTopic.flush();
    }

    private FileToProcess buildFileToProcess(
        ImportEvent importEvent,
        File file,
        boolean isCompressed,
        String rootFolderForNfs,
        String hostname
    ) {
        return FileToProcess.builder()
            .withRootForNfs(rootFolderForNfs)
            .withDataId(file.getName())
            .withName(file.getName())
            .withHost(hostname)
            .withImageId("<UNSET>")
            .withPath(file.getAbsolutePath())
            .withCompressedFile(isCompressed)
            .withImportEvent(importEvent)
            .build();
    }

    private FileToProcess buildFileToProcess(
        ImportEvent importEvent,
        File file,
        File associatedFile,
        String rootFolderForNfs,
        String hostname
    ) {
        return FileToProcess.builder()
            .withCompressedFile(false)
            .withRootForNfs(rootFolderForNfs)
            .withDataId(file.getName())
            .withName(file.getName())
            .withHost(hostname)
            .withPath(file.getAbsolutePath())
            .withImportDate(System.currentTimeMillis())
            .withImportEvent(importEvent)
            .withParent(
                this.buildFileToProcess(
                    importEvent,
                    associatedFile,
                    BeanScan.EXTENSTION_EIP.equals(FilenameUtils.getExtension(associatedFile.getName())),
                    rootFolderForNfs,
                    hostname))
            .build();
    }

    private void publishSubFile(ImportEvent importEvent, File file, String rootFolderForNfs, String hostname) {
        File parentFolder = file.getParentFile()
            .getParentFile();
        if (parentFolder.exists()) {
            File associatedFile = new File(parentFolder,
                file.getName()
                    .substring(
                        0,
                        file.getName()
                            .indexOf("."))
                    + BeanScan.EXTENSION_FILE_ARW);
            if (associatedFile.exists()) {
                this.publishFile(
                    this.buildFileToProcess(importEvent, file, associatedFile, rootFolderForNfs, hostname),
                    this.outputParentTopic);
            }
        }
    }
}
