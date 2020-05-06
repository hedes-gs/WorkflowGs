package com.gs.photo.workflow.impl;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.IScan;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.files.FileToProcess;

@Component
public class BeanScan implements IScan {

    private static final String               EXTENSTION_EIP         = "EIP";

    private static final String               EXTENSION_ARW_COMASK   = "COMASK";

    private static final String               EXTENSION_ARW_COS      = "COS";

    private static final String               EXTENSION_ARW_COP      = "COP";

    private static final String               EXTENSION_FILE_ARW_COF = "COF";

    private static final String               EXTENSION_FILE_ARW     = "ARW";

    protected static final org.slf4j.Logger   LOGGER                 = LoggerFactory.getLogger(IScan.class);

    protected ReentrantReadWriteLock          lock                   = new ReentrantReadWriteLock();

    @Value("${topic.topicScannedFiles}")
    protected String                          outputTopic;

    @Value("${topic.topicScannedFilesChild}")
    protected String                          outputParentTopic;

    @Value("${scan.folder}")
    protected String                          folder;

    protected Set<File>                       filesProcessed;

    @Autowired
    protected Producer<String, FileToProcess> producerForPublishingOnFileTopic;

    protected Map<String, String>             mapOfFiles             = new HashMap<>();

    protected String                          hostname;

    @Autowired
    protected IBeanTaskExecutor               beanTaskExecutor;

    @Autowired
    protected FileUtils                       fileUtils;

    @PostConstruct
    protected void init() {
        try {
            String[] splitFolder = this.folder.split("\\:");
            if ((splitFolder != null) && (splitFolder.length == 2)) {
                this.hostname = splitFolder[0];
                this.folder = splitFolder[1];
            } else {
            }
            InetAddress ip = InetAddress.getLocalHost();
            this.hostname = ip.getHostName();
        } catch (IOException e) {
            BeanScan.LOGGER.warn("Unable to start due to : ", e);
            throw new RuntimeException(e);
        }
        this.filesProcessed = ConcurrentHashMap.newKeySet();
        this.beanTaskExecutor.execute(() -> this.scan());
    }

    private void scan() {
        while (true) {
            try {
                try (
                    Stream<File> stream = this.fileUtils.toStream(
                        Paths.get(this.folder),
                        BeanScan.EXTENSTION_EIP,
                        BeanScan.EXTENSION_ARW_COMASK,
                        BeanScan.EXTENSION_ARW_COS,
                        BeanScan.EXTENSION_ARW_COP,
                        BeanScan.EXTENSION_FILE_ARW_COF,
                        BeanScan.EXTENSION_FILE_ARW)) {
                    stream.filter((f) -> this.isNotAlreadyProcessed(f))
                        .parallel()
                        .forEach((f) -> this.processFoundFile(f));
                }
            } catch (RuntimeException e) {
                if (e.getCause() instanceof NoSuchFileException) {
                    BeanScan.LOGGER.warn("Warning : {} is not available..", this.folder);
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
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                BeanScan.LOGGER.warn("Interruption received : stoping..");
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

    public void processFoundFile(File f) {
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
                    this.publishMainFile(f, true);
                    break;
                }
                case BeanScan.EXTENSION_FILE_ARW: {
                    this.publishMainFile(f, false);
                    break;
                }
                case BeanScan.EXTENSION_FILE_ARW_COF:
                case BeanScan.EXTENSION_ARW_COP:
                case BeanScan.EXTENSION_ARW_COS:
                case BeanScan.EXTENSION_ARW_COMASK: {
                    this.publishSubFile(f);
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

    private void publishMainFile(File mainFile, boolean isCompressed) {
        this.publishFile(this.buildFileToProcess(mainFile, isCompressed), this.outputTopic);
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

    private FileToProcess buildFileToProcess(File file, boolean isCompressed) {
        return FileToProcess.builder()
            .withDataId(file.getName())
            .withName(file.getName())
            .withHost(this.hostname)
            .withPath(file.getAbsolutePath())
            .withCompressedFile(isCompressed)
            .build();
    }

    private FileToProcess buildFileToProcess(File file, File associatedFile) {
        return FileToProcess.builder()
            .withCompressedFile(false)
            .withDataId(file.getName())
            .withName(file.getName())
            .withHost(this.hostname)
            .withPath(file.getAbsolutePath())
            .withParent(
                this.buildFileToProcess(
                    associatedFile,
                    BeanScan.EXTENSTION_EIP.equals(FilenameUtils.getExtension(associatedFile.getName()))))
            .build();
    }

    private void publishSubFile(File file) {
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
                this.publishFile(this.buildFileToProcess(file, associatedFile), this.outputParentTopic);
            }
        }
    }
}
