package com.gs.photo.workflow.scan.business;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gs.photo.common.workflow.impl.AbstractRemoteFile;
import com.gs.photo.common.workflow.impl.FileUtils;
import com.gs.photo.workflow.scan.config.SpecificApplicationProperties;

@Service
public class RetrieveFilesFromFolder implements IRetrieveFilesFromFolder {

    protected static final org.slf4j.Logger LOGGER                  = LoggerFactory
        .getLogger(RetrieveFilesFromFolder.class);
    private static final String             EXTENSTION_EIP          = "EIP";
    private static final String             EXTENSION_ARW_COMASK    = "COMASK";
    private static final String             EXTENSION_ARW_COS       = "COS";
    private static final String             EXTENSION_ARW_COP       = "COP";
    private static final String             EXTENSION_FILE_ARW_COF  = "COF";
    private static final String             EXTENSION_FILE_ARW      = "ARW";
    protected Set<AbstractRemoteFile>       filesProcessedOfSession = ConcurrentHashMap.newKeySet();
    protected Set<AbstractRemoteFile>       filesProcessed          = ConcurrentHashMap.newKeySet();

    @Autowired
    protected SpecificApplicationProperties applicationProperties;

    @Autowired
    protected FileUtils                     fileUtils;
    protected ReentrantReadWriteLock        lock                    = new ReentrantReadWriteLock();

    @Override
    public void process(
        String url,
        Consumer<AbstractRemoteFile> consumerMainFile,
        BiConsumer<AbstractRemoteFile, AbstractRemoteFile> consumerSubFile,
        int maxNbOfFileToProcess
    ) {
        this.filesProcessedOfSession.clear();
        try (
            Stream<AbstractRemoteFile> stream = this.fileUtils
                .toStream(new URL(url), this.applicationProperties.getFileExtensions())) {
            stream.filter((f) -> this.isNotAlreadyProcessed(f))
                .takeWhile((f) -> this.isNotTestModeOrMaxNbOfElementsReached(maxNbOfFileToProcess))
                .parallel()
                .forEach((f) -> this.processFoundFile(consumerMainFile, consumerSubFile, f));
        } catch (MalformedURLException e) {
            RetrieveFilesFromFolder.LOGGER.warn("Unexpected error ", ExceptionUtils.getStackTrace(e));
        } catch (IOException e) {
            RetrieveFilesFromFolder.LOGGER.warn("Unexpected error ", ExceptionUtils.getStackTrace(e));
        }
    }

    private void processFoundFile(
        Consumer<AbstractRemoteFile> consumerMainFile,
        BiConsumer<AbstractRemoteFile, AbstractRemoteFile> consumerSubFile,
        AbstractRemoteFile f
    ) {
        final String currentFileName = f.getName();
        String extension = FilenameUtils.getExtension(currentFileName);
        switch (extension.toUpperCase()) {
            case EXTENSTION_EIP:
            case EXTENSION_FILE_ARW: {
                consumerMainFile.accept(f);
                break;
            }
            case EXTENSION_FILE_ARW_COF:
            case EXTENSION_ARW_COP:
            case EXTENSION_ARW_COS:
            case EXTENSION_ARW_COMASK: {
                this.publishSubFile(consumerSubFile, f);
                break;
            }
        }

        this.lock.writeLock()
            .lock();
        try {
            this.filesProcessed.add(f);
            this.filesProcessedOfSession.add(f);
        } finally {
            this.lock.writeLock()
                .unlock();
        }
    }

    @Override
    public void process(
        String url,
        Consumer<AbstractRemoteFile> consumerMainFile,
        BiConsumer<AbstractRemoteFile, AbstractRemoteFile> consumerSubFile
    ) {
        this.filesProcessedOfSession.clear();
        try (
            Stream<AbstractRemoteFile> stream = this.fileUtils
                .toStream(new URL(url), this.applicationProperties.getFileExtensions())) {
            stream.filter((f) -> this.isNotAlreadyProcessed(f))
                .parallel()
                .forEach((f) -> this.processFoundFile(consumerMainFile, consumerSubFile, f));
        } catch (MalformedURLException e) {
            RetrieveFilesFromFolder.LOGGER.warn("Unexpected error ", ExceptionUtils.getStackTrace(e));
        } catch (IOException e) {
            RetrieveFilesFromFolder.LOGGER.warn("Unexpected error ", ExceptionUtils.getStackTrace(e));
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

    private boolean isNotTestModeOrMaxNbOfElementsReached(int maxNbOfFileToProcess) {
        this.lock.readLock()
            .lock();
        try {
            return this.filesProcessedOfSession.size() < maxNbOfFileToProcess;
        } finally {
            this.lock.readLock()
                .unlock();
        }
    }

    private void publishSubFile(
        BiConsumer<AbstractRemoteFile, AbstractRemoteFile> consumerSubFile,
        AbstractRemoteFile file
    ) {
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
                        + RetrieveFilesFromFolder.EXTENSION_FILE_ARW);
                if ((associatedFile != null) && associatedFile.exists()) {
                    consumerSubFile.accept(associatedFile, file);
                }
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
