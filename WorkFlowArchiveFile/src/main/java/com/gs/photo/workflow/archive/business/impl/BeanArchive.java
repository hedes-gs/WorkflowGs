package com.gs.photo.workflow.archive.business.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.gs.instrumentation.TimedBean;
import com.gs.photo.common.workflow.impl.FileUtils;
import com.gs.photo.workflow.archive.business.IBeanArchive;
import com.gs.photo.workflow.archive.config.SpecificApplicationProperties;
import com.gs.photo.workflow.archive.ports.IFileSystem;
import com.gs.photo.workflow.archive.ports.IFileUtils;
import com.workflow.model.files.FileToProcess;

import io.micrometer.core.annotation.Timed;

@TimedBean
public class BeanArchive implements IBeanArchive<FileToProcess> {
    private static Logger                   LOGGER = LoggerFactory.getLogger(BeanArchive.class);

    @Autowired
    protected SpecificApplicationProperties specificApplicationProperties;

    @Autowired
    protected IFileUtils                    fileUtils;

    @Override
    public CompletableFuture<Optional<FileToProcess>> asyncArchiveFile(IFileSystem hdfsFileSystem, FileToProcess f) {
        return CompletableFuture.supplyAsync(
            () -> this.doArchiveFileInHdfs(hdfsFileSystem, f),
            Executors.newVirtualThreadPerTaskExecutor());
    }

    @Timed
    private Optional<FileToProcess> doArchiveFileInHdfs(IFileSystem hdfsFileSystem, FileToProcess value) {
        BeanArchive.LOGGER.info("[EVENT {}] Process file to record in HDFS {} ", value.getDataId(), value);
        try {
            String importName = value.getImportEvent()
                .getImportName();
            importName = StringUtils.isEmpty(importName) ? "DEFAULT_IMPORT" : importName;
            final Path folderWhereRecord = new Path(
                new Path(this.specificApplicationProperties.getRootPath(), importName),
                new Path(value.getDataId()));
            boolean dirIsCreated = hdfsFileSystem.mkdirs(folderWhereRecord);
            if (dirIsCreated) {
                final Path hdfsFilePath = this
                    .build(folderWhereRecord, "/" + FileUtils.getSimpleNameFromUrl(value.getUrl()));
                try (
                    OutputStream fdsOs = hdfsFileSystem.create(hdfsFilePath, true)) {
                    try {
                        this.fileUtils.copyRemoteToLocal(value, fdsOs);
                        boolean isDeleted = this.fileUtils.deleteIfLocal(value, "/localcache");
                        if (!isDeleted) {
                            BeanArchive.LOGGER.warn("[ARCHIVE]File {} is not deleted ", value);
                        }
                    } catch (IOException e) {
                        if (hdfsFileSystem.exists(hdfsFilePath)) {
                            BeanArchive.LOGGER.warn("[ARCHIVE]File {} already exist - {} ", value);
                        } else {
                            BeanArchive.LOGGER
                                .error("[ARCHIVE]File {} does not  exist in hdfs - {}", value, hdfsFilePath);
                        }
                    }
                    return Optional.of(value);
                }
            } else {
                BeanArchive.LOGGER.warn("Unable to create the HDFS folder {} ", folderWhereRecord);
            }
        } catch (IOException e) {
            BeanArchive.LOGGER.warn("Exception while processing {} : {} ", value, ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
        BeanArchive.LOGGER.info("[EVENT][{}] End of record file in HDFS {} ", value.getDataId(), value);
        return Optional.empty();
    }

    private Path build(Path rootPath2, String key) { return Path.mergePaths(rootPath2, new Path(key)); }

}
