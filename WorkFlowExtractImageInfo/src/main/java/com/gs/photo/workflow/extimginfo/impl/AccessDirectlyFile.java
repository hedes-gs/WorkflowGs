package com.gs.photo.workflow.extimginfo.impl;

import java.io.IOException;
import java.util.Optional;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.impl.FileUtils;
import com.gs.photo.workflow.extimginfo.IAccessDirectlyFile;
import com.workflow.model.files.FileToProcess;

public class AccessDirectlyFile implements IAccessDirectlyFile {

    protected static Logger LOGGER    = LoggerFactory.getLogger(AccessDirectlyFile.class);
    protected FileUtils     fileUtils = new FileUtils();

    @Override
    public Optional<byte[]> readFirstBytesOfFileRetry(FileToProcess fileToProcess) {
        try {
            return Optional.ofNullable(this.fileUtils.readFirstBytesOfFile(fileToProcess));
        } catch (IOException e) {
            AccessDirectlyFile.LOGGER
                .warn("Unable to get bytes of {} : {} ", fileToProcess, ExceptionUtils.getStackTrace(e));
            return Optional.empty();
        }
    }

    @Override
    public Optional<byte[]> readFirstBytesOfFileRetryWithbufferIncreased(FileToProcess fileToProcess) {
        return Optional.ofNullable(this.fileUtils.readFirstBytesOfFileRetryWithbufferIncreased(fileToProcess));
    }

}
