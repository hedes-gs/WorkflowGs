package com.gs.photo.workflow.extimginfo;

import java.util.Optional;

import com.workflow.model.files.FileToProcess;

public interface IAccessDirectlyFile {

    Optional<byte[]> readFirstBytesOfFileRetry(FileToProcess fileToProcess);

    Optional<byte[]> readFirstBytesOfFileRetryWithbufferIncreased(FileToProcess fileToProcess);

}
