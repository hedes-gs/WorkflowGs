package com.gs.photo.workflow.cmphashkey;

import java.io.IOException;
import java.nio.file.Path;

import com.workflow.model.files.FileToProcess;

public interface IBeanImageFileHelper {

    public String getFullPathName(Path filePath);

    String computeHashKey(byte[] byteBuffer) throws IOException;

    byte[] readFirstBytesOfFile(FileToProcess file) throws IOException;

}
