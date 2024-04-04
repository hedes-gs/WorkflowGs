package com.gs.photo.workflow.cmphashkey.business;

import java.io.IOException;
import java.nio.file.Path;

public interface IBeanImageFileHelper {

    public String getFullPathName(Path filePath);

    String computeHashKey(byte[] byteBuffer) throws IOException;

}
