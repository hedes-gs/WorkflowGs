package com.gs.photo.workflow.cmphashkey.ports;

import com.workflow.model.files.FileToProcess;

public interface IFileUtils {

    byte[] readFirstBytesOfFileRetry(FileToProcess file);

}