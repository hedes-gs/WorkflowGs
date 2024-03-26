package com.gs.photo.workflow.archive.ports;

import java.io.IOException;
import java.io.OutputStream;

import com.workflow.model.files.FileToProcess;

public interface IFileUtils {

    void copyRemoteToLocal(FileToProcess value, OutputStream fdsOs) throws IOException;

    boolean deleteIfLocal(FileToProcess value, String string) throws IOException;

}
