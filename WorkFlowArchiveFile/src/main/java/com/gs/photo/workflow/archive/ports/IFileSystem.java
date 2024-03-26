package com.gs.photo.workflow.archive.ports;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.Path;

public interface IFileSystem extends Closeable {

    boolean mkdirs(Path folderWhereRecord) throws IOException;

    OutputStream create(Path hdfsFilePath, boolean b) throws IOException;

    boolean exists(Path hdfsFilePath) throws IOException;

}
