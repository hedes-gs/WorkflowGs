package com.gs.photo.workflow.scan.business;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.gs.photo.common.workflow.impl.AbstractRemoteFile;

public interface IRetrieveFilesFromFolder {

    public static interface IProcessFoundFile {

    }

    public void process(
        String url,
        Consumer<AbstractRemoteFile> processFile,
        BiConsumer<AbstractRemoteFile, AbstractRemoteFile> processSubFile
    );

    public void process(
        String url,
        Consumer<AbstractRemoteFile> processFile,
        BiConsumer<AbstractRemoteFile, AbstractRemoteFile> processSubFile,
        int nbMaxOfFilesToProcess
    );
}
