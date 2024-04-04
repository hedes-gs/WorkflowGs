package com.gs.photo.workflow.archive.business;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import com.gs.photo.workflow.archive.ports.IFileSystem;

public interface IBeanArchive<T> {

    public CompletableFuture<Optional<T>> asyncArchiveFile(IFileSystem hdfsFileSystem, T f);
}
