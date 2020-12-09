package com.gs.photo.workflow;

import java.util.Collection;
import java.util.Optional;

import com.gs.photos.workflow.metadata.IFD;

public interface IFileMetadataExtractor {

    Optional<Collection<IFD>> readIFDs(String filePath);
}
