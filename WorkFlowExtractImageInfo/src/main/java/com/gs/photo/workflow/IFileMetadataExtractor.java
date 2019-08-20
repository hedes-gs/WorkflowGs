package com.gs.photo.workflow;

import java.nio.file.Path;
import java.util.Collection;

import com.gs.photos.workflow.metadata.IFD;

public interface IFileMetadataExtractor {

	Collection<IFD> readIFDs(Path filePath);
}
