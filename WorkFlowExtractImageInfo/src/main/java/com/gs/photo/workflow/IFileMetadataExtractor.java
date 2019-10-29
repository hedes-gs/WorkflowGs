package com.gs.photo.workflow;

import java.util.Collection;

import com.gs.photos.workflow.metadata.IFD;

public interface IFileMetadataExtractor {

	Collection<IFD> readIFDs(String filePath);
}
