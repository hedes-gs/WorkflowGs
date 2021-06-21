package com.gs.photo.workflow.extimginfo;

import java.util.Collection;
import java.util.Optional;

import com.gs.photos.workflow.extimginfo.metadata.IFD;
import com.workflow.model.files.FileToProcess;

public interface IFileMetadataExtractor {

    Optional<Collection<IFD>> readIFDs(FileToProcess fileToProcess);
}
