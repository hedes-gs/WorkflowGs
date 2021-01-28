package com.gs.photo.workflow.extimginfo;

import java.util.Collection;
import java.util.Optional;

import com.gs.photos.workflow.extimginfo.metadata.IFD;

public interface IFileMetadataExtractor {

    Optional<Collection<IFD>> readIFDs(String filePath);
}
