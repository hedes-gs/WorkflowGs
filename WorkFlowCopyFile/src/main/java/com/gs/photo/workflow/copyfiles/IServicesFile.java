package com.gs.photo.workflow.copyfiles;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import com.workflow.model.files.FileToProcess;

public interface IServicesFile {

    Path getCurrentFolderInWhichCopyShouldBeDone(Path repositoryPath) throws IOException;

    void copyRemoteToLocal(FileToProcess value, File destFile) throws IOException;

}