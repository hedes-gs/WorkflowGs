package com.gs.photo.workflow.copyfiles;

import java.io.IOException;
import java.nio.file.Path;

public interface IServicesFile {

    Path getCurrentFolderInWhichCopyShouldBeDone(Path repositoryPath) throws IOException;

}