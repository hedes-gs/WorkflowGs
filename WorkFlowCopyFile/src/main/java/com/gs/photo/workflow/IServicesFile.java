package com.gs.photo.workflow;

import java.io.IOException;
import java.nio.file.Path;

public interface IServicesFile {

	String getCurrentFolderInWhichCopyShouldBeDone(Path repositoryPath) throws IOException;

}