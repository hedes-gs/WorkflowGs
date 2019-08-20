package com.gs.photo.workflow;

import java.io.IOException;
import java.nio.file.Path;

public interface IBeanImageFileHelper {
	public String computeHashKey(Path filePath) throws IOException;

	void waitForCopyComplete(Path filePath);

	public String getFullPathName(Path filePath);

}
