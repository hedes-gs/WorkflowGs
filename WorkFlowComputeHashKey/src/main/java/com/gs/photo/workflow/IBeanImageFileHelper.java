package com.gs.photo.workflow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public interface IBeanImageFileHelper {

	void waitForCopyComplete(Path filePath);

	public String getFullPathName(Path filePath);

	String computeHashKey(ByteBuffer byteBuffer) throws IOException;

	ByteBuffer readFirstBytesOfFile(Path filePath) throws IOException;

}
