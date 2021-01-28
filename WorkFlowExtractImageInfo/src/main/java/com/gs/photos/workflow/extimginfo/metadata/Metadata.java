package com.gs.photos.workflow.extimginfo.metadata;

import org.slf4j.LoggerFactory;
import org.springframework.core.type.classreading.MetadataReader;

public abstract class Metadata implements MetadataReader, Iterable<MetadataEntry> {
	protected Metadata(MetadataType type) {
		this.type = type;
	}

	protected Metadata(MetadataType exif, byte[] data) {
		this(exif);
		this.data = data;
	}

	public static final int IMAGE_MAGIC_NUMBER_LEN = 4;
	// Fields
	protected MetadataType type;
	protected byte[] data;
	protected boolean isDataRead;

	// Obtain a logger instance
	private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(Metadata.class);

}