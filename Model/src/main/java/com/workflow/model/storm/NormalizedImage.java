package com.workflow.model.storm;

import java.io.Serializable;

public class NormalizedImage implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	protected String id;
	protected byte[] compressedImage;

	public String getId() {
		return id;
	}

	public byte[] getCompressedImage() {
		return compressedImage;
	}

	public void setCompressedImage(byte[] compressedImage) {
		this.compressedImage = compressedImage;
	}

	public void setId(String id) {
		this.id = id;
	}

	public NormalizedImage(String id, byte[] compressedImage) {
		super();
		this.id = id;
		this.compressedImage = compressedImage;
	}

	public NormalizedImage() {
	}

}
