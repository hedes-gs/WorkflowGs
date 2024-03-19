package com.workflow.model.storm;

import java.io.Serializable;
import java.util.List;

public class ImageAndLut implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	protected String id;
	protected byte[] compressedImage;
	protected List<int[]> lut;

	public String getId() {
		return id;
	}

	public byte[] getCompressedImage() {
		return compressedImage;
	}

	public List<int[]> getLut() {
		return lut;
	}

	public ImageAndLut(String id, byte[] compressedImage, List<int[]> lut) {
		super();
		this.id = id;
		this.compressedImage = compressedImage;
		this.lut = lut;
	}

	public ImageAndLut() {
	}

}
