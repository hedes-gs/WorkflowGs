package com.workflow.model.storm;

import java.io.Serializable;

public class FinalImage implements Serializable {

	private static final long serialVersionUID = 1L;
	protected String id;
	protected boolean original;
	protected int width;
	protected int height;
	protected byte[] compressedData;

	public boolean isOriginal() {
		return original;
	}

	public void setOriginal(boolean original) {
		this.original = original;
	}

	public int getWidth() {
		return width;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	public byte[] getCompressedImage() {
		return compressedData;
	}

	public void setCompressedData(byte[] compressedData) {
		this.compressedData = compressedData;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public FinalImage(String id, boolean original, int width, int height, byte[] compressedData) {
		super();
		this.id = id;
		this.original = original;
		this.width = width;
		this.height = height;
		this.compressedData = compressedData;
	}

	@Override
	public String toString() {
		return "FinalImage [id=" + id + ", original=" + original + ", width=" + width + ", height=" + height
				+ ", length : " + (compressedData != null ? compressedData.length : -1) + "]";
	}

	public FinalImage() {
	}

}
