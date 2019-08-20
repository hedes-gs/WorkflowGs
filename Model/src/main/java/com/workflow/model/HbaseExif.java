package com.workflow.model;

import java.io.Serializable;

public class HbaseExif implements Serializable {
	private static final long serialVersionUID = 1L;
	// Row key
	protected short tagId;
	protected String imageId;

	// data
	protected String tagName;
	protected short tagType;
	protected byte[] data;
	protected int cardId;

	public short getTagId() {
		return tagId;
	}

	public void setTagId(short tagId) {
		this.tagId = tagId;
	}

	public String getImageId() {
		return imageId;
	}

	public void setImageId(String imageId) {
		this.imageId = imageId;
	}

	public String getTagName() {
		return tagName;
	}

	public void setTagName(String tagName) {
		this.tagName = tagName;
	}

	public short getTagType() {
		return tagType;
	}

	public void setTagType(short tagType) {
		this.tagType = tagType;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public int getCardId() {
		return cardId;
	}

	public void setCardId(int cardId) {
		this.cardId = cardId;
	}

	public HbaseExif() {
	}

}
