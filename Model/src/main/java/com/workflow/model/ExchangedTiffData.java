package com.workflow.model;

import java.io.Serializable;

public class ExchangedTiffData implements Serializable {

	private static final long serialVersionUID = 1L;
	protected String key;
	protected String imageId;
	protected short tag;
	protected FieldType fieldType;
	protected int length;
	protected int[] dataAsInt = {};
	protected short[] dataAsShort = {};
	protected byte[] dataAsByte = {};
	protected String id;
	private int intId;
	private int total;

	public int[] getDataAsInt() {
		return dataAsInt;
	}

	public void setDataAsInt(int[] dataAsInt) {
		this.dataAsInt = dataAsInt;
	}

	public short[] getDataAsShort() {
		return dataAsShort;
	}

	public void setDataAsShort(short[] dataAsShort) {
		this.dataAsShort = dataAsShort;
	}

	public byte[] getDataAsByte() {
		return dataAsByte;
	}

	public void setDataAsByte(byte[] dataAsByte) {
		this.dataAsByte = dataAsByte;
	}

	public short getTag() {
		return tag;
	}

	public FieldType getFieldType() {
		return fieldType;
	}

	public int getLength() {
		return length;
	}

	public String getId() {
		return id;
	}

	public String getKey() {
		return key;
	}

	public int getIntId() {
		return intId;
	}

	public int getTotal() {
		return total;
	}

	public String getImageId() {
		return imageId;
	}

	public ExchangedTiffData(
			String imageId,
			short tag,
			int length,
			short fieldType,
			Object data,
			String id,
			String key,
			int intId,
			int total) {
		super();
		this.imageId = imageId;
		this.tag = tag;
		this.fieldType = FieldType.fromShort(
			fieldType);
		this.length = length;
		Object internalData = data;
		this.key = key;
		this.intId = intId;
		this.total = total;
		if (internalData instanceof int[])
			this.dataAsInt = (int[]) internalData;
		else if (internalData instanceof short[])
			this.dataAsShort = (short[]) internalData;
		else if (internalData instanceof byte[])
			this.dataAsByte = (byte[]) internalData;
		else if (internalData instanceof String)
			this.dataAsByte = ((String) internalData).getBytes();
		else
			throw new IllegalArgumentException();
		this.id = id;
	}

	public ExchangedTiffData() {
		super();
		key = null;
	}

}
