package com.gs.photos.workflow.metadata.exif;

import com.gs.photos.workflow.metadata.StringUtils;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.tiff.TiffTag.Attribute;
import com.workflow.model.FieldType;

public enum RootTiffTag implements Tag {

	ROOT_TIFF("Root tiff ", (short) 0x002A, Attribute.BASELINE);

	private final String    name;

	private final short     value;

	private final Attribute attribute;

	@Override
	public String getFieldAsString(Object value) {
		return "";
	}

	@Override
	public FieldType getFieldType() {
		return FieldType.UNKNOWN;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public short getValue() {
		return this.value;
	}

	@Override
	public boolean isCritical() {
		return true;
	}

	@Override
	public String toString() {
		return this.name + " [Value: " + StringUtils.shortToHexStringMM(this.value) + "] (" + this.getAttribute() + ")";
	}

	public Attribute getAttribute() {
		return this.attribute;
	}

	@Override
	public boolean mayContainSomeSimpleFields() {
		return false;
	}

	private RootTiffTag(
			String name,
			short value,
			Attribute attribute) {
		this.name = name;
		this.value = value;
		this.attribute = attribute;
	}

}
