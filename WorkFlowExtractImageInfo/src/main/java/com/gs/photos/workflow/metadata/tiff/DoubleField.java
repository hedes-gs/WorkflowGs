package com.gs.photos.workflow.metadata.tiff;

import java.util.Arrays;

import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public class DoubleField extends TiffField<double[]> {
	private static final long serialVersionUID = 1L;

	public DoubleField(Tag tag, SimpleAbstractField<double[]> underLayingField, short tagValue) {
		super(tag, underLayingField, tagValue);
	}

	@Override
	public String getDataAsString() {
		return Arrays.toString(underLayingField.getData());
	}
}