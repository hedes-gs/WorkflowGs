package com.gs.photos.workflow.metadata.tiff;

import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public final class ASCIIField extends TiffField<String> {
	private static final long serialVersionUID = 1L;

	public ASCIIField(Tag tag, SimpleAbstractField<String> data, short tagValue) {
		super(tag, data, tagValue);
	}

	@Override
	public String getDataAsString() {
		return underLayingField.getData();
	}
}