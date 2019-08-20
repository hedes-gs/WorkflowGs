package com.gs.photos.workflow.metadata.tiff;

import com.gs.photos.workflow.metadata.StringUtils;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public class RationalField extends TiffField<int[]> {
	private static final long serialVersionUID = 1L;

	public RationalField(Tag tag, SimpleAbstractField<int[]> underLayingField, short tagValue) {
		super(tag, underLayingField, tagValue);
	}

	@Override
	public String getDataAsString() {
		return StringUtils.longArrayToString(getData(), 0, 10, true);
	}
}