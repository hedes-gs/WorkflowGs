package com.gs.photos.workflow.metadata.tiff;

import com.gs.photos.workflow.metadata.StringUtils;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public final class ShortField extends TiffField<short[]> {
	private static final long serialVersionUID = 1L;

	public ShortField(Tag tag, SimpleAbstractField<short[]> underLayingField, short tagValue) {
		super(tag, underLayingField, tagValue);
	}

	@Override
	public int[] getDataAsLong() {
		//
		final short[] data = underLayingField.getData();
		int[] temp = new int[data.length];

		for (int i = 0; i < data.length; i++) {
			temp[i] = data[i] & 0xffff;
		}

		return temp;
	}

	@Override
	public String getDataAsString() {
		return StringUtils.shortArrayToString(underLayingField.getData(), 0, 10, true);
	}
}