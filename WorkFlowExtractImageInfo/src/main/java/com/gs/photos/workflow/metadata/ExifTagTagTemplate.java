package com.gs.photos.workflow.metadata;

import com.gs.photos.workflow.metadata.exif.ExifTag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public class ExifTagTagTemplate extends AbstractTemplateTag {

	protected SimpleAbstractField<int[]> data;

	@Override
	protected void buildChildren(FileChannelDataInput rin) {

		if (data.getData()[0] != 0) {
			super.createSimpleTiffFields(rin, data.getData()[0]);
		}

	}

	public ExifTagTagTemplate(Tag tag, IFD ifdParent, SimpleAbstractField<int[]> data) {
		super(tag, ifdParent);
		this.data = data;
	}

	@Override
	public Tag convertTagValueToTag(short tag) {
		return ExifTag.fromShort(tag);
	}

}
