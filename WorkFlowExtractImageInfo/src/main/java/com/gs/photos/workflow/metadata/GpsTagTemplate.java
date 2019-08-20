package com.gs.photos.workflow.metadata;

import com.gs.photos.workflow.metadata.exif.GPSTag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public class GpsTagTemplate extends AbstractTemplateTag {

	protected SimpleAbstractField<int[]> data;

	@Override
	protected void buildChildren(FileChannelDataInput rin) {
		System.out.println("... buildChildren in  GpsTagTemplate ");

		if (data.getData()[0] != 0) {
			super.createSimpleTiffFields(rin, data.getData()[0]);
		}
	}

	public GpsTagTemplate(Tag tag, IFD ifdParent, SimpleAbstractField<int[]> data) {
		super(tag, ifdParent);
		this.data = data;
	}

	@Override
	public Tag convertTagValueToTag(short tag) {
		return GPSTag.fromShort(tag);
	}

}
