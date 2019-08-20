package com.gs.photos.workflow.metadata;

import com.gs.photos.workflow.metadata.tiff.TiffTag;

public class DefaultTagTemplate extends AbstractTemplateTag {

	public DefaultTagTemplate(Tag tag, IFD parent) {
		super(tag, new IFD());
	}

	public DefaultTagTemplate(Tag tag) {
		super(tag, new IFD());
	}

	@Override
	protected Tag convertTagValueToTag(short tagValue) {
		return TiffTag.fromShort(tagValue);
	}

}
