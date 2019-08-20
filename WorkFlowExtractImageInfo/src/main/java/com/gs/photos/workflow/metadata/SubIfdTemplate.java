package com.gs.photos.workflow.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;
import com.gs.photos.workflow.metadata.tiff.TiffTag;

public class SubIfdTemplate extends AbstractTemplateTag {

	private static final Logger LOGGER = LoggerFactory.getLogger(SubIfdTemplate.class);

	protected SimpleAbstractField<int[]> data;

	@Override
	protected void buildChildren(FileChannelDataInput rin) {
		System.out.println("... buildChildren in  SubIfdTemplate ");

		for (int ifd = 0; ifd < data.getData().length; ifd++) {
			try {
				super.createSimpleTiffFields(rin, data.getData()[0]);
			} catch (Exception e) {
				getIfdParent().removeField(tag);
				LOGGER.error("Unable to read TiffTag.SUB_IFDS", e);
			}
		}
		System.out.println("... end of buildChildren in  SubIfdTemplate ");

	}

	public SubIfdTemplate(Tag tag, IFD ifdParent, SimpleAbstractField<int[]> data) {
		super(tag, ifdParent);
		this.data = data;
	}

	@Override
	protected Tag convertTagValueToTag(short tagValue) {
		return TiffTag.fromShort(tagValue);
	}

}
