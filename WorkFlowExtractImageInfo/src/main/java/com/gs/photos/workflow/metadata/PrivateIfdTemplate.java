package com.gs.photos.workflow.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photos.workflow.metadata.IFD.IFDContext;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;
import com.gs.photos.workflow.metadata.tiff.TiffTag;

public class PrivateIfdTemplate extends AbstractTemplateTag {

    private static final Logger           LOGGER = LoggerFactory.getLogger(PrivateIfdTemplate.class);

    protected SimpleAbstractField<byte[]> data;

    @Override
    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {

        if (this.data.getData()[0] != 0) {
            // super.createSimpleTiffFields(rin, this.data.getData()[0]);
        }
    }

    public PrivateIfdTemplate(
        Tag tag,
        IFD ifdParent,
        SimpleAbstractField<byte[]> data
    ) {
        super(tag,
            ifdParent);
        this.data = data;
    }

    @Override
    protected Tag convertTagValueToTag(short tagValue) { return TiffTag.fromShort(tagValue); }

}
