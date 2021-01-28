package com.gs.photos.workflow.extimginfo.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.IFD.IFDContext;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;

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
        SimpleAbstractField<byte[]> data,
        IExifService exifService
    ) {
        super(tag,
            ifdParent,
            exifService);
        this.data = data;
    }

}
