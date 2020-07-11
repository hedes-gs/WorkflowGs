package com.gs.photos.workflow.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.workflow.exif.IExifService;
import com.gs.photo.workflow.exif.Tag;
import com.gs.photos.workflow.metadata.IFD.IFDContext;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;

public class SubIfdTemplate extends AbstractTemplateTag {

    private static final Logger          LOGGER = LoggerFactory.getLogger(SubIfdTemplate.class);

    protected SimpleAbstractField<int[]> data;

    @Override
    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {

        for (int ifd = 0; ifd < this.data.getData().length; ifd++) {
            try {
                super.createSimpleTiffFields(rin, this.data.getData()[0], ifdContext);
            } catch (Exception e) {
                SubIfdTemplate.LOGGER.error("Unable to read TiffTag.SUB_IFDS", e);
            }
        }

    }

    public SubIfdTemplate(
        Tag tag,
        IFD ifdParent,
        SimpleAbstractField<int[]> data,
        IExifService exifService
    ) {
        super(tag,
            ifdParent,
            exifService);
        this.data = data;
    }

}
