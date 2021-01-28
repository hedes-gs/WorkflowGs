package com.gs.photos.workflow.extimginfo.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.IFD.IFDContext;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;

public class MakerNoteTemplate extends AbstractTemplateTag {
    private Logger                        LOGGER = LogManager.getLogger(MakerNoteTemplate.class);

    protected SimpleAbstractField<byte[]> data;

    @Override
    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {

        int offset = super.createSimpleTiffFields(rin, this.data.getOffset(), ifdContext);
    }

    public MakerNoteTemplate(
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
