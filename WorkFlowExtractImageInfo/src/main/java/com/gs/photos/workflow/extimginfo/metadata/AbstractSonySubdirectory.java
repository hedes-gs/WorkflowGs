package com.gs.photos.workflow.extimginfo.metadata;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.exif.ExifRecordTag;
import com.gs.photo.common.workflow.exif.FieldType;
import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.IFD.IFDContext;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleFieldFactory;
import com.gs.photos.workflow.extimginfo.metadata.tiff.TiffField;

public abstract class AbstractSonySubdirectory extends AbstractTemplateTag {
    private Logger                        LOGGER = LoggerFactory.getLogger(AbstractSonySubdirectory.class);

    protected SimpleAbstractField<byte[]> data;

    protected void addByteField(
        FileChannelDataInput rin,
        IFDContext ifdContext,
        int offset,
        String tagName,
        short tag,
        int index
    ) throws IOException {
        rin.position(offset);
        Tag ftag = ExifRecordTag.builder()
            .withName(tagName)
            .withFieldType(FieldType.SBYTE)
            .withValue(tag)
            .build();
        if (this.LOGGER.isDebugEnabled()) {
            this.LOGGER.debug(
                " Found at index {} :  tag {}, decoded type is {}, at offset {} - field length {} - IFD is {} ",
                index,
                ftag,
                FieldType.SBYTE,
                offset,
                1,
                this.tiffIFD);
        }
        SimpleAbstractField<?> saf = SimpleFieldFactory.createField(FieldType.SBYTE.getValue(), 1, offset);
        saf.updateData(rin);
        final TiffField<?> tiffField = saf.createTiffField(this.tiffIFD.getTag(), ftag, tag);
        this.tiffIFD.addField(tiffField, ifdContext);
    }

    protected void addLongField(
        FileChannelDataInput rin,
        IFDContext ifdContext,
        int offset,
        String tagName,
        short tag,
        int index
    ) throws IOException {
        rin.position(offset);
        Tag ftag = ExifRecordTag.builder()
            .withName(tagName)
            .withFieldType(FieldType.SLONG)
            .withValue(tag)
            .build();
        SimpleAbstractField<?> saf = SimpleFieldFactory.createField(FieldType.SLONG.getValue(), 1, offset);
        if (this.LOGGER.isDebugEnabled()) {
            this.LOGGER.debug(
                " Found at index {} :  tag {}, decoded type is {}, at offset {} - field length {} - IFD is {} ",
                index,
                ftag,
                FieldType.SLONG,
                offset,
                1,
                this.tiffIFD);
        }
        saf.updateData(rin);
        final TiffField<?> tiffField = saf.createTiffField(this.tiffIFD.getTag(), ftag, tag);
        this.tiffIFD.addField(tiffField, ifdContext);
    }

    protected void addBytesField(
        FileChannelDataInput rin,
        IFDContext ifdContext,
        int offset,
        int length,
        String tagName,
        short tag,
        int index
    ) throws IOException {
        rin.position(offset);
        Tag ftag = ExifRecordTag.builder()
            .withName(tagName)
            .withFieldType(FieldType.SBYTE)
            .withValue(tag)
            .build();
        SimpleAbstractField<?> saf = SimpleFieldFactory.createField(FieldType.SBYTE.getValue(), length, offset);
        if (this.LOGGER.isDebugEnabled()) {
            this.LOGGER.debug(
                " Found at index {} :  tag {}, decoded type is {}, at offset {} - field length {} - IFD is {} ",
                index,
                ftag,
                FieldType.SBYTE,
                offset,
                1,
                this.tiffIFD);
        }
        saf.updateData(rin);
        final TiffField<?> tiffField = saf.createTiffField(this.tiffIFD.getTag(), ftag, tag);
        this.tiffIFD.addField(tiffField, ifdContext);
    }

    public AbstractSonySubdirectory(
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
