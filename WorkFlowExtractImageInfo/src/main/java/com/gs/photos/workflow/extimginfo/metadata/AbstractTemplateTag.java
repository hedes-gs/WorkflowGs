package com.gs.photos.workflow.extimginfo.metadata;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gs.photo.common.workflow.exif.ExifRecordTag;
import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.IFD.IFDContext;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleFieldFactory;
import com.gs.photos.workflow.extimginfo.metadata.tiff.TiffField;
import com.gs.photos.workflow.extimginfo.metadata.tiff.TiffTag;
import com.workflow.model.FieldType;

public abstract class AbstractTemplateTag {
    private Logger               LOGGER                  = LogManager.getLogger(AbstractTemplateTag.class);

    private static final int     NB_OF_BYTES_FOR_AN_INT  = 4;
    private static final int     NB_OF_BYTES_FOR_A_SHORT = 2;
    protected final Tag          tag;
    protected final IFD          tiffIFD;
    protected final IFD          ifdParent;
    protected final IExifService exifService;

    public IFD getRootIFD() { return this.tiffIFD; }

    public int createSimpleTiffFields(FileChannelDataInput rin, int offset, IFDContext ifdContext) {
        rin.position(offset);
        try {
            int nbOfFields = rin.readShort();
            offset += AbstractTemplateTag.NB_OF_BYTES_FOR_A_SHORT;
            if (this.LOGGER.isDebugEnabled()) {
                this.LOGGER.info(
                    "Started create simple tiff fields {} entries - {} offset is {} ",
                    this.tiffIFD,
                    nbOfFields,
                    offset);
            }
            for (int i = 0; i < nbOfFields; i++) {
                int currentOffset = offset;
                rin.position(offset);
                short tagValue = rin.readShort();
                offset += AbstractTemplateTag.NB_OF_BYTES_FOR_A_SHORT;
                rin.position(offset);
                short type = rin.readShort();
                offset += AbstractTemplateTag.NB_OF_BYTES_FOR_A_SHORT;
                rin.position(offset);
                int field_length = rin.readInt();
                offset += AbstractTemplateTag.NB_OF_BYTES_FOR_AN_INT;
                SimpleAbstractField<?> saf = SimpleFieldFactory.createField(type, field_length, offset);
                saf.updateData(rin);
                Tag ftag = this.convertTagValueToTag(tagValue, type);
                final TiffField<?> tiffField = saf.createTiffField(this.tiffIFD.getTag(), ftag, tagValue);
                this.tiffIFD.addField(tiffField, ifdContext);
                if (this.LOGGER.isDebugEnabled()) {
                    this.LOGGER.debug(
                        " Found at index {} :  tag {} with type {}, decoded type is {}, at offset {} - field length {} - next offset is {}  - IFD is {} ",
                        i,
                        ftag,
                        type,
                        FieldType.fromShort(type),
                        currentOffset,
                        field_length,
                        saf.getNextOffset(),
                        this.tiffIFD);
                }
                AbstractTemplateTag tagTemplate = TemplateTagFactory.create(ftag, this.tiffIFD, saf, this.exifService);
                tagTemplate.buildChildren(rin, ifdContext);
                offset = saf.getNextOffset();
            }
            if (this.LOGGER.isDebugEnabled()) {
                this.LOGGER.debug("End of simple tiff fields {} - {} ", this.tiffIFD, nbOfFields);
            }
            if (this.tiffIFD.imageIsPresent()) {
                this.buildImage(rin);
            }
            rin.position(offset);
            int nextOffset = rin.readInt();
            return nextOffset;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void buildImage(FileChannelDataInput rin) throws IOException {
        rin.position(this.tiffIFD.getJpegImagePosition());
        int endOffset = this.tiffIFD.getJpegImageLength() + rin.position();
        short imgHeight = 0;
        short imgWidth = 0;
        short SOIThumbnail = rin.readShortAsBigEndian();
        if (SOIThumbnail == (short) 0xffd8) {
            boolean finished = false;
            boolean found = false;
            while (!finished) {
                short marker = rin.readShortAsBigEndian();
                found = marker == (short) 0xffc0;
                finished = found || (rin.position() >= endOffset);
                if (!finished) {
                    short lengthOfMarker = rin.readShortAsBigEndian();
                    rin.skipBytes(lengthOfMarker - 2);
                }
            }
            if (found) {
                short lengthOfMarker = rin.readShortAsBigEndian();
                byte dataPrecision = rin.readByte();
                imgHeight = rin.readShortAsBigEndian();
                imgWidth = rin.readShortAsBigEndian();
            }
            rin.position(this.tiffIFD.getJpegImagePosition());
        }
        rin.readFully(this.tiffIFD.getJpegImage());
    }

    protected Tag convertTagValueToTag(short tagValue, short type) {
        try {
            return this.exifService.getTagFrom(
                this.tiffIFD.getTag()
                    .getValue(),
                tagValue);
        } catch (Exception e) {
            return ExifRecordTag.builder()
                .withName(TiffTag.UNKNOWN.getName())
                .withFieldType(com.gs.photo.common.workflow.exif.FieldType.fromShort(type))
                .withValue(tagValue)
                .build();
        }
    }

    protected void buildChildren(FileChannelDataInput rin, IFDContext ifdContext) {

    }

    protected AbstractTemplateTag(
        Tag tag,
        IFD ifdParent,
        IExifService exifService
    ) {
        this.ifdParent = ifdParent;
        this.tag = tag;
        this.tiffIFD = new IFD(tag);
        if (ifdParent != null) {
            ifdParent.addChild(tag, this.getTiffIFD());
        }
        this.exifService = exifService;
    }

    protected AbstractTemplateTag(
        Tag tag,
        IExifService exifService
    ) { this(tag,
        null,
        exifService); }

    public void buildChildrenIfNeeded() {

    }

    public Tag getTag() { return this.tag; }

    public IFD getTiffIFD() { return this.tiffIFD; }

    public IFD getIfdParent() { return this.ifdParent; }

}
