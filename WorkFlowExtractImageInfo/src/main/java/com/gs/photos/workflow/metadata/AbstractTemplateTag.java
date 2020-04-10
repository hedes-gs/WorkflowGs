package com.gs.photos.workflow.metadata;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;
import com.gs.photos.workflow.metadata.fields.SimpleFieldFactory;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public abstract class AbstractTemplateTag {
    private Logger           LOGGER                  = LogManager.getLogger(AbstractTemplateTag.class);

    private static final int NB_OF_BYTES_FOR_AN_INT  = 4;
    private static final int NB_OF_BYTES_FOR_A_SHORT = 2;
    protected final Tag      tag;
    protected final IFD      tiffIFD;
    protected final IFD      ifdParent;

    public IFD getRootIFD() { return this.tiffIFD; }

    public int createSimpleTiffFields(FileChannelDataInput rin, int offset) {
        rin.position(offset);
        try {
            int nbOfFields = rin.readShort();
            offset += AbstractTemplateTag.NB_OF_BYTES_FOR_A_SHORT;
            if (this.LOGGER.isDebugEnabled()) {
                this.LOGGER.info("Started create simple tiff fields {} - {} ", this.tiffIFD, nbOfFields);
            }
            for (int i = 0; i < nbOfFields; i++) {
                int currentOffset = offset;
                rin.position(offset);
                short tagValue = rin.readShort();
                Tag ftag = this.convertTagValueToTag(tagValue);
                offset += AbstractTemplateTag.NB_OF_BYTES_FOR_A_SHORT;
                rin.position(offset);
                short type = rin.readShort();
                offset += AbstractTemplateTag.NB_OF_BYTES_FOR_A_SHORT;
                rin.position(offset);
                int field_length = rin.readInt();
                offset += AbstractTemplateTag.NB_OF_BYTES_FOR_AN_INT;
                if (this.LOGGER.isDebugEnabled()) {
                    this.LOGGER.debug(
                        " Found tag {} at offset {} - field length {} - IFD {}  ",
                        ftag,
                        currentOffset,
                        field_length,
                        this.tiffIFD);
                }
                SimpleAbstractField<?> saf = SimpleFieldFactory.createField(type, field_length, offset);
                saf.updateData(rin);
                final TiffField<?> tiffField = saf.createTiffField(ftag, tagValue);
                this.tiffIFD.addField(tiffField);
                AbstractTemplateTag tagTemplate = TemplateTagFactory.create(ftag, this.tiffIFD, saf);
                tagTemplate.buildChildren(rin);
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

    protected abstract Tag convertTagValueToTag(short tagValue);

    protected void buildChildren(FileChannelDataInput rin) {

    }

    protected AbstractTemplateTag(
        Tag tag,
        IFD ifdParent
    ) {
        this.ifdParent = ifdParent;
        this.tag = tag;
        this.tiffIFD = new IFD(tag);
        if (ifdParent != null) {
            ifdParent.addChild(tag, this.getTiffIFD());
        }
    }

    protected AbstractTemplateTag(Tag tag) { this(tag,
        null); }

    public void buildChildrenIfNeeded() {

    }

    public Tag getTag() { return this.tag; }

    public IFD getTiffIFD() { return this.tiffIFD; }

    public IFD getIfdParent() { return this.ifdParent; }

}
