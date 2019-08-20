package com.gs.photos.workflow.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;
import com.gs.photos.workflow.metadata.fields.SimpleFieldFactory;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public abstract class AbstractTemplateTag {
	private Logger LOGGER = LogManager.getLogger(AbstractTemplateTag.class);

	private static final int NB_OF_BYTES_FOR_AN_INT = 4;
	private static final int NB_OF_BYTES_FOR_A_SHORT = 2;
	protected final Tag tag;
	protected final IFD tiffIFD;
	protected final IFD ifdParent;

	public List<IFD> getAllIfds() {
		List<IFD> allIfds = new ArrayList<>();
		explore(tiffIFD, allIfds);
		return allIfds;
	}

	private void explore(IFD tiffIFD2, List<IFD> allIfds) {
		allIfds.add(tiffIFD2);
		tiffIFD2.getAllChildren().forEach((ifd) -> explore(ifd, allIfds));
	}

	public int createSimpleTiffFields(FileChannelDataInput rin, int offset) {
		rin.position(offset);
		try {
			int nbOfFields = rin.readShort();
			offset += NB_OF_BYTES_FOR_A_SHORT;

			for (int i = 0; i < nbOfFields; i++) {
				int currentOffset = offset;
				rin.position(offset);
				short tagValue = rin.readShort();
				Tag ftag = convertTagValueToTag(tagValue);
				offset += NB_OF_BYTES_FOR_A_SHORT;
				rin.position(offset);
				short type = rin.readShort();
				offset += NB_OF_BYTES_FOR_A_SHORT;
				rin.position(offset);
				int field_length = rin.readInt();
				offset += NB_OF_BYTES_FOR_AN_INT;
				SimpleAbstractField<?> saf = SimpleFieldFactory.createField(type, field_length, offset);
				saf.updateData(rin);
				final TiffField<?> tiffField = saf.createTiffField(ftag, tagValue);
				tiffIFD.addField(tiffField);
				AbstractTemplateTag tagTemplate = TemplateTagFactory.create(ftag, this.tiffIFD, saf);
				tagTemplate.buildChildren(rin);
				offset = saf.getNextOffset();
			}
			if (tiffIFD.imageIsPresent()) {
				buildImage(rin);
			}
			rin.position(offset);
			int nextOffset = rin.readInt();
			return nextOffset;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private void buildImage(FileChannelDataInput rin) throws IOException {
		rin.position(tiffIFD.getJpegImagePosition());
		int endOffset = tiffIFD.getJpegImageLength() + rin.position();
		short imgHeight = 0;
		short imgWidth = 0;
		short SOIThumbnail = rin.readShortAsBigEndian();
		if (SOIThumbnail == (short) 0xffd8) {
			boolean finished = false;
			boolean found = false;
			while (!finished) {
				short marker = rin.readShortAsBigEndian();
				found = marker == (short) 0xffc0;
				finished = found || rin.position() >= endOffset;
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
			rin.position(tiffIFD.getJpegImagePosition());
		}
		rin.readFully(tiffIFD.getJpegImage());
	}

	protected abstract Tag convertTagValueToTag(short tagValue);

	protected void buildChildren(FileChannelDataInput rin) {

	}

	protected AbstractTemplateTag(Tag tag, IFD ifdParent) {
		this.ifdParent = ifdParent;
		this.tag = tag;
		this.tiffIFD = new IFD();
		if (ifdParent != null) {
			ifdParent.addChild(tag, getTiffIFD());
		}
	}

	protected AbstractTemplateTag(Tag tag) {
		this(tag, null);
	}

	public void buildChildrenIfNeeded() {

	}

	public Tag getTag() {
		return tag;
	}

	public IFD getTiffIFD() {
		return tiffIFD;
	}

	public IFD getIfdParent() {
		return ifdParent;
	}

}
