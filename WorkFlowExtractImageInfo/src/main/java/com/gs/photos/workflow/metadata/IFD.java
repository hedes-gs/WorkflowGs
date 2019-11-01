package com.gs.photos.workflow.metadata;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.gs.photos.workflow.metadata.tiff.TiffField;
import com.gs.photos.workflow.metadata.tiff.TiffTag;

public class IFD {

	private Map<Tag, IFD>          children          = new HashMap<Tag, IFD>();
	private Map<Tag, TiffField<?>> tiffFields        = new HashMap<Tag, TiffField<?>>();
	private int                    jpegImageLength   = -1;
	private int                    jpegImagePosition = -1;
	private byte[]                 jpegImage;
	private Tag                    tag;
	private int                    endOffset;
	private int                    startOffset;

	public IFD() {

	}

	public IFD(
			Tag tag) {
		this.tag = tag;
	}

	public int getTotalNumberOfTiffFields() {
		int nbOfTiffFields = this.tiffFields.size();
		int nbOFChildren = this.children.entrySet().stream().mapToInt((e) -> e.getValue().getTotalNumberOfTiffFields())
				.sum();
		return nbOfTiffFields + nbOFChildren;
	}

	public void addChild(Tag tag, IFD child) {
		this.children.put(tag,
				child);
	}

	public void addField(TiffField<?> tiffField) {
		if (tiffField.getTag() == TiffTag.JPEG_INTERCHANGE_FORMAT) {
			this.jpegImagePosition = ((int[]) tiffField.getData())[0];
		}
		if (tiffField.getTag() == TiffTag.JPEG_INTERCHANGE_FORMAT_LENGTH) {
			this.jpegImageLength = ((int[]) tiffField.getData())[0];
		}
		if ((this.jpegImageLength != -1) && (this.jpegImagePosition != -1) && (this.jpegImage == null)) {
			this.jpegImage = new byte[this.jpegImageLength];
		}
		this.tiffFields.put(tiffField.getTag(),
				tiffField);
	}

	public void addFields(Collection<TiffField<?>> tiffFields) {
		for (TiffField<?> field : tiffFields) {
			this.addField(field);
		}
	}

	public IFD getChild(Tag tag) {
		return this.children.get(tag);
	}

	public Map<Tag, IFD> getChildren() {
		return Collections.unmodifiableMap(this.children);
	}

	public int getEndOffset() {
		return this.endOffset;
	}

	public TiffField<?> getField(Tag tag) {
		return this.tiffFields.get(tag.getValue());
	}

	public Collection<IFD> getAllChildren() {
		return Collections.unmodifiableCollection(this.children.values());
	}

	public boolean imageIsPresent() {
		return this.jpegImage != null;
	}

	public int getJpegImageLength() {
		return this.jpegImageLength;
	}

	public int getJpegImagePosition() {
		return this.jpegImagePosition;
	}

	public byte[] getJpegImage() {
		return this.jpegImage;
	}

	/**
	 * Return a String representation of the field
	 *
	 * @param tag
	 *            Tag for the field
	 * @return a String representation of the field
	 */
	public String getFieldAsString(Tag tag) {
		TiffField<?> field = this.tiffFields.get(tag);

		if (field != null) {
			return field.getDataAsString();
		}

		return "";
	}

	/** Get all the fields for this IFD from the internal map. */
	public Collection<TiffField<?>> getFields() {
		return Collections.unmodifiableCollection(this.tiffFields.values());
	}

	public int getSize() {
		return this.tiffFields.size();
	}

	public int getStartOffset() {
		return this.startOffset;
	}

	/** Remove all the entries from the IDF fields map */
	public void removeAllFields() {
		this.tiffFields.clear();
	}

	public IFD removeChild(Tag tag) {
		return this.children.remove(tag);
	}

	/** Remove a specific field associated with the given tag */
	public TiffField<?> removeField(Tag tag) {
		return this.tiffFields.remove(tag.getValue());
	}

	@Override
	public String toString() {
		return "IFD [tag=" + this.tag + "]";
	}

	public Tag getTag() {
		return this.tag;
	}

}