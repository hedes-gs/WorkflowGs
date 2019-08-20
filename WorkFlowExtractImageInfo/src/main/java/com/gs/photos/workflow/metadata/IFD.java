package com.gs.photos.workflow.metadata;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.gs.photos.workflow.metadata.tiff.TiffField;
import com.gs.photos.workflow.metadata.tiff.TiffTag;

public class IFD {

	private Map<Tag, IFD> children = new HashMap<Tag, IFD>();
	private Map<Tag, TiffField<?>> tiffFields = new HashMap<Tag, TiffField<?>>();
	private int jpegImageLength = -1;
	private int jpegImagePosition = -1;
	private byte[] jpegImage;

	private int endOffset;
	private int startOffset;

	public IFD() {
	}

	public int getTotalNumberOfTiffFields() {
		int nbOfTiffFields = tiffFields.size();
		int nbOFChildren = children.entrySet().stream().mapToInt(
			(e) -> e.getValue().getTotalNumberOfTiffFields()).sum();
		return nbOfTiffFields + nbOFChildren;
	}

	public void addChild(Tag tag, IFD child) {
		children.put(
			tag,
			child);
	}

	public void addField(TiffField<?> tiffField) {
		if (tiffField.getTag() == TiffTag.JPEG_INTERCHANGE_FORMAT) {
			jpegImagePosition = ((int[]) tiffField.getData())[0];
		}
		if (tiffField.getTag() == TiffTag.JPEG_INTERCHANGE_FORMAT_LENGTH) {
			jpegImageLength = ((int[]) tiffField.getData())[0];
		}
		if (jpegImageLength != -1 && jpegImagePosition != -1 && jpegImage == null) {
			System.out.println(
				"... Found image at " + jpegImagePosition);
			jpegImage = new byte[jpegImageLength];
		}
		tiffFields.put(
			tiffField.getTag(),
			tiffField);
	}

	public void addFields(Collection<TiffField<?>> tiffFields) {
		for (TiffField<?> field : tiffFields) {
			addField(
				field);
		}
	}

	public IFD getChild(Tag tag) {
		return children.get(
			tag);
	}

	public Map<Tag, IFD> getChildren() {
		return Collections.unmodifiableMap(
			children);
	}

	public int getEndOffset() {
		return endOffset;
	}

	public TiffField<?> getField(Tag tag) {
		return tiffFields.get(
			tag.getValue());
	}

	public Collection<IFD> getAllChildren() {
		return Collections.unmodifiableCollection(
			children.values());
	}

	public boolean imageIsPresent() {
		return jpegImage != null;
	}

	public int getJpegImageLength() {
		return jpegImageLength;
	}

	public int getJpegImagePosition() {
		return jpegImagePosition;
	}

	public byte[] getJpegImage() {
		return jpegImage;
	}

	/**
	 * Return a String representation of the field
	 *
	 * @param tag
	 *            Tag for the field
	 * @return a String representation of the field
	 */
	public String getFieldAsString(Tag tag) {
		TiffField<?> field = tiffFields.get(
			tag);

		if (field != null) {
			return field.getDataAsString();
		}

		return "";
	}

	/** Get all the fields for this IFD from the internal map. */
	public Collection<TiffField<?>> getFields() {
		return Collections.unmodifiableCollection(
			tiffFields.values());
	}

	public int getSize() {
		return tiffFields.size();
	}

	public int getStartOffset() {
		return startOffset;
	}

	/** Remove all the entries from the IDF fields map */
	public void removeAllFields() {
		tiffFields.clear();
	}

	public IFD removeChild(Tag tag) {
		return children.remove(
			tag);
	}

	/** Remove a specific field associated with the given tag */
	public TiffField<?> removeField(Tag tag) {
		return tiffFields.remove(
			tag.getValue());
	}

}