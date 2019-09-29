package com.workflow.model;

import java.io.Serializable;
import java.util.Arrays;

@HbaseTableName("image_exif")
public class HbaseExifData extends HbaseData implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	// Row key
	@Column(hbaseName = "exif_tag", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_EXIF_TAG)
	protected long exifTag;
	@Column(hbaseName = "image_id", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
	protected String imageId;

	// Data

	@Column(hbaseName = "exif_value", toByte = ToByteIdempotent.class, columnFamily = "exv", rowKeyNumber = 100)
	protected byte[] exifValue;
	@Column(hbaseName = "thumb_name", toByte = ToByteString.class, columnFamily = "imd", rowKeyNumber = 101)
	protected String thumbName = "";
	@Column(hbaseName = "creation_date", toByte = ToByteString.class, columnFamily = "imd", rowKeyNumber = 102)
	protected String creationDate = "";
	@Column(hbaseName = "width", toByte = ToByteLong.class, columnFamily = "sz", rowKeyNumber = 103)
	protected long width;
	@Column(hbaseName = "height", toByte = ToByteLong.class, columnFamily = "sz", rowKeyNumber = 104)
	protected long height;

	public long getExifTag() {
		return exifTag;
	}

	public void setExifTag(long exifTag) {
		this.exifTag = exifTag;
	}

	public String getImageId() {
		return imageId;
	}

	public void setImageId(String imageId) {
		this.imageId = imageId;
	}

	public long getWidth() {
		return width;
	}

	public void setWidth(long width) {
		this.width = width;
	}

	public long getHeight() {
		return height;
	}

	public void setHeight(long height) {
		this.height = height;
	}

	public String getThumbName() {
		return thumbName;
	}

	public void setThumbName(String thumbName) {
		this.thumbName = thumbName;
	}

	public byte[] getExifValue() {
		return exifValue;
	}

	public void setExifValue(byte[] exifValue) {
		this.exifValue = exifValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (exifTag ^ (exifTag >>> 32));
		result = prime * result + Arrays.hashCode(
			exifValue);
		result = prime * result + (int) (height ^ (height >>> 32));
		result = prime * result + ((imageId == null) ? 0 : imageId.hashCode());
		result = prime * result + ((thumbName == null) ? 0 : thumbName.hashCode());
		result = prime * result + (int) (width ^ (width >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HbaseExifData other = (HbaseExifData) obj;
		if (exifTag != other.exifTag)
			return false;
		if (!Arrays.equals(
			exifValue,
			other.exifValue))
			return false;
		if (height != other.height)
			return false;
		if (imageId == null) {
			if (other.imageId != null)
				return false;
		} else if (!imageId.equals(
			other.imageId))
			return false;
		if (thumbName == null) {
			if (other.thumbName != null)
				return false;
		} else if (!thumbName.equals(
			other.thumbName))
			return false;
		if (width != other.width)
			return false;
		return true;
	}

}
