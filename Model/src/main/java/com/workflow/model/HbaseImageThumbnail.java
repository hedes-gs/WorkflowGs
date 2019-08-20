package com.workflow.model;

import java.io.Serializable;

@HbaseTableName("image_thumbnail")
public class HbaseImageThumbnail extends HbaseData implements Serializable, Cloneable {

	private static final long serialVersionUID = 1L;

	// Row key
	@Column(hbaseName = "creation_date", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_CREATION_DATE)
	protected long creationDate;
	@Column(hbaseName = "image_id", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
	protected String imageId;
	@Column(hbaseName = "original", isPartOfRowkey = true, rowKeyNumber = 2, toByte = ToByteBoolean.class, columnFamily = "thumb_data", fixedWidth = ModelConstants.FIXED_WIDTH_BOOLEAN)
	protected boolean orignal = false;
	@Column(hbaseName = "path", isPartOfRowkey = true, rowKeyNumber = 3, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_PATH)
	protected String path = "";
	@Column(hbaseName = "width", isPartOfRowkey = true, rowKeyNumber = 4, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_CREATION_DATE)
	protected long width;
	@Column(hbaseName = "height", isPartOfRowkey = true, rowKeyNumber = 5, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_CREATION_DATE)
	protected long height;

	// Data

	@Column(hbaseName = "image_name", toByte = ToByteString.class, columnFamily = "image_data", rowKeyNumber = 100)
	protected String imageName = "";
	@Column(hbaseName = "thumb_name", toByte = ToByteString.class, columnFamily = "image_data", rowKeyNumber = 101)
	protected String thumbName = "";
	@Column(hbaseName = "thumbnail", toByte = ToByteIdempotent.class, columnFamily = "thumb_data", rowKeyNumber = 102)
	protected byte[] thumbnail = {};

	public String getImageId() {
		return imageId;
	}

	public void setImageId(String imageId) {
		this.imageId = imageId;
	}

	public long getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(long creationDate) {
		this.creationDate = creationDate;
	}

	public byte[] getThumbnail() {
		return thumbnail;
	}

	public void setThumbnail(byte[] thumbnail) {
		this.thumbnail = thumbnail;
	}

	public boolean isOrignal() {
		return orignal;
	}

	public void setOrignal(boolean orignal) {
		this.orignal = orignal;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getImageName() {
		return imageName;
	}

	public void setImageName(String imageName) {
		this.imageName = imageName;
	}

	public String getThumbName() {
		return thumbName;
	}

	public void setThumbName(String thumbName) {
		this.thumbName = thumbName;
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

	public HbaseImageThumbnail() {
	}

	public HbaseImageThumbnail(long creationDate, String imageId, String path, long width, long height,
			String imageName, String thumbName, byte[] thumbnail) {
		super();
		this.creationDate = creationDate;
		this.imageId = imageId;
		this.path = path;
		this.width = width;
		this.height = height;
		this.imageName = imageName;
		this.thumbName = thumbName;
		this.thumbnail = thumbnail;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			throw e;
		}
	}

	@Override
	public String toString() {
		return "HbaseImageThumbnail [creationDate=" + creationDate + ", imageId=" + imageId + ", path=" + path
				+ ", Original=" + orignal + ", width=" + width + ", height=" + height + ", imageName=" + imageName
				+ ", thumbName=" + thumbName + ", thumbnail length =" + thumbnail.length + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (creationDate ^ (creationDate >>> 32));
		result = prime * result + (int) (height ^ (height >>> 32));
		result = prime * result + ((imageId == null) ? 0 : imageId.hashCode());
		result = prime * result + ((imageName == null) ? 0 : imageName.hashCode());
		result = prime * result + (orignal ? 1231 : 1237);
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
		HbaseImageThumbnail other = (HbaseImageThumbnail) obj;
		if (creationDate != other.creationDate)
			return false;
		if (height != other.height)
			return false;
		if (imageId == null) {
			if (other.imageId != null)
				return false;
		} else if (!imageId.equals(other.imageId))
			return false;
		if (imageName == null) {
			if (other.imageName != null)
				return false;
		} else if (!imageName.equals(other.imageName))
			return false;
		if (orignal != other.orignal)
			return false;
		if (width != other.width)
			return false;
		return true;
	}

}
