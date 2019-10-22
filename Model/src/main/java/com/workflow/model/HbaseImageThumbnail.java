package com.workflow.model;

import java.util.Arrays;

import javax.annotation.Generated;

@HbaseTableName("image_thumbnail")
public class HbaseImageThumbnail extends HbaseData {

	private static final long serialVersionUID = 1L;

	// Row key
	@Column(hbaseName = "creation_date", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_CREATION_DATE)
	protected long            creationDate;
	@Column(hbaseName = "image_id", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
	protected String          imageId;
	@Column(hbaseName = "original", isPartOfRowkey = true, rowKeyNumber = 2, toByte = ToByteBoolean.class, fixedWidth = ModelConstants.FIXED_WIDTH_BOOLEAN)
	protected boolean         orignal          = false;

	// Data

	@Column(hbaseName = "image_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 100)
	protected String          imageName        = "";
	@Column(hbaseName = "thumb_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 101)
	protected String          thumbName        = "";
	@Column(hbaseName = "thumbnail", toByte = ToByteIdempotent.class, columnFamily = "thb", rowKeyNumber = 102)
	protected byte[]          thumbnail        = {};
	@Column(hbaseName = "path", rowKeyNumber = 103, toByte = ToByteString.class, columnFamily = "img")
	protected String          path             = "";
	@Column(hbaseName = "width", rowKeyNumber = 104, toByte = ToByteLong.class, columnFamily = "sz")
	protected long            width;
	@Column(hbaseName = "height", rowKeyNumber = 105, toByte = ToByteLong.class, columnFamily = "sz")
	protected long            height;

	@Generated("SparkTools")
	private HbaseImageThumbnail(
			Builder builder) {
		this.creationDate = builder.creationDate;
		this.imageId = builder.imageId;
		this.orignal = builder.orignal;
		this.imageName = builder.imageName;
		this.thumbName = builder.thumbName;
		this.thumbnail = builder.thumbnail;
		this.path = builder.path;
		this.width = builder.width;
		this.height = builder.height;
	}

	public String getImageId() {
		return this.imageId;
	}

	public void setImageId(String imageId) {
		this.imageId = imageId;
	}

	public long getCreationDate() {
		return this.creationDate;
	}

	public void setCreationDate(long creationDate) {
		this.creationDate = creationDate;
	}

	public byte[] getThumbnail() {
		return this.thumbnail;
	}

	public void setThumbnail(byte[] thumbnail) {
		this.thumbnail = thumbnail;
	}

	public boolean isOrignal() {
		return this.orignal;
	}

	public void setOrignal(boolean orignal) {
		this.orignal = orignal;
	}

	public String getPath() {
		return this.path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getImageName() {
		return this.imageName;
	}

	public void setImageName(String imageName) {
		this.imageName = imageName;
	}

	public String getThumbName() {
		return this.thumbName;
	}

	public void setThumbName(String thumbName) {
		this.thumbName = thumbName;
	}

	public long getWidth() {
		return this.width;
	}

	public void setWidth(long width) {
		this.width = width;
	}

	public long getHeight() {
		return this.height;
	}

	public void setHeight(long height) {
		this.height = height;
	}

	public HbaseImageThumbnail() {
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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + (int) (this.creationDate ^ (this.creationDate >>> 32));
		result = (prime * result) + (int) (this.height ^ (this.height >>> 32));
		result = (prime * result) + ((this.imageId == null) ? 0 : this.imageId.hashCode());
		result = (prime * result) + ((this.imageName == null) ? 0 : this.imageName.hashCode());
		result = (prime * result) + (this.orignal ? 1231 : 1237);
		result = (prime * result) + ((this.path == null) ? 0 : this.path.hashCode());
		result = (prime * result) + ((this.thumbName == null) ? 0 : this.thumbName.hashCode());
		result = (prime * result) + Arrays.hashCode(this.thumbnail);
		result = (prime * result) + (int) (this.width ^ (this.width >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (this.getClass() != obj.getClass()) {
			return false;
		}
		HbaseImageThumbnail other = (HbaseImageThumbnail) obj;
		if (this.creationDate != other.creationDate) {
			return false;
		}
		if (this.height != other.height) {
			return false;
		}
		if (this.imageId == null) {
			if (other.imageId != null) {
				return false;
			}
		} else if (!this.imageId.equals(other.imageId)) {
			return false;
		}
		if (this.imageName == null) {
			if (other.imageName != null) {
				return false;
			}
		} else if (!this.imageName.equals(other.imageName)) {
			return false;
		}
		if (this.orignal != other.orignal) {
			return false;
		}
		if (this.path == null) {
			if (other.path != null) {
				return false;
			}
		} else if (!this.path.equals(other.path)) {
			return false;
		}
		if (this.thumbName == null) {
			if (other.thumbName != null) {
				return false;
			}
		} else if (!this.thumbName.equals(other.thumbName)) {
			return false;
		}
		if (!Arrays.equals(this.thumbnail,
				other.thumbnail)) {
			return false;
		}
		if (this.width != other.width) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "HbaseImageThumbnail [creationDate=" + this.creationDate + ", imageId=" + this.imageId + ", orignal="
				+ this.orignal + ", imageName=" + this.imageName + ", thumbName=" + this.thumbName + ", thumbnail="
				+ Arrays.toString(this.thumbnail) + ", path=" + this.path + ", width=" + this.width + ", height="
				+ this.height + "]";
	}

	/**
	 * Creates builder to build {@link HbaseImageThumbnail}.
	 *
	 * @return created builder
	 */
	@Generated("SparkTools")
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder to build {@link HbaseImageThumbnail}.
	 */
	@Generated("SparkTools")
	public static final class Builder {
		private long    creationDate;
		private String  imageId;
		private boolean orignal;
		private String  imageName;
		private String  thumbName;
		private byte[]  thumbnail;
		private String  path;
		private long    width;
		private long    height;

		private Builder() {
		}

		/**
		 * Builder method for creationDate parameter.
		 *
		 * @param creationDate
		 *            field to set
		 * @return builder
		 */
		public Builder withCreationDate(long creationDate) {
			this.creationDate = creationDate;
			return this;
		}

		/**
		 * Builder method for imageId parameter.
		 *
		 * @param imageId
		 *            field to set
		 * @return builder
		 */
		public Builder withImageId(String imageId) {
			this.imageId = imageId;
			return this;
		}

		/**
		 * Builder method for orignal parameter.
		 *
		 * @param orignal
		 *            field to set
		 * @return builder
		 */
		public Builder withOrignal(boolean orignal) {
			this.orignal = orignal;
			return this;
		}

		/**
		 * Builder method for imageName parameter.
		 *
		 * @param imageName
		 *            field to set
		 * @return builder
		 */
		public Builder withImageName(String imageName) {
			this.imageName = imageName;
			return this;
		}

		/**
		 * Builder method for thumbName parameter.
		 *
		 * @param thumbName
		 *            field to set
		 * @return builder
		 */
		public Builder withThumbName(String thumbName) {
			this.thumbName = thumbName;
			return this;
		}

		/**
		 * Builder method for thumbnail parameter.
		 *
		 * @param thumbnail
		 *            field to set
		 * @return builder
		 */
		public Builder withThumbnail(byte[] thumbnail) {
			this.thumbnail = thumbnail;
			return this;
		}

		/**
		 * Builder method for path parameter.
		 *
		 * @param path
		 *            field to set
		 * @return builder
		 */
		public Builder withPath(String path) {
			this.path = path;
			return this;
		}

		/**
		 * Builder method for width parameter.
		 *
		 * @param width
		 *            field to set
		 * @return builder
		 */
		public Builder withWidth(long width) {
			this.width = width;
			return this;
		}

		/**
		 * Builder method for height parameter.
		 *
		 * @param height
		 *            field to set
		 * @return builder
		 */
		public Builder withHeight(long height) {
			this.height = height;
			return this;
		}

		/**
		 * Builder method of the builder.
		 *
		 * @return built class
		 */
		public HbaseImageThumbnail build() {
			return new HbaseImageThumbnail(this);
		}
	}

}
