package com.workflow.model.storm;

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.Generated;

import com.workflow.model.HbaseData;

public class FinalImage extends HbaseData implements Serializable {

	private static final long serialVersionUID = 1L;
	protected String          id;
	protected short           version;
	protected int             width;
	protected int             height;
	protected byte[]          compressedData;

	@Generated("SparkTools")
	private FinalImage(
		Builder builder) {
		this.id = builder.id;
		this.version = builder.version;
		this.width = builder.width;
		this.height = builder.height;
		this.compressedData = builder.compressedData;
	}

	public short getVersion() {
		return this.version;
	}

	public byte[] getCompressedData() {
		return this.compressedData;
	}

	public int getWidth() {
		return this.width;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public int getHeight() {
		return this.height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	public byte[] getCompressedImage() {
		return this.compressedData;
	}

	public void setCompressedData(byte[] compressedData) {
		this.compressedData = compressedData;
	}

	public String getId() {
		return this.id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(this.compressedData);
		result = (prime * result) + this.height;
		result = (prime * result) + ((this.id == null) ? 0 : this.id.hashCode());
		result = (prime * result) + this.version;
		result = (prime * result) + this.width;
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
		FinalImage other = (FinalImage) obj;
		if (!Arrays.equals(this.compressedData,
			other.compressedData)) {
			return false;
		}
		if (this.height != other.height) {
			return false;
		}
		if (this.id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!this.id.equals(other.id)) {
			return false;
		}
		if (this.version != other.version) {
			return false;
		}
		if (this.width != other.width) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "FinalImage [id=" + this.id + ", version=" + this.version + ", width=" + this.width + ", height="
			+ this.height + ", compressedData=" + Arrays.toString(this.compressedData) + "]";
	}

	public FinalImage() {
	}

	/**
	 * Creates builder to build {@link FinalImage}.
	 *
	 * @return created builder
	 */
	@Generated("SparkTools")
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder to build {@link FinalImage}.
	 */
	@Generated("SparkTools")
	public static final class Builder {
		private String id;
		private short  version;
		private int    width;
		private int    height;
		private byte[] compressedData;

		private Builder() {
		}

		/**
		 * Builder method for id parameter.
		 *
		 * @param id
		 *            field to set
		 * @return builder
		 */
		public Builder withId(String id) {
			this.id = id;
			return this;
		}

		/**
		 * Builder method for version parameter.
		 *
		 * @param version
		 *            field to set
		 * @return builder
		 */
		public Builder withVersion(short version) {
			this.version = version;
			return this;
		}

		/**
		 * Builder method for width parameter.
		 *
		 * @param width
		 *            field to set
		 * @return builder
		 */
		public Builder withWidth(int width) {
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
		public Builder withHeight(int height) {
			this.height = height;
			return this;
		}

		/**
		 * Builder method for compressedData parameter.
		 *
		 * @param compressedData
		 *            field to set
		 * @return builder
		 */
		public Builder withCompressedData(byte[] compressedData) {
			this.compressedData = compressedData;
			return this;
		}

		/**
		 * Builder method of the builder.
		 *
		 * @return built class
		 */
		public FinalImage build() {
			return new FinalImage(this);
		}
	}

}
