package com.workflow.model;

import java.io.Serializable;
import java.util.Arrays;

import javax.annotation.Generated;

import org.apache.avro.reflect.Nullable;

public class ExchangedTiffData extends HbaseData implements Serializable {

	private static final long serialVersionUID = 1L;
	protected String          key;
	protected String          imageId;
	protected short           tag;
	protected FieldType       fieldType;
	protected int             length;
	protected short[]         path;
	@Nullable
	protected int[]           dataAsInt        = {};
	@Nullable
	protected short[]         dataAsShort      = {};
	@Nullable
	protected byte[]          dataAsByte       = {};
	@Nullable
	protected String          id;
	private int               intId;
	private int               total;

	@Generated("SparkTools")
	private ExchangedTiffData(
			Builder builder) {
		this.key = builder.key;
		this.imageId = builder.imageId;
		this.tag = builder.tag;
		this.fieldType = builder.fieldType;
		this.length = builder.length;
		this.path = builder.path;
		this.dataAsInt = builder.dataAsInt;
		this.dataAsShort = builder.dataAsShort;
		this.dataAsByte = builder.dataAsByte;
		this.id = builder.id;
		this.intId = builder.intId;
		this.total = builder.total;
	}

	public int[] getDataAsInt() {
		return this.dataAsInt;
	}

	public void setDataAsInt(int[] dataAsInt) {
		this.dataAsInt = dataAsInt;
	}

	public short[] getDataAsShort() {
		return this.dataAsShort;
	}

	public void setDataAsShort(short[] dataAsShort) {
		this.dataAsShort = dataAsShort;
	}

	public byte[] getDataAsByte() {
		return this.dataAsByte;
	}

	public void setDataAsByte(byte[] dataAsByte) {
		this.dataAsByte = dataAsByte;
	}

	public short getTag() {
		return this.tag;
	}

	public FieldType getFieldType() {
		return this.fieldType;
	}

	public int getLength() {
		return this.length;
	}

	public String getId() {
		return this.id;
	}

	public String getKey() {
		return this.key;
	}

	public int getIntId() {
		return this.intId;
	}

	public int getTotal() {
		return this.total;
	}

	public String getImageId() {
		return this.imageId;
	}

	public short[] getPath() {
		return this.path;
	}

	public ExchangedTiffData(
			String imageId,
			short tag,
			int length,
			short fieldType,
			Object data,
			String id,
			String key,
			int intId,
			int total) {
		super();
		this.imageId = imageId;
		this.tag = tag;
		this.fieldType = FieldType.fromShort(fieldType);
		this.length = length;
		Object internalData = data;
		this.key = key;
		this.intId = intId;
		this.total = total;
		if (internalData instanceof int[]) {
			this.dataAsInt = (int[]) internalData;
		} else if (internalData instanceof short[]) {
			this.dataAsShort = (short[]) internalData;
		} else if (internalData instanceof byte[]) {
			this.dataAsByte = (byte[]) internalData;
		} else if (internalData instanceof String) {
			this.dataAsByte = ((String) internalData).getBytes();
		} else {
			throw new IllegalArgumentException();
		}
		this.id = id;
	}

	public ExchangedTiffData() {
		super();
		this.key = null;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(this.dataAsByte);
		result = (prime * result) + Arrays.hashCode(this.dataAsInt);
		result = (prime * result) + Arrays.hashCode(this.dataAsShort);
		result = (prime * result) + ((this.fieldType == null) ? 0 : this.fieldType.hashCode());
		result = (prime * result) + ((this.id == null) ? 0 : this.id.hashCode());
		result = (prime * result) + ((this.imageId == null) ? 0 : this.imageId.hashCode());
		result = (prime * result) + this.intId;
		result = (prime * result) + ((this.key == null) ? 0 : this.key.hashCode());
		result = (prime * result) + this.length;
		result = (prime * result) + this.tag;
		result = (prime * result) + this.total;
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
		ExchangedTiffData other = (ExchangedTiffData) obj;
		if (!Arrays.equals(this.dataAsByte,
				other.dataAsByte)) {
			return false;
		}
		if (!Arrays.equals(this.dataAsInt,
				other.dataAsInt)) {
			return false;
		}
		if (!Arrays.equals(this.dataAsShort,
				other.dataAsShort)) {
			return false;
		}
		if (this.fieldType != other.fieldType) {
			return false;
		}
		if (this.id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!this.id.equals(other.id)) {
			return false;
		}
		if (this.imageId == null) {
			if (other.imageId != null) {
				return false;
			}
		} else if (!this.imageId.equals(other.imageId)) {
			return false;
		}
		if (this.intId != other.intId) {
			return false;
		}
		if (this.key == null) {
			if (other.key != null) {
				return false;
			}
		} else if (!this.key.equals(other.key)) {
			return false;
		}
		if (this.length != other.length) {
			return false;
		}
		if (this.tag != other.tag) {
			return false;
		}
		if (this.total != other.total) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "ExchangedTiffData [key=" + this.key + ", imageId=" + this.imageId + ", tag=" + this.tag + ", fieldType="
				+ this.fieldType + ", length=" + this.length + ", dataAsInt=" + Arrays.toString(this.dataAsInt)
				+ ", dataAsShort=" + Arrays.toString(this.dataAsShort) + ", dataAsByte="
				+ Arrays.toString(this.dataAsByte) + ", id=" + this.id + ", intId=" + this.intId + ", total="
				+ this.total + "]";
	}

	/**
	 * Creates builder to build {@link ExchangedTiffData}.
	 *
	 * @return created builder
	 */
	@Generated("SparkTools")
	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder to build {@link ExchangedTiffData}.
	 */
	@Generated("SparkTools")
	public static final class Builder {
		private String    key;
		private String    imageId;
		private short     tag;
		private FieldType fieldType;
		private int       length;
		private short[]   path;
		private int[]     dataAsInt;
		private short[]   dataAsShort;
		private byte[]    dataAsByte;
		private String    id;
		private int       intId;
		private int       total;

		private Builder() {
		}

		/**
		 * Builder method for key parameter.
		 *
		 * @param key
		 *            field to set
		 * @return builder
		 */
		public Builder withKey(String key) {
			this.key = key;
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
		 * Builder method for tag parameter.
		 *
		 * @param tag
		 *            field to set
		 * @return builder
		 */
		public Builder withTag(short tag) {
			this.tag = tag;
			return this;
		}

		/**
		 * Builder method for fieldType parameter.
		 *
		 * @param fieldType
		 *            field to set
		 * @return builder
		 */
		public Builder withFieldType(FieldType fieldType) {
			this.fieldType = fieldType;
			return this;
		}

		/**
		 * Builder method for length parameter.
		 *
		 * @param length
		 *            field to set
		 * @return builder
		 */
		public Builder withLength(int length) {
			this.length = length;
			return this;
		}

		/**
		 * Builder method for path parameter.
		 *
		 * @param path
		 *            field to set
		 * @return builder
		 */
		public Builder withPath(short[] path) {
			this.path = path;
			return this;
		}

		/**
		 * Builder method for dataAsInt parameter.
		 *
		 * @param dataAsInt
		 *            field to set
		 * @return builder
		 */
		public Builder withDataAsInt(int[] dataAsInt) {
			this.dataAsInt = dataAsInt;
			return this;
		}

		/**
		 * Builder method for dataAsShort parameter.
		 *
		 * @param dataAsShort
		 *            field to set
		 * @return builder
		 */
		public Builder withDataAsShort(short[] dataAsShort) {
			this.dataAsShort = dataAsShort;
			return this;
		}

		/**
		 * Builder method for dataAsByte parameter.
		 *
		 * @param dataAsByte
		 *            field to set
		 * @return builder
		 */
		public Builder withDataAsByte(byte[] dataAsByte) {
			this.dataAsByte = dataAsByte;
			return this;
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
		 * Builder method for intId parameter.
		 *
		 * @param intId
		 *            field to set
		 * @return builder
		 */
		public Builder withIntId(int intId) {
			this.intId = intId;
			return this;
		}

		/**
		 * Builder method for total parameter.
		 *
		 * @param total
		 *            field to set
		 * @return builder
		 */
		public Builder withTotal(int total) {
			this.total = total;
			return this;
		}

		/**
		 * Builder method of the builder.
		 *
		 * @return built class
		 */
		public ExchangedTiffData build() {
			return new ExchangedTiffData(this);
		}
	}

}
