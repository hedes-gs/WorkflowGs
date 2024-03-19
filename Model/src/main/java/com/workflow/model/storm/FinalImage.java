package com.workflow.model.storm;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

import com.workflow.model.HbaseData;

public class FinalImage extends HbaseData implements Serializable {

    private static final long serialVersionUID = 1L;
    protected short           version;
    protected int             width;
    protected int             height;
    protected byte[]          compressedData;

    private FinalImage(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.version = builder.version;
        this.width = builder.width;
        this.height = builder.height;
        this.compressedData = builder.compressedData;
    }

    public FinalImage() { super(null,
        0); }

    public short getVersion() { return this.version; }

    public byte[] getCompressedData() { return this.compressedData; }

    public int getWidth() { return this.width; }

    public void setWidth(int width) { this.width = width; }

    public int getHeight() { return this.height; }

    public void setHeight(int height) { this.height = height; }

    public byte[] getCompressedImage() { return this.compressedData; }

    public void setCompressedData(byte[] compressedData) { this.compressedData = compressedData; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Arrays.hashCode(this.compressedData);
        result = (prime * result) + Objects.hash(this.height, this.version, this.width);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (!(obj instanceof FinalImage)) { return false; }
        FinalImage other = (FinalImage) obj;
        return Arrays.equals(this.compressedData, other.compressedData) && (this.height == other.height)
            && (this.version == other.version) && (this.width == other.width);
    }

    @Override
    public String toString() {
        return "FinalImage [version=" + this.version + ", width=" + this.width + ", height=" + this.height
            + ", compressedData=" + Arrays.toString(this.compressedData) + "]";
    }

    /**
     * Creates builder to build {@link FinalImage}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link FinalImage}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private short  version;
        private int    width;
        private int    height;
        private byte[] compressedData;

        private Builder() {}

        /**
         * Builder method for dataCreationDate parameter.
         * 
         * @param dataCreationDate
         *            field to set
         * @return builder
         */
        public Builder withDataCreationDate(long dataCreationDate) {
            this.dataCreationDate = dataCreationDate;
            return this;
        }

        /**
         * Builder method for dataId parameter.
         * 
         * @param dataId
         *            field to set
         * @return builder
         */
        public Builder withDataId(String dataId) {
            this.dataId = dataId;
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
        public FinalImage build() { return new FinalImage(this); }
    }

}
