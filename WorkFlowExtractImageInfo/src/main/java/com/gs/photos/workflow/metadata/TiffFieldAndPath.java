package com.gs.photos.workflow.metadata;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Generated;

import com.gs.photos.workflow.metadata.tiff.TiffField;

public class TiffFieldAndPath {
    protected TiffField<?> tiffField;
    protected short[]      path;
    protected int          tiffNumber;

    @Override
    public String toString() {
        return "TiffFieldAndPath [tiffField=" + this.tiffField + ", path=" + Arrays.toString(this.path)
            + ", tiffNumber=" + this.tiffNumber + "]";
    }

    @Generated("SparkTools")
    private TiffFieldAndPath(Builder builder) {
        this.tiffField = builder.tiffField;
        this.path = builder.path;
        this.tiffNumber = builder.tiffNumber;
    }

    public TiffField<?> getTiffField() { return this.tiffField; }

    public short[] getPath() { return this.path; }

    public int getTiffNumber() { return this.tiffNumber; }

    /**
     * Creates builder to build {@link TiffFieldAndPath}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link TiffFieldAndPath}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private TiffField<?> tiffField;
        private short[]      path;
        private int          tiffNumber;

        private Builder() {}

        /**
         * Builder method for tiffField parameter.
         *
         * @param tiffField
         *            field to set
         * @return builder
         */
        public Builder withTiffField(TiffField<?> tiffField) {
            this.tiffField = tiffField;
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

        public Builder withPath(List<Short> path) {
            this.path = new short[path.size()];
            for (int k = 0; k < path.size(); k++) {
                this.path[k] = path.get(k);
            }
            return this;
        }

        /**
         * Builder method for tiffNumber parameter.
         *
         * @param tiffNumber
         *            field to set
         * @return builder
         */
        public Builder withTiffNumber(int tiffNumber) {
            this.tiffNumber = tiffNumber;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public TiffFieldAndPath build() { return new TiffFieldAndPath(this); }
    }

}
