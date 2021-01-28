package com.gs.photos.workflow.extimginfo.metadata;

import java.util.Arrays;
import java.util.List;

import com.gs.photos.workflow.extimginfo.metadata.tiff.TiffField;

public class TiffFieldAndPath {
    protected TiffField<?> tiffField;
    protected short[]      path;
    protected int          tiffNumber;

    @Override
    public String toString() {

        return String.format(
            "TiffFieldAndPath [ TiffNumber: %4d, tiffField=%4s path: %4s  ",
            this.tiffNumber,
            this.tiffField,
            this.toString(this.path));
    }

    private String toString(short[] path2) {
        String[] pathsAsString = new String[path2.length];
        for (int k = 0; k < path2.length; k++) {
            pathsAsString[k] = "0x" + Integer.toHexString((path2[k]) & 0xffff);
        }
        return Arrays.deepToString(pathsAsString);
    }

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
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link TiffFieldAndPath}.
     */
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
