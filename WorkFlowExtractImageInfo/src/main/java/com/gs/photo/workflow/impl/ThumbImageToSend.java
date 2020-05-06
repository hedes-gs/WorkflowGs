package com.gs.photo.workflow.impl;

import javax.annotation.Generated;

public class ThumbImageToSend {
    protected short   tag = (short) 0x9003;
    protected String  imageKey;
    protected byte[]  jpegImage;
    protected short[] path;
    protected int     currentNb;

    @Generated("SparkTools")
    private ThumbImageToSend(Builder builder) {
        this.imageKey = builder.imageKey;
        this.jpegImage = builder.jpegImage;
        this.path = builder.path;
        this.currentNb = builder.currentNb;
    }

    public String getImageKey() { return this.imageKey; }

    public byte[] getJpegImage() { return this.jpegImage; }

    public short[] getPath() { return this.path; }

    public short getTag() { return this.tag; }

    public int getCurrentNb() { return this.currentNb; }

    /**
     * Creates builder to build {@link ThumbImageToSend}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ThumbImageToSend}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private String  imageKey;
        private byte[]  jpegImage;
        private short[] path;
        private int     currentNb;

        private Builder() {}

        /**
         * Builder method for imageKey parameter.
         *
         * @param imageKey
         *            field to set
         * @return builder
         */
        public Builder withImageKey(String imageKey) {
            this.imageKey = imageKey;
            return this;
        }

        /**
         * Builder method for jpegImage parameter.
         *
         * @param jpegImage
         *            field to set
         * @return builder
         */
        public Builder withJpegImage(byte[] jpegImage) {
            this.jpegImage = jpegImage;
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
         * Builder method for currentNb parameter.
         *
         * @param currentNb
         *            field to set
         * @return builder
         */
        public Builder withCurrentNb(int currentNb) {
            this.currentNb = currentNb;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public ThumbImageToSend build() { return new ThumbImageToSend(this); }
    }

}
