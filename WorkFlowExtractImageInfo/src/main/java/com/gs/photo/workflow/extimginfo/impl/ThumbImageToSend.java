package com.gs.photo.workflow.extimginfo.impl;

import com.workflow.model.HbaseData;
import com.workflow.model.HbaseImageAsByteArray;

public class ThumbImageToSend extends HbaseData {
    /**
     *
     */
    private static final long  serialVersionUID = 1L;
    protected short            tag              = (short) 0x9003;
    protected String           imageKey;
    protected HbaseImageAsByteArray jpegImage;
    protected short[]          path;
    protected int              currentNb;

    private ThumbImageToSend(Builder builder) {
        this.tag = builder.tag;
        this.imageKey = builder.imageKey;
        this.jpegImage = builder.jpegImage;
        this.path = builder.path;
        this.currentNb = builder.currentNb;
    }

    public ThumbImageToSend() {}

    public String getImageKey() { return this.imageKey; }

    public HbaseImageAsByteArray getJpegImage() { return this.jpegImage; }

    public short[] getPath() { return this.path; }

    public short getTag() { return this.tag; }

    public int getCurrentNb() { return this.currentNb; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private short            tag = (short) 0x9003;
        private String           imageKey;
        private HbaseImageAsByteArray jpegImage;
        private short[]          path;
        private int              currentNb;

        private Builder() {}

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
        public Builder withJpegImage(HbaseImageAsByteArray jpegImage) {
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
