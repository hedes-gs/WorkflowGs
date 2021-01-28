package com.workflow.model;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public class SizeAndJpegContent implements Serializable {

    public SizeAndJpegContent() {}

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    protected int             height;
    protected int             width;
    protected byte[]          jpegContent;

    private SizeAndJpegContent(Builder builder) {
        this.height = builder.height;
        this.width = builder.width;
        this.jpegContent = builder.jpegContent;
    }

    public int getHeight() { return this.height; }

    public void setHeight(int height) { this.height = height; }

    public int getWidth() { return this.width; }

    public void setWidth(int width) { this.width = width; }

    public byte[] getJpegContent() { return this.jpegContent; }

    public void setJpegContent(byte[] jpegContent) { this.jpegContent = jpegContent; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + Arrays.hashCode(this.jpegContent);
        result = (prime * result) + Objects.hash(this.height, this.width);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        SizeAndJpegContent other = (SizeAndJpegContent) obj;
        return (this.height == other.height) && (this.width == other.width)
            && Arrays.equals(this.jpegContent, other.jpegContent);
    }

    @Override
    public String toString() {
        final int maxLen = 10;
        StringBuilder builder2 = new StringBuilder();
        builder2.append("SizeAndJpegContent [height=");
        builder2.append(this.height);
        builder2.append(", width=");
        builder2.append(this.width);
        builder2.append(", jpegContent=");
        builder2.append(
            this.jpegContent != null
                ? Arrays.toString(Arrays.copyOf(this.jpegContent, Math.min(this.jpegContent.length, maxLen)))
                : null);
        builder2.append("]");
        return builder2.toString();
    }

    /**
     * Creates builder to build {@link SizeAndJpegContent}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link SizeAndJpegContent}.
     */
    public static final class Builder {
        private int    height;
        private int    width;
        private byte[] jpegContent;

        private Builder() {}

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
         * Builder method for jpegContent parameter.
         * 
         * @param jpegContent
         *            field to set
         * @return builder
         */
        public Builder withJpegContent(byte[] jpegContent) {
            this.jpegContent = jpegContent;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public SizeAndJpegContent build() { return new SizeAndJpegContent(this); }
    }

}
