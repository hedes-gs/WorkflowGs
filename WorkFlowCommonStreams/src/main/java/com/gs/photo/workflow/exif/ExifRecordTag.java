package com.gs.photo.workflow.exif;

import javax.annotation.Generated;

public class ExifRecordTag implements Tag {

    protected FieldType fieldType;
    protected String    name;
    protected short     value;

    @Generated("SparkTools")
    private ExifRecordTag(Builder builder) {
        this.fieldType = builder.fieldType;
        this.name = builder.name;
        this.value = builder.value;
    }

    @Override
    public String toString() {
        return "ExifRecordTag [fieldType=" + this.fieldType + ", name=" + this.name + ", value="
            + Integer.toHexString(this.value) + "]";
    }

    @Override
    public FieldType getFieldType() { return this.fieldType; }

    public void setFieldType(FieldType fieldType) { this.fieldType = fieldType; }

    @Override
    public String getName() { return this.name; }

    public void setName(String name) { this.name = name; }

    @Override
    public short getValue() { return this.value; }

    public void setValue(short value) { this.value = value; }

    /**
     * Creates builder to build {@link ExifRecordTag}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ExifRecordTag}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private FieldType fieldType;
        private String    name;
        private short     value;

        private Builder() {}

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
         * Builder method for name parameter.
         *
         * @param name
         *            field to set
         * @return builder
         */
        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        /**
         * Builder method for value parameter.
         *
         * @param value
         *            field to set
         * @return builder
         */
        public Builder withValue(short value) {
            this.value = value;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public ExifRecordTag build() { return new ExifRecordTag(this); }
    }

}