package com.workflow.model.dtos;

import javax.xml.bind.annotation.XmlType;

@XmlType
public class ImageMetaData {

    protected String id;
    protected String key;
    protected String value;

    private ImageMetaData(Builder builder) {
        this.id = builder.id;
        this.key = builder.key;
        this.value = builder.value;
    }

    public ImageMetaData(
        String id,
        String key,
        String value
    ) {
        super();
        this.id = id;
        this.key = key;
        this.value = value;
    }

    public ImageMetaData() {}

    /**
     * Creates builder to build {@link ImageMetaData}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ImageMetaData}.
     */
    public static final class Builder {
        private String id;
        private String key;
        private String value;

        private Builder() {}

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
         * Builder method for value parameter.
         *
         * @param value
         *            field to set
         * @return builder
         */
        public Builder withValue(String value) {
            this.value = value;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public ImageMetaData build() { return new ImageMetaData(this); }
    }

}
