package com.workflow.model.dtos;

import jakarta.xml.bind.annotation.XmlType;

@XmlType
public class ExifDTO {

    protected ImageKeyDto imageOwner;
    protected String      displayableName;
    protected String      description;
    protected short       tagValue;
    protected short[]     path;
    protected String      displayableValue;

    private ExifDTO(Builder builder) {
        this.imageOwner = builder.imageOwner;
        this.displayableName = builder.displayableName;
        this.description = builder.description;
        this.tagValue = builder.tagValue;
        this.path = builder.path;
        this.displayableValue = builder.displayableValue;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + this.tagValue;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (obj == null) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        ExifDTO other = (ExifDTO) obj;
        if (this.tagValue != other.tagValue) { return false; }
        return true;
    }

    public String getDisplayableName() { return this.displayableName; }

    public String getDescription() { return this.description; }

    public short getTagValue() { return this.tagValue; }

    public String getDisplayableValue() { return this.displayableValue; }

    public ImageKeyDto getImageOwner() { return this.imageOwner; }

    public short[] getPath() { return this.path; }

    public ExifDTO() {}

    /**
     * Creates builder to build {@link ExifDTO}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ExifDTO}.
     */
    public static final class Builder {
        private ImageKeyDto imageOwner;
        private String      displayableName;
        private String      description;
        private short       tagValue;
        private short[]     path;
        private String      displayableValue;

        private Builder() {}

        /**
         * Builder method for imageOwner parameter.
         *
         * @param imageOwner
         *            field to set
         * @return builder
         */
        public Builder withImageOwner(ImageKeyDto imageOwner) {
            this.imageOwner = imageOwner;
            return this;
        }

        /**
         * Builder method for displayableName parameter.
         *
         * @param displayableName
         *            field to set
         * @return builder
         */
        public Builder withDisplayableName(String displayableName) {
            this.displayableName = displayableName;
            return this;
        }

        /**
         * Builder method for description parameter.
         *
         * @param description
         *            field to set
         * @return builder
         */
        public Builder withDescription(String description) {
            this.description = description;
            return this;
        }

        /**
         * Builder method for tagValue parameter.
         *
         * @param tagValue
         *            field to set
         * @return builder
         */
        public Builder withTagValue(short tagValue) {
            this.tagValue = tagValue;
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
         * Builder method for displayableValue parameter.
         *
         * @param displayableValue
         *            field to set
         * @return builder
         */
        public Builder withDisplayableValue(String displayableValue) {
            this.displayableValue = displayableValue;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public ExifDTO build() { return new ExifDTO(this); }
    }

}
