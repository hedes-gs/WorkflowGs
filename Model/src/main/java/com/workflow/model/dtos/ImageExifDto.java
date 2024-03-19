package com.workflow.model.dtos;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import jakarta.xml.bind.annotation.XmlType;

@XmlType
public class ImageExifDto implements Serializable {

    private static final long serialVersionUID = 1L;

    private ImageKeyDto       imageOwner;
    private List<ExifDTO>     exifs;

    private ImageExifDto(Builder builder) {
        this.imageOwner = builder.imageOwner;
        this.exifs = builder.exifs;
    }

    public ImageExifDto() {}

    public List<ExifDTO> getExifs() { return this.exifs; }

    public ImageKeyDto getImageOwner() { return this.imageOwner; }

    /**
     * Creates builder to build {@link ImageExifDto}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link ImageExifDto}.
     */
    public static final class Builder {
        private ImageKeyDto   imageOwner;
        private List<ExifDTO> exifs = Collections.emptyList();

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
         * Builder method for exifs parameter.
         *
         * @param exifs
         *            field to set
         * @return builder
         */
        public Builder withExifs(List<ExifDTO> exifs) {
            this.exifs = exifs;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public ImageExifDto build() { return new ImageExifDto(this); }
    }

}
