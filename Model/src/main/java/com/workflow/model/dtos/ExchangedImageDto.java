package com.workflow.model.dtos;

import java.io.Serializable;
import java.time.LocalDateTime;

import jakarta.xml.bind.annotation.XmlType;

@XmlType
public class ExchangedImageDto implements Serializable {

    private static final long serialVersionUID = 1L;
    protected ImageDto        image;
    protected int             currentPage;
    protected int             pageSize;
    protected int             totalNbOfElements;
    protected LocalDateTime   minDate;
    protected LocalDateTime   maxDate;

    private ExchangedImageDto(Builder builder) {
        this.image = builder.image;
        this.currentPage = builder.currentPage;
        this.pageSize = builder.pageSize;
        this.totalNbOfElements = builder.totalNbOfElements;
        this.minDate = builder.minDate;
        this.maxDate = builder.maxDate;
    }

    public ImageDto getImage() { return this.image; }

    public void setImage(ImageDto image) { this.image = image; }

    public int getCurrentPage() { return this.currentPage; }

    public void setCurrentPage(int currentPage) { this.currentPage = currentPage; }

    public int getPageSize() { return this.pageSize; }

    public void setPageSize(int pageSize) { this.pageSize = pageSize; }

    public LocalDateTime getMinDate() { return this.minDate; }

    public void setMinDate(LocalDateTime minDate) { this.minDate = minDate; }

    public LocalDateTime getMaxDate() { return this.maxDate; }

    public void setMaxDate(LocalDateTime maxDate) { this.maxDate = maxDate; }

    public int getTotalNbOfElements() { return this.totalNbOfElements; }

    public void setTotalNbOfElements(int totalNbOfElements) { this.totalNbOfElements = totalNbOfElements; }

    public ExchangedImageDto() {

    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private ImageDto      image;
        private int           currentPage;
        private int           pageSize;
        private int           totalNbOfElements;
        private LocalDateTime minDate;
        private LocalDateTime maxDate;

        private Builder() {}

        /**
         * Builder method for image parameter.
         *
         * @param image
         *            field to set
         * @return builder
         */
        public Builder withImage(ImageDto image) {
            this.image = image;
            return this;
        }

        /**
         * Builder method for currentPage parameter.
         *
         * @param currentPage
         *            field to set
         * @return builder
         */
        public Builder withCurrentPage(int currentPage) {
            this.currentPage = currentPage;
            return this;
        }

        /**
         * Builder method for pageSize parameter.
         *
         * @param pageSize
         *            field to set
         * @return builder
         */
        public Builder withPageSize(int pageSize) {
            this.pageSize = pageSize;
            return this;
        }

        /**
         * Builder method for totalNbOfElements parameter.
         *
         * @param totalNbOfElements
         *            field to set
         * @return builder
         */
        public Builder withTotalNbOfElements(int totalNbOfElements) {
            this.totalNbOfElements = totalNbOfElements;
            return this;
        }

        /**
         * Builder method for minDate parameter.
         *
         * @param minDate
         *            field to set
         * @return builder
         */
        public Builder withMinDate(LocalDateTime minDate) {
            this.minDate = minDate;
            return this;
        }

        /**
         * Builder method for maxDate parameter.
         *
         * @param maxDate
         *            field to set
         * @return builder
         */
        public Builder withMaxDate(LocalDateTime maxDate) {
            this.maxDate = maxDate;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public ExchangedImageDto build() { return new ExchangedImageDto(this); }
    }

}
