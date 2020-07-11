package com.workflow.model.dtos;

import java.io.Serializable;
import java.time.OffsetDateTime;

import javax.annotation.Generated;
import javax.xml.bind.annotation.XmlType;

@XmlType
public class MinMaxDatesDto implements Serializable {

    private static final long serialVersionUID = 1L;
    protected OffsetDateTime  minDate;
    protected OffsetDateTime  maxDate;

    @Generated("SparkTools")
    private MinMaxDatesDto(Builder builder) {
        this.minDate = builder.minDate;
        this.maxDate = builder.maxDate;
    }

    public OffsetDateTime getMinDate() { return this.minDate; }

    public void setMinDate(OffsetDateTime minDate) { this.minDate = minDate; }

    public OffsetDateTime getMaxDate() { return this.maxDate; }

    public void setMaxDate(OffsetDateTime maxDate) { this.maxDate = maxDate; }

    public MinMaxDatesDto() {}

    public static long getSerialversionuid() { return MinMaxDatesDto.serialVersionUID; }

    /**
     * Creates builder to build {@link MinMaxDatesDto}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link MinMaxDatesDto}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private OffsetDateTime minDate;
        private OffsetDateTime maxDate;

        private Builder() {}

        /**
         * Builder method for minDate parameter.
         *
         * @param minDate
         *            field to set
         * @return builder
         */
        public Builder withMinDate(OffsetDateTime minDate) {
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
        public Builder withMaxDate(OffsetDateTime maxDate) {
            this.maxDate = maxDate;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public MinMaxDatesDto build() { return new MinMaxDatesDto(this); }
    }

}
