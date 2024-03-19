package com.workflow.model.dtos;

import java.io.Serializable;
import java.time.OffsetDateTime;

import jakarta.xml.bind.annotation.XmlType;

@XmlType
public class MinMaxDatesDto implements Serializable {

    private static final long serialVersionUID = 1L;
    protected int             countNumber;
    protected String          intervallType;
    protected OffsetDateTime  minDate;

    public String getIntervallType() { return this.intervallType; }

    public void setIntervallType(String intervallType) { this.intervallType = intervallType; }

    protected OffsetDateTime maxDate;

    private MinMaxDatesDto(Builder builder) {
        this.countNumber = builder.countNumber;
        this.intervallType = builder.intervallType;
        this.minDate = builder.minDate;
        this.maxDate = builder.maxDate;
    }

    public OffsetDateTime getMinDate() { return this.minDate; }

    public void setMinDate(OffsetDateTime minDate) { this.minDate = minDate; }

    public OffsetDateTime getMaxDate() { return this.maxDate; }

    public void setMaxDate(OffsetDateTime maxDate) { this.maxDate = maxDate; }

    public int getCountNumber() { return this.countNumber; }

    public void setCountNumber(int countNumber) { this.countNumber = countNumber; }

    public MinMaxDatesDto() {}

    public static long getSerialversionuid() { return MinMaxDatesDto.serialVersionUID; }

    /**
     * Creates builder to build {@link MinMaxDatesDto}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link MinMaxDatesDto}.
     */
    public static final class Builder {
        private int            countNumber;
        private String         intervallType;
        private OffsetDateTime minDate;
        private OffsetDateTime maxDate;

        private Builder() {}

        /**
         * Builder method for countNumber parameter.
         *
         * @param countNumber
         *            field to set
         * @return builder
         */
        public Builder withCountNumber(int countNumber) {
            this.countNumber = countNumber;
            return this;
        }

        /**
         * Builder method for intervallType parameter.
         *
         * @param intervallType
         *            field to set
         * @return builder
         */
        public Builder withIntervallType(String intervallType) {
            this.intervallType = intervallType;
            return this;
        }

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
