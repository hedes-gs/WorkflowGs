package com.workflow.model;

import java.io.Serializable;

import org.apache.avro.reflect.Nullable;

public class HbaseImageAsByteArray extends HbaseData implements Serializable {

    private static final long serialVersionUID = 1L;
    @Nullable
    protected byte[]          dataAsByte       = {};

    private HbaseImageAsByteArray(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.dataAsByte = builder.dataAsByte;
    }

    public HbaseImageAsByteArray() {}

    public byte[] getDataAsByte() { return this.dataAsByte; }

    public void setDataAsByte(byte[] dataAsByte) { this.dataAsByte = dataAsByte; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private byte[] dataAsByte = {};

        private Builder() {}

        /**
         * Builder method for dataCreationDate parameter.
         *
         * @param dataCreationDate
         *            field to set
         * @return builder
         */
        public Builder withDataCreationDate(long dataCreationDate) {
            this.dataCreationDate = dataCreationDate;
            return this;
        }

        /**
         * Builder method for dataId parameter.
         *
         * @param dataId
         *            field to set
         * @return builder
         */
        public Builder withDataId(String dataId) {
            this.dataId = dataId;
            return this;
        }

        /**
         * Builder method for dataAsByte parameter.
         *
         * @param dataAsByte
         *            field to set
         * @return builder
         */
        public Builder withDataAsByte(byte[] dataAsByte) {
            this.dataAsByte = dataAsByte;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public HbaseImageAsByteArray build() { return new HbaseImageAsByteArray(this); }
    }

}
