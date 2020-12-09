package com.workflow.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import javax.annotation.Generated;

public class CollectionOfHbaseData extends HbaseData implements Serializable {
    private static final long       serialVersionUID = 1L;
    protected Collection<HbaseData> dataCollection;

    @Generated("SparkTools")
    private CollectionOfHbaseData(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.dataCollection = builder.dataCollection;
    }

    public Collection<? extends HbaseData> getDataCollection() { return this.dataCollection; }

    public void setDataCollection(Collection<HbaseData> dataCollection) { this.dataCollection = dataCollection; }

    /**
     * Creates builder to build {@link CollectionOfHbaseData}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link CollectionOfHbaseData}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private long                  dataCreationDate;
        private String                dataId;
        private Collection<HbaseData> dataCollection = Collections.emptyList();

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
         * Builder method for dataCollection parameter.
         *
         * @param dataCollection
         *            field to set
         * @return builder
         */
        public Builder withDataCollection(Collection<HbaseData> dataCollection) {
            this.dataCollection = dataCollection;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public CollectionOfHbaseData build() { return new CollectionOfHbaseData(this); }
    }

}
