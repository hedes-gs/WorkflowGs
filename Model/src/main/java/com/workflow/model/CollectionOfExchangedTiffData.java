package com.workflow.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

public class CollectionOfExchangedTiffData extends HbaseData implements Serializable {
    private static final long               serialVersionUID = 1L;
    protected Collection<ExchangedTiffData> dataCollection;

    private CollectionOfExchangedTiffData(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.dataCollection = builder.dataCollection;
    }

    public CollectionOfExchangedTiffData() { this(null,
        0,
        Collections.EMPTY_LIST); }

    public CollectionOfExchangedTiffData(
        String dataId,
        long dataCreationDate,
        Collection<ExchangedTiffData> dataCollection
    ) {
        super(dataId,
            dataCreationDate);
        this.dataCollection = dataCollection;
    }

    public Collection<ExchangedTiffData> getDataCollection() { return this.dataCollection; }

    public void setDataCollection(Collection<ExchangedTiffData> dataCollection) {
        this.dataCollection = dataCollection;
    }

    /**
     * Creates builder to build {@link CollectionOfExchangedTiffData}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link CollectionOfExchangedTiffData}.
     */
    public static final class Builder {
        private long                          dataCreationDate;
        private String                        dataId;
        private Collection<ExchangedTiffData> dataCollection = Collections.emptyList();

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
        public Builder withDataCollection(Collection<ExchangedTiffData> dataCollection) {
            this.dataCollection = dataCollection;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public CollectionOfExchangedTiffData build() { return new CollectionOfExchangedTiffData(this); }
    }

}
