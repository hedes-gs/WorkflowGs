package com.workflow.model;

import java.util.Objects;

@HbaseTableName(value = "ratings")
public class HbaseRatings extends HbaseData {
    private static final long serialVersionUID = 1L;

    @Column(hbaseName = "ratings", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_RATINGS)
    protected long            ratings;

    @Column(hbaseName = "nbOfElements", rowKeyNumber = 101, toByte = ToByteLong.class, columnFamily = "infos")
    protected long            nbOfElements;

    private HbaseRatings(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.ratings = builder.ratings;
        this.nbOfElements = builder.nbOfElements;
    }

    public HbaseRatings() { super(); }

    public HbaseRatings(
        String dataId,
        long dataCreationDate
    ) { super(dataId,
        dataCreationDate); }

    public long getRatings() { return this.ratings; }

    public void setRatings(long ratings) { this.ratings = ratings; }

    public long getNbOfElements() { return this.nbOfElements; }

    public void setNbOfElements(long nbOfElements) { this.nbOfElements = nbOfElements; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Objects.hash(this.ratings);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseRatings other = (HbaseRatings) obj;
        return this.ratings == other.ratings;
    }

    @Override
    public String toString() { return "HbaseRatings [ratings=" + this.ratings + "]"; }

    /**
     * Creates builder to build {@link HbaseRatings}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseRatings}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private long   ratings;
        private long   nbOfElements;

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
         * Builder method for ratings parameter.
         * 
         * @param ratings
         *            field to set
         * @return builder
         */
        public Builder withRatings(long ratings) {
            this.ratings = ratings;
            return this;
        }

        /**
         * Builder method for nbOfElements parameter.
         * 
         * @param nbOfElements
         *            field to set
         * @return builder
         */
        public Builder withNbOfElements(long nbOfElements) {
            this.nbOfElements = nbOfElements;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public HbaseRatings build() { return new HbaseRatings(this); }
    }

}
