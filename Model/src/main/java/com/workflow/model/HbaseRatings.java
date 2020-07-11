package com.workflow.model;

import javax.annotation.Generated;

@HbaseTableName("ratings")
public class HbaseRatings extends HbaseData {
    private static final long serialVersionUID = 1L;

    @Column(hbaseName = "ratings", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteInt.class, fixedWidth = ModelConstants.FIXED_WIDTH_RATINGS)
    protected int             ratings;

    @Generated("SparkTools")
    private HbaseRatings(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.ratings = builder.ratings;
    }

    public HbaseRatings() { super(); }

    public HbaseRatings(
        String dataId,
        long dataCreationDate
    ) { super(dataId,
        dataCreationDate); }

    public int getRatings() { return this.ratings; }

    public void setRatings(int ratings) { this.ratings = ratings; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + this.ratings;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseRatings other = (HbaseRatings) obj;
        if (this.ratings != other.ratings) { return false; }
        return true;
    }

    @Override
    public String toString() { return "HbaseRatings [ratings=" + this.ratings + "]"; }

    /**
     * Creates builder to build {@link HbaseRatings}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseRatings}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private int    ratings;

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
        public Builder withRatings(int ratings) {
            this.ratings = ratings;
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
