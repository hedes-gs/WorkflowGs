package com.workflow.model;

import java.util.Objects;

import javax.annotation.Generated;

@HbaseTableName(value = "keywords")
public class HbaseKeywords extends HbaseData {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Column(hbaseName = "keyword", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_KEYWORD)
    protected String          keyword;

    @Column(hbaseName = "nbOfElements", rowKeyNumber = 101, toByte = ToByteLong.class, columnFamily = "infos")
    protected long            nbOfElements;

    @Generated("SparkTools")
    private HbaseKeywords(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.keyword = builder.keyword;
    }

    public HbaseKeywords() { super(); }

    public HbaseKeywords(
        String dataId,
        long dataCreationDate
    ) { super(dataId,
        dataCreationDate); }

    public String getKeyword() { return this.keyword; }

    public void setKeyword(String keyword) { this.keyword = keyword; }

    public long getNbOfElements() { return this.nbOfElements; }

    public void setNbOfElements(long nbOfElements) { this.nbOfElements = nbOfElements; }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Objects.hash(this.keyword);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseKeywords other = (HbaseKeywords) obj;
        return Objects.equals(this.keyword, other.keyword);
    }

    @Override
    public String toString() { return "HbaseKeywords [keyword=" + this.keyword + "]"; }

    /**
     * Creates builder to build {@link HbaseKeywords}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseKeywords}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private String keyword;

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
         * Builder method for keyword parameter.
         *
         * @param keyword
         *            field to set
         * @return builder
         */
        public Builder withKeyword(String keyword) {
            this.keyword = keyword;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public HbaseKeywords build() { return new HbaseKeywords(this); }
    }

}
