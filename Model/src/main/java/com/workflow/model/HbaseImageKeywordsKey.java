package com.workflow.model;

@HbaseTableName("image_keyword_key")
public class HbaseImageKeywordsKey extends HbaseData {

    private static final long serialVersionUID = 1L;

    // Row key
    @Column(hbaseName = "keyWord", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_KEYWORD)
    protected String          keyWord;

    private HbaseImageKeywordsKey(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.keyWord = builder.keyWord;
    }

    public String getKeyWord() { return this.keyWord; }

    public void setKeyWord(String keyWord) { this.keyWord = keyWord; }

    public HbaseImageKeywordsKey() { super(null,
        0); }

    @Override
    public Object clone() throws CloneNotSupportedException {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw e;
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + ((this.keyWord == null) ? 0 : this.keyWord.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        HbaseImageKeywordsKey other = (HbaseImageKeywordsKey) obj;
        if (this.keyWord == null) {
            if (other.keyWord != null) { return false; }
        } else if (!this.keyWord.equals(other.keyWord)) { return false; }
        return true;
    }

    @Override
    public String toString() { return "HbaseImageKeywordsKey [keyWord=" + this.keyWord + "]"; }

    /**
     * Creates builder to build {@link HbaseImageKeywordsKey}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseImageKeywordsKey}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private String keyWord;

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
         * Builder method for keyWord parameter.
         * 
         * @param keyWord
         *            field to set
         * @return builder
         */
        public Builder withKeyWord(String keyWord) {
            this.keyWord = keyWord;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public HbaseImageKeywordsKey build() { return new HbaseImageKeywordsKey(this); }
    }

}
