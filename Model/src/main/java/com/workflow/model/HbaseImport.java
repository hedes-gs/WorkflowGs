package com.workflow.model;

import org.apache.avro.reflect.Nullable;

@HbaseTableName(value = "import")
public class HbaseImport extends HbaseData {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Column(hbaseName = "import_name", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMPORT_NAME)
    protected String          importName;

    @Column(hbaseName = "description", rowKeyNumber = 100, toByte = ToByteString.class, columnFamily = "meta")
    @Nullable
    protected String          description;

    @Column(hbaseName = "nbOfElements", rowKeyNumber = 101, toByte = ToByteLong.class, columnFamily = "infos")
    protected long            nbOfElements;

    private HbaseImport(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.importName = builder.importName;
        this.description = builder.description;
        this.nbOfElements = builder.nbOfElements;
    }

    public HbaseImport() { super(); }

    public HbaseImport(
        String dataId,
        long dataCreationDate
    ) { super(dataId,
        dataCreationDate); }

    public String getimportName() { return this.importName; }

    public void setimportName(String importName) { this.importName = importName; }

    public String getDescription() { return this.description; }

    public void setDescription(String description) { this.description = description; }

    public long getNbOfElements() { return this.nbOfElements; }

    public void setNbOfElements(long nbOfElements) { this.nbOfElements = nbOfElements; }

    /**
     * Creates builder to build {@link HbaseImport}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseImport}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private String importName;
        private String description;
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
         * Builder method for importName parameter.
         *
         * @param importName
         *            field to set
         * @return builder
         */
        public Builder withImportName(String importName) {
            this.importName = importName;
            return this;
        }

        /**
         * Builder method for description parameter.
         *
         * @param description
         *            field to set
         * @return builder
         */
        public Builder withDescription(String description) {
            this.description = description;
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
        public HbaseImport build() { return new HbaseImport(this); }
    }

}
