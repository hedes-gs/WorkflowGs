package com.workflow.model;

@HbaseTableName("image_project")
public class HbaseImageImportProject extends HbaseData {

    private static final long serialVersionUID = 1L;

    // Row key
    @Column(hbaseName = "import_date", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
    protected long            importDate;
    @Column(hbaseName = "import_id", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMPORT_ID)
    protected String          importId;

    // Data

    @Column(hbaseName = "image_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 100)
    protected String          imageName        = "";
    @Column(hbaseName = "thumb_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 101)
    protected String          thumbName        = "";
    @Column(hbaseName = "path", rowKeyNumber = 102, toByte = ToByteString.class, columnFamily = "img")
    protected String          path             = "";
    @Column(hbaseName = "width", rowKeyNumber = 103, toByte = ToByteLong.class, columnFamily = "sz")
    protected long            width;
    @Column(hbaseName = "height", rowKeyNumber = 104, toByte = ToByteLong.class, columnFamily = "sz")
    protected long            height;

    private HbaseImageImportProject(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.importDate = builder.importDate;
        this.importId = builder.importId;
        this.imageName = builder.imageName;
        this.thumbName = builder.thumbName;
        this.path = builder.path;
        this.width = builder.width;
        this.height = builder.height;
    }

    public HbaseImageImportProject() { super(null,
        System.currentTimeMillis()); }

    /**
     * Creates builder to build {@link HbaseImageImportProject}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link HbaseImageImportProject}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private long   importDate;
        private String importId;
        private String imageName;
        private String thumbName;
        private String path;
        private long   width;
        private long   height;

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
         * Builder method for importDate parameter.
         *
         * @param importDate
         *            field to set
         * @return builder
         */
        public Builder withImportDate(long importDate) {
            this.importDate = importDate;
            return this;
        }

        /**
         * Builder method for importId parameter.
         *
         * @param importId
         *            field to set
         * @return builder
         */
        public Builder withImportId(String importId) {
            this.importId = importId;
            return this;
        }

        /**
         * Builder method for imageName parameter.
         *
         * @param imageName
         *            field to set
         * @return builder
         */
        public Builder withImageName(String imageName) {
            this.imageName = imageName;
            return this;
        }

        /**
         * Builder method for thumbName parameter.
         *
         * @param thumbName
         *            field to set
         * @return builder
         */
        public Builder withThumbName(String thumbName) {
            this.thumbName = thumbName;
            return this;
        }

        /**
         * Builder method for path parameter.
         *
         * @param path
         *            field to set
         * @return builder
         */
        public Builder withPath(String path) {
            this.path = path;
            return this;
        }

        /**
         * Builder method for width parameter.
         *
         * @param width
         *            field to set
         * @return builder
         */
        public Builder withWidth(long width) {
            this.width = width;
            return this;
        }

        /**
         * Builder method for height parameter.
         *
         * @param height
         *            field to set
         * @return builder
         */
        public Builder withHeight(long height) {
            this.height = height;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public HbaseImageImportProject build() { return new HbaseImageImportProject(this); }
    }

}
