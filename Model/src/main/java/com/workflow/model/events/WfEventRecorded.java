package com.workflow.model.events;

public class WfEventRecorded extends WfEvent {

    protected long imageCreationDate;

    public static enum RecordedEventType {
        THUMB, EXIF, ARCHIVE
    }

    public WfEventRecorded() {}

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private RecordedEventType recordedEventType;

    private WfEventRecorded(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.imgId = builder.imgId;
        this.parentDataId = builder.parentDataId;
        this.step = builder.step;
        this.imageCreationDate = builder.imageCreationDate;
        this.recordedEventType = builder.recordedEventType;
    }

    public RecordedEventType getRecordedEventType() { return this.recordedEventType; }

    public long getImageCreationDate() { return this.imageCreationDate; }

    /**
     * Creates builder to build {@link WfEventRecorded}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link WfEventRecorded}.
     */
    public static final class Builder {
        private long              dataCreationDate;
        private String            dataId;
        private String            imgId;
        private String            parentDataId;
        private WfEventStep       step;
        private long              imageCreationDate;
        private RecordedEventType recordedEventType;

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
         * Builder method for imgId parameter.
         * 
         * @param imgId
         *            field to set
         * @return builder
         */
        public Builder withImgId(String imgId) {
            this.imgId = imgId;
            return this;
        }

        /**
         * Builder method for parentDataId parameter.
         * 
         * @param parentDataId
         *            field to set
         * @return builder
         */
        public Builder withParentDataId(String parentDataId) {
            this.parentDataId = parentDataId;
            return this;
        }

        /**
         * Builder method for step parameter.
         * 
         * @param step
         *            field to set
         * @return builder
         */
        public Builder withStep(WfEventStep step) {
            this.step = step;
            return this;
        }

        /**
         * Builder method for imageCreationDate parameter.
         * 
         * @param imageCreationDate
         *            field to set
         * @return builder
         */
        public Builder withImageCreationDate(long imageCreationDate) {
            this.imageCreationDate = imageCreationDate;
            return this;
        }

        /**
         * Builder method for recordedEventType parameter.
         * 
         * @param recordedEventType
         *            field to set
         * @return builder
         */
        public Builder withRecordedEventType(RecordedEventType recordedEventType) {
            this.recordedEventType = recordedEventType;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public WfEventRecorded build() { return new WfEventRecorded(this); }
    }

}
