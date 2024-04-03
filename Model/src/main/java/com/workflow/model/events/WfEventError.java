package com.workflow.model.events;

public class WfEventError extends WfEvent {

    protected String error;

    public WfEventError() {

    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private WfEventError(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.imgId = builder.imgId;
        this.parentDataId = builder.parentDataId;
        this.step = builder.step;
        this.error = builder.error;
    }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private long        dataCreationDate;
        private String      dataId;
        private String      imgId;
        private String      parentDataId;
        private WfEventStep step;
        private String      error;

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
         * Builder method for error parameter.
         *
         * @param error
         *            field to set
         * @return builder
         */
        public Builder withError(String error) {
            this.error = error;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public WfEventError build() { return new WfEventError(this); }
    }

}
