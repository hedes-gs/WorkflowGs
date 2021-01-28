package com.workflow.model.events;

public class WfEventFinal extends WfEvent {

    protected int nbOFExpectedEvents;

    public WfEventFinal() {

    }

    public int getNbOFExpectedEvents() { return this.nbOFExpectedEvents; }

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private WfEventFinal(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.imgId = builder.imgId;
        this.parentDataId = builder.parentDataId;
        this.step = builder.step;
        this.nbOFExpectedEvents = builder.nbOFExpectedEvents;
    }

    /**
     * Creates builder to build {@link WfEventFinal}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link WfEventFinal}.
     */
    public static final class Builder {
        private long        dataCreationDate;
        private String      dataId;
        private String      imgId;
        private String      parentDataId;
        private WfEventStep step;
        private int         nbOFExpectedEvents;

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
         * Builder method for nbOFExpectedEvents parameter.
         * 
         * @param nbOFExpectedEvents
         *            field to set
         * @return builder
         */
        public Builder withNbOFExpectedEvents(int nbOFExpectedEvents) {
            this.nbOFExpectedEvents = nbOFExpectedEvents;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public WfEventFinal build() { return new WfEventFinal(this); }
    }

}
