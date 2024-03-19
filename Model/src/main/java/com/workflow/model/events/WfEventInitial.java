package com.workflow.model.events;

public class WfEventInitial extends WfEvent {

    protected int             nbOfInitialEvents;

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private WfEventInitial(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.imgId = builder.imgId;
        this.parentDataId = builder.parentDataId;
        this.step = builder.step;
        this.nbOfInitialEvents = builder.nbOfInitialEvents;
    }

    public WfEventInitial() {}

    public int getNbOfInitialEvents() { return this.nbOfInitialEvents; }

    public void setNbOfInitialEvents(int nbOfInitialEvents) { this.nbOfInitialEvents = nbOfInitialEvents; }

    /**
     * Creates builder to build {@link WfEventInitial}.
     * 
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link WfEventInitial}.
     */
    public static final class Builder {
        private long        dataCreationDate;
        private String      dataId;
        private String      imgId;
        private String      parentDataId;
        private WfEventStep step;
        private int         nbOfInitialEvents;

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
         * Builder method for nbOfInitialEvents parameter.
         * 
         * @param nbOfInitialEvents
         *            field to set
         * @return builder
         */
        public Builder withNbOfInitialEvents(int nbOfInitialEvents) {
            this.nbOfInitialEvents = nbOfInitialEvents;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public WfEventInitial build() { return new WfEventInitial(this); }
    }

}
