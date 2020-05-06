package com.workflow.model.events;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Generated;

import com.workflow.model.HbaseData;

public class WfEvent extends HbaseData implements Serializable, Comparable<WfEvent> {

    private static final long     serialVersionUID = 1L;
    protected String              imgId;
    protected String              parentDataId;
    protected WfEventStep         step;
    protected List<String>        parentPath;
    protected Collection<WfEvent> createdEvents;

    @Generated("SparkTools")
    private WfEvent(Builder builder) {
        super(builder.dataId,
            System.currentTimeMillis());
        this.imgId = builder.imgId;
        this.parentDataId = builder.parentDataId;
        this.step = builder.step;
        this.parentPath = builder.parentPath;
        this.createdEvents = builder.createdEvents;
    }

    public WfEvent() { super(null,
        0); }

    public String getParentDataId() { return this.parentDataId; }

    public WfEventStep getStep() { return this.step; }

    public String getImgId() { return this.imgId; }

    /**
     * Creates builder to build {@link WfEvent}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link WfEvent}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private String              imgId;
        private String              dataId;
        private String              parentDataId;
        private WfEventStep         step;
        private List<String>        parentPath    = Collections.emptyList();
        private Collection<WfEvent> createdEvents = Collections.emptyList();

        private Builder() {}

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
         * Builder method for parentPath parameter.
         *
         * @param parentPath
         *            field to set
         * @return builder
         */
        public Builder withParentPath(List<String> parentPath) {
            this.parentPath = parentPath;
            return this;
        }

        /**
         * Builder method for createdEvents parameter.
         *
         * @param createdEvents
         *            field to set
         * @return builder
         */
        public Builder withCreatedEvents(Collection<WfEvent> createdEvents) {
            this.createdEvents = createdEvents;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public WfEvent build() { return new WfEvent(this); }
    }

    @Override
    public int compareTo(WfEvent o) { return this.imgId.compareTo(o.imgId); }

}
