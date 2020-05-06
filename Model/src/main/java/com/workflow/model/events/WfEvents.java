package com.workflow.model.events;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;

import javax.annotation.Generated;
import javax.annotation.Nullable;

import com.workflow.model.HbaseData;

public class WfEvents extends HbaseData implements Serializable {

    private static final long     serialVersionUID = 1L;

    @Nullable
    protected String              producer;
    protected Collection<WfEvent> events;

    @Override
    public String toString() {
        return "WfEvents [producer=" + this.producer + ", dataId " + this.getDataId() + ", nb of events="
            + this.events.size() + "]";
    }

    public WfEvents() { super(null,
        0); }

    @Generated("SparkTools")
    private WfEvents(Builder builder) {
        super(builder.dataId,
            builder.dataCreationDate);
        this.producer = builder.producer;
        this.events = builder.events;
    }

    public Collection<WfEvent> getEvents() { return this.events; }

    public WfEvents addEvent(WfEvent wfe) {
        this.events.add(wfe);
        return this;
    }

    public WfEvents addEvents(Collection<WfEvent> wfe) {
        this.events.addAll(wfe);
        return this;
    }

    /**
     * Creates builder to build {@link WfEvents}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link WfEvents}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private String              dataId;
        private long                dataCreationDate;
        private String              producer;
        private Collection<WfEvent> events = Collections.emptyList();

        private Builder() {}

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
         * Builder method for producer parameter.
         *
         * @param producer
         *            field to set
         * @return builder
         */
        public Builder withProducer(String producer) {
            this.producer = producer;
            return this;
        }

        /**
         * Builder method for events parameter.
         *
         * @param events
         *            field to set
         * @return builder
         */
        public Builder withEvents(Collection<WfEvent> events) {
            this.events = events;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public WfEvents build() { return new WfEvents(this); }
    }

}
