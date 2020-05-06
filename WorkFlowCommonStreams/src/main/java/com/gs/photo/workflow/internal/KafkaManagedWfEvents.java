package com.gs.photo.workflow.internal;

import java.util.Optional;

import javax.annotation.Generated;

import com.workflow.model.events.WfEvents;

public class KafkaManagedWfEvents extends GenericKafkaManagedObject<Optional<WfEvents>> {

    @Generated("SparkTools")
    private KafkaManagedWfEvents(Builder builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
        this.value = builder.wfEvents;
    }

    /**
     * Creates builder to build {@link KafkaManagedWfEvents}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link KafkaManagedWfEvents}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private int                partition;
        private long               kafkaOffset;
        private Optional<WfEvents> wfEvents = Optional.empty();

        private Builder() {}

        /**
         * Builder method for partition parameter.
         *
         * @param partition
         *            field to set
         * @return builder
         */
        public Builder withPartition(int partition) {
            this.partition = partition;
            return this;
        }

        /**
         * Builder method for kafkaOffset parameter.
         *
         * @param kafkaOffset
         *            field to set
         * @return builder
         */
        public Builder withKafkaOffset(long kafkaOffset) {
            this.kafkaOffset = kafkaOffset;
            return this;
        }

        /**
         * Builder method for wfEvents parameter.
         *
         * @param wfEvents
         *            field to set
         * @return builder
         */
        public Builder withWfEvents(Optional<WfEvents> wfEvents) {
            this.wfEvents = wfEvents;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public KafkaManagedWfEvents build() { return new KafkaManagedWfEvents(this); }
    }

}