package com.gs.photo.common.workflow.internal;

import java.util.Optional;

import javax.annotation.Generated;

import com.workflow.model.events.WfEvent;

public class KafkaManagedWfEvent<T extends WfEvent> extends GenericKafkaManagedObject<T> {

    @Generated("SparkTools")
    private KafkaManagedWfEvent(Builder<T> builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
        this.value = builder.value;
        this.topic = builder.topic;
        this.imageKey = builder.imageKey;
        this.objectKey = builder.objectKey;
    }

    /**
     * Creates builder to build {@link KafkaManagedWfEvent}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    static public <T extends WfEvent> Builder<T> builder() { return new Builder<>(); }

    /**
     * Builder to build {@link KafkaManagedWfEvent}.
     */
    @Generated("SparkTools")
    public static final class Builder<T extends WfEvent> {
        private int         partition;
        private long        kafkaOffset;
        private Optional<T> value;
        private String      topic;
        private String      imageKey;
        private String      objectKey;

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
         * Builder method for value parameter.
         *
         * @param value
         *            field to set
         * @return builder
         */
        public Builder withValue(T value) {
            this.value = Optional.ofNullable(value);
            return this;
        }

        /**
         * Builder method for topic parameter.
         *
         * @param topic
         *            field to set
         * @return builder
         */
        public Builder withTopic(String topic) {
            this.topic = topic;
            return this;
        }

        /**
         * Builder method for imageKey parameter.
         *
         * @param imageKey
         *            field to set
         * @return builder
         */
        public Builder withImageKey(String imageKey) {
            this.imageKey = imageKey;
            return this;
        }

        /**
         * Builder method for objectKey parameter.
         *
         * @param objectKey
         *            field to set
         * @return builder
         */
        public Builder withObjectKey(String objectKey) {
            this.objectKey = objectKey;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public KafkaManagedWfEvent build() { return new KafkaManagedWfEvent(this); }
    }

}
