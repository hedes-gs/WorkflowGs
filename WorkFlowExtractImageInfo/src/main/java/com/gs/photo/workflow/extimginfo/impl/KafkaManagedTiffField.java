package com.gs.photo.workflow.extimginfo.impl;

import java.util.Optional;

import javax.annotation.Generated;

import com.gs.photo.common.workflow.internal.GenericKafkaManagedObject;
import com.gs.photos.workflow.extimginfo.metadata.TiffFieldAndPath;

public class KafkaManagedTiffField extends GenericKafkaManagedObject<TiffFieldAndPath> {

    @Generated("SparkTools")
    private KafkaManagedTiffField(Builder builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
        this.value = builder.value;
        this.topic = builder.topic;
        this.imageKey = builder.imageKey;
        this.objectKey = builder.objectKey;
    }

    /**
     * Creates builder to build {@link KafkaManagedTiffField}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link KafkaManagedTiffField}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private int                        partition;
        private long                       kafkaOffset;
        private Optional<TiffFieldAndPath> value;
        private String                     topic;
        private String                     imageKey;
        private String                     objectKey;

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
        public Builder withValue(TiffFieldAndPath value) {
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
        public KafkaManagedTiffField build() { return new KafkaManagedTiffField(this); }
    }

}
