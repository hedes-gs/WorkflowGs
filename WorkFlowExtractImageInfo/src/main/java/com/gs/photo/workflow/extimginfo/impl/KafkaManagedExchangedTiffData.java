package com.gs.photo.workflow.extimginfo.impl;

import java.util.Optional;

import javax.annotation.Generated;

import com.gs.photo.common.workflow.internal.GenericKafkaManagedObject;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.WfEventProduced;
import com.workflow.model.events.WfEventStep;

public class KafkaManagedExchangedTiffData extends GenericKafkaManagedObject<ExchangedTiffData, WfEventProduced> {

    @Override
    public WfEventProduced createWfEvent() {
        return WfEventProduced.builder()
            .withImgId(this.imageKey)
            .withParentDataId(
                this.getValue()
                    .get()
                    .getImageId())
            .withDataId(
                KeysBuilder.ExchangedTiffDataKeyBuilder.build(
                    this.getValue()
                        .get()))
            .withStep(
                WfEventStep.builder()
                    .withStep(WfEventStep.CREATED_FROM_STEP_IMAGE_FILE_READ)
                    .build())
            .build();
    }

    @Generated("SparkTools")
    private KafkaManagedExchangedTiffData(Builder builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
        this.value = Optional.ofNullable(builder.value);
        this.topic = builder.topic;
        this.imageKey = builder.imageKey;
        this.objectKey = builder.objectKey;
    }

    /**
     * Creates builder to build {@link KafkaManagedExchangedTiffData}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link KafkaManagedExchangedTiffData}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private int               partition;
        private long              kafkaOffset;
        private ExchangedTiffData value;
        private String            topic;
        private String            imageKey;
        private String            objectKey;

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
        public Builder withValue(ExchangedTiffData value) {
            this.value = value;
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
        public KafkaManagedExchangedTiffData build() { return new KafkaManagedExchangedTiffData(this); }
    }

}
