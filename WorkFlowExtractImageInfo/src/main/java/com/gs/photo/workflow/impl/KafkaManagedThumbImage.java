package com.gs.photo.workflow.impl;

import java.nio.charset.Charset;

import javax.annotation.Generated;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.gs.photo.workflow.internal.GenericKafkaManagedObject;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventProduced;
import com.workflow.model.events.WfEventStep;

public class KafkaManagedThumbImage extends GenericKafkaManagedObject<ThumbImageToSend> {

    @Override
    public Object getObjectToSend() { return this.getValue()
        .getJpegImage(); }

    @Override
    public WfEvent createWfEvent() {
        HashFunction hf = Hashing.goodFastHash(256);
        String hbedoiHashCode = hf.newHasher()
            .putString(
                this.getValue()
                    .getImageKey(),
                Charset.forName("UTf-8"))
            .putLong(
                this.getValue()
                    .getTag())
            .putObject(
                this.getValue()
                    .getPath(),
                (path, sink) -> {
                    for (short t : path) {
                        sink.putShort(t);
                    }
                })
            .hash()
            .toString();

        return this.buildEvent(
            this.getValue()
                .getImageKey(),
            hbedoiHashCode,
            this.getValue()
                .getCurrentNb());
    }

    private WfEvent buildEvent(String imageKey, String dataId, int version) {
        return WfEventProduced.builder()
            .withImgId(imageKey)
            .withParentDataId(dataId)
            .withDataId(dataId + "-" + version)
            .withStep(
                WfEventStep.builder()
                    .withStep(WfEventStep.CREATED_FROM_STEP_IMAGE_FILE_READ)
                    .build())
            .build();
    }

    @Generated("SparkTools")
    private KafkaManagedThumbImage(Builder builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
        this.value = builder.value;
        this.topic = builder.topic;
        this.imageKey = builder.imageKey;
        this.objectKey = builder.objectKey;
    }

    /**
     * Creates builder to build {@link KafkaManagedThumbImage}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link KafkaManagedThumbImage}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private int              partition;
        private long             kafkaOffset;
        private ThumbImageToSend value;
        private String           topic;
        private String           imageKey;
        private String           objectKey;

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
        public Builder withValue(ThumbImageToSend value) {
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
        public KafkaManagedThumbImage build() { return new KafkaManagedThumbImage(this); }
    }

}
