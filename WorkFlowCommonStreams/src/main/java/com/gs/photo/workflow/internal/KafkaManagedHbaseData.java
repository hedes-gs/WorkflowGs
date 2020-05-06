package com.gs.photo.workflow.internal;

import javax.annotation.Generated;

import com.workflow.model.HbaseData;

public class KafkaManagedHbaseData extends KafkaManagedObject {

    protected String    imageKey;
    protected String    topic;
    protected HbaseData value;

    @Generated("SparkTools")
    private KafkaManagedHbaseData(Builder builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
        this.imageKey = builder.imageKey;
        this.topic = builder.topic;
        this.value = builder.value;
    }

    public HbaseData getValue() { return this.value; }

    public String getTopic() { return this.topic; }

    public String getImageKey() { return this.imageKey; }

    /**
     * Creates builder to build {@link KafkaManagedHbaseData}.
     * 
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link KafkaManagedHbaseData}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private int       partition;
        private long      kafkaOffset;
        private String    imageKey;
        private String    topic;
        private HbaseData value;

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
         * Builder method for value parameter.
         * 
         * @param value
         *            field to set
         * @return builder
         */
        public Builder withValue(HbaseData value) {
            this.value = value;
            return this;
        }

        /**
         * Builder method of the builder.
         * 
         * @return built class
         */
        public KafkaManagedHbaseData build() { return new KafkaManagedHbaseData(this); }
    }

}
