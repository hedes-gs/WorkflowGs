package com.gs.photo.common.workflow.internal;

import java.util.Optional;

import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEvent;

public class GenericKafkaManagedObject<T extends HbaseData, U extends WfEvent> extends KafkaManagedObject {
    protected Optional<T> value;
    protected String      topic;
    protected String      imageKey;
    protected String      objectKey;

    private GenericKafkaManagedObject(GenericKafkaManagedObjectBuilder<T, U> builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
        this.value = builder.value;
        this.topic = builder.topic;
        this.imageKey = builder.imageKey;
        this.objectKey = builder.objectKey;
    }

    public GenericKafkaManagedObject() {}

    public Optional<T> getValue() { return this.value; }

    public String getTopic() { return this.topic; }

    public String getImageKey() { return this.imageKey; }

    public String getObjectKey() { return this.objectKey; }

    public Optional<T> getObjectToSend() { return this.getValue(); }

    public U createWfEvent() { return null; }

    public static GenericKafkaManagedObjectBuilder<? extends HbaseData, ? extends WfEvent> genericKafkaManagedObjectBuilder() {
        return new GenericKafkaManagedObjectBuilder<>();
    }

    public static final class GenericKafkaManagedObjectBuilder<T extends HbaseData, U extends WfEvent> {
        private int         partition;
        private long        kafkaOffset;
        private Optional<T> value = Optional.empty();
        private String      topic;
        private String      imageKey;
        private String      objectKey;

        private GenericKafkaManagedObjectBuilder() {}

        /**
         * Builder method for partition parameter.
         *
         * @param partition
         *            field to set
         * @return builder
         */
        public GenericKafkaManagedObjectBuilder<T, U> withPartition(int partition) {
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
        public GenericKafkaManagedObjectBuilder<T, U> withKafkaOffset(long kafkaOffset) {
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
        public GenericKafkaManagedObjectBuilder<T, U> withValue(Optional<T> value) {
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
        public GenericKafkaManagedObjectBuilder<T, U> withTopic(String topic) {
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
        public GenericKafkaManagedObjectBuilder<T, U> withImageKey(String imageKey) {
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
        public GenericKafkaManagedObjectBuilder<T, U> withObjectKey(String objectKey) {
            this.objectKey = objectKey;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public GenericKafkaManagedObject<T, U> build() { return new GenericKafkaManagedObject<>(this); }
    }

}
