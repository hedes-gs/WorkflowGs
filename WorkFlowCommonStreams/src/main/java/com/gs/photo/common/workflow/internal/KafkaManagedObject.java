package com.gs.photo.common.workflow.internal;

public class KafkaManagedObject {

    protected int  partition;
    protected long kafkaOffset;

    private KafkaManagedObject(Builder builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
    }

    public KafkaManagedObject() {}

    public int getPartition() { return this.partition; }

    public long getKafkaOffset() { return this.kafkaOffset; }

    public static Builder builderForKafkaManagedObject() { return new Builder(); }

    public static final class Builder {
        private int  partition;
        private long kafkaOffset;

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
         * Builder method of the builder.
         *
         * @return built class
         */
        public KafkaManagedObject build() { return new KafkaManagedObject(this); }
    }

}
