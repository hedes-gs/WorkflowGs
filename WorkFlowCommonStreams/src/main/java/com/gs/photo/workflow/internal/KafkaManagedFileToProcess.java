package com.gs.photo.workflow.internal;

import java.util.Optional;
import java.util.StringJoiner;

import javax.annotation.Generated;

import com.workflow.model.files.FileToProcess;

public class KafkaManagedFileToProcess extends KafkaManagedObject {
    protected byte[]                  rawFile;
    protected String                  hashKey;
    protected Optional<FileToProcess> origin;

    @Generated("SparkTools")
    private KafkaManagedFileToProcess(Builder builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
        this.rawFile = builder.rawFile;
        this.hashKey = builder.hashKey;
        this.origin = builder.origin;
    }

    public byte[] getRawFile() { return this.rawFile; }

    public Optional<FileToProcess> getOrigin() { return this.origin; }

    public String getHashKey() { return this.hashKey; }

    public static String toString(KafkaManagedFileToProcess r) {
        StringJoiner strJoiner = new StringJoiner("-");
        r.origin.ifPresent(
            (o) -> strJoiner.add(o.getHost())
                .add(o.getPath())
                .add(o.getName()));
        return strJoiner.toString();
    }

    public void setRawFile(Object object) { this.rawFile = null; }

    /**
     * Creates builder to build {@link KafkaManagedFileToProcess}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link KafkaManagedFileToProcess}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private int                     partition;
        private long                    kafkaOffset;
        private byte[]                  rawFile;
        private String                  hashKey;
        private Optional<FileToProcess> origin = Optional.empty();

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
         * Builder method for rawFile parameter.
         *
         * @param rawFile
         *            field to set
         * @return builder
         */
        public Builder withRawFile(byte[] rawFile) {
            this.rawFile = rawFile;
            return this;
        }

        /**
         * Builder method for hashKey parameter.
         *
         * @param hashKey
         *            field to set
         * @return builder
         */
        public Builder withHashKey(String hashKey) {
            this.hashKey = hashKey;
            return this;
        }

        /**
         * Builder method for origin parameter.
         *
         * @param origin
         *            field to set
         * @return builder
         */
        public Builder withOrigin(Optional<FileToProcess> origin) {
            this.origin = origin;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public KafkaManagedFileToProcess build() { return new KafkaManagedFileToProcess(this); }
    }

}