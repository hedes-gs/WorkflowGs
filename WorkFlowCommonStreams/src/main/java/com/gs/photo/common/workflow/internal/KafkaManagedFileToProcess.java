package com.gs.photo.common.workflow.internal;

import java.util.Optional;
import java.util.StringJoiner;

import javax.annotation.Generated;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.workflow.model.builder.KeysBuilder.TopicCopyKeyBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventCopy;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEventRecorded.RecordedEventType;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.files.FileToProcess;

public class KafkaManagedFileToProcess extends GenericKafkaManagedObject<Optional<FileToProcess>> {

    protected static Logger LOGGER = LoggerFactory.getLogger(KafkaManagedFileToProcess.class);
    protected byte[]        rawFile;
    protected String        hashKey;

    @Override
    public WfEvent createWfEvent() {

        String hbedoiHashCode = TopicCopyKeyBuilder.build(
            this.getValue()
                .get());
        return this.buildEvent(
            this.getValue()
                .get()
                .getImageId(),
            this.getValue()
                .get()
                .getImageId(),
            hbedoiHashCode,
            WfEventStep.WF_STEP_CREATED_FROM_STEP_LOCAL_COPY,
            WfEventCopy.class);
    }

    public WfEvent createWfEvent(String parentDataId, WfEventStep step) {
        String hbedoiHashCode = TopicCopyKeyBuilder.build(
            this.getValue()
                .get());
        return this.buildEvent(
            this.getValue()
                .get()
                .getImageId(),
            parentDataId,
            hbedoiHashCode,
            step,
            WfEventRecorded.class);
    }

    private WfEvent buildEvent(
        String imageKey,
        String parentDataId,
        String dataId,
        WfEventStep step,
        Class<? extends WfEvent> cl
    ) {
        if (cl.isAssignableFrom(WfEventCopy.class)) {
            return this.buildCopyEvent(imageKey, parentDataId, dataId, step);
        } else {
            return WfEventRecorded.builder()
                .withImgId(imageKey)
                .withParentDataId(parentDataId)
                .withDataId(dataId)
                .withStep(step)
                .withRecordedEventType(RecordedEventType.ARCHIVE)
                .build();
        }
    }

    protected WfEvent buildCopyEvent(String imageKey, String parentDataId, String dataId, WfEventStep step) {
        return WfEventCopy.builder()
            .withImgId(imageKey)
            .withParentDataId(parentDataId)
            .withDataId(dataId)
            .withStep(step)
            .build();
    }

    @Generated("SparkTools")
    private KafkaManagedFileToProcess(Builder builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
        this.rawFile = builder.rawFile;
        this.hashKey = builder.hashKey;
        this.value = builder.value;
    }

    public byte[] getRawFile() { return this.rawFile; }

    public String getHashKey() { return this.hashKey; }

    public static String toString(KafkaManagedFileToProcess r) {
        StringJoiner strJoiner = new StringJoiner("-");
        r.getValue()
            .ifPresent(
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
        private Optional<FileToProcess> value = Optional.empty();

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
        public Builder withValue(Optional<FileToProcess> origin) {
            this.value = origin;
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