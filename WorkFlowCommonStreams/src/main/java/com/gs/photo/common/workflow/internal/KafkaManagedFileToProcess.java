package com.gs.photo.common.workflow.internal;

import java.util.Optional;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.workflow.model.builder.KeysBuilder.TopicCopyKeyBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventCopy;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEventRecorded.RecordedEventType;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.files.FileToProcess;

public class KafkaManagedFileToProcess extends GenericKafkaManagedObject<FileToProcess, WfEvent> {

    protected static Logger LOGGER = LoggerFactory.getLogger(KafkaManagedFileToProcess.class);
    protected byte[]        rawFile;
    protected String        hashKey;
    protected WfEventStep   step;

    private KafkaManagedFileToProcess(Builder builder) {
        this.partition = builder.partition;
        this.kafkaOffset = builder.kafkaOffset;
        this.value = builder.value;
        this.topic = builder.topic;
        this.imageKey = builder.imageKey;
        this.objectKey = builder.objectKey;
        this.rawFile = builder.rawFile;
        this.hashKey = builder.hashKey;
        this.step = builder.step;
    }

    public KafkaManagedFileToProcess() {}

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
            this.step,
            WfEventCopy.class);
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

    protected WfEventCopy buildCopyEvent(String imageKey, String parentDataId, String dataId, WfEventStep step) {
        return WfEventCopy.builder()
            .withImgId(imageKey)
            .withParentDataId(parentDataId)
            .withDataId(dataId)
            .withStep(step)
            .build();
    }

    public byte[] getRawFile() { return this.rawFile; }

    public void setRawFile(byte[] rawFile) { this.rawFile = rawFile; }

    public String getHashKey() { return this.hashKey; }

    public void setHashKey(String hashKey) { this.hashKey = hashKey; }

    public WfEventStep getStep() { return this.step; }

    public void setStep(WfEventStep step) { this.step = step; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private int                     partition;
        private long                    kafkaOffset;
        private Optional<FileToProcess> value = Optional.empty();
        private String                  topic;
        private String                  imageKey;
        private String                  objectKey;
        private byte[]                  rawFile;
        private String                  hashKey;
        private WfEventStep             step;

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
        public Builder withValue(Optional<FileToProcess> value) {
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
         * Builder method for step parameter.
         *
         * @param step
         *            field to set
         * @return builder
         */
        public Builder withStep(WfEventStep step) {
            this.step = step;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public KafkaManagedFileToProcess build() { return new KafkaManagedFileToProcess(this); }
    }

    public static String toString(KafkaManagedFileToProcess r) {
        StringJoiner strJoiner = new StringJoiner("-");
        r.getValue()
            .ifPresent((o) -> strJoiner.add(o.getUrl()));
        return strJoiner.toString();
    }

}