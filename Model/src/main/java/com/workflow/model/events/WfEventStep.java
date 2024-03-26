package com.workflow.model.events;

import java.io.Serializable;
import java.util.Objects;

import com.workflow.model.HbaseData;

public class WfEventStep extends HbaseData implements Serializable {

    public final static String      CREATED_FROM_STEP_IMAGE_FILE_READ             = "CREATED_FROM_STEP_EXTRACT_IMAGE_FILE";
    public final static String      CREATED_FROM_STEP_IMG_PROCESSOR               = "CREATED_FROM_STEP_IMG_PROCESSOR";
    public final static String      CREATED_FROM_STEP_COMPUTE_HASH_KEY            = "CREATED_FROM_STEP_COMPUTE_HASH_KEY";
    public final static String      CREATED_FROM_STEP_PREPARE_FOR_PERSIST         = "CREATED_FROM_STEP_PREPARE_FOR_PERSISTENCE";
    public final static String      CREATED_FROM_STEP_RECORDED_IN_HBASE           = "CREATED_FROM_STEP_RECORDED_IN_HBASE";
    public final static String      CREATED_FROM_STEP_ARCHIVED_IN_HDFS            = "CREATED_FROM_STEP_ARCHIVED_IN_HDFS";
    public final static String      CREATED_FROM_STEP_LOCAL_COPY                  = "CREATED_FROM_STEP_LOCAL_COPY";

    private static final long       serialVersionUID                              = 1L;

    public final static WfEventStep WF_STEP_CREATED_FROM_STEP_IMAGE_FILE_READ     = WfEventStep.builder()
        .withStep(WfEventStep.CREATED_FROM_STEP_IMAGE_FILE_READ)
        .build();
    public final static WfEventStep WF_STEP_CREATED_FROM_STEP_IMG_PROCESSOR       = WfEventStep.builder()
        .withStep(WfEventStep.CREATED_FROM_STEP_IMG_PROCESSOR)
        .build();
    public final static WfEventStep WF_STEP_CREATED_FROM_STEP_PREPARE_FOR_PERSIST = WfEventStep.builder()
        .withStep(WfEventStep.CREATED_FROM_STEP_PREPARE_FOR_PERSIST)
        .build();
    public final static WfEventStep WF_STEP_CREATED_FROM_STEP_RECORDED_IN_HBASE   = WfEventStep.builder()
        .withStep(WfEventStep.CREATED_FROM_STEP_RECORDED_IN_HBASE)
        .build();
    public final static WfEventStep WF_STEP_CREATED_FROM_STEP_ARCHIVED_IN_HDFS    = WfEventStep.builder()
        .withStep(WfEventStep.CREATED_FROM_STEP_ARCHIVED_IN_HDFS)
        .build();
    public final static WfEventStep WF_STEP_CREATED_FROM_STEP_LOCAL_COPY          = WfEventStep.builder()
        .withStep(WfEventStep.CREATED_FROM_STEP_LOCAL_COPY)
        .build();
    public final static WfEventStep WF_CREATED_FROM_STEP_COMPUTE_HASH_KEY         = WfEventStep.builder()
        .withStep(WfEventStep.CREATED_FROM_STEP_COMPUTE_HASH_KEY)
        .build();

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = (prime * result) + Objects.hash(this.step);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { return true; }
        if (!super.equals(obj)) { return false; }
        if (this.getClass() != obj.getClass()) { return false; }
        WfEventStep other = (WfEventStep) obj;
        return Objects.equals(this.step, other.step);
    }

    public WfEventStep() {}

    protected String step;

    private WfEventStep(Builder builder) {
        this.dataCreationDate = builder.dataCreationDate;
        this.dataId = builder.dataId;
        this.step = builder.step;
    }

    public String getStep() { return this.step; }

    @Override
    public String toString() {
        StringBuilder builder2 = new StringBuilder();
        builder2.append("WfEventStep [step=");
        builder2.append(this.step);
        builder2.append("]");
        return builder2.toString();
    }

    /**
     * Creates builder to build {@link WfEventStep}.
     *
     * @return created builder
     */
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link WfEventStep}.
     */
    public static final class Builder {
        private long   dataCreationDate;
        private String dataId;
        private String step;

        private Builder() {}

        /**
         * Builder method for dataCreationDate parameter.
         *
         * @param dataCreationDate
         *            field to set
         * @return builder
         */
        public Builder withDataCreationDate(long dataCreationDate) {
            this.dataCreationDate = dataCreationDate;
            return this;
        }

        /**
         * Builder method for dataId parameter.
         *
         * @param dataId
         *            field to set
         * @return builder
         */
        public Builder withDataId(String dataId) {
            this.dataId = dataId;
            return this;
        }

        /**
         * Builder method for step parameter.
         *
         * @param step
         *            field to set
         * @return builder
         */
        public Builder withStep(String step) {
            this.step = step;
            return this;
        }

        /**
         * Builder method of the builder.
         *
         * @return built class
         */
        public WfEventStep build() { return new WfEventStep(this); }
    }

}
