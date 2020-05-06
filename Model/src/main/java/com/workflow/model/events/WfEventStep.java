package com.workflow.model.events;

import java.io.Serializable;

import javax.annotation.Generated;

public class WfEventStep implements Serializable {

    public final static String      CREATED_FROM_STEP_IMAGE_FILE_READ             = "CREATED_FROM_STEP_EXTRACT_IMAGE_FILE";
    public final static String      CREATED_FROM_STEP_IMG_PROCESSOR               = "CREATED_FROM_STEP_IMG_PROCESSOR";
    public final static String      CREATED_FROM_STEP_PREPARE_FOR_PERSIST         = "CREATED_FROM_STEP_PREPARE_FOR_PERSISTENCE";
    public final static String      CREATED_FROM_STEP_RECORDED_IN_HBASE           = "CREATED_FROM_STEP_RECORDED_IN_HBASE";
    public final static String      CREATED_FROM_STEP_ARCHIVED_IN_HDFS            = "CREATED_FROM_STEP_ARCHIVED_IN_HDFS";

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

    public WfEventStep() {}

    protected String step;

    public String getStep() { return this.step; }

    @Generated("SparkTools")
    private WfEventStep(Builder builder) { this.step = builder.step; }

    /**
     * Creates builder to build {@link WfEventStep}.
     *
     * @return created builder
     */
    @Generated("SparkTools")
    public static Builder builder() { return new Builder(); }

    /**
     * Builder to build {@link WfEventStep}.
     */
    @Generated("SparkTools")
    public static final class Builder {
        private String step;

        private Builder() {}

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
