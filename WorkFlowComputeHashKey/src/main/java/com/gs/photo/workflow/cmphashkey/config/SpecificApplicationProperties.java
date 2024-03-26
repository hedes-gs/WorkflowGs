package com.gs.photo.workflow.cmphashkey.config;

public class SpecificApplicationProperties implements ISpecificApplicationProperties {
    protected int batchSizeForParallelProcessingIncomingRecords;
    protected int kafkaPollTimeInMillisecondes;

    @Override
    public int getBatchSizeForParallelProcessingIncomingRecords() {
        return this.batchSizeForParallelProcessingIncomingRecords;
    }

    public void setBatchSizeForParallelProcessingIncomingRecords(int batchSizeForParallelProcessingIncomingRecords) {
        this.batchSizeForParallelProcessingIncomingRecords = batchSizeForParallelProcessingIncomingRecords;
    }

    @Override
    public int getKafkaPollTimeInMillisecondes() { return this.kafkaPollTimeInMillisecondes; }

    public void setKafkaPollTimeInMillisecondes(int kafkaPollTimeInMillisecondes) {
        this.kafkaPollTimeInMillisecondes = kafkaPollTimeInMillisecondes;
    }

}
