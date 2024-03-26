package com.gs.photo.workflow.cmphashkey.config;

public interface ISpecificApplicationProperties {

    public int getBatchSizeForParallelProcessingIncomingRecords();

    public int getKafkaPollTimeInMillisecondes();
}
