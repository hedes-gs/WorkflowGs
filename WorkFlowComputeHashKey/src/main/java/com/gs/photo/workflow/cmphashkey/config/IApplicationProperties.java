package com.gs.photo.workflow.cmphashkey.config;

public interface IApplicationProperties {

    public int batchSizeForParallelProcessingIncomingRecords();

    public int kafkaPollTimeInMillisecondes();
}
