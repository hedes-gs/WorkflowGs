package com.gs.photo.workflow.extimginfo.config;

import java.util.List;

public class SpecificApplicationProperties {
    protected List<String> exifFiles;
    protected int          batchSizeForParallelProcessingIncomingRecords;

    public List<String> getExifFiles() { return this.exifFiles; }

    public void setExifFiles(List<String> exifFiles) { this.exifFiles = exifFiles; }

    public int getBatchSizeForParallelProcessingIncomingRecords() {
        return this.batchSizeForParallelProcessingIncomingRecords;
    }

    public void setBatchSizeForParallelProcessingIncomingRecords(int batchSizeForParallelProcessingIncomingRecords) {
        this.batchSizeForParallelProcessingIncomingRecords = batchSizeForParallelProcessingIncomingRecords;
    }

}
