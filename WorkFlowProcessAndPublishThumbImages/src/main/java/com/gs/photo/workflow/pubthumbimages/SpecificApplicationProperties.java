package com.gs.photo.workflow.pubthumbimages;

import java.util.List;

public class SpecificApplicationProperties {
    protected List<String> exifFiles;
    protected int          collectorEventsBufferSize;
    protected int          collectorEventsTimeWindow;

    public int getCollectorEventsBufferSize() { return this.collectorEventsBufferSize; }

    public void setCollectorEventsBufferSize(int collectorEventsBufferSize) {
        this.collectorEventsBufferSize = collectorEventsBufferSize;
    }

    public int getCollectorEventsTimeWindow() { return this.collectorEventsTimeWindow; }

    public void setCollectorEventsTimeWindow(int collectorEventsTimeWindow) {
        this.collectorEventsTimeWindow = collectorEventsTimeWindow;
    }

    public List<String> getExifFiles() { return this.exifFiles; }

    public void setExifFiles(List<String> exifFiles) { this.exifFiles = exifFiles; }

}
