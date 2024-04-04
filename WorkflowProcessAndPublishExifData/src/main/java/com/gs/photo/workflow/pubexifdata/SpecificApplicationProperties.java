package com.gs.photo.workflow.pubexifdata;

public class SpecificApplicationProperties {
    protected int collectorEventsBufferSize;
    protected int collectorEventsTimeWindow;

    public int getCollectorEventsBufferSize() { return this.collectorEventsBufferSize; }

    public void setCollectorEventsBufferSize(int collectorEventsBufferSize) {
        this.collectorEventsBufferSize = collectorEventsBufferSize;
    }

    public int getCollectorEventsTimeWindow() { return this.collectorEventsTimeWindow; }

    public void setCollectorEventsTimeWindow(int collectorEventsTimeWindow) {
        this.collectorEventsTimeWindow = collectorEventsTimeWindow;
    }

}
