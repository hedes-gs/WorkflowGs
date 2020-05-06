package com.gs.photo.workflow.internal;

import com.workflow.model.events.WfEvent;

public class GenericKafkaManagedObject<T> extends KafkaManagedObject {
    protected T      value;
    protected String topic;
    protected String imageKey;
    protected String objectKey;

    public T getValue() { return this.value; }

    public String getTopic() { return this.topic; }

    public String getImageKey() { return this.imageKey; }

    public String getObjectKey() { return this.objectKey; }

    public Object getObjectToSend() { return this.getValue(); }

    public WfEvent createWfEvent() { return null; }

}
