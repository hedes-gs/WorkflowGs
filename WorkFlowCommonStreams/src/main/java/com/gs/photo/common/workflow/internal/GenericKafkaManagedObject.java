package com.gs.photo.common.workflow.internal;

import java.util.Optional;

import com.workflow.model.HbaseData;
import com.workflow.model.events.WfEvent;

public class GenericKafkaManagedObject<T extends HbaseData> extends KafkaManagedObject {
    protected Optional<T> value;
    protected String      topic;
    protected String      imageKey;
    protected String      objectKey;

    public Optional<T> getValue() { return this.value; }

    public String getTopic() { return this.topic; }

    public String getImageKey() { return this.imageKey; }

    public String getObjectKey() { return this.objectKey; }

    public Optional<T> getObjectToSend() { return this.getValue(); }

    public WfEvent createWfEvent() { return null; }

}
