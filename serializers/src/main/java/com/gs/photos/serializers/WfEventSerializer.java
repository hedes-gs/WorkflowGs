package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.events.WfEvent;

public class WfEventSerializer extends AbstractModelSerializerAndDeserializer<WfEvent> implements Serializer<WfEvent> {

    @Override
    public byte[] serialize(String topic, WfEvent data) { return super.toBytesGeneric(data); }

    public WfEventSerializer() { super(); }

}
