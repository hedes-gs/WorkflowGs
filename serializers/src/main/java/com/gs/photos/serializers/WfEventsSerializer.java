package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.events.WfEvents;

public class WfEventsSerializer extends AbstractModelSerializerAndDeserializer<WfEvents>
    implements Serializer<WfEvents> {

    @Override
    public byte[] serialize(String topic, WfEvents data) { return super.toBytesGeneric(data); }

    public WfEventsSerializer() { super(); }

}
