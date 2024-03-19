package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.events.WfEvents;

public class WfEventsDeserializer extends AbstractModelSerializerAndDeserializer<WfEvents>
    implements Deserializer<WfEvents> {

    @Override
    public WfEvents deserialize(String topic, byte[] data) { return this.fromBytesGeneric(topic, data); }

    public WfEventsDeserializer() { super(); }

}
