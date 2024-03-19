package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.events.WfEvent;

public class WfEventDeserializer extends AbstractModelSerializerAndDeserializer<WfEvent>
    implements Deserializer<WfEvent> {

    @Override
    public WfEvent deserialize(String topic, byte[] data) { return this.fromBytesGeneric(topic, data); }

    public WfEventDeserializer() { super(); }

}
