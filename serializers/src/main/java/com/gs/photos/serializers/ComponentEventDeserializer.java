package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.events.ComponentEvent;

public class ComponentEventDeserializer extends AbstractModelSerializerAndDeserializer<ComponentEvent>
    implements Deserializer<ComponentEvent> {

    @Override
    public ComponentEvent deserialize(String topic, byte[] data) { return this.fromBytesGeneric(topic, data); }

    public ComponentEventDeserializer() { super(); }

}
