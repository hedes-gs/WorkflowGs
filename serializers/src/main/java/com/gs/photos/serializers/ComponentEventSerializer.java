package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.events.ComponentEvent;

public class ComponentEventSerializer extends AbstractModelSerializerAndDeserializer<ComponentEvent>
    implements Serializer<ComponentEvent> {
    @Override
    public byte[] serialize(String topic, ComponentEvent data) { return super.toBytesGeneric(topic, data); }

    public ComponentEventSerializer() { super(); }

}
