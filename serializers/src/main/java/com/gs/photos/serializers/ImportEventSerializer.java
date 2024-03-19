package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.events.ImportEvent;

public class ImportEventSerializer extends AbstractModelSerializerAndDeserializer<ImportEvent>
    implements Serializer<ImportEvent> {

    @Override
    public byte[] serialize(String topic, ImportEvent data) { return super.toBytesGeneric(topic, data); }

    public ImportEventSerializer() { super(); }

}
