package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.events.ImportEvent;

public class ImportEventDeserializer extends AbstractModelSerializerAndDeserializer<ImportEvent>
    implements Deserializer<ImportEvent> {

    @Override
    public ImportEvent deserialize(String topic, byte[] data) { return this.fromBytesGeneric(data); }

    public ImportEventDeserializer() { super(); }

}
