package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Deserializer;

import com.workflow.model.files.FileToProcess;

public class FileToProcessDeserializer extends AbstractModelSerializerAndDeserializer<FileToProcess>
    implements Deserializer<FileToProcess> {

    public FileToProcessDeserializer() { super(); }

    @Override
    public FileToProcess deserialize(String topic, byte[] t) { return super.fromBytesGeneric(t); }

}
