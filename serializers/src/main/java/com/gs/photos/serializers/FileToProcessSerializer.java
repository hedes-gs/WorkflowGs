package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.files.FileToProcess;

public class FileToProcessSerializer extends AbstractModelSerializerAndDeserializer<FileToProcess>
    implements Serializer<FileToProcess> {

    public FileToProcessSerializer() { super(); }

    @Override
    public byte[] serialize(String topic, FileToProcess data) { return super.toBytesGeneric(data); }

}
