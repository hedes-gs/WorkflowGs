package com.gs.photos.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.files.FileToProcess;;

public final class FileToProcessSerDe implements Serde<FileToProcess> {

    protected FileToProcessSerializer   fileToProcessDataSerializer   = new FileToProcessSerializer();
    protected FileToProcessDeserializer fileToProcessDataDeserializer = new FileToProcessDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {}

    @Override
    public Serializer<FileToProcess> serializer() { return this.fileToProcessDataSerializer; }

    @Override
    public Deserializer<FileToProcess> deserializer() { return this.fileToProcessDataDeserializer; }

}