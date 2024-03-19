package com.gs.photos.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.events.WfEvent;;

public final class WfEventSerDe implements Serde<WfEvent> {

    protected WfEventSerializer   hbaseExifDataSerializer   = new WfEventSerializer();
    protected WfEventDeserializer hbaseExifDataDeserializer = new WfEventDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {}

    @Override
    public Serializer<WfEvent> serializer() { return this.hbaseExifDataSerializer; }

    @Override
    public Deserializer<WfEvent> deserializer() { return this.hbaseExifDataDeserializer; }

}