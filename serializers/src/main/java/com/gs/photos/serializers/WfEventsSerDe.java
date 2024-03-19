package com.gs.photos.serializers;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.events.WfEvents;;

public final class WfEventsSerDe implements Serde<WfEvents> {

    protected WfEventsSerializer   hbaseExifDataSerializer   = new WfEventsSerializer();
    protected WfEventsDeserializer hbaseExifDataDeserializer = new WfEventsDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {}

    @Override
    public Serializer<WfEvents> serializer() { return this.hbaseExifDataSerializer; }

    @Override
    public Deserializer<WfEvents> deserializer() { return this.hbaseExifDataDeserializer; }

}