package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.ExchangedTiffData;
import com.workflow.model.events.WfEvents;

public class HbaseExifOrImageOrWfEventsSerializer implements Serializer<Object> {

    protected ExchangedDataSerializer exchangedDataSerializer = new ExchangedDataSerializer();
    protected ByteArraySerializer     byteArraySerializer     = new ByteArraySerializer();
    protected WfEventsSerializer      wfEventsSerializer      = new WfEventsSerializer();

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data instanceof byte[]) {
            return this.byteArraySerializer.serialize(topic, (byte[]) data);
        } else if (data instanceof ExchangedTiffData) {
            return this.exchangedDataSerializer.serialize(topic, (ExchangedTiffData) data);
        } else if (data instanceof WfEvents) { return this.wfEventsSerializer.serialize(topic, (WfEvents) data); }
        throw new IllegalArgumentException("Unexpected object " + data);
    }

    public HbaseExifOrImageOrWfEventsSerializer() { super(); }

}
