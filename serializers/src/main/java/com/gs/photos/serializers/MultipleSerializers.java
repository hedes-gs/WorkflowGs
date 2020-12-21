package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;

import com.nurkiewicz.typeof.TypeOf;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;

public class MultipleSerializers implements Serializer<Object> {

    protected ExchangedDataSerializer exchangedDataSerializer = new ExchangedDataSerializer();
    protected ByteArraySerializer     byteArraySerializer     = new ByteArraySerializer();
    protected WfEventsSerializer      wfEventsSerializer      = new WfEventsSerializer();
    protected FileToProcessSerializer fileToProcessSerializer = new FileToProcessSerializer();

    @Override
    public byte[] serialize(String topic, Object data) {

        if (data instanceof byte[]) {
            return this.byteArraySerializer.serialize(topic, (byte[]) data);
        } else {
            return TypeOf.whenTypeOf(data)
                .is(ExchangedTiffData.class)
                .thenReturn((v) -> this.exchangedDataSerializer.serialize(topic, v))
                .is(WfEvents.class)
                .thenReturn((v) -> this.wfEventsSerializer.serialize(topic, v))
                .is(FileToProcess.class)
                .thenReturn((v) -> this.fileToProcessSerializer.serialize(topic, v))
                .get();

        }

    }

    public MultipleSerializers() { super(); }

}
