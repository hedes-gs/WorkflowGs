package com.gs.photos.serializers;

import org.apache.kafka.common.serialization.Serializer;

import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseImageAsByteArray;
import com.workflow.model.events.ComponentEvent;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;

public class MultipleSerializers implements Serializer<HbaseData> {

    protected ExchangedDataSerializer        exchangedDataSerializer        = new ExchangedDataSerializer();
    protected WfEventsSerializer             wfEventsSerializer             = new WfEventsSerializer();
    protected FileToProcessSerializer        fileToProcessSerializer        = new FileToProcessSerializer();
    protected ImageAsByteArrayDataSerializer imageAsByteArrayDataSerializer = new ImageAsByteArrayDataSerializer();
    protected ComponentEventSerializer       componentEventSerializer       = new ComponentEventSerializer();

    @Override
    public byte[] serialize(String topic, HbaseData data) {

        return switch (data) {
            case ExchangedTiffData d -> this.exchangedDataSerializer.serialize(topic, d);
            case WfEvents d -> this.wfEventsSerializer.serialize(topic, d);
            case FileToProcess d -> this.fileToProcessSerializer.serialize(topic, d);
            case HbaseImageAsByteArray d -> this.imageAsByteArrayDataSerializer.serialize(topic, d);
            case ComponentEvent d -> this.componentEventSerializer.serialize(topic, d);
            default -> throw new IllegalArgumentException("Unexpected value: " + data);
        };
    }

    public MultipleSerializers() { super(); }

}
