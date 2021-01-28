package com.gs.photo.common.workflow;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import com.workflow.model.ExchangedTiffData;
import com.workflow.model.storm.FinalImage;

public interface IStreamsHelper {

    KTable<String, String> buildKTableToStoreCreatedImages(StreamsBuilder builder);

    KStream<String, FinalImage> buildKStreamToGetThumbImages(StreamsBuilder streamsBuilder);

    KStream<String, ExchangedTiffData> buildKStreamToGetExifValue(StreamsBuilder streamsBuilder);

    KStream<String, String> buildKTableToGetPathValue(StreamsBuilder streamsBuilder);

}