package com.gs.photo.common.workflow.impl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.stereotype.Service;

import com.gs.photo.common.workflow.IStreamsHelper;
import com.gs.photos.serializers.ExchangedDataSerDe;
import com.gs.photos.serializers.FinalImageSerDe;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.storm.FinalImage;

@Service
@ConditionalOnClass(value = org.apache.kafka.streams.StreamsBuilder.class)
public class StreamsHelper implements IStreamsHelper {

    @Value("${topic.topicDupFilteredFile}")
    protected String topicDupFilteredFile;

    @Value("${topic.topicExif}")
    protected String topicExif;

    @Value("${topic.pathNameTopic}")
    protected String pathNameTopic;

    @Value("${topic.topicTransformedThumb}")
    protected String topicTransformedThumb;

    /*
     * (non-Javadoc)
     *
     * @see com.gs.photo.workflow.IStreamHelper#buildKTableToStoreCreatedImages(org.
     * apache.kafka.streams.StreamsBuilder)
     */
    @Override
    public KTable<String, String> buildKTableToStoreCreatedImages(StreamsBuilder builder) {
        return builder.table(this.topicDupFilteredFile, Consumed.with(Serdes.String(), Serdes.String()));
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.gs.photo.workflow.IStreamHelper#buildKStreamToGetThumbImages(org.apache.
     * kafka.streams.StreamsBuilder)
     */
    @Override
    public KStream<String, FinalImage> buildKStreamToGetThumbImages(StreamsBuilder streamsBuilder) {
        KStream<String, FinalImage> stream = streamsBuilder
            .stream(this.topicTransformedThumb, Consumed.with(Serdes.String(), new FinalImageSerDe()));
        return stream;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.gs.photo.workflow.IStreamHelper#buildKStreamToGetExifValue(org.apache.
     * kafka.streams.StreamsBuilder)
     */
    @Override
    public KStream<String, ExchangedTiffData> buildKStreamToGetExifValue(StreamsBuilder streamsBuilder) {
        KStream<String, ExchangedTiffData> stream = streamsBuilder
            .stream(this.topicExif, Consumed.with(Serdes.String(), new ExchangedDataSerDe()));
        return stream;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.gs.photo.workflow.IStreamHelper#buildKTableToGetPathValue(org.apache.
     * kafka.streams.StreamsBuilder)
     */
    @Override
    public KStream<String, String> buildKTableToGetPathValue(StreamsBuilder streamsBuilder) {
        KStream<String, String> stream = streamsBuilder
            .stream(this.pathNameTopic, Consumed.with(Serdes.String(), Serdes.String()));
        return stream;
    }

}
