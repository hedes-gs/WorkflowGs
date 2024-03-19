package com.gs.photo.common.workflow;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import com.gs.photos.serializers.ExchangedDataSerDe;
import com.gs.photos.serializers.FinalImageSerDe;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.storm.FinalImage;

public class StreamsHelper implements IStreamsHelper {

    protected IKafkaProperties kafkaProperties;

    /*
     * (non-Javadoc)
     *
     * @see com.gs.photo.workflow.IStreamHelper#buildKTableToStoreCreatedImages(org.
     * apache.kafka.streams.StreamsBuilder)
     */
    @Override
    public KTable<String, String> buildKTableToStoreCreatedImages(StreamsBuilder builder) {
        return builder.table(
            this.kafkaProperties.getTopics()
                .topicDupFilteredFile(),
            Consumed.with(Serdes.String(), Serdes.String()));
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
        KStream<String, FinalImage> stream = streamsBuilder.stream(
            this.kafkaProperties.getTopics()
                .topicTransformedThumb(),
            Consumed.with(Serdes.String(), new FinalImageSerDe()));
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
        KStream<String, ExchangedTiffData> stream = streamsBuilder.stream(
            this.kafkaProperties.getTopics()
                .topicExif(),
            Consumed.with(Serdes.String(), new ExchangedDataSerDe()));
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
        KStream<String, String> stream = streamsBuilder.stream(
            this.kafkaProperties.getTopics()
                .pathNameTopic(),
            Consumed.with(Serdes.String(), Serdes.String()));
        return stream;
    }

    public StreamsHelper(IKafkaProperties kafkaProperties) {
        super();
        this.kafkaProperties = kafkaProperties;
    }

}
