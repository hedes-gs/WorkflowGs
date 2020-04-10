package com.gs.photo.workflow;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.gs.photos.serializers.ExchangedDataSerDe;
import com.gs.photos.serializers.HbaseDataSerDe;
import com.gs.photos.serializers.HbaseExifDataSerDe;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
import com.gs.photos.serializers.WfEventSerDe;
import com.gs.photos.serializers.WfEventsSerDe;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseExifDataOfImages;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.builder.KeysBuilder.HbaseExifDataKeyBuilder;
import com.workflow.model.builder.KeysBuilder.HbaseExifDataOfImagesKeyBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;

@Configuration
public class ApplicationConfig extends AbstractApplicationConfig {

    private static final int    EVENTS_WINDOW_DURATION = 500;
    private static final String HEIGHT_STRING          = "-HEIGHT";
    private static final String WIDTH_STRING           = "-WIDTH";
    private static final String CREATION_DATE          = "-CREATION_DATE";

    public static final int     JOIN_WINDOW_TIME       = 86400;
    public static final short   EXIF_CREATION_DATE_ID  = (short) 0x9003;
    public static final short   EXIF_SIZE_WIDTH        = (short) 0xA002;
    public static final short   EXIF_SIZE_HEIGHT       = (short) 0xA003;

    @Bean
    public Properties kafkaStreamProperties(
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafkaStreamDir.dir}") String kafkaStreamDir,
        @Value("${group.id}") String applicationGroupId

    ) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationGroupId + "-wf-proc-publ-exif-data");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String()
                .getClass());
        config.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String()
                .getClass());
        config.put(StreamsConfig.STATE_DIR_CONFIG, kafkaStreamDir);
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        config.put("sasl.kerberos.service.name", "kafka");
        return config;
    }

    @Bean
    public Topology kafkaStreamsTopology2(
        @Value("${topic.topicExifSizeOfImageStream}") String topicExifSizeOfImageStream,
        @Value("${topic.topicImageDataToPersist}") String topicImageDataToPersist,
        @Value("${topic.topicExifImageDataToPersist}") String topicExifImageDataToPersist,
        @Value("${topic.topicExif}") String topicExif,
        @Value("${topic.topicEvent}") String topicEvent

    ) {
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, ExchangedTiffData> kstreamToGetExifValue = this
            .buildKStreamToGetExifValue(builder, topicExif);

        /*
         * Stream of HbaseExifData from topicExif : [ key-EXIF-tiffId, ExchnaedTiffData
         * ] -> [key,HbaseExifData]
         */
        final KStream<String, HbaseExifData> streamOfHbaseExifData = kstreamToGetExifValue.map(
            (k, v) -> {
                // We create the HbaseExifData that will be stored in hbase
                // the HbaseExifData value is streamed with the current key.
                return new KeyValue<String, HbaseExifData>(k, this.buildHbaseExifData(v));
            });

        /*
         * Stream of the {height, width} of image
         */
        KStream<String, Long> exifSizeOfImageStream = kstreamToGetExifValue.filter((key, exif) -> {
            boolean b = (exif.getTag() == ApplicationConfig.EXIF_SIZE_WIDTH)
                || (exif.getTag() == ApplicationConfig.EXIF_CREATION_DATE_ID)
                || (exif.getTag() == ApplicationConfig.EXIF_SIZE_HEIGHT);
            return b;
        })
            .map((k, exif) -> {
                Optional<String> newKey;
                Optional<Long> value = Optional.empty();
                switch (exif.getTag()) {
                    case ApplicationConfig.EXIF_SIZE_WIDTH: {
                        newKey = Optional.of(
                            KeysBuilder.topicExifSizeOfImageStreamBuilder()
                                .withKey(k)
                                .withSizeWidth()
                                .build());
                        value = Optional.of((long) exif.getDataAsInt()[0]);
                        break;
                    }
                    case ApplicationConfig.EXIF_SIZE_HEIGHT: {
                        newKey = Optional.of(
                            KeysBuilder.topicExifSizeOfImageStreamBuilder()
                                .withKey(k)
                                .withSizeHeight()
                                .build());
                        value = Optional.of((long) exif.getDataAsInt()[0]);
                        break;
                    }
                    case ApplicationConfig.EXIF_CREATION_DATE_ID: {
                        newKey = Optional.of(
                            KeysBuilder.topicExifSizeOfImageStreamBuilder()
                                .withKey(k)
                                .withCreationDate()
                                .build());
                        try {
                            value = Optional
                                .of(DateTimeHelper.toEpochMillis(new String(exif.getDataAsByte(), "UTF-8").trim()));
                        } catch (UnsupportedEncodingException e) {
                            AbstractApplicationConfig.LOGGER.error("Error", e);
                        }
                        break;
                    }
                    default: {
                        newKey = Optional.empty();
                        break;
                    }
                }
                final KeyValue<String, Long> keyValue = new KeyValue<String, Long>(
                    newKey.orElseThrow(() -> new IllegalArgumentException("Filter failed : received " + exif.getTag())),
                    value.orElseThrow(() -> new IllegalArgumentException("No value found")));
                return keyValue;
            })
            .through(topicExifSizeOfImageStream, Produced.with(Serdes.String(), Serdes.Long()));

        /*
         * Streams to retrieve {height, width, creation date} of image
         */
        KStream<String, Long> streamForWidthForHbaseExifData = exifSizeOfImageStream
            .filter((key, hbaseExifData) -> { return key.contains(ApplicationConfig.WIDTH_STRING); })
            .map(
                (k, v) -> {
                    return new KeyValue<String, Long>(KeysBuilder.topicExifSizeOfImageStreamBuilder()
                        .getOriginalKey(k), v);
                });
        KStream<String, Long> streamForHeightForHbaseExifData = exifSizeOfImageStream
            .filter((key, hbaseExifData) -> { return key.contains(ApplicationConfig.HEIGHT_STRING); })
            .map(
                (k, v) -> {
                    return new KeyValue<String, Long>(KeysBuilder.topicExifSizeOfImageStreamBuilder()
                        .getOriginalKey(k), v);
                });
        KStream<String, Long> streamForCreationDateForHbaseExifData = exifSizeOfImageStream
            .filter((key, hbaseExifData) -> { return key.contains(ApplicationConfig.CREATION_DATE); })
            .map(
                (k, v) -> {
                    return new KeyValue<String, Long>(KeysBuilder.topicExifSizeOfImageStreamBuilder()
                        .getOriginalKey(k), v);
                });

        Joined<String, HbaseExifData, Long> join = Joined
            .with(Serdes.String(), new HbaseExifDataSerDe(), Serdes.Long());
        ValueJoiner<HbaseExifData, Long, HbaseExifData> joinerWidth = (value1, width) -> {
            value1.setWidth(width);
            return value1;
        };
        ValueJoiner<HbaseExifData, Long, HbaseExifData> joinerHeight = (value1, height) -> {
            value1.setHeight(height);
            return value1;
        };
        ValueJoiner<HbaseExifData, Long, HbaseExifData> joinerCreationDate = (value1, creationDate) -> {
            value1.setCreationDate(creationDate);
            return value1;
        };
        // KStream<String, HbaseExifData> streamOfHbaseExifData
        // KStream<String, Integer> streamForWidthForHbaseExifData
        KStream<String, HbaseExifData> streamForUpdatingHbaseExifData = streamOfHbaseExifData.join(
            streamForWidthForHbaseExifData,
            joinerWidth,
            JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
            join);

        streamForUpdatingHbaseExifData = streamForUpdatingHbaseExifData.join(
            streamForHeightForHbaseExifData,
            joinerHeight,
            JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
            join);

        streamForUpdatingHbaseExifData = streamForUpdatingHbaseExifData.join(
            streamForCreationDateForHbaseExifData,
            joinerCreationDate,
            JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
            join);

        KStream<String, HbaseImageThumbnail> hbaseThumbMailStream = this
            .buildKStreamToGetHbaseImageThumbNail(builder, topicImageDataToPersist);
        KStream<String, HbaseExifData> hbaseExifUpdate = streamForUpdatingHbaseExifData.join(
            hbaseThumbMailStream,
            (v_HbaseExifData, v_HbaseImageThumbnail) -> {
                return this.updateHbaseExifData(v_HbaseExifData, v_HbaseImageThumbnail);
            },
            JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
            Joined.with(Serdes.String(), new HbaseExifDataSerDe(), new HbaseImageThumbnailSerDe()));

        KStream<String, HbaseData> finalStreamOfHbaseExifData = hbaseExifUpdate.flatMapValues((key, value) -> {
            final Collection<HbaseData> asList = Arrays.asList(value, this.buildHbaseExifDataOfImages(value));
            return asList;
        });

        KStream<String, WfEvent> eventStream = finalStreamOfHbaseExifData
            .filter((k, v) -> v instanceof HbaseExifDataOfImages)
            .map(
                (k, v) -> new KeyValue<String, WfEvent>(k,
                    this.buildEvent(v)
                        .orElseThrow(() -> new IllegalArgumentException())));

        finalStreamOfHbaseExifData = finalStreamOfHbaseExifData
            .map((k, v) -> new KeyValue<String, HbaseData>(this.buildKeyForHbaseExifData(v), v));

        this.publishImageDataInRecordTopic(finalStreamOfHbaseExifData, topicExifImageDataToPersist);

        final KTable<Windowed<String>, WfEvents> streamAggregatedByKey = eventStream
            .groupByKey(Grouped.with(Serdes.String(), new WfEventSerDe()))
            .windowedBy(TimeWindows.of(Duration.ofMillis(ApplicationConfig.EVENTS_WINDOW_DURATION)))
            .aggregate(
                () -> WfEvents.builder()
                    .withProducer("PUBLISH_THB_EXIF")
                    .withDataId("<not used>")
                    .withEvents(new ArrayList<WfEvent>())
                    .build(),
                (k, v, wfevents) -> wfevents.addEvent(v),
                Materialized.with(Serdes.String(), new WfEventsSerDe()));
        final KStream<Windowed<String>, WfEvents> streamOfEventsInAWindow = streamAggregatedByKey.toStream();
        KStream<String, WfEvents> wfEventsStream = streamOfEventsInAWindow.map((k, v) -> new KeyValue<>(k.key(), v));

        this.publishImageDataInEventTopic(wfEventsStream, topicEvent);

        return builder.build();
    }

    private String buildKeyForHbaseExifData(HbaseData v) {
        if (v instanceof HbaseExifDataOfImages) {
            HbaseExifDataOfImages hbedoi = (HbaseExifDataOfImages) v;
            return KeysBuilder.topicExifImageDataToPersistKeyBuilder()
                .withExifPath(hbedoi.getExifPath())
                .withExifTag(hbedoi.getExifTag())
                .withOriginalImageKey(hbedoi.getImageId())
                .build();
        } else if (v instanceof HbaseExifData) {
            HbaseExifData hbed = (HbaseExifData) v;
            return KeysBuilder.topicExifImageDataToPersistKeyBuilder()
                .withExifPath(hbed.getExifPath())
                .withExifTag(hbed.getExifTag())
                .withOriginalImageKey(hbed.getImageId())
                .build();
        }
        throw new IllegalArgumentException("Unexpected class " + v);

    }

    // @Bean
    public Topology kafkaStreamsTopology(
        @Value("${topic.topicExifSizeOfImageStream}") String topicExifSizeOfImageStream,
        @Value("${topic.topicImageDataToPersist}") String topicImageDataToPersist,
        @Value("${topic.topicExifImageDataToPersist}") String topicExifImageDataToPersist,
        @Value("${topic.topicExif}") String topicExif
    ) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, HbaseExifData> exifOfImageStream = this
            .buildKStreamToGetExifValue(builder, topicImageDataToPersist)
            .map(
                (key, v_ExchangedTiffData) -> {
                    return new KeyValue<>(key, this.buildHbaseExifData(v_ExchangedTiffData));
                });
        KStream<String, HbaseImageThumbnail> hbaseThumbMailStream = this
            .buildKStreamToGetHbaseImageThumbNail(builder, topicImageDataToPersist);
        KStream<String, HbaseExifData> hbaseExifUpdate = exifOfImageStream.join(
            hbaseThumbMailStream,
            (v_HbaseExifData, v_HbaseImageThumbnail) -> {
                return this.updateHbaseExifData(v_HbaseExifData, v_HbaseImageThumbnail);
            },
            JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
            Joined.with(Serdes.String(), new HbaseExifDataSerDe(), new HbaseImageThumbnailSerDe()));

        KStream<String, HbaseData> finalStream = hbaseExifUpdate.flatMapValues((key, value) -> {
            final Collection<HbaseData> asList = Arrays.asList(value, this.buildHbaseExifDataOfImages(value));
            return asList;
        });
        this.publishImageDataInRecordTopic(finalStream, topicExifImageDataToPersist);

        return builder.build();

    }

    private HbaseExifData updateHbaseExifData(
        HbaseExifData v_HbaseExifData,
        HbaseImageThumbnail v_HbaseImageThumbnail
    ) {
        v_HbaseExifData.setImageId(v_HbaseImageThumbnail.getImageId());
        v_HbaseExifData.setThumbName(v_HbaseImageThumbnail.getThumbName());
        return v_HbaseExifData;
    }

    protected HbaseExifDataOfImages buildHbaseExifDataOfImages(HbaseExifData hbaseExifData) {
        HbaseExifDataOfImages.Builder builder = HbaseExifDataOfImages.builder();
        builder.withCreationDate(DateTimeHelper.toDateTimeAsString(hbaseExifData.getCreationDate()))
            .withDataId(hbaseExifData.getDataId())
            .withExifTag(hbaseExifData.getExifTag())
            .withExifValueAsByte(hbaseExifData.getExifValueAsByte())
            .withExifValueAsInt(hbaseExifData.getExifValueAsInt())
            .withExifValueAsShort(hbaseExifData.getExifValueAsShort())
            .withHeight(hbaseExifData.getHeight())
            .withImageId(hbaseExifData.getImageId())
            .withThumbName(hbaseExifData.getThumbName())
            .withWidth(hbaseExifData.getWidth())
            .withExifPath(hbaseExifData.getExifPath());
        return builder.build();
    }

    protected HbaseExifData buildHbaseExifData(ExchangedTiffData v_hbaseImageThumbnail) {
        HbaseExifData.Builder builder = HbaseExifData.builder();
        builder.withExifValueAsByte(v_hbaseImageThumbnail.getDataAsByte())
            .withDataId(v_hbaseImageThumbnail.getKey())
            .withExifValueAsInt(v_hbaseImageThumbnail.getDataAsInt())
            .withExifValueAsShort(v_hbaseImageThumbnail.getDataAsShort())
            .withImageId(v_hbaseImageThumbnail.getImageId())
            .withExifTag(v_hbaseImageThumbnail.getTag())
            .withExifPath(v_hbaseImageThumbnail.getPath());
        return builder.build();
    }

    private KStream<String, HbaseImageThumbnail> buildKStreamToGetHbaseImageThumbNail(
        StreamsBuilder builder,
        String topicImageDataToPersist
    ) {
        KStream<String, HbaseImageThumbnail> stream = builder
            .stream(topicImageDataToPersist, Consumed.with(Serdes.String(), new HbaseImageThumbnailSerDe()));
        return stream;
    }

    private KStream<String, ExchangedTiffData> buildKStreamToGetExifValue(
        StreamsBuilder streamsBuilder,
        String topicExif
    ) {
        KStream<String, ExchangedTiffData> stream = streamsBuilder
            .stream(topicExif, Consumed.with(Serdes.String(), new ExchangedDataSerDe()))
            .map((k_string, v_exchangedTiffData) -> {
                String newKey = KeysBuilder.topicExifKeyBuilder()
                    .build(k_string)
                    .getOriginalKey();
                // The key is of the form <img-key>-EXIF-<tiff-id>
                // We extract the "<img-key>" in order to be able to perform a joint later.
                return new KeyValue<String, ExchangedTiffData>(newKey, v_exchangedTiffData);
            });
        return stream;
    }

    protected void publishImageDataInRecordTopic(
        KStream<String, HbaseData> finalStream,
        String topicExifImageDataToPersist
    ) {
        finalStream.to(topicExifImageDataToPersist, Produced.with(Serdes.String(), new HbaseDataSerDe()));
    }

    private void publishImageDataInEventTopic(KStream<String, WfEvents> eventsStream, String topicEvent) {
        eventsStream.to(topicEvent, Produced.with(Serdes.String(), new WfEventsSerDe()));
    }

    private Optional<WfEvent> buildEvent(HbaseData v) {
        Optional<WfEvent> retValue = Optional.empty();
        if (v instanceof HbaseExifDataOfImages) {
            HbaseExifDataOfImages hbedoi = (HbaseExifDataOfImages) v;
            String hbedoiHashCode = HbaseExifDataOfImagesKeyBuilder.build(hbedoi);
            retValue = Optional.of(this.buildEvent(hbedoi.getImageId(), hbedoi.getDataId(), hbedoiHashCode));
        } else if (v instanceof HbaseExifData) {
            HbaseExifData hbd = (HbaseExifData) v;
            String hbdHashCode = HbaseExifDataKeyBuilder.build(hbd);
            retValue = Optional.of(this.buildEvent(hbd.getImageId(), hbd.getDataId(), hbdHashCode));
        }
        return retValue;
    }

    private WfEvent buildEvent(String imageKey, String parentDataId, String dataId) {
        return WfEvent.builder()
            .withDataId(dataId)
            .withParentDataId(parentDataId)
            .withImgId(imageKey)
            .withStep(
                WfEventStep.builder()
                    .withStep(WfEventStep.CREATED_FROM_STEP_PREPARE_FOR_PERSIST)
                    .build())
            .build();
    }
}
