package com.gs.photo.workflow;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.gs.photo.ks.transformers.MapCollector;
import com.gs.photos.serializers.ExchangedDataSerDe;
import com.gs.photos.serializers.HbaseDataSerDe;
import com.gs.photos.serializers.HbaseExifDataSerDe;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
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

    private static Logger     LOGGER                = LoggerFactory.getLogger(ApplicationConfig.class);

    public static final int   JOIN_WINDOW_TIME      = 86400;
    public static final short EXIF_CREATION_DATE_ID = (short) 0x9003;
    public static final short EXIF_SIZE_WIDTH       = (short) 0xA002;
    public static final short EXIF_SIZE_HEIGHT      = (short) 0xA003;

    @Bean
    public Properties kafkaStreamProperties(
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafkaStreamDir.dir}") String kafkaStreamDir,
        @Value("${application.kafkastreams.id}") String applicationId,
        @Value("${kafka.pollTimeInMillisecondes}") int pollTimeInMillisecondes,
        @Value("${kafka.consumer.batchRecords}") int consumerBatch,
        @Value("${kafka.stream.commit.interval.ms}") int commitIntervalIms,
        @Value("${kafka.stream.metadata.age.ms}") int metaDataAgeIms,
        @Value("${kafka.stream.nb.of.threads}") int nbOfThreads,
        @Value("${kafka.producer.maxBlockMsConfig}") int maxBlockMsConfig

    ) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
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
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalIms);
        config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, metaDataAgeIms);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 5 * 1024 * 1024);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollTimeInMillisecondes);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerBatch);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, nbOfThreads);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 30);
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1024 * 1024);
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMsConfig);
        config.put("sasl.kerberos.service.name", "kafka");
        return config;
    }

    @Bean
    public Topology kafkaStreamsTopology2(
        @Value("${topic.topicExifSizeOfImageStream}") String topicExifSizeOfImageStream,
        @Value("${topic.topicImageDataToPersist}") String topicImageDataToPersist,
        @Value("${topic.topicExifImageDataToPersist}") String topicExifImageDataToPersist,
        @Value("${topic.topicExif}") String topicExif,
        @Value("${topic.topicEvent}") String topicEvent,
        @Value("${kafkastreams.windowsOfEventsInMs}") int windowsOfEvents,
        @Value("${events.collectorEventsBufferSize}") int collectorEventsBufferSize,
        @Value("${events.collectorEventsTimeWindow}") int collectorEventsTimeWindow
    ) {
        ApplicationConfig.LOGGER.info("Starting application with windowsOfEvents : {}", windowsOfEvents);
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, ExchangedTiffData> kstreamToGetExifValue = this
            .buildKStreamToGetExifValue(builder, topicExif);

        /*
         * Stream of HbaseExifData from topicExif : [ key-EXIF-tiffId, ExchnaedTiffData
         * ] -> [key,HbaseExifData]
         */
        // We create the HbaseExifData that will be stored in hbase
        // the HbaseExifData value is streamed with the current key.
        final KStream<String, HbaseExifData> streamOfHbaseExifData = kstreamToGetExifValue
            .mapValues((v) -> this.buildHbaseExifData(v));

        /*
         * Streams to retrieve {height, width, creation date} of image
         */
        KStream<String, Long> streamForWidthForHbaseExifData = kstreamToGetExifValue
            .filter((key, hbaseExifData) -> hbaseExifData.getTag() == ApplicationConfig.EXIF_SIZE_WIDTH)
            .mapValues((hbaseExifData) -> (long) hbaseExifData.getDataAsInt()[0]);
        KStream<String, Long> streamForHeightForHbaseExifData = kstreamToGetExifValue
            .filter((key, hbaseExifData) -> hbaseExifData.getTag() == ApplicationConfig.EXIF_SIZE_HEIGHT)
            .mapValues((hbaseExifData) -> (long) hbaseExifData.getDataAsInt()[0]);

        KStream<String, Long> streamForCreationDateForHbaseExifData = kstreamToGetExifValue
            .filter((key, hbaseExifData) -> hbaseExifData.getTag() == ApplicationConfig.EXIF_CREATION_DATE_ID)
            .mapValues((hbaseExifData) -> this.toEpochMilli(hbaseExifData));

        /*
         * Streams to retrieve {height, width, creation date} of image
         */

        Joined<String, HbaseExifData, Long> join = Joined
            .with(Serdes.String(), new HbaseExifDataSerDe(), Serdes.Long());
        ValueJoiner<HbaseExifData, Long, HbaseExifData> joinerWidth = (value1, width) -> {
            try {
                HbaseExifData hbaseExifData = (HbaseExifData) value1.clone();
                hbaseExifData.setWidth(width);
                return hbaseExifData;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseExifData, Long, HbaseExifData> joinerHeight = (value1, height) -> {
            try {
                HbaseExifData hbaseExifData = (HbaseExifData) value1.clone();
                hbaseExifData.setHeight(height);
                return hbaseExifData;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseExifData, Long, HbaseExifData> joinerCreationDate = (value1, creationDate) -> {
            try {
                HbaseExifData hbaseExifData = (HbaseExifData) value1.clone();
                hbaseExifData.setCreationDate(creationDate);
                return hbaseExifData;
            } catch (CloneNotSupportedException e) {
                // TODO Auto-generated catch block
                throw new RuntimeException(e);
            }
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
            .buildKStreamToGetOnlyVersion2HbaseImageThumbNail(builder, topicImageDataToPersist);
        KStream<String, HbaseExifData> hbaseExifUpdate = streamForUpdatingHbaseExifData.join(
            hbaseThumbMailStream,
            (v_HbaseExifData, v_HbaseImageThumbnail) -> this
                .updateHbaseExifData(v_HbaseExifData, v_HbaseImageThumbnail),
            JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
            Joined.with(Serdes.String(), new HbaseExifDataSerDe(), new HbaseImageThumbnailSerDe()));

        KStream<String, HbaseData> finalStreamOfHbaseExifData = hbaseExifUpdate
            .flatMapValues((key, value) -> Arrays.asList(value, this.buildHbaseExifDataOfImages(value)));

        KStream<String, WfEvent> eventStream = finalStreamOfHbaseExifData.mapValues(
            (k, v) -> this.buildEvent(v)
                .orElseThrow(() -> new IllegalArgumentException()));

        this.publishImageDataInRecordTopic(finalStreamOfHbaseExifData, topicExifImageDataToPersist);

        StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore("MyTransformer", Duration.ofMillis(10000), Duration.ofMillis(10000), false),
            Serdes.String(),
            Serdes.Long());
        builder.addStateStore(dedupStoreBuilder);
        KStream<String, WfEvents> wfEventsStream = eventStream.transform(
            () -> MapCollector.of(
                (k, wfEventList) -> WfEvents.builder()
                    .withDataId(k)
                    .withProducer("PUBLISH_EXIF_DATA_OF_IMGS")
                    .withEvents(new ArrayList<WfEvent>(wfEventList))
                    .build(),
                (k, values) -> ApplicationConfig.LOGGER
                    .info("[EVENT][{}] processed and {} events produced ", k, values.size()),
                collectorEventsBufferSize,
                collectorEventsTimeWindow),
            "MyTransformer");
        this.publishImageDataInEventTopic(wfEventsStream, topicEvent);
        return builder.build();
    }

    protected long toEpochMilli(ExchangedTiffData hbaseExifData) {
        try {
            return DateTimeHelper.toEpochMillis(new String(hbaseExifData.getDataAsByte(), "UTF-8").trim());
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private String buildKeyForHbaseExifData(HbaseData v) {
        if (v instanceof HbaseExifDataOfImages) {
            HbaseExifDataOfImages hbedoi = (HbaseExifDataOfImages) v;
            return hbedoi.getImageId();
        } else if (v instanceof HbaseExifData) {
            HbaseExifData hbed = (HbaseExifData) v;
            return hbed.getImageId();
        }
        throw new IllegalArgumentException("Unexpected class " + v);
    }

    private HbaseExifData updateHbaseExifData(
        HbaseExifData v_HbaseExifData,
        HbaseImageThumbnail v_HbaseImageThumbnail
    ) {
        v_HbaseExifData.setImageId(v_HbaseImageThumbnail.getImageId());
        v_HbaseExifData.setThumbName(v_HbaseImageThumbnail.getThumbName());
        v_HbaseExifData.setThumbnail(v_HbaseImageThumbnail.getThumbnail());
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

    private KStream<String, HbaseImageThumbnail> buildKStreamToGetOnlyVersion2HbaseImageThumbNail(
        StreamsBuilder builder,
        String topicImageDataToPersist
    ) {
        KeysBuilder.topicImageDataToPersistKeyBuilder();
        KStream<String, HbaseImageThumbnail> stream = builder
            .stream(topicImageDataToPersist, Consumed.with(Serdes.String(), new HbaseImageThumbnailSerDe()))
            .filter((k, v) -> v.getVersion() == 2);
        return stream;
    }

    private KStream<String, ExchangedTiffData> buildKStreamToGetExifValue(
        StreamsBuilder streamsBuilder,
        String topicExif
    ) {
        KStream<String, ExchangedTiffData> stream = streamsBuilder
            .stream(topicExif, Consumed.with(Serdes.String(), new ExchangedDataSerDe()));
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
