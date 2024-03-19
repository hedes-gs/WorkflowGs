package com.gs.photo.workflow.pubexifdata;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.gs.photo.common.ks.transformers.AdvancedMapCollector;
import com.gs.photo.common.ks.transformers.MapCollector;
import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photos.serializers.CollectionOfExchangedDataSerDe;
import com.gs.photos.serializers.ExchangedDataSerDe;
import com.gs.photos.serializers.HbaseDataSerDe;
import com.gs.photos.serializers.HbaseExifDataSerDe;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
import com.gs.photos.serializers.WfEventsSerDe;
import com.workflow.model.CollectionOfExchangedTiffData;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseExifDataOfImages;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.builder.KeysBuilder.HbaseExifDataKeyBuilder;
import com.workflow.model.builder.KeysBuilder.HbaseExifDataOfImagesKeyBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventProduced;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {

    private static Logger       LOGGER                     = LoggerFactory.getLogger(ApplicationConfig.class);

    public static final int     JOIN_WINDOW_TIME           = 86400;
    public static final short   EXIF_CREATION_DATE_ID      = (short) 0x9003;
    public static final short   EXIF_SIZE_WIDTH            = (short) 0xA002;
    public static final short   EXIF_SIZE_HEIGHT           = (short) 0xA003;
    public static final short[] EXIF_CREATION_DATE_ID_PATH = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_WIDTH_HEIGHT_PATH     = { (short) 0, (short) 0x8769 };

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
        @Value("${kafka.producer.maxBlockMsConfig}") int maxBlockMsConfig,
        @Value("${kafka.consumer.consumerFetchMaxBytes}") int consumerFetchMaxBytes,
        @Value("${kafka.producer.maxRequestSize}") int producerRequestMaxBytes,
        @Value("${kafka.consumer.retryBackoffMsmaxRequestSize}") int retryBackoffMs,
        @Value("${kafka.consumer.reconnectBackoffMs}") int reconnectBackoffMs,
        @Value("${kafka.consumer.heartbeatIntervalMs}") int heartbeatIntervalMs,
        @Value("${kafka.consumer.sessionTimeoutMs}") int sessionTimeoutMs,
        @Value("${transaction.timeout}") String transactionTimeout

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
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalIms);
        config.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, metaDataAgeIms);
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 5 * 1024 * 1024);
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollTimeInMillisecondes);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerBatch);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMsConfig);
        config.put("sasl.kerberos.service.name", "kafka");
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, nbOfThreads);
        config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, producerRequestMaxBytes);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, consumerFetchMaxBytes);
        config.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        config.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG, reconnectBackoffMs);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 4 * 1024 * 1024);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        return config;
    }

    @Bean
    public Topology kafkaStreamsTopology3(
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
        final KStream<String, HbaseExifData> kstreamToGetHbaseExifData = kstreamToGetExifValue
            .mapValues((v) -> this.buildHbaseExifData(v));
        KStream<String, HbaseImageThumbnail> hbaseThumbMailStream = this
            .buildKStreamToGetOnlyVersion2HbaseImageThumbNail(builder, topicImageDataToPersist);

        StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore("MyTransformer", Duration.ofMillis(10000), Duration.ofMillis(10000), false),
            Serdes.String(),
            Serdes.Long());
        StoreBuilder<WindowStore<String, Long>> dedupStoreBuilderForExchangedTiffData = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                "store-for-collection-of-tiffdata",
                Duration.ofMillis(10000),
                Duration.ofMillis(10000),
                false),
            Serdes.String(),
            Serdes.Long());

        builder.addStateStore(dedupStoreBuilderForExchangedTiffData);
        builder.addStateStore(dedupStoreBuilder);

        Transformer<String, ExchangedTiffData, KeyValue<String, CollectionOfExchangedTiffData>> transfomer = AdvancedMapCollector
            .of(
                (k, v) -> new CollectionOfExchangedTiffData(k, 0, new ArrayList<>(v)),
                (k, values) -> ApplicationConfig.LOGGER
                    .debug("[EVENT][{}] processed found all elements {} ", k, values.size()),
                (k, values) -> this.isComplete(k, values));

        KStream<String, CollectionOfExchangedTiffData> aggregatedUsefulsExif = kstreamToGetExifValue
            .filter((key, exif) -> this.isARecordedField(key, exif))
            .transform(() -> transfomer, "store-for-collection-of-tiffdata");
        final StreamJoined<String, CollectionOfExchangedTiffData, HbaseExifData> joined = StreamJoined
            .with(Serdes.String(), new CollectionOfExchangedDataSerDe(), new HbaseExifDataSerDe());
        /*
         * Stream of HbaseExifData from topicExif : [ key-EXIF-tiffId, ExchnaedTiffData
         * ] -> [key,HbaseExifData]
         */
        // We create the HbaseExifData that will be stored in hbase
        // the HbaseExifData value is streamed with the current key.
        final KStream<String, HbaseExifData> streamOfHbaseExifData = aggregatedUsefulsExif.join(
            kstreamToGetHbaseExifData,
            (v1, v2) -> this.updateHbaseExifData(v1, v2),
            JoinWindows.of(Duration.ofDays(2)),
            joined);
        KStream<String, HbaseData> hbaseExifUpdate = streamOfHbaseExifData.join(
            hbaseThumbMailStream,
            (v_HbaseExifData, v_HbaseImageThumbnail) -> this
                .updateHbaseExifData(v_HbaseExifData, v_HbaseImageThumbnail),
            JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
            StreamJoined.with(Serdes.String(), new HbaseExifDataSerDe(), new HbaseImageThumbnailSerDe()));

        this.publishImageDataInRecordTopic(hbaseExifUpdate, topicExifImageDataToPersist);
        KStream<String, WfEvent> eventStream = streamOfHbaseExifData.mapValues(
            (k, v) -> this.buildEvent(v)
                .orElseThrow(() -> new IllegalArgumentException()));
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

    private HbaseExifData updateHbaseExifData(CollectionOfExchangedTiffData exifList, HbaseExifData value) {
        exifList.getDataCollection()
            .forEach((etd) -> {
                if ((etd.getTag() == ApplicationConfig.EXIF_CREATION_DATE_ID)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_CREATION_DATE_ID_PATH))) {
                    value.setCreationDate(this.toEpochMilli(etd));
                } else if ((etd.getTag() == ApplicationConfig.EXIF_SIZE_WIDTH)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH))) {
                    value.setWidth(etd.getDataAsInt()[0]);
                } else if ((etd.getTag() == ApplicationConfig.EXIF_SIZE_HEIGHT)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH))) {
                    value.setHeight(etd.getDataAsInt()[0]);
                }
            });
        return value;
    }

    private boolean isARecordedField(String key, ExchangedTiffData hbaseExifData) {

        return ((hbaseExifData.getTag() == ApplicationConfig.EXIF_CREATION_DATE_ID)
            && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_CREATION_DATE_ID_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_SIZE_WIDTH)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_SIZE_HEIGHT)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH)));

    }

    private boolean isComplete(String k, Collection<ExchangedTiffData> values) {
        if (values.size() > 3) {
            ApplicationConfig.LOGGER
                .error("[EVENT}[{}] Found {} values, mor than expected one, key is more completed", k, values.size());
        }
        return values.size() == 3;
    }

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

        StreamJoined<String, HbaseExifData, Long> join = StreamJoined
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
        KStream<String, HbaseData> hbaseExifUpdate = streamForUpdatingHbaseExifData.join(
            hbaseThumbMailStream,
            (v_HbaseExifData, v_HbaseImageThumbnail) -> this
                .updateHbaseExifData(v_HbaseExifData, v_HbaseImageThumbnail),
            JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
            StreamJoined.with(Serdes.String(), new HbaseExifDataSerDe(), new HbaseImageThumbnailSerDe()));

        // KStream<String, HbaseData> finalStreamOfHbaseExifData = hbaseExifUpdate
        // .flatMapValues((key, value) -> Arrays.asList(value,
        // this.buildHbaseExifDataOfImages(value)));

        KStream<String, WfEvent> eventStream = hbaseExifUpdate.mapValues(
            (k, v) -> this.buildEvent(v)
                .orElseThrow(() -> new IllegalArgumentException()));

        this.publishImageDataInRecordTopic(hbaseExifUpdate, topicExifImageDataToPersist);

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
                    .info("[EVENT][{}] processed - {} events produced ", k, values.size()),
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

    private HbaseExifData updateHbaseExifData(
        HbaseExifData v_HbaseExifData,
        HbaseImageThumbnail v_HbaseImageThumbnail
    ) {
        if (v_HbaseImageThumbnail.getThumbnail()
            .containsKey(2)) {
            v_HbaseExifData.setImageId(v_HbaseImageThumbnail.getImageId());
            v_HbaseExifData.setThumbName(v_HbaseImageThumbnail.getThumbName());
            v_HbaseExifData.setThumbnail(
                v_HbaseImageThumbnail.getThumbnail()
                    .get(2));
            return v_HbaseExifData;
        }
        ApplicationConfig.LOGGER.error(
            "Unable to process EXIF data :  {} does not contain thumbnail 2  ",
            v_HbaseImageThumbnail.getImageId());
        throw new IllegalArgumentException(
            "Unable to process EXIF data :  does not contain thumbnail 2  " + v_HbaseImageThumbnail.getImageId());
    }

    protected HbaseExifData buildHbaseExifData(ExchangedTiffData v_hbaseImageThumbnail) {
        HbaseExifData.Builder builder = HbaseExifData.builder();
        builder.withExifValueAsByte(v_hbaseImageThumbnail.getDataAsByte())
            .withExifValueAsInt(v_hbaseImageThumbnail.getDataAsInt())
            .withExifValueAsShort(v_hbaseImageThumbnail.getDataAsShort())
            .withImageId(v_hbaseImageThumbnail.getImageId())
            .withExifTag(v_hbaseImageThumbnail.getTag())
            .withExifPath(v_hbaseImageThumbnail.getPath());
        HbaseExifData hbd = builder.build();

        String hbdHashCode = HbaseExifDataKeyBuilder.build(hbd);
        hbd.setDataId(hbdHashCode);

        return hbd;
    }

    private KStream<String, HbaseImageThumbnail> buildKStreamToGetOnlyVersion2HbaseImageThumbNail(
        StreamsBuilder builder,
        String topicImageDataToPersist
    ) {
        KStream<String, HbaseImageThumbnail> stream = builder
            .stream(topicImageDataToPersist, Consumed.with(Serdes.String(), new HbaseImageThumbnailSerDe()))
            .peek(
                (k, v) -> ApplicationConfig.LOGGER.info(
                    "[EVENT][{}] Received thumbnail with versions [ {} ] produced received",
                    k,
                    v.getThumbnail()
                        .keySet()))
            .filter(
                (k, v) -> v.getThumbnail()
                    .containsKey(2));
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
            retValue = Optional.of(
                this.buildEvent(
                    hbd.getImageId(),
                    KeysBuilder.buildKeyForExifData(hbd.getImageId(), hbd.getExifTag(), hbd.getExifPath()),
                    hbd.getDataId()));
        }
        return retValue;
    }

    private WfEvent buildEvent(String imageKey, String parentDataId, String dataId) {
        return WfEventProduced.builder()
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
