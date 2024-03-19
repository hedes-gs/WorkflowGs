package com.gs.photo.workflow.pubthumbimages;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableObject;
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
import com.gs.photo.common.workflow.exif.FieldType;
import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;
import com.gs.photos.serializers.CollectionOfExchangedDataSerDe;
import com.gs.photos.serializers.ExchangedDataSerDe;
import com.gs.photos.serializers.FileToProcessSerDe;
import com.gs.photos.serializers.FinalImageSerDe;
import com.gs.photos.serializers.HbaseImageThumbnailKeySerDe;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
import com.gs.photos.serializers.IntArrayDeserializer;
import com.gs.photos.serializers.IntArraySerializer;
import com.gs.photos.serializers.WfEventsSerDe;
import com.workflow.model.CollectionOfExchangedTiffData;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImageThumbnailKey;
import com.workflow.model.SizeAndJpegContent;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventProduced;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;
import com.workflow.model.storm.FinalImage;

@Configuration
@ComponentScan(basePackages = "com.gs.photo")
public class ApplicationConfig extends AbstractApplicationConfig {

    private static final byte[] EMPTY_ARRAY_BYTE           = new byte[] {};
    private static final String NOT_SET                    = "<not set>";
    private static final Logger LOGGER                     = LoggerFactory.getLogger(ApplicationConfig.class);
    public static final int     JOIN_WINDOW_TIME           = 86400;
    public static final short   EXIF_CREATION_DATE_ID      = (short) 0x9003;
    public static final short   EXIF_SIZE_WIDTH            = (short) 0xA002;
    public static final short   EXIF_SIZE_HEIGHT           = (short) 0xA003;
    public static final short   EXIF_ORIENTATION           = (short) 0x0112;

    public static final short   SONY_EXIF_LENS             = (short) 0xB027;
    public static final short   EXIF_LENS                  = (short) 0xA434;
    public static final short   EXIF_FOCAL_LENS            = (short) 0x920A;
    public static final short   EXIF_SHIFT_EXPO            = (short) 0x9204;
    public static final short   EXIF_SPEED_ISO             = (short) 0x8827;
    public static final short   EXIF_APERTURE              = (short) 0x829D;
    public static final short   EXIF_SPEED                 = (short) 0x829A;
    public static final short   EXIF_COPYRIGHT             = (short) 0x8298;
    public static final short   EXIF_ARTIST                = (short) 0x13B;
    public static final short   EXIF_CAMERA_MODEL          = (short) 0x110;

    public static final short[] EXIF_CREATION_DATE_ID_PATH = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_WIDTH_HEIGHT_PATH     = { (short) 0, (short) 0x8769 };

    public static final short[] EXIF_LENS_PATH             = { (short) 0, (short) 0x8769 };
    public static final short[] SONY_EXIF_LENS_PATH        = { (short) 0, (short) 0x8769, (short) 0x927c };
    public static final short[] EXIF_FOCAL_LENS_PATH       = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_SHIFT_EXPO_PATH       = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_SPEED_ISO_PATH        = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_APERTURE_PATH         = { (short) 0, (short) 0x8769 };
    public static final short[] EXIF_SPEED_PATH            = { (short) 0, (short) 0x8769 };

    public static final short[] EXIF_COPYRIGHT_PATH        = { (short) 0 };
    public static final short[] EXIF_ARTIST_PATH           = { (short) 0 };
    public static final short[] EXIF_CAMERA_MODEL_PATH     = { (short) 0 };
    public static final short[] EXIF_ORIENTATION_PATH      = { (short) 0 };

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
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout);
        // config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, consumerFetchMaxBytes);
        // config.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs);
        // config.put(CommonClientConfigs.RECONNECT_BACKOFF_MS_CONFIG,
        // reconnectBackoffMs);
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 4 * 1024 * 1024);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 1000);

        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, heartbeatIntervalMs);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);

        return config;
    }

    @Bean
    public Topology kafkaStreamsTopology(
        @Value("${topic.topicEvent}") String topicEvent,
        @Value("${topic.topicExifImageDataToPersist}") String topicExifImageDataToPersist,
        @Value("${topic.topicTransformedThumb}") String topicTransformedThumb,
        @Value("${topic.topicImageDataToPersist}") String topicImageDataToPersist,
        @Value("${topic.topicDupFilteredFile}") String topicDupFilteredFile,
        @Value("${topic.topicCountOfImagesPerDate}") String topicCountOfImagesPerDate,
        @Value("${topic.topicExif}") String topicExif,
        @Value("${events.collectorEventsBufferSize}") int collectorEventsBufferSize,
        @Value("${events.collectorEventsTimeWindow}") int collectorEventsTimeWindow,
        @Value("${kafkastreams.windowsOfEventsInMs}") int windowsOfEvents,
        IExifService exifService
    ) {
        ApplicationConfig.LOGGER.info("Starting application with windowsOfEvents : {}", windowsOfEvents);
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, FileToProcess> pathOfImageKStream = this
            .buildKTableToStoreCreatedImages(builder, topicDupFilteredFile);
        KStream<String, FinalImage> thumbImages = this.buildKStreamToGetThumbImages(builder, topicTransformedThumb);
        KStream<String, ExchangedTiffData> exifOfImageStream = this.buildKStreamToGetExifValue(builder, topicExif);
        StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                "store-for-collection-of-tiffdata",
                Duration.ofMillis(10000),
                Duration.ofMillis(10000),
                false),
            Serdes.String(),
            Serdes.Long());

        builder.addStateStore(dedupStoreBuilder);

        Transformer<String, ExchangedTiffData, KeyValue<String, CollectionOfExchangedTiffData>> transfomer = AdvancedMapCollector
            .of(
                (k, v) -> new CollectionOfExchangedTiffData(k, 0, new ArrayList<>(v)),
                (k, values) -> ApplicationConfig.LOGGER
                    .info("[EVENT][{}] processed found all elements {} ", k, values.size()),
                (k, values) -> this.isComplete(k, values));

        KStream<String, CollectionOfExchangedTiffData> aggregatedUsefulsExif = exifOfImageStream
            .filter((key, exif) -> this.isARecordedField(key, exif))
            .transform(() -> transfomer, "store-for-collection-of-tiffdata");

        final StreamJoined<String, FileToProcess, CollectionOfExchangedTiffData> joined = StreamJoined
            .with(Serdes.String(), new FileToProcessSerDe(), new CollectionOfExchangedDataSerDe());

        KStream<String, HbaseImageThumbnail> HbaseImageThumbnailBuiltStream = pathOfImageKStream
            .join(
                aggregatedUsefulsExif,
                (v1, v2) -> this.buildHBaseImageThumbnail(exifService, v1, v2),
                JoinWindows.of(Duration.ofDays(2)),
                joined)
            .join(thumbImages, (v_hbaseImageThumbnail, v_finalImage) -> {
                final HbaseImageThumbnail buildHbaseImageThumbnail = this
                    .buildHbaseImageThumbnail(v_finalImage, v_hbaseImageThumbnail);
                return buildHbaseImageThumbnail;
            },
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                StreamJoined.with(Serdes.String(), new HbaseImageThumbnailSerDe(), new FinalImageSerDe()));

        this.publishImageDataInRecordTopic(HbaseImageThumbnailBuiltStream, topicImageDataToPersist);

        KStream<String, WfEvent> eventStream = HbaseImageThumbnailBuiltStream.mapValues((k, v) -> this.buildEvent(v));

        StoreBuilder<WindowStore<String, Long>> eventStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                "event-store-builder",
                Duration.ofMillis(10000),
                Duration.ofMillis(10000),
                false),
            Serdes.String(),
            Serdes.Long());

        builder.addStateStore(eventStoreBuilder);

        KStream<String, WfEvents> wfEventsStream = eventStream.transform(
            () -> MapCollector.of(
                (k, wfEventList) -> WfEvents.builder()
                    .withDataId(k)
                    .withProducer("PUBLISH_THB_IMGS")
                    .withEvents(new ArrayList<WfEvent>(wfEventList))
                    .build(),
                (k, values) -> ApplicationConfig.LOGGER
                    .info("[EVENT][{}] processed and {} events produced ", k, values.size()),
                collectorEventsBufferSize,
                collectorEventsTimeWindow),
            "event-store-builder");
        this.publishEventInEventTopic(wfEventsStream, topicEvent);

        /*
         * KStream<String, ExchangedTiffData> streamForCreationDateForHbaseExifData =
         * exifOfImageStream .filter( (key, exif) -> (exif.getTag() ==
         * ApplicationConfig.EXIF_CREATION_DATE_ID) &&
         * (Objects.deepEquals(exif.getPath(),
         * ApplicationConfig.EXIF_CREATION_DATE_ID_PATH))) .peek((key, hbi) -> {
         * ApplicationConfig.LOGGER.info(" [EVENT][{}] found the creation date {}",
         * key); });
         *
         *
         * KStream<String, HbaseImageThumbnail> jointureToFindTheCreationDate =
         * streamForCreationDateForHbaseExifData.join( pathOfImageKStream,
         * (v_exchangedTiffData, v_imagePath) ->
         * this.buildHBaseImageThumbnail(v_exchangedTiffData, v_imagePath),
         * JoinWindows.of(Duration.ofDays(2)), Joined.with(Serdes.String(), new
         * ExchangedDataSerDe(), new FileToProcessSerDe()));
         *
         * KStream<String, HbaseImageThumbnailKey> imageCountsStream =
         * jointureToFindTheCreationDate .flatMap((key, value) ->
         * this.splitCreationDateToYearMonthDayAndHour(value)) .peek( (key, hbi) -> {
         * ApplicationConfig.LOGGER
         * .info(" [EVENT][{}] was built and published to time intervall {}",
         * hbi.getImageId(), key); });
         *
         * imageCountsStream .to(topicCountOfImagesPerDate,
         * Produced.with(Serdes.String(), new HbaseImageThumbnailKeySerDe()));
         */
        return builder.build();
    }

    private boolean isARecordedField(String key, ExchangedTiffData hbaseExifData) {

        return ((hbaseExifData.getTag() == ApplicationConfig.EXIF_CREATION_DATE_ID)
            && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_CREATION_DATE_ID_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_SIZE_WIDTH)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_SIZE_HEIGHT)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_ORIENTATION)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_ORIENTATION_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_LENS)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_LENS_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_FOCAL_LENS)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_FOCAL_LENS_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_SHIFT_EXPO)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_SHIFT_EXPO_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_SPEED_ISO)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_SPEED_ISO_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_APERTURE)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_APERTURE_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_SPEED)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_SPEED_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_COPYRIGHT)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_COPYRIGHT_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_ARTIST)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_ARTIST_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.EXIF_CAMERA_MODEL)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_CAMERA_MODEL_PATH)))

            || ((hbaseExifData.getTag() == ApplicationConfig.SONY_EXIF_LENS)
                && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.SONY_EXIF_LENS_PATH)));

    }

    private boolean isComplete(String k, Collection<ExchangedTiffData> values) {
        if (values.size() == 14) {
            ApplicationConfig.LOGGER.info("[EVENT}[{}] Found {} values, key is completed", k, values.size());
        } else if (values.size() > 14) {
            ApplicationConfig.LOGGER
                .error("[EVENT}[{}] Found {} values, mor than expected one, key is more completed", k, values.size());
        }
        return values.size() >= 14;
    }

    @Bean
    public Topology kafkaStreamsTopologyV1(
        @Value("${topic.topicEvent}") String topicEvent,
        @Value("${topic.topicExifImageDataToPersist}") String topicExifImageDataToPersist,
        @Value("${topic.topicTransformedThumb}") String topicTransformedThumb,
        @Value("${topic.topicImageDataToPersist}") String topicImageDataToPersist,
        @Value("${topic.topicDupFilteredFile}") String topicDupFilteredFile,
        @Value("${topic.topicCountOfImagesPerDate}") String topicCountOfImagesPerDate,
        @Value("${topic.topicExif}") String topicExif,
        @Value("${events.collectorEventsBufferSize}") int collectorEventsBufferSize,
        @Value("${events.collectorEventsTimeWindow}") int collectorEventsTimeWindow,
        @Value("${kafkastreams.windowsOfEventsInMs}") int windowsOfEvents,
        IExifService exifService
    ) {
        ApplicationConfig.LOGGER.info("Starting application with windowsOfEvents : {}", windowsOfEvents);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, FileToProcess> pathOfImageKStream = this
            .buildKTableToStoreCreatedImages(builder, topicDupFilteredFile);
        KStream<String, FinalImage> thumbImages = this.buildKStreamToGetThumbImages(builder, topicTransformedThumb);
        KStream<String, ExchangedTiffData> exifOfImageStream = this.buildKStreamToGetExifValue(builder, topicExif);

        /*
         * Stream to get the creation date on topic topicCountOfImagesPerDate
         */
        KStream<String, ExchangedTiffData> streamForCreationDateForHbaseExifData = exifOfImageStream
            .filter(
                (key, exif) -> (exif.getTag() == ApplicationConfig.EXIF_CREATION_DATE_ID)
                    && (Objects.deepEquals(exif.getPath(), ApplicationConfig.EXIF_CREATION_DATE_ID_PATH)))
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the creation date {}", key); });
        KStream<String, Long> streamForWidthForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_SIZE_WIDTH)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH)))
            .mapValues((hbaseExifData) -> (long) hbaseExifData.getDataAsInt()[0])
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the width {}", key); });

        KStream<String, Long> streamForHeightForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_SIZE_HEIGHT)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH)))
            .mapValues((hbaseExifData) -> (long) hbaseExifData.getDataAsInt()[0])
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the height {}", key); });

        KStream<String, Long> streamForOrientationForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_ORIENTATION)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_ORIENTATION_PATH)))
            .mapValues((hbaseExifData) -> (long) hbaseExifData.getDataAsShort()[0])
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the orientation {}", key); });

        KStream<String, byte[]> streamForLensForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_LENS)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_LENS_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsByte())
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the lens {}", key); });

        KStream<String, int[]> streamForSonyLensForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.SONY_EXIF_LENS)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.SONY_EXIF_LENS_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsInt())
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the lens {}", key); });

        KStream<String, int[]> streamForFocalLensForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_FOCAL_LENS)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_FOCAL_LENS_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsInt())
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the focal lens {}", key); });

        KStream<String, int[]> streamForShifExpoForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_SHIFT_EXPO)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_SHIFT_EXPO_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsInt())
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the shift expor {}", key); });

        KStream<String, Short> streamForSpeedIsoForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_SPEED_ISO)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_SPEED_ISO_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsShort()[0])
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the iso speef {}", key); });

        KStream<String, int[]> streamForApertureForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_APERTURE)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_APERTURE_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsInt())
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the aperture {}", key); });

        KStream<String, int[]> streamForSpeedForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_SPEED)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_SPEED_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsInt())
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the speed {}", key); });

        KStream<String, byte[]> streamForCopyrightForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_COPYRIGHT)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_COPYRIGHT_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsByte())
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the cpy {}", key); });

        KStream<String, byte[]> streamForArtistForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_ARTIST)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_ARTIST_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsByte())
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the artist {}", key); });

        KStream<String, byte[]> streamForCameraModelForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_CAMERA_MODEL)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_CAMERA_MODEL_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsByte())
            .peek((key, hbi) -> { ApplicationConfig.LOGGER.info(" [EVENT][{}] found the camera model {}", key); });

        /*
         * join to build the HbaseImageThumbnail which will be stored in hbase. We just
         * set the creation date.
         */
        final ValueJoiner<ExchangedTiffData, FileToProcess, HbaseImageThumbnail> joiner = (
            v_exchangedTiffData,
            v_imagePath) -> { return this.buildHBaseImageThumbnail(v_exchangedTiffData, v_imagePath); };

        final StreamJoined<String, ExchangedTiffData, FileToProcess> joined = StreamJoined
            .with(Serdes.String(), new ExchangedDataSerDe(), new FileToProcessSerDe());

        KStream<String, HbaseImageThumbnail> jointureToFindTheCreationDate = streamForCreationDateForHbaseExifData
            .join(pathOfImageKStream, joiner, JoinWindows.of(Duration.ofDays(2)), joined)
            .peek((key, hbi) -> {
                ApplicationConfig.LOGGER.info(
                    " Object with key {} was built and set the creation date {}, the path {}, the image name {} ",
                    key,
                    hbi.getCreationDate(),
                    hbi.getPath(),
                    hbi.getImageName());
            });

        StreamJoined<String, HbaseImageThumbnail, Long> join = StreamJoined
            .with(Serdes.String(), new HbaseImageThumbnailSerDe(), Serdes.Long());
        StreamJoined<String, HbaseImageThumbnail, byte[]> joinArrayOfBytes = StreamJoined
            .with(Serdes.String(), new HbaseImageThumbnailSerDe(), Serdes.ByteArray());
        StreamJoined<String, HbaseImageThumbnail, int[]> joinArrayOfInt = StreamJoined.with(
            Serdes.String(),
            new HbaseImageThumbnailSerDe(),
            Serdes.serdeFrom(new IntArraySerializer(), new IntArrayDeserializer()));
        StreamJoined<String, HbaseImageThumbnail, Short> joinShort = StreamJoined
            .with(Serdes.String(), new HbaseImageThumbnailSerDe(), Serdes.Short());

        ValueJoiner<HbaseImageThumbnail, Long, HbaseImageThumbnail> joinerWidth = (hbi, width) -> {
            hbi.setOriginalWidth(width);
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, Long, HbaseImageThumbnail> joinerHeight = (hbi, height) -> {
            hbi.setOriginalHeight(height);
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, Long, HbaseImageThumbnail> joinerOrientation = (hbi, orientation) -> {
            hbi.setOrientation(orientation);
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, byte[], HbaseImageThumbnail> joinerLens = (hbi, lens) -> {
            hbi.setLens(lens);
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, int[], HbaseImageThumbnail> joinerSonyLens = (hbi, lens) -> {
            if (hbi.getLens() == null) {
                hbi.setLens(exifService.getSonyLens(lens));
            }
            return hbi;
        };

        ValueJoiner<HbaseImageThumbnail, int[], HbaseImageThumbnail> joinerFocalLens = (hbi, focalLens) -> {
            hbi.setFocalLens(focalLens);
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, int[], HbaseImageThumbnail> joinerShiftExpo = (hbi, shift) -> {
            hbi.setShiftExpo(shift);
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, Short, HbaseImageThumbnail> joinerSpeedIso = (hbi, speedIso) -> {
            hbi.setIsoSpeed(speedIso);
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, int[], HbaseImageThumbnail> joinerAperture = (hbi, aperture) -> {
            hbi.setAperture(aperture);
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, int[], HbaseImageThumbnail> joinerSpeed = (hbi, speed) -> {
            hbi.setSpeed(speed);
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, byte[], HbaseImageThumbnail> joinerCopyright = (hbi, copyright) -> {
            hbi.setCopyright(exifService.toString(FieldType.ASCII, copyright));
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, byte[], HbaseImageThumbnail> joinerArtist = (hbi, artist) -> {
            hbi.setArtist(exifService.toString(FieldType.ASCII, artist));
            return hbi;
        };
        ValueJoiner<HbaseImageThumbnail, byte[], HbaseImageThumbnail> joinerCameraModel = (hbi, model) -> {
            hbi.setCamera(exifService.toString(FieldType.ASCII, model));
            return hbi;
        };

        // KStream<String, HbaseExifData> streamOfHbaseExifData
        // KStream<String, Integer> streamForWidthForHbaseExifData
        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForWidthForHbaseExifData,
                joinerWidth,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                join)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the width {}", key, hbi.getOriginalWidth());
                });

        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForHeightForHbaseExifData,
                joinerHeight,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                join)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the height {}", key, hbi.getOriginalHeight());
                });

        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForOrientationForHbaseExifData,
                joinerOrientation,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                join)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the orientation {}", key, hbi.getOrientation());
                });
        // Tech
        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForLensForHbaseExifData,
                joinerLens,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                joinArrayOfBytes)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the lens {}", key, hbi.getLens());
                });

        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForSonyLensForHbaseExifData,
                joinerSonyLens,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                joinArrayOfInt)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the lens {}", key, hbi.getLens());
                });

        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForFocalLensForHbaseExifData,
                joinerFocalLens,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                joinArrayOfInt)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the focal length {}", key, hbi.getFocalLens());
                });
        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForShifExpoForHbaseExifData,
                joinerShiftExpo,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                joinArrayOfInt)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the shif expo {}", key, hbi.getShiftExpo());
                });
        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForSpeedIsoForHbaseExifData,
                joinerSpeedIso,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                joinShort)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the iso {}", key, hbi.getIsoSpeed());
                });
        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForApertureForHbaseExifData,
                joinerAperture,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                joinArrayOfInt)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the aperure {}", key, hbi.getAperture());
                });
        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForSpeedForHbaseExifData,
                joinerSpeed,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                joinArrayOfInt)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the speed {}", key, hbi.getSpeed());
                });
        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForCopyrightForHbaseExifData,
                joinerCopyright,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                joinArrayOfBytes)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the copyright {}", key, hbi.getCopyright());
                });
        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForArtistForHbaseExifData,
                joinerArtist,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                joinArrayOfBytes)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the artist {}", key, hbi.getArtist());
                });
        jointureToFindTheCreationDate = jointureToFindTheCreationDate
            .join(
                streamForCameraModelForHbaseExifData,
                joinerCameraModel,
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                joinArrayOfBytes)
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER.info(" [EVENT][{}] was set the camera model {}", key, hbi.getCamera());
                });

        /*
         * imageCountsStream : stream to create the number of images per hour/minutes
         * etc. this is published on topic topicCountOfImagesPerDate
         */
        KStream<String, HbaseImageThumbnailKey> imageCountsStream = jointureToFindTheCreationDate
            .flatMap((key, value) -> this.splitCreationDateToYearMonthDayAndHour(value))
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER
                        .info(" [EVENT][{}] was built and published to time intervall {}", hbi.getImageId(), key);
                });

        imageCountsStream
            .to(topicCountOfImagesPerDate, Produced.with(Serdes.String(), new HbaseImageThumbnailKeySerDe()));

        /*
         * finalStream : we update the HbaseImageThumbnail which was created with the
         * creation date only.
         */
        final ValueJoiner<? super HbaseImageThumbnail, ? super FinalImage, ? extends HbaseImageThumbnail> joiner2 = (
            v_hbaseImageThumbnail,
            v_finalImage) -> {

            final HbaseImageThumbnail buildHbaseImageThumbnail = this
                .buildHbaseImageThumbnail(v_finalImage, v_hbaseImageThumbnail);
            return buildHbaseImageThumbnail;
        };
        KStream<String, HbaseImageThumbnail> finalStream = jointureToFindTheCreationDate.join(
            thumbImages,
            joiner2,
            JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
            StreamJoined.with(Serdes.String(), new HbaseImageThumbnailSerDe(), new FinalImageSerDe()));
        finalStream.peek(
            (key, hbi) -> ApplicationConfig.LOGGER
                .info(" [EVENT][{}] was created with the thumb {} ", key, hbi.getThumbnail()));
        this.publishImageDataInRecordTopic(finalStream, topicImageDataToPersist);

        KStream<String, WfEvent> eventStream = finalStream.mapValues((k, v) -> this.buildEvent(v));

        StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
            Stores.persistentWindowStore("MyTransformer", Duration.ofMillis(10000), Duration.ofMillis(10000), false),
            Serdes.String(),
            Serdes.Long());

        builder.addStateStore(dedupStoreBuilder);

        KStream<String, WfEvents> wfEventsStream = eventStream.transform(
            () -> MapCollector.of(
                (k, wfEventList) -> WfEvents.builder()
                    .withDataId(k)
                    .withProducer("PUBLISH_THB_IMGS")
                    .withEvents(new ArrayList<WfEvent>(wfEventList))
                    .build(),
                (k, values) -> ApplicationConfig.LOGGER
                    .info("[EVENT][{}] processed and {} events produced ", k, values.size()),
                collectorEventsBufferSize,
                collectorEventsTimeWindow),
            "MyTransformer");
        this.publishEventInEventTopic(wfEventsStream, topicEvent);
        return builder.build();
    }

    private WfEvent buildEvent(HbaseImageThumbnail v) {
        return WfEventProduced.builder()
            .withImgId(v.getImageId())
            .withParentDataId(v.getDataId())
            .withDataId(v.getDataId())
            .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_PREPARE_FOR_PERSIST)
            .build();
    }

    protected HbaseImageThumbnail buildHbaseImageThumbnail(
        FinalImage v_FinalImage,
        HbaseImageThumbnail v_hbaseImageThumbnail
    ) {
        try {
            HashMap<Integer, SizeAndJpegContent> thb = new HashMap<>();
            thb.put(
                (int) v_FinalImage.getVersion(),
                SizeAndJpegContent.builder()
                    .withJpegContent(v_FinalImage.getCompressedImage())
                    .withHeight(v_FinalImage.getHeight())
                    .withWidth(v_FinalImage.getWidth())
                    .build());
            HbaseImageThumbnail retValue = (HbaseImageThumbnail) v_hbaseImageThumbnail.clone();
            retValue = v_hbaseImageThumbnail;
            retValue.setDataId(v_FinalImage.getDataId());
            retValue.setThumbnail(thb);
            // retValue.setHeight(v_FinalImage.getHeight());
            // retValue.setWidth(v_FinalImage.getWidth());
            return retValue;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    private HbaseImageThumbnail buildHBaseImageThumbnail(ExchangedTiffData key, FileToProcess value) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        try {
            builder.withPath(value.getUrl())
                .withImageName(value.getName())
                .withThumbnail(new HashMap<>())
                .withThumbName(value.getName())
                .withImageId(key.getImageId())
                .withDataId(ApplicationConfig.NOT_SET)
                .withAlbums(
                    new HashSet<>(Collections.singleton(
                        value.getImportEvent()
                            .getAlbum()
                            .trim())))
                .withImportName(
                    new HashSet<>(Collections.singleton(
                        value.getImportEvent()
                            .getImportName()
                            .trim())))
                .withKeyWords(this.getKeywordsInImportEvent(value))
                .withCreationDate(DateTimeHelper.toEpochMillis(new String(key.getDataAsByte(), "UTF-8").trim()));
        } catch (UnsupportedEncodingException e) {
            ApplicationConfig.LOGGER.error("unsupported charset ", e);
        }
        return builder.build();
    }

    private HbaseImageThumbnail buildHBaseImageThumbnail(
        IExifService exifService,
        FileToProcess value,
        CollectionOfExchangedTiffData exifList
    ) {

        MutableObject<byte[]> lens = new MutableObject<>();
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        builder.withPath(value.getUrl())
            .withImageName(value.getName())
            .withThumbnail(new HashMap<>())
            .withThumbName(value.getName())
            .withImageId(value.getImageId())
            .withDataId(ApplicationConfig.NOT_SET)
            .withAlbums(
                new HashSet<>(Collections.singleton(
                    value.getImportEvent()
                        .getAlbum()
                        .trim())))
            .withImportName(
                new HashSet<>(Collections.singleton(
                    value.getImportEvent()
                        .getImportName()
                        .trim())))
            .withKeyWords(this.getKeywordsInImportEvent(value));
        exifList.getDataCollection()
            .forEach((etd) -> {
                if ((etd.getTag() == ApplicationConfig.EXIF_CREATION_DATE_ID)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_CREATION_DATE_ID_PATH))) {
                    try {
                        builder.withCreationDate(
                            DateTimeHelper.toEpochMillis(new String(etd.getDataAsByte(), "UTF-8").trim()));
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                } else if ((etd.getTag() == ApplicationConfig.EXIF_SIZE_WIDTH)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH))) {
                    builder.withOriginalWidth(etd.getDataAsInt()[0]);
                } else if ((etd.getTag() == ApplicationConfig.EXIF_SIZE_HEIGHT)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH))) {
                    builder.withOriginalHeight(etd.getDataAsInt()[0]);
                } else if ((etd.getTag() == ApplicationConfig.EXIF_ORIENTATION)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_ORIENTATION_PATH))) {
                    builder.withOrientation(etd.getDataAsShort()[0]);
                } else if ((etd.getTag() == ApplicationConfig.EXIF_LENS)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_LENS_PATH))) {
                    if (lens.getValue() == null) {
                        lens.setValue(Arrays.copyOf(etd.getDataAsByte(), etd.getDataAsByte().length - 1));
                    }

                } else if ((etd.getTag() == ApplicationConfig.EXIF_FOCAL_LENS)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_FOCAL_LENS_PATH))) {
                    builder.withFocalLens(etd.getDataAsInt());
                } else if ((etd.getTag() == ApplicationConfig.EXIF_SHIFT_EXPO)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_SHIFT_EXPO_PATH))) {
                    builder.withShiftExpo(etd.getDataAsInt());
                } else if ((etd.getTag() == ApplicationConfig.EXIF_SPEED_ISO)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_SPEED_ISO_PATH))) {
                    builder.withIsoSpeed(etd.getDataAsShort()[0]);
                } else if ((etd.getTag() == ApplicationConfig.EXIF_APERTURE)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_APERTURE_PATH))) {
                    builder.withAperture(etd.getDataAsInt());
                } else if ((etd.getTag() == ApplicationConfig.EXIF_SPEED)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_SPEED_PATH))) {
                    builder.withSpeed(etd.getDataAsInt());
                } else if ((etd.getTag() == ApplicationConfig.EXIF_COPYRIGHT)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_COPYRIGHT_PATH))) {
                    builder.withCopyright(exifService.toString(FieldType.ASCII, etd.getDataAsByte()));
                    ApplicationConfig.LOGGER.info(
                        "[EVENT][{}]Found copyright {}",
                        value.getImageId(),
                        exifService.toString(FieldType.ASCII, etd.getDataAsByte()));
                } else if ((etd.getTag() == ApplicationConfig.EXIF_ARTIST)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_ARTIST_PATH))) {
                    builder.withArtist(exifService.toString(FieldType.ASCII, etd.getDataAsByte()));
                    ApplicationConfig.LOGGER.info(
                        "[EVENT][{}]Found artist : {}",
                        value.getImageId(),
                        exifService.toString(FieldType.ASCII, etd.getDataAsByte()));
                } else if ((etd.getTag() == ApplicationConfig.EXIF_CAMERA_MODEL)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.EXIF_CAMERA_MODEL_PATH))) {
                    builder.withCamera(exifService.toString(FieldType.ASCII, etd.getDataAsByte()));
                } else if ((etd.getTag() == ApplicationConfig.SONY_EXIF_LENS)
                    && (Objects.deepEquals(etd.getPath(), ApplicationConfig.SONY_EXIF_LENS_PATH))) {
                    if (lens != null) {
                        if (lens.getValue() == null) {
                            lens.setValue(exifService.getSonyLens(etd.getDataAsInt()));
                        }
                    }

                }
            });
        builder.withLens(lens.getValue());
        return builder.build();

    }

    protected HashSet<String> getKeywordsInImportEvent(FileToProcess value) {
        return new HashSet<>(value.getImportEvent()
            .getKeyWords()
            .stream()
            .map((k) -> k.trim())
            .collect(Collectors.toList()));
    }

    private Iterable<? extends KeyValue<String, HbaseImageThumbnailKey>> splitCreationDateToYearMonthDayAndHour(
        HbaseImageThumbnail value
    ) {
        OffsetDateTime ldt = DateTimeHelper.toLocalDateTime(value.getCreationDate());
        List<KeyValue<String, HbaseImageThumbnailKey>> retValue;
        HbaseImageThumbnailKey hbaseImageThumbnailKey = HbaseImageThumbnailKey.builder()
            .withDataId(DateTimeHelper.toDateTimeAsString(value.getCreationDate()))
            .withImageId(value.getImageId())
            .withCreationDate(value.getCreationDate())
            .build();
        Map<AbstractHbaseStatsDAO.KeyEnumType, String> keys = AbstractHbaseStatsDAO.toKey(ldt, KeyEnumType.ALL);
        keys.remove(KeyEnumType.ALL);
        retValue = keys.values()
            .stream()
            .map((v) -> new KeyValue<String, HbaseImageThumbnailKey>(v, hbaseImageThumbnailKey))
            .collect(Collectors.toList());
        return retValue;
    }

    protected KStream<String, FileToProcess> buildKTableToStoreCreatedImages(
        StreamsBuilder builder,
        String topicDupFilteredFile
    ) {
        ApplicationConfig.LOGGER.info("building ktable from topic topicDupFilteredFile {}", topicDupFilteredFile);
        return builder.stream(topicDupFilteredFile, Consumed.with(Serdes.String(), new FileToProcessSerDe()));
    }

    protected KStream<String, String> buildKTableToGetPathValue(
        StreamsBuilder streamsBuilder,
        String topicDupFilteredFile
    ) {
        ApplicationConfig.LOGGER.info("building-1 ktable from topic topicDupFilteredFile {}", topicDupFilteredFile);

        KStream<String, String> stream = streamsBuilder
            .stream(topicDupFilteredFile, Consumed.with(Serdes.String(), Serdes.String()));
        return stream;
    }

    protected KStream<String, FinalImage> buildKStreamToGetThumbImages(
        StreamsBuilder streamsBuilder,
        String topicTransformedThumb
    ) {
        ApplicationConfig.LOGGER.info("building ktable from topic topicTransformedThumb {}", topicTransformedThumb);

        KStream<String, FinalImage> stream = streamsBuilder
            .stream(topicTransformedThumb, Consumed.with(Serdes.String(), new FinalImageSerDe()))
            .peek(
                (key, hbi) -> {
                    ApplicationConfig.LOGGER
                        .info("[EVENT][{}] received the thumb with version {} ", key, hbi.getVersion());
                });
        return stream;
    }

    public KStream<String, ExchangedTiffData> buildKStreamToGetExifValue(
        StreamsBuilder streamsBuilder,
        String topicExif
    ) {
        ApplicationConfig.LOGGER.info("building ktable from topic topicExif {}", topicExif);
        KStream<String, ExchangedTiffData> stream = streamsBuilder
            .stream(topicExif, Consumed.with(Serdes.String(), new ExchangedDataSerDe()));
        return stream;
    }

    private void publishImageDataInRecordTopic(
        KStream<String, HbaseImageThumbnail> finalStream,
        String topicImageDataToPersist
    ) {
        ApplicationConfig.LOGGER.info("building finalStream to publish in  {}", topicImageDataToPersist);
        finalStream.to(topicImageDataToPersist, Produced.with(Serdes.String(), new HbaseImageThumbnailSerDe()));
    }

    private void publishEventInEventTopic(KStream<String, WfEvents> eventStream, String topicEvent) {
        eventStream.to(topicEvent, Produced.with(Serdes.String(), new WfEventsSerDe()));
    }

}
