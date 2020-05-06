package com.gs.photo.workflow;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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
import com.gs.photos.serializers.FinalImageSerDe;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
import com.gs.photos.serializers.WfEventsSerDe;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;
import com.workflow.model.storm.FinalImage;

@Configuration
public class ApplicationConfig extends AbstractApplicationConfig {

    private static final byte[] EMPTY_ARRAY_BYTE           = new byte[] {};
    private static final String NOT_SET                    = "<not set>";
    private static final Logger LOGGER                     = LoggerFactory.getLogger(ApplicationConfig.class);
    public static final int     JOIN_WINDOW_TIME           = 86400;
    public static final short   EXIF_CREATION_DATE_ID      = (short) 0x9003;
    public static final short[] EXIF_CREATION_DATE_ID_PATH = { (short) 0, (short) 0x8769 };

    @Bean
    public Properties kafkaStreamProperties(
        @Value("${bootstrap.servers}") String bootstrapServers,
        @Value("${kafkaStreamDir.dir}") String kafkaStreamDir,
        @Value("${application.kafkastreams.id}") String applicationId,
        @Value("${kafka.pollTimeInMillisecondes}") int pollTimeInMillisecondes,
        @Value("${kafka.consumer.batchRecords}") int consumerBatch,
        @Value("${kafka.stream.commit.interval.ms}") int commitIntervalIms,
        @Value("${kafka.stream.metadata.age.ms}") int metaDataAgeIms,
        @Value("${kafka.stream.nb.of.threads}") int nbOfThreads
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
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollTimeInMillisecondes);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerBatch);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        config.put("sasl.kerberos.service.name", "kafka");
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, nbOfThreads);
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
        @Value("${kafkastreams.windowsOfEventsInMs}") int windowsOfEvents
    ) {
        ApplicationConfig.LOGGER.info("Starting application with windowsOfEvents : {}", windowsOfEvents);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> pathOfImageKStream = this
            .buildKTableToStoreCreatedImages(builder, topicDupFilteredFile);
        KStream<String, FinalImage> thumbImages = this.buildKStreamToGetThumbImages(builder, topicTransformedThumb);
        KStream<String, ExchangedTiffData> exifOfImageStream = this.buildKStreamToGetExifValue(builder, topicExif);

        /*
         * Stream to get the creation date on topic topicCountOfImagesPerDate
         */
        KStream<String, ExchangedTiffData> filteredImageKStreamForCreationDate = exifOfImageStream
            .filter((key, exif) -> {
                boolean b = (exif.getTag() == ApplicationConfig.EXIF_CREATION_DATE_ID)
                    && (Objects.deepEquals(exif.getPath(), ApplicationConfig.EXIF_CREATION_DATE_ID_PATH));
                return b;
            });

        /*
         * join to build the HbaseImageThumbnail which will be stored in hbase. We just
         * set the creation date.
         */
        final ValueJoiner<ExchangedTiffData, String, HbaseImageThumbnail> joiner = (
            v_exchangedTiffData,
            v_imagePath) -> { return this.buildHBaseImageThumbnail(v_exchangedTiffData, v_imagePath); };
        final Joined<String, ExchangedTiffData, String> joined = Joined
            .with(Serdes.String(), new ExchangedDataSerDe(), Serdes.String());

        KStream<String, HbaseImageThumbnail> jointureToFindTheCreationDate = filteredImageKStreamForCreationDate
            .join(pathOfImageKStream, joiner, JoinWindows.of(Duration.ofDays(2)), joined);

        /*
         * imageCountsStream : stream to create the number of images per hour/minutes
         * etc. this is published on topic topicCountOfImagesPerDate
         */
        KStream<String, Long> imageCountsStream = jointureToFindTheCreationDate.flatMap((key, value) -> {

            return this.splitCreationDateToYearMonthDayAndHour(value);
        });

        imageCountsStream.to(topicCountOfImagesPerDate, Produced.with(Serdes.String(), Serdes.Long()));

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
            Joined.with(Serdes.String(), new HbaseImageThumbnailSerDe(), new FinalImageSerDe()));

        /*
         * final2Stream : we update the HbaseImageThumbnail which was created with the
         * creation date and the thumb images only. At the end the key is the original
         * image key
         */
        KStream<String, HbaseImageThumbnail> final2Stream = finalStream
            .join(pathOfImageKStream, (v_hbaseImageThumbnail, v_path) -> {
                v_hbaseImageThumbnail.setPath(v_path);
                final String imageName = v_path.substring(v_path.lastIndexOf("/"));
                v_hbaseImageThumbnail.setImageName(imageName);
                v_hbaseImageThumbnail.setThumbName(imageName);
                return v_hbaseImageThumbnail;
            },
                JoinWindows.of(Duration.ofSeconds(ApplicationConfig.JOIN_WINDOW_TIME)),
                Joined.with(Serdes.String(), new HbaseImageThumbnailSerDe(), Serdes.String()));
        this.publishImageDataInRecordTopic(final2Stream, topicImageDataToPersist);

        KStream<String, WfEvent> eventStream = final2Stream.mapValues((k, v) -> this.buildEvent(v));

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
        return WfEvent.builder()
            .withImgId(v.getImageId())
            .withParentDataId(v.getDataId())
            .withDataId(v.getDataId() + "-" + v.getVersion())
            .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_PREPARE_FOR_PERSIST)
            .build();
    }

    private String buildKey(HbaseImageThumbnail v) {
        return KeysBuilder.topicImageDataToPersistKeyBuilder()
            .withOriginalImageKey(v.getImageId())
            .withVersion(v.getVersion())
            .build();
    }

    protected HbaseImageThumbnail buildHbaseImageThumbnail(
        FinalImage v_FinalImage,
        HbaseImageThumbnail v_hbaseImageThumbnail
    ) {
        HbaseImageThumbnail retValue = null;
        if (v_hbaseImageThumbnail != null) {
            retValue = v_hbaseImageThumbnail;
            retValue.setDataId(v_FinalImage.getDataId());
            retValue.setThumbnail(v_FinalImage.getCompressedImage());
            retValue.setHeight(v_FinalImage.getHeight());
            retValue.setWidth(v_FinalImage.getWidth());
            retValue.setVersion(v_FinalImage.getVersion());
        }
        return retValue;
    }

    private HbaseImageThumbnail buildHBaseImageThumbnail(ExchangedTiffData key, String value) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        try {
            builder.withImageName(ApplicationConfig.NOT_SET)
                .withPath(ApplicationConfig.NOT_SET)
                .withThumbnail(ApplicationConfig.EMPTY_ARRAY_BYTE)
                .withThumbName(ApplicationConfig.NOT_SET)
                .withImageId(key.getImageId())
                .withDataId(ApplicationConfig.NOT_SET)
                .withCreationDate(DateTimeHelper.toEpochMillis(new String(key.getDataAsByte(), "UTF-8").trim()));
        } catch (UnsupportedEncodingException e) {
            ApplicationConfig.LOGGER.error("unsupported charset ", e);
        }
        return builder.build();
    }

    private Iterable<? extends KeyValue<String, Long>> splitCreationDateToYearMonthDayAndHour(
        HbaseImageThumbnail value
    ) {
        OffsetDateTime ldt = DateTimeHelper.toLocalDateTime(value.getCreationDate());

        List<KeyValue<String, Long>> retValue;
        String keyYear = "Y:" + (long) ldt.getYear();
        String keyMonth = keyYear + "/M:" + (long) ldt.getMonthValue();
        String keyDay = keyMonth + "/D:" + (long) ldt.getDayOfMonth();
        String keyHour = keyDay + "/H:" + (long) ldt.getHour();
        String keyMinute = keyHour + "/Mn:" + (long) ldt.getMinute();
        String keySeconde = keyMinute + "/S:" + (long) ldt.getSecond();
        retValue = Arrays.asList(
            new KeyValue<String, Long>(keyYear, 1L),
            new KeyValue<String, Long>(keyMonth, 1L),
            new KeyValue<String, Long>(keyDay, 1L),
            new KeyValue<String, Long>(keyHour, 1L),
            new KeyValue<String, Long>(keyMinute, 1L),
            new KeyValue<String, Long>(keySeconde, 1L));
        return retValue;
    }

    protected KStream<String, String> buildKTableToStoreCreatedImages(
        StreamsBuilder builder,
        String topicDupFilteredFile
    ) {
        ApplicationConfig.LOGGER.info("building ktable from topic topicDupFilteredFile {}", topicDupFilteredFile);
        return builder.stream(topicDupFilteredFile, Consumed.with(Serdes.String(), Serdes.String()));
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
            .stream(topicTransformedThumb, Consumed.with(Serdes.String(), new FinalImageSerDe()));
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

    protected void publishImageDataInRecordTopic(
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
