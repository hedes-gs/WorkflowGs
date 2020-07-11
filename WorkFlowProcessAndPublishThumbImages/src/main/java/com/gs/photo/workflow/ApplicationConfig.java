package com.gs.photo.workflow;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

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
import com.gs.photo.workflow.exif.FieldType;
import com.gs.photo.workflow.exif.IExifService;
import com.gs.photo.workflow.hbase.dao.AbstractHbaseStatsDAO;
import com.gs.photo.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;
import com.gs.photos.serializers.ExchangedDataSerDe;
import com.gs.photos.serializers.FileToProcessSerDe;
import com.gs.photos.serializers.FinalImageSerDe;
import com.gs.photos.serializers.HbaseImageThumbnailKeySerDe;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
import com.gs.photos.serializers.IntArrayDeserializer;
import com.gs.photos.serializers.IntArraySerializer;
import com.gs.photos.serializers.WfEventsSerDe;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.HbaseImageThumbnailKey;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;
import com.workflow.model.storm.FinalImage;

@Configuration
public class ApplicationConfig extends AbstractApplicationConfig {

    private static final byte[] EMPTY_ARRAY_BYTE           = new byte[] {};
    private static final String NOT_SET                    = "<not set>";
    private static final Logger LOGGER                     = LoggerFactory.getLogger(ApplicationConfig.class);
    public static final int     JOIN_WINDOW_TIME           = 86400;
    public static final short   EXIF_CREATION_DATE_ID      = (short) 0x9003;
    public static final short   EXIF_SIZE_WIDTH            = (short) 0xA002;
    public static final short   EXIF_SIZE_HEIGHT           = (short) 0xA003;
    public static final short   EXIF_ORIENTATION           = (short) 0x0112;

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
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, pollTimeInMillisecondes);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerBatch);
        config.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMsConfig);
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
        KStream<String, ExchangedTiffData> streamForCreationDateForHbaseExifData = exifOfImageStream.filter(
            (key, exif) -> (exif.getTag() == ApplicationConfig.EXIF_CREATION_DATE_ID)
                && (Objects.deepEquals(exif.getPath(), ApplicationConfig.EXIF_CREATION_DATE_ID_PATH)));
        KStream<String, Long> streamForWidthForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_SIZE_WIDTH)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH)))
            .mapValues((hbaseExifData) -> (long) hbaseExifData.getDataAsInt()[0]);
        KStream<String, Long> streamForHeightForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_SIZE_HEIGHT)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_WIDTH_HEIGHT_PATH)))
            .mapValues((hbaseExifData) -> (long) hbaseExifData.getDataAsInt()[0]);
        KStream<String, Long> streamForOrientationForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_ORIENTATION)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_ORIENTATION_PATH)))
            .mapValues((hbaseExifData) -> (long) hbaseExifData.getDataAsShort()[0]);

        KStream<String, byte[]> streamForLensForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_LENS)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_LENS_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsByte());
        KStream<String, int[]> streamForFocalLensForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_FOCAL_LENS)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_FOCAL_LENS_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsInt());
        KStream<String, int[]> streamForShifExpoForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_SHIFT_EXPO)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_SHIFT_EXPO_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsInt());
        KStream<String, Short> streamForSpeedIsoForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_SPEED_ISO)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_SPEED_ISO_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsShort()[0]);
        KStream<String, int[]> streamForApertureForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_APERTURE)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_APERTURE_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsInt());
        KStream<String, int[]> streamForSpeedForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_SPEED)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_SPEED_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsInt());
        KStream<String, byte[]> streamForCopyrightForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_COPYRIGHT)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_COPYRIGHT_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsByte());
        KStream<String, byte[]> streamForArtistForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_ARTIST)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_ARTIST_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsByte());
        KStream<String, byte[]> streamForCameraModelForHbaseExifData = exifOfImageStream
            .filter(
                (key, hbaseExifData) -> (hbaseExifData.getTag() == ApplicationConfig.EXIF_CAMERA_MODEL)
                    && (Objects.deepEquals(hbaseExifData.getPath(), ApplicationConfig.EXIF_CAMERA_MODEL_PATH)))
            .mapValues((hbaseExifData) -> hbaseExifData.getDataAsByte());

        /*
         * join to build the HbaseImageThumbnail which will be stored in hbase. We just
         * set the creation date.
         */
        final ValueJoiner<ExchangedTiffData, FileToProcess, HbaseImageThumbnail> joiner = (
            v_exchangedTiffData,
            v_imagePath) -> { return this.buildHBaseImageThumbnail(v_exchangedTiffData, v_imagePath); };

        final Joined<String, ExchangedTiffData, FileToProcess> joined = Joined
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

        Joined<String, HbaseImageThumbnail, Long> join = Joined
            .with(Serdes.String(), new HbaseImageThumbnailSerDe(), Serdes.Long());
        Joined<String, HbaseImageThumbnail, byte[]> joinArrayOfBytes = Joined
            .with(Serdes.String(), new HbaseImageThumbnailSerDe(), Serdes.ByteArray());
        Joined<String, HbaseImageThumbnail, int[]> joinArrayOfInt = Joined.with(
            Serdes.String(),
            new HbaseImageThumbnailSerDe(),
            Serdes.serdeFrom(new IntArraySerializer(), new IntArrayDeserializer()));
        Joined<String, HbaseImageThumbnail, Short> joinShort = Joined
            .with(Serdes.String(), new HbaseImageThumbnailSerDe(), Serdes.Short());

        ValueJoiner<HbaseImageThumbnail, Long, HbaseImageThumbnail> joinerWidth = (value1, width) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setOriginalWidth(width);
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseImageThumbnail, Long, HbaseImageThumbnail> joinerHeight = (value1, height) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setOriginalHeight(height);
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseImageThumbnail, Long, HbaseImageThumbnail> joinerOrientation = (value1, orientation) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setOrientation(orientation);
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };

        // new
        ValueJoiner<HbaseImageThumbnail, byte[], HbaseImageThumbnail> joinerLens = (value1, lens) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setLens(lens);
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseImageThumbnail, int[], HbaseImageThumbnail> joinerFocalLens = (value1, focalLens) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setFocalLens(focalLens);
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseImageThumbnail, int[], HbaseImageThumbnail> joinerShiftExpo = (value1, shift) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setShiftExpo(shift);
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseImageThumbnail, Short, HbaseImageThumbnail> joinerSpeedIso = (value1, speedIso) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setIsoSpeed(speedIso);
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseImageThumbnail, int[], HbaseImageThumbnail> joinerAperture = (value1, aperture) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setAperture(aperture);
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseImageThumbnail, int[], HbaseImageThumbnail> joinerSpeed = (value1, speed) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setSpeed(speed);
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseImageThumbnail, byte[], HbaseImageThumbnail> joinerCopyright = (value1, copyright) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setCopyright(exifService.toString(FieldType.ASCII, copyright));
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseImageThumbnail, byte[], HbaseImageThumbnail> joinerArtist = (value1, artist) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setArtist(exifService.toString(FieldType.ASCII, artist));
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        };
        ValueJoiner<HbaseImageThumbnail, byte[], HbaseImageThumbnail> joinerCameraModel = (value1, model) -> {
            try {
                HbaseImageThumbnail hbi = (HbaseImageThumbnail) value1.clone();
                hbi.setCamera(exifService.toString(FieldType.ASCII, model));
                return hbi;
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
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
            Joined.with(Serdes.String(), new HbaseImageThumbnailSerDe(), new FinalImageSerDe()));
        finalStream.peek(
            (key, hbi) -> ApplicationConfig.LOGGER
                .info(" [EVENT][{}] was created with the thumb with version {} ", key, hbi.getVersion()));
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
        return WfEvent.builder()
            .withImgId(v.getImageId())
            .withParentDataId(v.getDataId())
            .withDataId(v.getDataId() + "-" + v.getVersion())
            .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_PREPARE_FOR_PERSIST)
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

    private HbaseImageThumbnail buildHBaseImageThumbnail(ExchangedTiffData key, FileToProcess value) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        try {
            builder.withPath(value.getPath())
                .withImageName(value.getName())
                .withThumbnail(ApplicationConfig.EMPTY_ARRAY_BYTE)
                .withThumbName(value.getName())
                .withImageId(key.getImageId())
                .withDataId(ApplicationConfig.NOT_SET)
                .withAlbums(
                    new HashSet<>(Collections.singleton(
                        value.getImportEvent()
                            .getAlbum())))
                .withImportName(
                    value.getImportEvent()
                        .getImportName())
                .withKeyWords(
                    new HashSet<>(value.getImportEvent()
                        .getKeyWords()))
                .withCreationDate(DateTimeHelper.toEpochMillis(new String(key.getDataAsByte(), "UTF-8").trim()));
        } catch (UnsupportedEncodingException e) {
            ApplicationConfig.LOGGER.error("unsupported charset ", e);
        }
        return builder.build();
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
            .withVersion(value.getVersion())
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
