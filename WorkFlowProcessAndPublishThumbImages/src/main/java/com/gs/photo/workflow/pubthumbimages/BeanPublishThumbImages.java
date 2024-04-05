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
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gs.photo.common.ks.transformers.AdvancedMapCollector;
import com.gs.photo.common.ks.transformers.MapCollector;
import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photo.common.workflow.IKafkaStreamProperties;
import com.gs.photo.common.workflow.exif.FieldType;
import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO;
import com.gs.photo.common.workflow.hbase.dao.AbstractHbaseStatsDAO.KeyEnumType;
import com.gs.photos.serializers.CollectionOfExchangedDataSerDe;
import com.gs.photos.serializers.ExchangedDataSerDe;
import com.gs.photos.serializers.FileToProcessSerDe;
import com.gs.photos.serializers.FinalImageSerDe;
import com.gs.photos.serializers.HbaseImageThumbnailSerDe;
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

@Service 
public class BeanPublishThumbImages implements IBeanPublishThumbImages {

    private static Logger                   LOGGER  = LoggerFactory.getLogger(BeanPublishThumbImages.class);
    private static final String             NOT_SET = "<not set>";

    @Autowired
    protected SpecificApplicationProperties specificApplicationProperties;

    @Autowired
    protected IKafkaStreamProperties        kafkaStreamProperties;

    @Autowired
    protected IExifService                  exifService;

    @Override
    public Topology buildKafkaStreamsTopology() {
        BeanPublishThumbImages.LOGGER.info(
            "Starting application with windowsOfEvents : {}",
            this.specificApplicationProperties.getCollectorEventsTimeWindow());
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, FileToProcess> pathOfImageKStream = this.buildKTableToStoreCreatedImages(
            builder,
            this.kafkaStreamProperties.getTopics()
                .topicDupFilteredFile());
        KStream<String, FinalImage> thumbImages = this.buildKStreamToGetThumbImages(
            builder,
            this.kafkaStreamProperties.getTopics()
                .topicTransformedThumb());
        KStream<String, ExchangedTiffData> exifOfImageStream = this.buildKStreamToGetExifValue(
            builder,
            this.kafkaStreamProperties.getTopics()
                .topicExif());
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
                (k, values) -> BeanPublishThumbImages.LOGGER
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
                (v1, v2) -> this.buildHBaseImageThumbnail(this.exifService, v1, v2),
                JoinWindows.of(Duration.ofDays(2)),
                joined)
            .join(thumbImages, (v_hbaseImageThumbnail, v_finalImage) -> {
                final HbaseImageThumbnail buildHbaseImageThumbnail = this
                    .buildHbaseImageThumbnail(v_finalImage, v_hbaseImageThumbnail);
                return buildHbaseImageThumbnail;
            },
                JoinWindows.of(Duration.ofSeconds(IBeanPublishThumbImages.JOIN_WINDOW_TIME)),
                StreamJoined.with(Serdes.String(), new HbaseImageThumbnailSerDe(), new FinalImageSerDe()));

        this.publishImageDataInRecordTopic(
            HbaseImageThumbnailBuiltStream,
            this.kafkaStreamProperties.getTopics()
                .topicImageDataToPersist());

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
                (k, values) -> BeanPublishThumbImages.LOGGER
                    .info("[EVENT][{}] processed and {} events produced ", k, values.size()),
                this.specificApplicationProperties.getCollectorEventsBufferSize(),
                this.specificApplicationProperties.getCollectorEventsTimeWindow()),
            "event-store-builder");
        this.publishEventInEventTopic(
            wfEventsStream,
            this.kafkaStreamProperties.getTopics()
                .topicEvent());
        return builder.build();
    }

    private boolean isARecordedField(String key, ExchangedTiffData hbaseExifData) {

        return ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_CREATION_DATE_ID)
            && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_CREATION_DATE_ID_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_SIZE_WIDTH)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_WIDTH_HEIGHT_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_SIZE_HEIGHT)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_WIDTH_HEIGHT_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_ORIENTATION)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_ORIENTATION_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_LENS)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_LENS_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_FOCAL_LENS)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_FOCAL_LENS_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_SHIFT_EXPO)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_SHIFT_EXPO_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_SPEED_ISO)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_SPEED_ISO_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_APERTURE)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_APERTURE_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_SPEED)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_SPEED_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_COPYRIGHT)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_COPYRIGHT_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_ARTIST)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_ARTIST_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.EXIF_CAMERA_MODEL)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.EXIF_CAMERA_MODEL_PATH)))

            || ((hbaseExifData.getTag() == IBeanPublishThumbImages.SONY_EXIF_LENS)
                && (Objects.deepEquals(hbaseExifData.getPath(), IBeanPublishThumbImages.SONY_EXIF_LENS_PATH)));

    }

    private boolean isComplete(String k, Collection<ExchangedTiffData> values) {
        if (values.size() == 14) {
            BeanPublishThumbImages.LOGGER.info("[EVENT}[{}] Found {} values, key is completed", k, values.size());
        } else if (values.size() > 14) {
            BeanPublishThumbImages.LOGGER
                .error("[EVENT}[{}] Found {} values, mor than expected one, key is more completed", k, values.size());
        }
        return values.size() >= 14;
    }

    private HbaseImageThumbnail buildHBaseImageThumbnail(ExchangedTiffData key, FileToProcess value) {
        HbaseImageThumbnail.Builder builder = HbaseImageThumbnail.builder();
        try {
            builder.withPath(value.getUrl())
                .withImageName(value.getName())
                .withThumbnail(new HashMap<>())
                .withThumbName(value.getName())
                .withImageId(key.getImageId())
                .withDataId(BeanPublishThumbImages.NOT_SET)
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
            BeanPublishThumbImages.LOGGER.error("unsupported charset ", e);
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
            .withDataId(BeanPublishThumbImages.NOT_SET)
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
                if ((etd.getTag() == IBeanPublishThumbImages.EXIF_CREATION_DATE_ID)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_CREATION_DATE_ID_PATH))) {
                    try {
                        builder.withCreationDate(
                            DateTimeHelper.toEpochMillis(new String(etd.getDataAsByte(), "UTF-8").trim()));
                    } catch (UnsupportedEncodingException e) {
                        throw new RuntimeException(e);
                    }
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_SIZE_WIDTH)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_WIDTH_HEIGHT_PATH))) {
                    builder.withOriginalWidth(etd.getDataAsInt()[0]);
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_SIZE_HEIGHT)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_WIDTH_HEIGHT_PATH))) {
                    builder.withOriginalHeight(etd.getDataAsInt()[0]);
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_ORIENTATION)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_ORIENTATION_PATH))) {
                    builder.withOrientation(etd.getDataAsShort()[0]);
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_LENS)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_LENS_PATH))) {
                    if (lens.getValue() == null) {
                        lens.setValue(Arrays.copyOf(etd.getDataAsByte(), etd.getDataAsByte().length - 1));
                    }

                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_FOCAL_LENS)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_FOCAL_LENS_PATH))) {
                    builder.withFocalLens(etd.getDataAsInt());
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_SHIFT_EXPO)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_SHIFT_EXPO_PATH))) {
                    builder.withShiftExpo(etd.getDataAsInt());
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_SPEED_ISO)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_SPEED_ISO_PATH))) {
                    builder.withIsoSpeed(etd.getDataAsShort()[0]);
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_APERTURE)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_APERTURE_PATH))) {
                    builder.withAperture(etd.getDataAsInt());
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_SPEED)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_SPEED_PATH))) {
                    builder.withSpeed(etd.getDataAsInt());
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_COPYRIGHT)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_COPYRIGHT_PATH))) {
                    builder.withCopyright(exifService.toString(FieldType.ASCII, etd.getDataAsByte()));
                    BeanPublishThumbImages.LOGGER.info(
                        "[EVENT][{}]Found copyright {}",
                        value.getImageId(),
                        exifService.toString(FieldType.ASCII, etd.getDataAsByte()));
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_ARTIST)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_ARTIST_PATH))) {
                    builder.withArtist(exifService.toString(FieldType.ASCII, etd.getDataAsByte()));
                    BeanPublishThumbImages.LOGGER.info(
                        "[EVENT][{}]Found artist : {}",
                        value.getImageId(),
                        exifService.toString(FieldType.ASCII, etd.getDataAsByte()));
                } else if ((etd.getTag() == IBeanPublishThumbImages.EXIF_CAMERA_MODEL)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.EXIF_CAMERA_MODEL_PATH))) {
                    builder.withCamera(exifService.toString(FieldType.ASCII, etd.getDataAsByte()));
                } else if ((etd.getTag() == IBeanPublishThumbImages.SONY_EXIF_LENS)
                    && (Objects.deepEquals(etd.getPath(), IBeanPublishThumbImages.SONY_EXIF_LENS_PATH))) {
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
        BeanPublishThumbImages.LOGGER.info("building ktable from topic topicDupFilteredFile {}", topicDupFilteredFile);
        return builder.stream(topicDupFilteredFile, Consumed.with(Serdes.String(), new FileToProcessSerDe()));
    }

    protected KStream<String, String> buildKTableToGetPathValue(
        StreamsBuilder streamsBuilder,
        String topicDupFilteredFile
    ) {
        BeanPublishThumbImages.LOGGER
            .info("building-1 ktable from topic topicDupFilteredFile {}", topicDupFilteredFile);

        KStream<String, String> stream = streamsBuilder
            .stream(topicDupFilteredFile, Consumed.with(Serdes.String(), Serdes.String()));
        return stream;
    }

    protected KStream<String, FinalImage> buildKStreamToGetThumbImages(
        StreamsBuilder streamsBuilder,
        String topicTransformedThumb
    ) {
        BeanPublishThumbImages.LOGGER
            .info("building ktable from topic topicTransformedThumb {}", topicTransformedThumb);

        KStream<String, FinalImage> stream = streamsBuilder
            .stream(topicTransformedThumb, Consumed.with(Serdes.String(), new FinalImageSerDe()))
            .peek((key, hbi) -> {
                BeanPublishThumbImages.LOGGER
                    .info("[EVENT][{}] received the thumb with version {} ", key, hbi.getVersion());
            });
        return stream;
    }

    public KStream<String, ExchangedTiffData> buildKStreamToGetExifValue(
        StreamsBuilder streamsBuilder,
        String topicExif
    ) {
        BeanPublishThumbImages.LOGGER.info("building ktable from topic topicExif {}", topicExif);
        KStream<String, ExchangedTiffData> stream = streamsBuilder
            .stream(topicExif, Consumed.with(Serdes.String(), new ExchangedDataSerDe()));
        return stream;
    }

    private void publishImageDataInRecordTopic(
        KStream<String, HbaseImageThumbnail> finalStream,
        String topicImageDataToPersist
    ) {
        BeanPublishThumbImages.LOGGER.info("building finalStream to publish in  {}", topicImageDataToPersist);
        finalStream.to(topicImageDataToPersist, Produced.with(Serdes.String(), new HbaseImageThumbnailSerDe()));
    }

    private void publishEventInEventTopic(KStream<String, WfEvents> eventStream, String topicEvent) {
        eventStream.to(topicEvent, Produced.with(Serdes.String(), new WfEventsSerDe()));
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

}
