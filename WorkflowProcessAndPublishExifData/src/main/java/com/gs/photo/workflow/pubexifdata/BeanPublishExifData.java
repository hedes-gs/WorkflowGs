package com.gs.photo.workflow.pubexifdata;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

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
import com.gs.photo.common.workflow.IStream;
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

@Service
public class BeanPublishExifData implements IStream, IBeanPublishExifData {

    private static Logger                   LOGGER = LoggerFactory.getLogger(BeanPublishExifData.class);

    @Autowired
    protected SpecificApplicationProperties specificApplicationProperties;

    @Autowired
    protected IKafkaStreamProperties        kafkaStreamProperties;

    @Override
    public Topology buildKafkaStreamsTopology() { // TODO Auto-generated method stub
        BeanPublishExifData.LOGGER.info(
            "Starting application with windowsOfEvents : {}",
            this.specificApplicationProperties.getCollectorEventsTimeWindow());
        StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, ExchangedTiffData> kstreamToGetExifValue = this.buildKStreamToGetExifValue(
            builder,
            this.kafkaStreamProperties.getTopics()
                .topicExif());
        final KStream<String, HbaseExifData> kstreamToGetHbaseExifData = kstreamToGetExifValue
            .mapValues((v) -> this.buildHbaseExifData(v));
        KStream<String, HbaseImageThumbnail> hbaseThumbMailStream = this
            .buildKStreamToGetOnlyVersion2HbaseImageThumbNail(
                builder,
                this.kafkaStreamProperties.getTopics()
                    .topicImageDataToPersist());

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
                (k, values) -> BeanPublishExifData.LOGGER
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

        this.publishImageDataInRecordTopic(
            hbaseExifUpdate,
            this.kafkaStreamProperties.getTopics()
                .topicExifImageDataToPersist());
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
                (k, values) -> BeanPublishExifData.LOGGER
                    .info("[EVENT][{}] processed and {} events produced ", k, values.size()),
                this.specificApplicationProperties.getCollectorEventsBufferSize(),
                this.specificApplicationProperties.getCollectorEventsTimeWindow()),
            "MyTransformer");
        this.publishImageDataInEventTopic(
            wfEventsStream,
            this.kafkaStreamProperties.getTopics()
                .topicEvent());

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
            BeanPublishExifData.LOGGER
                .error("[EVENT}[{}] Found {} values, mor than expected one, key is more completed", k, values.size());
        }
        return values.size() == 3;
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
        BeanPublishExifData.LOGGER.error(
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
                (k, v) -> BeanPublishExifData.LOGGER.info(
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
