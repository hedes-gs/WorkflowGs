package com.gs.photo.workflow.extimginfo.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.common.workflow.IIgniteDAO;
import com.gs.photo.common.workflow.TimeMeasurement;
import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.impl.KafkaUtils;
import com.gs.photo.common.workflow.internal.GenericKafkaManagedObject;
import com.gs.photo.common.workflow.internal.KafkaManagedObject;
import com.gs.photo.common.workflow.internal.KafkaManagedWfEvent;
import com.gs.photo.workflow.extimginfo.IFileMetadataExtractor;
import com.gs.photo.workflow.extimginfo.IProcessIncomingFiles;
import com.gs.photos.workflow.extimginfo.metadata.IFD;
import com.gs.photos.workflow.extimginfo.metadata.TiffFieldAndPath;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventFinal;
import com.workflow.model.events.WfEventInitial;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;

@Component
public class BeanProcessIncomingFile implements IProcessIncomingFiles {
    protected static final byte[]  NOT_FOUND_FOR_OPTIONAL_PARAMETER;
    protected static final int[]   DEFAULT_SONY_LENS_NOT_FOUND_FOR_OPTIONAL_PARAMETER;
    protected static final short   EXIF_COPYRIGHT      = (short) 0x8298;
    protected static final short   EXIF_ARTIST         = (short) 0x13B;
    protected static final short[] EXIF_COPYRIGHT_PATH = { (short) 0 };
    protected static final short[] EXIF_ARTIST_PATH    = { (short) 0 };
    protected static final short   SONY_EXIF_LENS      = (short) 0xB027;
    protected static final short   EXIF_LENS           = (short) 0xA434;
    protected static final short[] SONY_EXIF_LENS_PATH = { (short) 0, (short) 0x8769, (short) 0x927c };
    protected static final short[] EXIF_LENS_PATH      = { (short) 0, (short) 0x8769 };

    static {
        try {

            NOT_FOUND_FOR_OPTIONAL_PARAMETER = "<NOT FOUND>".getBytes("UTF-8");
            DEFAULT_SONY_LENS_NOT_FOUND_FOR_OPTIONAL_PARAMETER = new int[] { -1 };

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    protected static Logger                   LOGGER = LoggerFactory.getLogger(BeanProcessIncomingFile.class);

    @Autowired
    protected IBeanTaskExecutor               beanTaskExecutor;

    @Autowired
    protected IExifService                    exifService;

    @Value("${topic.topicDupFilteredFile}")
    protected String                          topicDupFilteredFile;

    @Value("${topic.topicExif}")
    protected String                          topicExif;

    @Value("${topic.topicThumb}")
    protected String                          topicThumb;

    @Value("${topic.topicEvent}")
    protected String                          topicEvent;

    @Value("${group.id}")
    private String                            groupId;

    @Autowired
    protected IFileMetadataExtractor          beanFileMetadataExtractor;

    @Autowired
    @Qualifier("consumerForTopicWithFileToProcessValue")
    protected Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue;

    @Autowired
    @Qualifier("producerForTransactionPublishingOnExifOrImageTopic")
    protected Producer<String, Object>        producerForTransactionPublishingOnExifOrImageTopic;

    @Value("${kafka.consumer.batchSizeForParallelProcessingIncomingRecords}")
    protected int                             batchSizeForParallelProcessingIncomingRecords;

    @Autowired
    protected IIgniteDAO                      iIgniteDAO;

    @Autowired
    private ApplicationContext                context;

    @Override
    public void init() { this.beanTaskExecutor.execute(() -> this.processInputFile()); }

    protected void processInputFile() {
        boolean ready = true;
        ready = this.waitForIgnite();
        BeanProcessIncomingFile.LOGGER.info("Ignite is finally ready, let's go !!!");
        while (ready) {
            this.consumerForTopicWithFileToProcessValue.subscribe(Collections.singleton(this.topicDupFilteredFile));
            this.producerForTransactionPublishingOnExifOrImageTopic.initTransactions();
            BeanProcessIncomingFile.LOGGER.info("Starting process input file...");
            try {
                this.doProcessFile();
            } catch (Throwable e) {
                BeanProcessIncomingFile.LOGGER.error("An error is raised {}", ExceptionUtils.getStackTrace(e)));
                ready = !((e instanceof InterruptedException) || (e.getCause() instanceof InterruptedException));
            } finally {
                try {
                    this.consumerForTopicWithFileToProcessValue.close();
                    this.producerForTransactionPublishingOnExifOrImageTopic.close();
                } catch (Exception e) {
                    BeanProcessIncomingFile.LOGGER.error("Error on closing consumer and producer...", e);
                }
            }
            if (ready) {
                try {
                    BeanProcessIncomingFile.LOGGER.info("Waiting for ignite... again.. ");
                    ready = this.waitForIgnite();
                    TimeUnit.SECONDS.sleep(1);
                    this.consumerForTopicWithFileToProcessValue = this.context
                        .getBean("consumerForTopicWithFileToProcessValue", Consumer.class);
                    this.producerForTransactionPublishingOnExifOrImageTopic = this.context
                        .getBean("producerForTransactionPublishingOnExifOrImageTopic", Producer.class);
                } catch (InterruptedException e) {
                    BeanProcessIncomingFile.LOGGER.warn("Interrupted, stopping", e);
                    break;
                }
            }
        }
    }

    protected boolean waitForIgnite() {
        boolean ready = true;
        do {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                ready = false;
                break;
            }
        } while (!this.iIgniteDAO.isReady());
        return ready;
    }

    protected void doProcessFile() throws Throwable {
        while (true) {

            try (
                TimeMeasurement timeMeasurement = TimeMeasurement.of(
                    "BATCH_PROCESS_FILES",
                    (d) -> BeanProcessIncomingFile.LOGGER.debug(" Perf. metrics {}", d),
                    System.currentTimeMillis())) {
                Stream<ConsumerRecord<String, FileToProcess>> filesToCopyStream = KafkaUtils.toStreamV2(
                    200,
                    this.consumerForTopicWithFileToProcessValue,
                    this.batchSizeForParallelProcessingIncomingRecords,
                    true,
                    (i) -> this.startTransactionForRecords(i),
                    timeMeasurement);
                Map<String, List<GenericKafkaManagedObject<? extends WfEvent>>> eventsToSend = filesToCopyStream
                    .flatMap((rec) -> this.toKafkaManagedObject(rec))
                    .map((kmo) -> this.send(kmo))
                    .collect(Collectors.groupingByConcurrent(GenericKafkaManagedObject::getImageKey));

                eventsToSend.entrySet()
                    .forEach((e) -> this.createInitEvent(e.getKey(), e.getValue()));
                long eventsNumber = eventsToSend.keySet()
                    .stream()
                    .map(
                        (img) -> WfEvents.builder()
                            .withDataId(img)
                            .withProducer("PRODUCER_IMG_METADATA")
                            .withEvents(
                                (Collection<WfEvent>) eventsToSend.get(img)
                                    .stream()
                                    .map(GenericKafkaManagedObject::getValue)
                                    .collect(Collectors.toList()))
                            .build())

                    .map((evts) -> this.send(evts))
                    .collect(Collectors.toList())
                    .stream()
                    .map((t) -> this.getRecordMetaData(t))
                    .count();

                Map<TopicPartition, OffsetAndMetadata> offsets = eventsToSend.values()
                    .stream()
                    .flatMap((c) -> c.stream())
                    .collect(
                        () -> new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>(),
                        (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                        (r, t) -> this.merge(r, t));

                BeanProcessIncomingFile.LOGGER.debug("Offset to commit {} ", offsets.toString());
                this.producerForTransactionPublishingOnExifOrImageTopic.sendOffsetsToTransaction(offsets, this.groupId);
                this.producerForTransactionPublishingOnExifOrImageTopic.commitTransaction();
                this.cleanIgniteCache(new HashSet<>(eventsToSend.keySet()));
            } catch (IOException e) {
                BeanProcessIncomingFile.LOGGER.error("Unexpected error ", ExceptionUtils.getStackTrace(e));
                this.producerForTransactionPublishingOnExifOrImageTopic.abortTransaction();
                throw new RuntimeException(e);
            } catch (Throwable e) {
                BeanProcessIncomingFile.LOGGER.error("Unexpected error {}", ExceptionUtils.getStackTrace(e));
                this.producerForTransactionPublishingOnExifOrImageTopic.abortTransaction();
                throw e;
            }
        }
    }

    private void cleanIgniteCache(Set<String> img) {
        BeanProcessIncomingFile.LOGGER.debug("Deleting in cache {}", img);
        this.iIgniteDAO.delete(img);
    }

    protected RecordMetadata getRecordMetaData(Future<RecordMetadata> t) {
        try {
            return t.get();
        } catch (
            InterruptedException |
            ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Future<RecordMetadata> send(WfEvents wfe) {
        ProducerRecord<String, Object> pr = new ProducerRecord<>(this.topicEvent, wfe.getDataId(), wfe);
        BeanProcessIncomingFile.LOGGER.info(
            "EVENT[{}] nb of events sent {}",
            wfe.getDataId(),
            wfe.getEvents()
                .size());
        return this.producerForTransactionPublishingOnExifOrImageTopic.send(pr);
    }

    private GenericKafkaManagedObject<? extends WfEvent> send(GenericKafkaManagedObject<?> kmo) {
        ProducerRecord<String, Object> pr = new ProducerRecord<>(kmo.getTopic(),
            kmo.getObjectKey(),
            kmo.getObjectToSend());
        if (this.topicThumb.equalsIgnoreCase(kmo.getTopic())) {
            BeanProcessIncomingFile.LOGGER.info("[EVENT][{}]Find an image to send", kmo.getObjectKey());
            this.producerForTransactionPublishingOnExifOrImageTopic
                .send(pr, (r, e) -> this.process(kmo.getObjectKey(), r, e));
        } else {
            this.producerForTransactionPublishingOnExifOrImageTopic
                .send(pr, (r, e) -> this.processError(kmo.getObjectKey(), r, e));
        }

        return KafkaManagedWfEvent.builder()
            .withKafkaOffset(kmo.getKafkaOffset())
            .withImageKey(kmo.getImageKey())
            .withPartition(kmo.getPartition())
            .withValue(kmo.createWfEvent())
            .withTopic(this.topicEvent)
            .build();
    }

    private void processError(String imgId, RecordMetadata r, Exception e) {
        if (e != null) {
            BeanProcessIncomingFile.LOGGER
                .error("Error detected when trying to send object {}  : {}", imgId, ExceptionUtils.getStackTrace(e));
        }
    }

    private void process(String imgId, RecordMetadata r, Exception e) {
        if (e == null) {
            BeanProcessIncomingFile.LOGGER.info("[EVENT][{}] send image to {} ", imgId, r.toString());
        } else {
            BeanProcessIncomingFile.LOGGER
                .error("Error detected when trying to send image {}  : {}", imgId, ExceptionUtils.getStackTrace(e));
        }
    }

    private void merge(Map<TopicPartition, OffsetAndMetadata> r, Map<TopicPartition, OffsetAndMetadata> t) {
        KafkaUtils.merge(r, t);
    }

    private void updateMapOfOffset(
        Map<TopicPartition, OffsetAndMetadata> mapOfOffset,
        KafkaManagedObject fileToProcess
    ) {
        KafkaUtils.updateMapOfOffset(
            mapOfOffset,
            fileToProcess,
            (f) -> f.getPartition(),
            (f) -> this.topicDupFilteredFile,
            (f) -> f.getKafkaOffset());
    }

    private void startTransactionForRecords(int i) {
        this.producerForTransactionPublishingOnExifOrImageTopic.beginTransaction();
        BeanProcessIncomingFile.LOGGER.debug("Start processing {} file records ", i);
    }

    protected Stream<GenericKafkaManagedObject<?>> toKafkaManagedObject(ConsumerRecord<String, FileToProcess> rec) {
        final Optional<Collection<IFD>> optionalFDs = this.beanFileMetadataExtractor.readIFDs(rec.key());
        final Optional<Stream<GenericKafkaManagedObject<?>>> kmoAsStreamed = optionalFDs
            .map((ifd) -> this.toStreamOfKMO(ifd, rec));
        kmoAsStreamed.ifPresentOrElse(
            (k) -> {},
            () -> BeanProcessIncomingFile.LOGGER.warn(
                "Unable to get IFDs for file = {}, at offset = {}, topic = {} ",
                rec.value(),
                rec.offset(),
                rec.topic()));
        return kmoAsStreamed.orElseGet(() -> Stream.empty());
    }

    protected Stream<GenericKafkaManagedObject<?>> toStreamOfKMO(
        Collection<IFD> ifd,
        ConsumerRecord<String, FileToProcess> rec
    ) {
        List<TiffFieldAndPath> optionalParameters = IFD.tiffFieldsAsStream(ifd.stream())
            .filter((i) -> this.checkIfOptionalParametersArePresent(i))
            .collect(Collectors.toList());
        Stream<GenericKafkaManagedObject<?>> streamOfDefaultOptionalParameters = Stream.empty();
        if (optionalParameters.size() < 4) {
            List<GenericKafkaManagedObject<?>> optionalParametersList = new ArrayList<>(optionalParameters.size());
            this.updateListOfOptionalParameter(
                rec,
                optionalParameters,
                optionalParametersList,
                BeanProcessIncomingFile.EXIF_COPYRIGHT,
                BeanProcessIncomingFile.EXIF_COPYRIGHT_PATH);
            this.updateListOfOptionalParameter(
                rec,
                optionalParameters,
                optionalParametersList,
                BeanProcessIncomingFile.EXIF_ARTIST,
                BeanProcessIncomingFile.EXIF_ARTIST_PATH);
            this.updateListOfOptionalParameter(
                rec,
                optionalParameters,
                optionalParametersList,
                BeanProcessIncomingFile.EXIF_LENS,
                BeanProcessIncomingFile.EXIF_LENS_PATH);
            this.updateListOfOptionalParameter(
                rec,
                optionalParameters,
                optionalParametersList,
                BeanProcessIncomingFile.SONY_EXIF_LENS,
                BeanProcessIncomingFile.SONY_EXIF_LENS_PATH,
                FieldType.UNKNOWN,
                BeanProcessIncomingFile.DEFAULT_SONY_LENS_NOT_FOUND_FOR_OPTIONAL_PARAMETER);
            streamOfDefaultOptionalParameters = optionalParametersList.stream();
        } else {
            optionalParameters.clear();
        }
        List<IFD> foundImages = this.getSortedByLengthImages(ifd, rec);
        Stream<GenericKafkaManagedObject<ThumbImageToSend>> imagesToSend = foundImages.stream()
            .peek(
                (i) -> BeanProcessIncomingFile.LOGGER.info(
                    "[EVENT][{}] While extracting info, found JPEG IMAGE : {} - length is {} ",
                    rec.key(),
                    i.getCurrentImageNumber(),
                    i.getJpegImage().length))
            .map(
                (i) -> ThumbImageToSend.builder()
                    .withImageKey(rec.key())
                    .withJpegImage(i.getJpegImage())
                    .withPath(i.getPath())
                    .withCurrentNb(i.getCurrentImageNumber())
                    .build())
            .map(
                (thumbImage) -> KafkaManagedThumbImage.builder()
                    .withKafkaOffset(rec.offset())
                    .withPartition(rec.partition())
                    .withImageKey(rec.key())
                    .withValue(thumbImage)
                    .withObjectKey(
                        KeysBuilder.topicThumbKeyBuilder()
                            .withOriginalImageKey(thumbImage.getImageKey())
                            .withPathInExifTags(thumbImage.getPath())
                            .withThumbNb(thumbImage.getCurrentNb())
                            .build())
                    .withTopic(this.topicThumb)
                    .build());
        Stream<GenericKafkaManagedObject<?>> tiffFields = IFD.tiffFieldsAsStream(ifd.stream())
            .map(
                (tfp) -> KafkaManagedTiffField.builder()
                    .withKafkaOffset(rec.offset())
                    .withPartition(rec.partition())
                    .withValue(tfp)
                    .withTopic(this.topicExif)
                    .withImageKey(rec.key())
                    .build())
            .map((kmtf) -> this.buildExchangedTiffData(kmtf));
        return Stream.concat(imagesToSend, Stream.concat(tiffFields, streamOfDefaultOptionalParameters));
    }

    protected List<IFD> getSortedByLengthImages(Collection<IFD> ifd, ConsumerRecord<String, FileToProcess> rec) {
        List<IFD> foundImages = IFD.ifdsAsStream(ifd)
            .filter((i) -> i.imageIsPresent())
            .collect((Collectors.toList()));
        foundImages.sort((a, b) -> b.getJpegImage().length - a.getJpegImage().length);
        for (int k = 0; k < foundImages.size(); k++) {
            if ((k + 1) != foundImages.get(k)
                .getCurrentImageNumber()) {
                BeanProcessIncomingFile.LOGGER.info(
                    "[EVENT][{}]change number of current image number new is : {}, previous is {} ",
                    rec.key(),
                    (k + 1),
                    foundImages.get(k)
                        .getCurrentImageNumber());
            }
            foundImages.get(k)
                .setCurrentImageNumber(k + 1);
        }
        return foundImages;
    }

    protected void updateListOfOptionalParameter(
        ConsumerRecord<String, FileToProcess> rec,
        List<TiffFieldAndPath> optionalParameters,
        List<GenericKafkaManagedObject<?>> optionalParametersList,
        short tag,
        short[] path
    ) {
        boolean exifCopyrightIsPresent = optionalParameters.stream()
            .filter(
                (t) -> (t.getTiffField()
                    .getTag()
                    .getValue() == tag) && (Objects.deepEquals(t.getPath(), path)))
            .findFirst()
            .isPresent();
        if (!exifCopyrightIsPresent) {
            optionalParametersList.add(
                this.buildExchangedTiffDataForOptionalParameter(
                    KafkaManagedTiffField.builder()
                        .withKafkaOffset(rec.offset())
                        .withPartition(rec.partition())
                        .withTopic(this.topicExif)
                        .withImageKey(rec.key())
                        .build(),
                    rec.key(),
                    path,
                    tag,
                    BeanProcessIncomingFile.NOT_FOUND_FOR_OPTIONAL_PARAMETER,
                    FieldType.ASCII,
                    -1));
        }
    }

    protected void updateListOfOptionalParameter(
        ConsumerRecord<String, FileToProcess> rec,
        List<TiffFieldAndPath> optionalParameters,
        List<GenericKafkaManagedObject<?>> optionalParametersList,
        short tag,
        short[] path,
        FieldType field,
        int[] defaultValue
    ) {
        boolean exifCopyrightIsPresent = optionalParameters.stream()
            .filter(
                (t) -> (t.getTiffField()
                    .getTag()
                    .getValue() == tag) && (Objects.deepEquals(t.getPath(), path)))
            .findFirst()
            .isPresent();
        if (!exifCopyrightIsPresent) {
            optionalParametersList.add(
                this.buildExchangedTiffDataForOptionalParameter(
                    KafkaManagedTiffField.builder()
                        .withKafkaOffset(rec.offset())
                        .withPartition(rec.partition())
                        .withTopic(this.topicExif)
                        .withImageKey(rec.key())
                        .build(),
                    rec.key(),
                    path,
                    tag,
                    defaultValue,
                    field,
                    -1));
        }
    }

    private boolean checkIfOptionalParametersArePresent(TiffFieldAndPath ifdFieldAndPath) {

        short currentTag = ifdFieldAndPath.getTiffField()
            .getTag()
            .getValue();
        return ((currentTag == BeanProcessIncomingFile.EXIF_COPYRIGHT)
            && (Objects.deepEquals(ifdFieldAndPath.getPath(), BeanProcessIncomingFile.EXIF_COPYRIGHT_PATH)))
            || ((currentTag == BeanProcessIncomingFile.SONY_EXIF_LENS)
                && (Objects.deepEquals(ifdFieldAndPath.getPath(), BeanProcessIncomingFile.SONY_EXIF_LENS_PATH)))
            || ((currentTag == BeanProcessIncomingFile.EXIF_ARTIST)
                && (Objects.deepEquals(ifdFieldAndPath.getPath(), BeanProcessIncomingFile.EXIF_ARTIST_PATH)));
    }

    protected GenericKafkaManagedObject<?> buildExchangedTiffDataForOptionalParameter(
        GenericKafkaManagedObject<?> kmtf,
        String imageKey,
        short[] path,
        short tag,
        byte[] defaultValue,
        FieldType fieldType,
        int tiffNumber
    ) {
        String tiffKey = KeysBuilder.topicExifKeyBuilder()
            .withOriginalImageKey(imageKey)
            .withTiffId(tag)
            .withPath(path)
            .build();

        ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
        builder.withImageId(imageKey)
            .withKey(tiffKey)
            .withTag(tag)
            .withLength(defaultValue.length)
            .withFieldType(fieldType)
            .withIntId(tiffNumber)
            .withDataAsByte(defaultValue)
            .withDataId(KeysBuilder.buildKeyForExifData(imageKey, tag, path))
            .withPath(path);
        ExchangedTiffData etd = builder.build();
        return KafkaManagedExchangedTiffData.builder()
            .withValue(etd)
            .withImageKey(kmtf.getImageKey())
            .withKafkaOffset(kmtf.getKafkaOffset())
            .withPartition(kmtf.getPartition())
            .withObjectKey(kmtf.getImageKey())
            .withTopic(kmtf.getTopic())
            .build();

    }

    protected GenericKafkaManagedObject<?> buildExchangedTiffDataForOptionalParameter(
        GenericKafkaManagedObject<?> kmtf,
        String imageKey,
        short[] path,
        short tag,
        int[] defaultValue,
        FieldType fieldType,
        int tiffNumber
    ) {
        String tiffKey = KeysBuilder.topicExifKeyBuilder()
            .withOriginalImageKey(imageKey)
            .withTiffId(tag)
            .withPath(path)
            .build();

        ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
        builder.withImageId(imageKey)
            .withKey(tiffKey)
            .withTag(tag)
            .withLength(defaultValue.length)
            .withFieldType(fieldType)
            .withIntId(tiffNumber)
            .withDataAsInt(defaultValue)
            .withDataId(KeysBuilder.buildKeyForExifData(imageKey, tag, path))
            .withPath(path);
        ExchangedTiffData etd = builder.build();
        return KafkaManagedExchangedTiffData.builder()
            .withValue(etd)
            .withImageKey(kmtf.getImageKey())
            .withKafkaOffset(kmtf.getKafkaOffset())
            .withPartition(kmtf.getPartition())
            .withObjectKey(kmtf.getImageKey())
            .withTopic(kmtf.getTopic())
            .build();

    }

    protected GenericKafkaManagedObject<?> buildExchangedTiffData(GenericKafkaManagedObject<?> kmo) {
        if (kmo instanceof KafkaManagedTiffField) {
            KafkaManagedTiffField kmtf = (KafkaManagedTiffField) kmo;
            TiffFieldAndPath f = kmtf.getValue();

            short[] path = f.getPath();

            String tiffKey = KeysBuilder.topicExifKeyBuilder()
                .withOriginalImageKey(kmtf.getImageKey())
                .withTiffId(
                    f.getTiffField()
                        .getTagValue())
                .withPath(path)
                .build();
            if (BeanProcessIncomingFile.LOGGER.isDebugEnabled()) {
                try {
                    BeanProcessIncomingFile.LOGGER.debug(
                        "[EVENT][{}] publishing an exif {} ",
                        tiffKey,
                        this.exifService.toString(
                            f.getTiffField()
                                .getIfdTagParent(),
                            f.getTiffField()
                                .getTag(),
                            f.getTiffField()
                                .getData()));
                } catch (Exception e) {
                    BeanProcessIncomingFile.LOGGER.debug(
                        "Unable to process {} - {} - {} ",
                        f.getTiffField()
                            .getTag(),
                        f.getTiffField()
                            .getIfdTagParent(),
                        f.getTiffField()
                            .getData());
                }
            }
            ExchangedTiffData.Builder builder = ExchangedTiffData.builder();
            Object internalData = f.getTiffField()
                .getData();
            if (internalData instanceof int[]) {
                builder.withDataAsInt((int[]) internalData);
            } else if (internalData instanceof short[]) {
                builder.withDataAsShort((short[]) internalData);
            } else if (internalData instanceof byte[]) {
                builder.withDataAsByte((byte[]) internalData);
            } else if (internalData instanceof String) {
                try {
                    builder.withDataAsByte(((String) internalData).getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    BeanProcessIncomingFile.LOGGER.error("Error", e);
                }
            } else {
                throw new IllegalArgumentException();
            }
            builder.withImageId(kmtf.getImageKey())
                .withKey(tiffKey)
                .withTag(
                    f.getTiffField()
                        .getTagValue())
                .withLength(
                    f.getTiffField()
                        .getLength())
                .withFieldType(
                    FieldType.fromShort(
                        f.getTiffField()
                            .getFieldType()))
                .withIntId(f.getTiffNumber())
                .withDataId(
                    KeysBuilder.buildKeyForExifData(
                        kmtf.getImageKey(),
                        f.getTiffField()
                            .getTagValue(),
                        f.getPath()))
                .withPath(path);
            ExchangedTiffData etd = builder.build();
            return KafkaManagedExchangedTiffData.builder()
                .withValue(etd)
                .withImageKey(kmtf.getImageKey())
                .withKafkaOffset(kmtf.getKafkaOffset())
                .withPartition(kmtf.getPartition())
                .withObjectKey(kmtf.getImageKey())
                .withTopic(kmtf.getTopic())
                .build();
        }
        return kmo;
    }

    private void createInitEvent(String parentDataId, List<GenericKafkaManagedObject<? extends WfEvent>> list) {
        int nbOfElements = list.size();
        if (list.size() > 0) {
            GenericKafkaManagedObject<?> elem = list.get(0);
            list.add(
                0,
                KafkaManagedWfEvent.builder()
                    .withKafkaOffset(elem.getKafkaOffset())
                    .withImageKey(elem.getImageKey())
                    .withPartition(elem.getPartition())
                    .withValue(
                        WfEventFinal.builder()
                            .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_IMAGE_FILE_READ)
                            .withNbOFExpectedEvents(nbOfElements + 2 + 1)
                            .withParentDataId(parentDataId)
                            .withDataId(elem.getImageKey())
                            .withImgId(elem.getImageKey())
                            .build())
                    .withTopic(this.topicEvent)
                    .build());
            list.add(
                0,
                KafkaManagedWfEvent.builder()
                    .withKafkaOffset(elem.getKafkaOffset())
                    .withImageKey(elem.getImageKey())
                    .withPartition(elem.getPartition())
                    .withValue(
                        WfEventInitial.builder()
                            .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_IMAGE_FILE_READ)
                            .withParentDataId(parentDataId)
                            .withNbOfInitialEvents(nbOfElements + 1)
                            .withDataId(elem.getImageKey())
                            .withImgId(elem.getImageKey())
                            .build())
                    .withTopic(this.topicEvent)
                    .build());
        }
    }
}
