package com.gs.photo.workflow.extimginfo.impl;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import com.gs.instrumentation.KafkaSpy;
import com.gs.instrumentation.TimedBean;
import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.impl.KafkaUtils;
import com.gs.photo.common.workflow.internal.GenericKafkaManagedObject;
import com.gs.photo.common.workflow.internal.KafkaManagedObject;
import com.gs.photo.common.workflow.internal.KafkaManagedWfEvent;
import com.gs.photo.common.workflow.ports.IIgniteDAO;
import com.gs.photo.workflow.extimginfo.IFileMetadataExtractor;
import com.gs.photo.workflow.extimginfo.IProcessIncomingFiles;
import com.gs.photo.workflow.extimginfo.config.SpecificApplicationProperties;
import com.gs.photo.workflow.extimginfo.ports.IAccessDirectlyFile;
import com.gs.photos.workflow.extimginfo.metadata.IFD;
import com.gs.photos.workflow.extimginfo.metadata.TiffFieldAndPath;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseImageAsByteArray;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventError;
import com.workflow.model.events.WfEventFinal;
import com.workflow.model.events.WfEventInitial;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;

@Service
@KafkaSpy
@TimedBean
public class BeanProcessIncomingFile implements IProcessIncomingFiles {
    protected static final InheritableThreadLocal<MeterRegistry> local                                = new InheritableThreadLocal<MeterRegistry>();
    private static final ExecutorService                         NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR = Executors
        .newVirtualThreadPerTaskExecutor();
    protected static final byte[]                                NOT_FOUND_FOR_OPTIONAL_PARAMETER;
    protected static final int[]                                 DEFAULT_SONY_LENS_NOT_FOUND_FOR_OPTIONAL_PARAMETER;
    protected static final short                                 EXIF_COPYRIGHT                       = (short) 0x8298;
    protected static final short                                 EXIF_ARTIST                          = (short) 0x13B;
    protected static final short[]                               EXIF_COPYRIGHT_PATH                  = { (short) 0 };
    protected static final short[]                               EXIF_ARTIST_PATH                     = { (short) 0 };
    protected static final short                                 SONY_EXIF_LENS                       = (short) 0xB027;
    protected static final short                                 EXIF_LENS                            = (short) 0xA434;
    protected static final short[]                               SONY_EXIF_LENS_PATH                  = {
            (short) 0, (short) 0x8769, (short) 0x927c };
    protected static final short[]                               EXIF_LENS_PATH                       = {
            (short) 0, (short) 0x8769 };

    record EventsAndGenericKafkaManagedObject(
        WfEvents wfEvents,
        Collection<GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent>> genericKafkaManagedObject
    ) {}

    record FileToProcessInformation(
        Optional<byte[]> image,
        FileToProcess fileToProcess
    ) {}

    static {
        try {

            NOT_FOUND_FOR_OPTIONAL_PARAMETER = "<NOT FOUND>".getBytes("UTF-8");
            DEFAULT_SONY_LENS_NOT_FOUND_FOR_OPTIONAL_PARAMETER = new int[] { -1 };

        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
    protected static Logger                             LOGGER = LoggerFactory.getLogger(BeanProcessIncomingFile.class);

    @Autowired
    protected ThreadPoolTaskExecutor                    threadPoolTaskExecutor;

    @Autowired
    protected IExifService                              exifService;

    @Autowired
    protected IKafkaProperties                          kafkaProperties;

    @Autowired
    protected IFileMetadataExtractor                    beanFileMetadataExtractor;

    @Autowired
    protected Supplier<Consumer<String, FileToProcess>> kafkaConsumerSupplierForFileToProcessValue;

    @Autowired
    protected Supplier<Producer<String, HbaseData>>     producerSupplierForTransactionPublishingOnExifTopic;

    protected int                                       batchSizeForParallelProcessingIncomingRecords;
    @Autowired
    protected SpecificApplicationProperties             specificApplicationProperties;

    @Autowired
    protected IIgniteDAO                                iIgniteDAO;

    @Autowired
    protected IAccessDirectlyFile                       accessDirectlyFile;

    @Autowired
    protected ObservationRegistry                       observationRegistry;

    @Autowired
    protected MeterRegistry                             meterRegistry;

    @Override
    @Timed
    public void start() { this.threadPoolTaskExecutor.execute(() -> this.processInputFile()); }

    protected void processInputFile() {

        boolean ready = true;
        BeanProcessIncomingFile.LOGGER.info("Waiting for ignite...");
        ready = this.waitForIgnite();
        while (ready) {
            try (
                Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue = this.kafkaConsumerSupplierForFileToProcessValue
                    .get();
                Producer<String, HbaseData> producerForTransactionPublishingOnExifOrImageTopic = this.producerSupplierForTransactionPublishingOnExifTopic
                    .get()) {
                consumerForTopicWithFileToProcessValue.subscribe(
                    Collections.singleton(
                        this.kafkaProperties.getTopics()
                            .topicDupFilteredFile()));
                producerForTransactionPublishingOnExifOrImageTopic.initTransactions();
                BeanProcessIncomingFile.LOGGER.info("Starting process input file...");
                try {
                    ready = this.doProcessFile(
                        consumerForTopicWithFileToProcessValue,
                        producerForTransactionPublishingOnExifOrImageTopic);
                } catch (Throwable e) {
                    BeanProcessIncomingFile.LOGGER.error("An error is raised ", e);
                    ready = !((e instanceof InterruptedException) || (e.getCause() instanceof InterruptedException));
                }
            }
            if (ready) {
                BeanProcessIncomingFile.LOGGER.info("Waiting for ignite again...");
                ready = this.waitForIgnite();

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

    protected boolean doProcessFile(
        Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue,
        Producer<String, HbaseData> producerForTransactionPublishingOnExifOrImageTopic
    ) throws Throwable {
        boolean end = false;
        boolean recover = true;
        while (!end) {
            try {
                this.processRecords(
                    consumerForTopicWithFileToProcessValue,
                    producerForTransactionPublishingOnExifOrImageTopic);
            } catch (
                ProducerFencedException |
                OutOfOrderSequenceException |
                AuthorizationException e) {
                BeanProcessIncomingFile.LOGGER.error(" Error - closing ", e);
                recover = false;
                end = true;
            } catch (KafkaException e) {
                // For all other exceptions, just abort the transaction and try again.
                BeanProcessIncomingFile.LOGGER.error(" Error - aborting, trying to recover", e);
                producerForTransactionPublishingOnExifOrImageTopic.abortTransaction();
                end = true;
                recover = true;
            } catch (Exception e) {
                if (!(e.getCause() instanceof InterruptedException)) {
                    BeanProcessIncomingFile.LOGGER.error("Unexpected error - closing  ", e);
                }
                end = true;
                recover = false;
            }
        }
        return recover;
    }

    @Timed
    @KafkaSpy
    private void processRecords(
        Consumer<String, FileToProcess> consumerForTopicWithFileToProcessValue,
        Producer<String, HbaseData> producerForTransactionPublishingOnExifOrImageTopic
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsets = KafkaUtils
            .buildParallelKafkaBatchStreamPerTopicAndPartition(
                producerForTransactionPublishingOnExifOrImageTopic,
                consumerForTopicWithFileToProcessValue,
                this.specificApplicationProperties.getKafkaPollTimeInMillisecondes(),
                this.specificApplicationProperties.getBatchSizeForParallelProcessingIncomingRecords(),
                true,
                (i, p) -> this.startRecordsProcessing(i, p))
            .map((rec) -> this.asyncExtractMetaDataFromImage(rec))
            .map((kmo) -> this.asyncSendFoundTiffObjects(kmo, producerForTransactionPublishingOnExifOrImageTopic))
            .map(CompletableFuture::join)
            .flatMap(t -> t.stream())
            .collect(Collectors.groupingByConcurrent(GenericKafkaManagedObject::getImageKey))
            .entrySet()
            .stream()
            .map(e -> this.toEventsAndGenericKafkaManagedObject(e.getKey(), e.getValue()))
            .map(e -> this.asyncSendEvents(e, producerForTransactionPublishingOnExifOrImageTopic))
            .map(e -> this.asyncCleanIgniteCache(e))
            .map(CompletableFuture::join)
            .flatMap(
                t -> t.genericKafkaManagedObject()
                    .stream())
            .collect(
                () -> new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>(),
                (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                (r, t) -> this.merge(r, t));

        producerForTransactionPublishingOnExifOrImageTopic
            .sendOffsetsToTransaction(offsets, consumerForTopicWithFileToProcessValue.groupMetadata());
        producerForTransactionPublishingOnExifOrImageTopic.commitTransaction();
    }

    private EventsAndGenericKafkaManagedObject toEventsAndGenericKafkaManagedObject(
        String key,
        List<GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent>> objectsList
    ) {
        return new EventsAndGenericKafkaManagedObject(WfEvents.builder()
            .withDataId(key)
            .withProducer("PRODUCER_IMG_METADATA")
            .withEvents(
                objectsList.stream()
                    .map(GenericKafkaManagedObject::getValue)
                    .map(x -> x.get())
                    .collect(Collectors.toList()))
            .build(), objectsList);
    }

    private CompletableFuture<EventsAndGenericKafkaManagedObject> asyncCleanIgniteCache(
        CompletableFuture<EventsAndGenericKafkaManagedObject> cf
    ) {
        return cf.thenApply((kmo) -> this.cleanIgniteCache(kmo));
    }

    @Timed
    private EventsAndGenericKafkaManagedObject cleanIgniteCache(EventsAndGenericKafkaManagedObject kmo) {
        this.cleanIgniteCache(
            kmo.genericKafkaManagedObject()
                .stream()
                .map(t -> t.getImageKey())
                .toArray(String[]::new));

        return kmo;
    }

    private void cleanIgniteCache(String[] img) {
        final Set<String> keys = new HashSet<>(Arrays.asList(img));
        BeanProcessIncomingFile.LOGGER.info("[EVENT][{}][{}]Deleting in cache ", keys, Thread.currentThread());
        this.iIgniteDAO.delete(keys);
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

    private CompletableFuture<EventsAndGenericKafkaManagedObject> asyncSendEvents(
        EventsAndGenericKafkaManagedObject wfe,
        Producer<String, HbaseData> producerForTransactionPublishingOnExifOrImageTopic
    ) {

        return CompletableFuture.supplyAsync(() -> {
            return this.doSendEvents(wfe, producerForTransactionPublishingOnExifOrImageTopic);
        }, BeanProcessIncomingFile.NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR);
    }

    @Timed
    private EventsAndGenericKafkaManagedObject doSendEvents(
        EventsAndGenericKafkaManagedObject wfe,
        Producer<String, HbaseData> producerForTransactionPublishingOnExifOrImageTopic
    ) {
        try {
            ProducerRecord<String, HbaseData> pr = new ProducerRecord<>(this.kafkaProperties.getTopics()
                .topicEvent(),
                wfe.wfEvents()
                    .getDataId(),
                wfe.wfEvents());
            BeanProcessIncomingFile.LOGGER.info(
                "EVENT[{}][{}] nb of events sent {}",
                wfe.wfEvents()
                    .getDataId(),
                Thread.currentThread(),
                wfe.wfEvents()
                    .getEvents()
                    .size());
            producerForTransactionPublishingOnExifOrImageTopic.send(pr);
            return wfe;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Timed
    private Collection<GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent>> processKmoStream(
        Collection<GenericKafkaManagedObject<?, ?>> kmoStream,
        Producer<String, HbaseData> producerForTransactionPublishingOnExifOrImageTopic
    ) {
        Collection<GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent>> returnValue = kmoStream.stream()
            .map(kmo -> this.sendDataAndBuildWfEvent(producerForTransactionPublishingOnExifOrImageTopic, kmo))
            .collect(Collectors.toList());
        return returnValue;
    }

    private GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent> sendDataAndBuildWfEvent(
        Producer<String, HbaseData> producerForTransactionPublishingOnExifOrImageTopic,
        GenericKafkaManagedObject<?, ?> kmo
    ) {
        GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent> defaultValue = this.buildEvent(
            kmo,
            WfEventError.builder()
                .withDataId(kmo.getObjectKey())
                .withError("File not found")
                .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_IMAGE_FILE_READ)
                .build());
        Optional<GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent>> retValue = Optional
            .ofNullable(switch (kmo) {
                case KafkaManagedThumbImage kti -> kti.getObjectToSend()
                    .get()
                    .getJpegImage();
                case KafkaManagedExchangedTiffData ketf -> ketf.getObjectToSend()
                    .get();
                case GenericKafkaManagedObject<?, ?> gkmo -> null;
            })
            .map(o -> {
                ProducerRecord<String, HbaseData> pr = new ProducerRecord<>(kmo.getTopic(), kmo.getObjectKey(), o);
                producerForTransactionPublishingOnExifOrImageTopic
                    .send(pr, (r, e) -> this.processErrorWhileSending(kmo.getObjectKey(), r, e));
                return this.buildEvent(kmo);
            });
        return retValue.orElse(defaultValue);
    }

    private GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent> buildEvent(
        GenericKafkaManagedObject<? extends HbaseData, ? extends WfEvent> kmo,
        WfEvent ev
    ) {
        return KafkaManagedWfEvent.builder()
            .withKafkaOffset(kmo.getKafkaOffset())
            .withImageKey(kmo.getImageKey())
            .withPartition(kmo.getPartition())
            .withValue(ev)
            .withTopic(
                this.kafkaProperties.getTopics()
                    .topicEvent())
            .build();

    }

    private GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent> buildEvent(
        GenericKafkaManagedObject<? extends HbaseData, ? extends WfEvent> kmo
    ) {
        return KafkaManagedWfEvent.builder()
            .withKafkaOffset(kmo.getKafkaOffset())
            .withImageKey(kmo.getImageKey())
            .withPartition(kmo.getPartition())
            .withValue(kmo.createWfEvent())
            .withTopic(
                this.kafkaProperties.getTopics()
                    .topicEvent())
            .build();
    }

    private CompletableFuture<Collection<GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent>>> asyncSendFoundTiffObjects(
        CompletableFuture<Collection<GenericKafkaManagedObject<?, ?>>> kmoFuture,
        Producer<String, HbaseData> producerForTransactionPublishingOnExifOrImageTopic
    ) {
        return kmoFuture.thenApply(
            (kmoStream) -> this.processKmoStream(kmoStream, producerForTransactionPublishingOnExifOrImageTopic));
    }

    private void processErrorWhileSending(String imgId, RecordMetadata r, Exception e) {
        if (e != null) {
            BeanProcessIncomingFile.LOGGER
                .error("Error detected when trying to send object {}  : {}", imgId, ExceptionUtils.getStackTrace(e));
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
            (f) -> this.kafkaProperties.getTopics()
                .topicDupFilteredFile(),
            (f) -> f.getKafkaOffset());
    }

    private void startRecordsProcessing(
        int i,
        Producer<String, HbaseData> producerForTransactionPublishingOnExifOrImageTopic
    ) {
        producerForTransactionPublishingOnExifOrImageTopic.beginTransaction();
        BeanProcessIncomingFile.LOGGER.info("Start processing {} file records ", i);
    }

    protected Optional<byte[]> retrieveDirectly(FileToProcess fileToProcess) {

        BeanProcessIncomingFile.LOGGER.warn(
            "[EVENT][{}][{}]Unable to get key Ignite - get direct value from source {}",
            fileToProcess.getImageId(),
            Thread.currentThread(),
            fileToProcess);
        try {
            Optional<byte[]> retValue = this.accessDirectlyFile.readFirstBytesOfFileRetry(fileToProcess);
            return retValue;
        } catch (Exception e) {
            BeanProcessIncomingFile.LOGGER.warn(
                "[EVENT][{}]Unable to get key Ignite for {},  unexpected error {}",
                fileToProcess.getImageId(),
                fileToProcess,
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        } finally {
            BeanProcessIncomingFile.LOGGER
                .info("[EVENT][{}][{}]Retrieved directly {}  ", fileToProcess.getImageId(), Thread.currentThread());
        }
    }

    protected CompletableFuture<Collection<GenericKafkaManagedObject<?, ?>>> asyncExtractMetaDataFromImage(
        ConsumerRecord<String, FileToProcess> rec
    ) {

        BeanProcessIncomingFile.LOGGER.info(
            "[EVENT][{}][{}] Reading IFDs for file = {}, at offset = {}, topic = {} ",
            Thread.currentThread(),
            rec.key(),
            rec.value(),
            rec.offset(),
            rec.topic());
        final String imageId = rec.value()
            .getImageId();
        final FileToProcess value = rec.value();
        CompletableFuture<Collection<GenericKafkaManagedObject<?, ?>>> future = CompletableFuture
            .supplyAsync(
                () -> new FileToProcessInformation(this.iIgniteDAO.get(imageId), value),
                BeanProcessIncomingFile.NEW_VIRTUAL_THREAD_PER_TASK_EXECUTOR)
            .thenApply(v -> this.ensureImageIsPresent(v))
            .thenApply(v -> this.readIfds(v))
            .thenApply(v -> this.toACollectionOfKMOs(rec, v));
        return future;
    }

    @Timed
    private List<GenericKafkaManagedObject<?, ?>> toACollectionOfKMOs(
        ConsumerRecord<String, FileToProcess> rec,
        Optional<Collection<IFD>> v
    ) {
        if (v.isEmpty()) {
            return List.of(
                GenericKafkaManagedObject.genericKafkaManagedObjectBuilder()
                    .withImageKey(rec.key())
                    .withKafkaOffset(rec.offset())
                    .withPartition(rec.partition())
                    .build());
        }
        return v.stream()
            .flatMap(t -> this.toStreamOfKMO(t, rec))
            .collect(Collectors.toList());
    }

    @Timed
    private Optional<Collection<IFD>> readIfds(FileToProcessInformation v) {
        return this.beanFileMetadataExtractor.readIFDs(v.image(), v.fileToProcess());
    }

    @Timed
    protected FileToProcessInformation ensureImageIsPresent(FileToProcessInformation v) {
        return v.image()
            .isPresent() ? v
                : new FileToProcessInformation(this.retrieveDirectly(v.fileToProcess()), v.fileToProcess());
    }

    protected Stream<GenericKafkaManagedObject<?, ?>> toStreamOfKMO(
        Collection<IFD> ifd,
        ConsumerRecord<String, FileToProcess> rec
    ) {
        List<TiffFieldAndPath> optionalParameters = IFD.tiffFieldsAsStream(ifd.stream())
            .filter((i) -> this.checkIfOptionalParametersArePresent(i))
            .collect(Collectors.toList());
        Stream<GenericKafkaManagedObject<?, ?>> streamOfDefaultOptionalParameters = Stream.empty();
        if (optionalParameters.size() < 4) {
            List<GenericKafkaManagedObject<?, ?>> optionalParametersList = new ArrayList<>(optionalParameters.size());
            this.updateListOfMandatoryParameter(
                rec,
                optionalParameters,
                optionalParametersList,
                BeanProcessIncomingFile.EXIF_COPYRIGHT,
                BeanProcessIncomingFile.EXIF_COPYRIGHT_PATH);
            this.updateListOfMandatoryParameter(
                rec,
                optionalParameters,
                optionalParametersList,
                BeanProcessIncomingFile.EXIF_ARTIST,
                BeanProcessIncomingFile.EXIF_ARTIST_PATH);
            this.updateListOfMandatoryParameter(
                rec,
                optionalParameters,
                optionalParametersList,
                BeanProcessIncomingFile.EXIF_LENS,
                BeanProcessIncomingFile.EXIF_LENS_PATH);
            this.updateListOfMandatoryParameter(
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
        Stream<GenericKafkaManagedObject<ThumbImageToSend, WfEvent>> imagesToSend = foundImages.stream()
            .peek(
                (i) -> BeanProcessIncomingFile.LOGGER.info(
                    "[EVENT][{}][{}] While extracting info, found JPEG IMAGE : {} - length is {} ",
                    rec.key(),
                    Thread.currentThread(),
                    i.getCurrentImageNumber(),
                    i.getJpegImage().length))
            .map(
                (i) -> ThumbImageToSend.builder()
                    .withImageKey(rec.key())
                    .withJpegImage(
                        HbaseImageAsByteArray.builder()
                            .withDataAsByte(i.getJpegImage())
                            .build())
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
                    .withTopic(
                        this.kafkaProperties.getTopics()
                            .topicThumb())
                    .build());
        Stream<GenericKafkaManagedObject<?, ?>> tiffFields = IFD.tiffFieldsAsStream(ifd.stream())
            .map(
                (tfp) -> KafkaManagedTiffField.builder()
                    .withKafkaOffset(rec.offset())
                    .withPartition(rec.partition())
                    .withValue(tfp)
                    .withTopic(
                        this.kafkaProperties.getTopics()
                            .topicExif())
                    .withImageKey(rec.key())
                    .build())
            .map((kmtf) -> this.buildExchangedTiffData(kmtf));
        return Stream.concat(imagesToSend, Stream.concat(tiffFields, streamOfDefaultOptionalParameters));
    }

    protected List<IFD> getSortedByLengthImages(Collection<IFD> ifd, ConsumerRecord<String, FileToProcess> rec) {
        List<IFD> foundImages = IFD.ifdsAsStream(ifd)
            .filter((i) -> i.imageIsPresent())
            .filter((x) -> x.getJpegImage().length <= (2 * 1024 * 1024))
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

    protected void updateListOfMandatoryParameter(
        ConsumerRecord<String, FileToProcess> rec,
        List<TiffFieldAndPath> optionalParameters,
        List<GenericKafkaManagedObject<?, ?>> optionalParametersList,
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
                        .withTopic(
                            this.kafkaProperties.getTopics()
                                .topicExif())
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

    protected void updateListOfMandatoryParameter(
        ConsumerRecord<String, FileToProcess> rec,
        List<TiffFieldAndPath> optionalParameters,
        List<GenericKafkaManagedObject<?, ?>> optionalParametersList,
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
                        .withTopic(
                            this.kafkaProperties.getTopics()
                                .topicExif())
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

    protected GenericKafkaManagedObject<?, ?> buildExchangedTiffDataForOptionalParameter(
        GenericKafkaManagedObject<?, ?> kmtf,
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

    protected GenericKafkaManagedObject<?, ?> buildExchangedTiffDataForOptionalParameter(
        GenericKafkaManagedObject<?, ?> kmtf,
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

    protected GenericKafkaManagedObject<?, ?> buildExchangedTiffData(GenericKafkaManagedObject<?, ?> kmo) {
        if (kmo instanceof KafkaManagedTiffField) {
            KafkaManagedTiffField kmtf = (KafkaManagedTiffField) kmo;
            TiffFieldAndPath f = kmtf.getValue()
                .get();

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

    private void createInitEvent(
        String parentDataId,
        List<GenericKafkaManagedObject<? extends WfEvent, ? extends WfEvent>> list
    ) {
        int nbOfElements = list.size();
        if (list.size() > 0) {
            GenericKafkaManagedObject<?, ?> elem = list.get(0);
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
                    .withTopic(
                        this.kafkaProperties.getTopics()
                            .topicEvent())
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
                    .withTopic(
                        this.kafkaProperties.getTopics()
                            .topicEvent())
                    .build());
        }
    }
}
