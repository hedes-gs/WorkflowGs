package com.gs.photo.workflow.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photo.workflow.IIgniteDAO;
import com.gs.photo.workflow.IProcessIncomingFiles;
import com.gs.photo.workflow.TimeMeasurement;
import com.gs.photo.workflow.internal.GenericKafkaManagedObject;
import com.gs.photo.workflow.internal.KafkaManagedObject;
import com.gs.photo.workflow.internal.KafkaManagedWfEvent;
import com.gs.photos.workflow.metadata.IFD;
import com.gs.photos.workflow.metadata.TiffFieldAndPath;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEvents;
import com.workflow.model.files.FileToProcess;

@Component
public class BeanProcessIncomingFile implements IProcessIncomingFiles {

    protected static Logger                   LOGGER = LoggerFactory.getLogger(IProcessIncomingFiles.class);

    @Autowired
    protected IBeanTaskExecutor               beanTaskExecutor;

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

    @Override
    public void init() {
        this.producerForTransactionPublishingOnExifOrImageTopic.initTransactions();
        this.consumerForTopicWithFileToProcessValue.subscribe(Collections.singleton(this.topicDupFilteredFile));
        this.beanTaskExecutor.execute(() -> this.processInputFile());
    }

    protected void processInputFile() {

        BeanProcessIncomingFile.LOGGER.info("Starting process input file...");
        while (true) {
            try (
                TimeMeasurement timeMeasurement = TimeMeasurement.of(
                    "BATCH_PROCESS_FILES",
                    (d) -> BeanProcessIncomingFile.LOGGER.info(" Perf. metrics {}", d),
                    System.currentTimeMillis())) {
                Stream<ConsumerRecord<String, FileToProcess>> filesToCopyStream = KafkaUtils.toStreamV2(
                    200,
                    this.consumerForTopicWithFileToProcessValue,
                    this.batchSizeForParallelProcessingIncomingRecords,
                    true,
                    (i) -> this.startTransactionForRecords(i),
                    timeMeasurement);
                Map<String, List<GenericKafkaManagedObject<?>>> eventsToSend = filesToCopyStream
                    .flatMap((rec) -> this.toKafkaManagedObject(rec))
                    .map((kmo) -> this.send(kmo))
                    .collect(Collectors.groupingByConcurrent(GenericKafkaManagedObject::getImageKey));

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
                        () -> new HashMap<TopicPartition, OffsetAndMetadata>(),
                        (mapOfOffset, t) -> this.updateMapOfOffset(mapOfOffset, t),
                        (r, t) -> this.merge(r, t));

                BeanProcessIncomingFile.LOGGER.info(" {} events are sent ", eventsNumber);
                BeanProcessIncomingFile.LOGGER.info("Offset to commit {} ", offsets.toString());
                this.producerForTransactionPublishingOnExifOrImageTopic.sendOffsetsToTransaction(offsets, this.groupId);
                this.producerForTransactionPublishingOnExifOrImageTopic.commitTransaction();
                this.beanTaskExecutor.execute(() -> this.cleanIgniteCache(eventsToSend.keySet()));
            } catch (IOException e) {
            }
        }
    }

    private void cleanIgniteCache(Set<String> img) { this.iIgniteDAO.delete(img); }

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

    private GenericKafkaManagedObject<?> send(GenericKafkaManagedObject<?> kmo) {
        ProducerRecord<String, Object> pr = new ProducerRecord<>(kmo.getTopic(),
            kmo.getObjectKey(),
            kmo.getObjectToSend());
        this.producerForTransactionPublishingOnExifOrImageTopic.send(pr);
        return KafkaManagedWfEvent.builder()
            .withKafkaOffset(kmo.getKafkaOffset())
            .withImageKey(kmo.getImageKey())
            .withPartition(kmo.getPartition())
            .withValue(kmo.createWfEvent())
            .withTopic(this.topicEvent)
            .build();
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
        BeanProcessIncomingFile.LOGGER.info("Start processing {} file records ", i);
    }

    protected Stream<GenericKafkaManagedObject<?>> toKafkaManagedObject(ConsumerRecord<String, FileToProcess> rec) {
        final Collection<IFD> IFDs = this.beanFileMetadataExtractor.readIFDs(rec.key());
        Stream<GenericKafkaManagedObject<?>> imagesToSend = IFD.ifdsAsStream(IFDs)
            .filter((ifd) -> ifd.imageIsPresent())
            .map(
                (ifd) -> ThumbImageToSend.builder()
                    .withImageKey(rec.key())
                    .withJpegImage(ifd.getJpegImage())
                    .withPath(ifd.getPath())
                    .withCurrentNb(ifd.getCurrentImageNumber())
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
        Stream<GenericKafkaManagedObject<?>> tiffFields = IFD.tiffFieldsAsStream(IFDs.stream())
            .map(
                (tfp) -> KafkaManagedTiffField.builder()
                    .withKafkaOffset(rec.offset())
                    .withPartition(rec.partition())
                    .withValue(tfp)
                    .withTopic(this.topicExif)
                    .withImageKey(rec.key())
                    .build())
            .map((kmtf) -> this.buildExchangedTiffData(kmtf));
        return Stream.concat(imagesToSend, tiffFields);
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
            BeanProcessIncomingFile.LOGGER.debug(
                "[EVENT][{}] publishing an exif {} ",
                tiffKey,
                f.getTiffField()
                    .getData() != null ? f.getTiffField()
                        .getData() : " <null> ");
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

}
