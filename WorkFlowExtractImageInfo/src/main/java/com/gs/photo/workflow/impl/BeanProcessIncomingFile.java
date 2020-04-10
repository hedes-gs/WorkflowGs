package com.gs.photo.workflow.impl;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collector;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.hbase.thirdparty.com.google.common.base.Objects;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photo.workflow.IProcessIncomingFiles;
import com.gs.photos.workflow.metadata.IFD;
import com.gs.photos.workflow.metadata.TiffFieldAndPath;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.FieldType;
import com.workflow.model.builder.KeysBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventStep;
import com.workflow.model.events.WfEvents;

@Component
public class BeanProcessIncomingFile implements IProcessIncomingFiles {

    private static final String        WF_EVENT_CLASS_NAME              = "com.workflow.model.events.WfEvent";

    private static final String        EXCHANGED_TIFF_DATA_CLASS_NAME   = "com.workflow.model.ExchangedTiffData";
    private static final String        THUMB_IMAGE_TO_SEND_CLASS_NAME   = "com.workflow.model.ThumbImageToSend";

    public static final int            NB_OF_THREADS_TO_RECORD_IN_HBASE = 3;

    protected static Logger            LOGGER                           = LoggerFactory
        .getLogger(IProcessIncomingFiles.class);
    protected ExecutorService[]        services;

    @Autowired
    protected IBeanTaskExecutor        beanTaskExecutor;

    @Value("${topic.topicDupFilteredFile}")
    protected String                   topicDupFilteredFile;

    @Value("${topic.topicExif}")
    protected String                   topicExif;

    @Value("${topic.topicThumb}")
    protected String                   topicThumb;

    @Value("${topic.topicEvent}")
    protected String                   topicEvent;

    @Value("${group.id}")
    private String                     groupId;

    @Autowired
    protected IFileMetadataExtractor   beanFileMetadataExtractor;

    @Autowired
    @Qualifier("consumerForTopicWithStringKey")
    protected Consumer<String, String> consumerForTopicWithStringKey;

    @Autowired
    @Qualifier("producerForTransactionPublishingOnExifOrImageTopic")
    protected Producer<String, Object> producerForTransactionPublishingOnExifOrImageTopic;

    private Map<Class<?>, ComputeKey>  toKey;

    private Map<Class<?>, String>      toTopic;

    protected static interface ComputeKey { String getKey(Object t); }

    protected static class DbTaskResult {
        private final TopicPartition                partition;
        private final OffsetAndMetadata             offsetAndMetadata;
        private final Table<String, String, Object> objectsToSend;
        private final Map<String, Integer>          metrics;

        public TopicPartition getPartition() { return this.partition; }

        public OffsetAndMetadata getOffsetAndMetadata() { return this.offsetAndMetadata; }

        public Table<String, String, Object> getObjectsToSend() { return this.objectsToSend; }

        public Map<String, Integer> getMetrics() { return this.metrics; }

        public DbTaskResult(
            TopicPartition partition,
            OffsetAndMetadata offsetAndMetadata,
            Table<String, String, Object> objectsToSend,
            Map<String, Integer> metrics
        ) {
            super();
            this.partition = partition;
            this.offsetAndMetadata = offsetAndMetadata;
            this.objectsToSend = objectsToSend;
            this.metrics = metrics;
        }

    }

    protected final class DbTask implements Callable<DbTaskResult> {
        protected List<ConsumerRecord<String, String>> records;
        protected TopicPartition                       partition;
        protected long                                 lastOffset;

        public DbTask(
            List<ConsumerRecord<String, String>> records,
            TopicPartition partition,
            long offsetToCommit
        ) {
            super();
            this.records = records;
            this.partition = partition;
            this.lastOffset = offsetToCommit + 1;
        }

        @Override
        public DbTaskResult call() throws Exception {
            Table<String, String, Object> objectsToSend;
            Map<String, Integer> metrics = new HashMap<>();
            objectsToSend = this.records.stream()
                .map((r) -> BeanProcessIncomingFile.this.processIncomingRecord(r, metrics))
                .collect(Collector.of(() -> HashBasedTable.create(), (t, u) -> t.putAll(u), (t, u) -> {
                    t.putAll(u);
                    return t;
                }));

            DbTaskResult result = new DbTaskResult(this.partition,
                new OffsetAndMetadata(this.lastOffset + 1),
                objectsToSend,
                metrics);
            return result;
        }

    }

    @Override
    public void init() {
        this.toTopic = new HashMap<Class<?>, String>() {
            private static final long serialVersionUID = 1L;
            {
                this.put(ExchangedTiffData.class, BeanProcessIncomingFile.this.topicExif);
                this.put(WfEvents.class, BeanProcessIncomingFile.this.topicEvent);
                this.put(WfEvent.class, BeanProcessIncomingFile.this.topicEvent);
                this.put(ThumbImageToSend.class, BeanProcessIncomingFile.this.topicThumb);
            }
        };

        this.toKey = new HashMap<Class<?>, ComputeKey>() {
            private static final long serialVersionUID = 1L;
            {
                this.put(ExchangedTiffData.class, (etf) -> ((ExchangedTiffData) etf).getKey());
                this.put(WfEvent.class, (wfe) -> ((WfEvent) wfe).getImgId());
                this.put(
                    ThumbImageToSend.class,
                    (etf) -> KeysBuilder.topicThumbKeyBuilder()
                        .withOriginalImageKey(((ThumbImageToSend) etf).getImageKey())
                        .withPathInExifTags(((ThumbImageToSend) etf).getPath())
                        .withThumbNb(((ThumbImageToSend) etf).getCurrentNb())
                        .build());
            }
        };
        this.services = new ExecutorService[BeanProcessIncomingFile.NB_OF_THREADS_TO_RECORD_IN_HBASE];
        for (int k = 0; k < BeanProcessIncomingFile.NB_OF_THREADS_TO_RECORD_IN_HBASE; k++) {
            this.services[k] = Executors.newFixedThreadPool(5);
        }
        this.beanTaskExecutor.execute(() -> this.processInputFile());
    }

    protected void processInputFile() {
        this.consumerForTopicWithStringKey.subscribe(Collections.singleton(this.topicDupFilteredFile));
        BeanProcessIncomingFile.LOGGER.info("Starting process input file...");
        Collection<Future<DbTaskResult>> futuresList = new ArrayList<>();
        this.producerForTransactionPublishingOnExifOrImageTopic.initTransactions();
        Map<String, Long> nbOfSentElements = new HashMap<String, Long>();
        Map<String, Integer> metrics = new HashMap<>();

        while (true) {
            try {
                nbOfSentElements.clear();
                futuresList.clear();
                metrics.clear();
                ConsumerRecords<String, String> records = this.consumerForTopicWithStringKey
                    .poll(Duration.ofMillis(500));
                if (records != null) {
                    if (!records.isEmpty()) {
                        BeanProcessIncomingFile.LOGGER.info("Processing {} records", records.count());
                        this.producerForTransactionPublishingOnExifOrImageTopic.beginTransaction();
                        this.submitRecordsToGetMsgsToSend(futuresList, records);
                        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
                        futuresList.forEach((f) -> {
                            try {
                                DbTaskResult taskResult = f.get();
                                offsets.put(taskResult.getPartition(), taskResult.getOffsetAndMetadata());
                                taskResult.getObjectsToSend()
                                    .cellSet()
                                    .stream()
                                    .map((c) -> new ProducerRecord<>(c.getRowKey(), c.getColumnKey(), c.getValue()))
                                    .forEach(
                                        (c) -> { this.producerForTransactionPublishingOnExifOrImageTopic.send(c); });
                                metrics.putAll(taskResult.getMetrics());
                            } catch (
                                CommitFailedException |
                                InterruptedException |
                                ExecutionException e) {
                                BeanProcessIncomingFile.LOGGER.warn("Error while commiting ", e);
                            }
                        });
                        this.producerForTransactionPublishingOnExifOrImageTopic
                            .sendOffsetsToTransaction(offsets, this.groupId);
                        this.producerForTransactionPublishingOnExifOrImageTopic.commitTransaction();
                        metrics.entrySet()
                            .forEach((e) -> {
                                BeanProcessIncomingFile.LOGGER.info(
                                    "[EVENT][{}] processed file of hash key {}, sent {} Exif or image values ",
                                    e.getKey(),
                                    e.getKey(),
                                    e.getValue());
                            });
                    }
                } else {
                    break;
                }
            } catch (Exception e) {
                BeanProcessIncomingFile.LOGGER.error("error in processInputFile ", e);
            }
        }
    }

    protected void submitRecordsToGetMsgsToSend(
        Collection<Future<DbTaskResult>> futuresList,
        ConsumerRecords<String, String> records
    ) {
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            List<ConsumerRecord<String, String>> foundRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : partitionRecords) {
                foundRecords.add(record);
            }
            Future<DbTaskResult> f = this.services[partition.partition()
                % BeanProcessIncomingFile.NB_OF_THREADS_TO_RECORD_IN_HBASE].submit(
                    new DbTask(foundRecords,
                        partition,
                        partitionRecords.get(partitionRecords.size() - 1)
                            .offset()));
            futuresList.add(f);
        }
    }

    protected Table<String, String, Object> processIncomingRecord(
        ConsumerRecord<String, String> rec,
        Map<String, Integer> metrics
    ) {
        Table<String, String, Object> retValue = null;
        final String imageKey = rec.key();
        try {
            BeanProcessIncomingFile.LOGGER.info(
                "[EVENT][{}] Processing record [ topic: {}, offset: {}, timestamp: {}, path: ]",
                imageKey,
                rec.topic(),
                rec.offset(),
                rec.timestamp(),
                imageKey);
            Collection<IFD> metaData = this.beanFileMetadataExtractor.readIFDs(imageKey);
            final int nbOfTiffFields = IFD.getNbOfTiffFields(metaData);
            final WfEvents wfEvents = WfEvents.builder()
                .withEvents(new ArrayList<>(nbOfTiffFields))
                .build();

            Table<String, String, Object> objectsToSend = IFD.tiffFieldsAsStream(metaData.stream())
                .map((tiffField) -> this.buildExchangedTiffData(imageKey, tiffField, nbOfTiffFields))
                .flatMap(
                    (tif) -> Arrays.asList(tif, this.buildEvent(tif))
                        .stream())
                .collect(Collector.of(() -> {
                    Table<String, String, Object> table = HashBasedTable.create();
                    table.put(this.topicEvent, imageKey, wfEvents);
                    return table;
                }, (t, u) -> {
                    Object previousValue = t.put(this.getTopic(u), this.getKey(u), this.toObjectToSend(wfEvents, u));
                    if ((previousValue != null) && (u instanceof ExchangedTiffData)) {
                        System.out.println("... Already seen old " + previousValue + " / " + u);
                    }
                }, (t, u) -> { return t; }));
            MutableInt imageNumber = new MutableInt(1);
            Table<String, String, Object> imagesToSend = IFD.ifdsAsStream(metaData)
                .filter((ifd) -> ifd.imageIsPresent())
                .map(
                    (ifd) -> ThumbImageToSend.builder()
                        .withImageKey(imageKey)
                        .withJpegImage(ifd.getJpegImage())
                        .withCurrentNb(imageNumber.getAndIncrement())
                        .withPath(ifd.getPath())
                        .build())
                .flatMap(
                    (tis) -> Arrays.asList(tis, this.buildEvent(tis))
                        .stream())
                .collect(Collector.of(() -> {
                    Table<String, String, Object> table = HashBasedTable.create();
                    table.put(this.topicEvent, imageKey, wfEvents);
                    return table;
                },
                    (t, u) -> { t.put(this.getTopic(u), this.getKey(u), this.toObjectToSend(wfEvents, u)); },
                    (t, u) -> { return t; }));

            imagesToSend.cellSet()
                .forEach((cell) -> {
                    Object value = cell.getValue();
                    if (Objects.equal(this.topicEvent, cell.getRowKey())) {
                        WfEvents we = (WfEvents) objectsToSend.get(cell.getRowKey(), cell.getColumnKey());
                        we.addEvents(((WfEvents) value).getEvents());
                    } else {
                        objectsToSend.put(cell.getRowKey(), cell.getColumnKey(), value);
                    }
                });
            BeanProcessIncomingFile.LOGGER.info(
                "[EVENT][{}] End of Processing record : nb of images {}, nb of exifs {}, total images+exif+event: {}  ",
                imageKey,
                imageNumber.getValue() - 1,
                nbOfTiffFields,
                objectsToSend.size());
            retValue = objectsToSend;
            metrics.put(imageKey, objectsToSend.size());
        } catch (Exception e) {
            BeanProcessIncomingFile.LOGGER.error("[EVENT][{}] error when processing incoming record ", imageKey, e);
        }
        return retValue;
    }

    private Object toObjectToSend(WfEvents objectToSend, Object u) {
        switch (u.getClass()
            .getName()) {

            case BeanProcessIncomingFile.WF_EVENT_CLASS_NAME: {
                objectToSend.addEvent((WfEvent) u);
                return objectToSend;
            }
            case BeanProcessIncomingFile.THUMB_IMAGE_TO_SEND_CLASS_NAME: {
                return ((ThumbImageToSend) u).getJpegImage();
            }
            default: {
                return u;
            }
        }
    }

    private WfEvent buildEvent(ExchangedTiffData tif) {
        return this.buildEvent(tif.getImageId(), KeysBuilder.ExchangedTiffDataKeyBuilder.build(tif));
    }

    private WfEvent buildEvent(ThumbImageToSend tis) {
        HashFunction hf = Hashing.goodFastHash(256);
        String hbedoiHashCode = hf.newHasher()
            .putString(tis.getImageKey(), Charset.forName("UTf-8"))
            .putLong(tis.getTag())
            .putObject(tis.getPath(), (path, sink) -> {
                for (short t : path) {
                    sink.putShort(t);
                }
            })
            .hash()
            .toString();

        return this.buildEvent(tis.getImageKey(), hbedoiHashCode, tis.getCurrentNb());
    }

    private String getTopic(Object u) { return this.toTopic.get(u.getClass()); }

    private String getKey(Object u) { return this.toKey.get(u.getClass())
        .getKey(u); }

    private WfEvent buildEvent(String imageKey, String dataId) {
        return WfEvent.builder()
            .withImgId(imageKey)
            .withParentDataId(dataId)
            .withDataId(dataId)
            .withStep(
                WfEventStep.builder()
                    .withStep(WfEventStep.CREATED_FROM_STEP_IMAGE_FILE_READ)
                    .build())
            .build();
    }

    private WfEvent buildEvent(String imageKey, String dataId, int version) {
        return WfEvent.builder()
            .withImgId(imageKey)
            .withParentDataId(dataId)
            .withDataId(dataId + "-" + version)
            .withStep(
                WfEventStep.builder()
                    .withStep(WfEventStep.CREATED_FROM_STEP_IMAGE_FILE_READ)
                    .build())
            .build();
    }

    protected ExchangedTiffData buildExchangedTiffData(String key, TiffFieldAndPath f, int totalNbOfTiffFields) {
        short[] path = f.getPath();

        String tiffKey = KeysBuilder.topicExifKeyBuilder()
            .withOriginalImageKey(key)
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
        builder.withImageId(key)
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
            .withTotal(totalNbOfTiffFields)
            .withDataId(
                KeysBuilder.buildKeyForExifData(
                    key,
                    f.getTiffField()
                        .getTagValue(),
                    f.getPath()))
            .withPath(path);
        ExchangedTiffData etd = builder.build();
        return etd;
    }

}
