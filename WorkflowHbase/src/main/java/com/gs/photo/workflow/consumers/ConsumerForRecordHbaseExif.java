package com.gs.photo.workflow.consumers;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.HbaseApplicationConfig;
import com.gs.photo.workflow.dao.HbaseExifDataDAO;
import com.gs.photo.workflow.dao.HbaseExifDataOfImagesDAO;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseExifDataOfImages;
import com.workflow.model.builder.KeysBuilder.HbaseExifDataKeyBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEventRecorded.RecordedEventType;
import com.workflow.model.events.WfEventStep;

@Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class ConsumerForRecordHbaseExif extends AbstractConsumerForRecordHbase<HbaseData>
    implements IConsumerForRecordHbaseExif {

    private static Logger                 LOGGER = LogManager.getLogger(ConsumerForRecordHbaseExif.class);

    @Value("${topic.topicExifImageDataToPersist}")
    protected String                      topicExifImageDataToPersist;

    @Autowired
    @Qualifier("consumerToRecordExifDataOfImages")
    protected Consumer<String, HbaseData> consumerToRecordExifDataOfImages;

    @Autowired
    protected HbaseExifDataDAO            HbaseExifDataDAO;

    @Autowired
    protected HbaseExifDataOfImagesDAO    hbaseExifDataOfImagesDAO;

    @Value("${group.id}")
    private String                        groupId;

    @Autowired
    private ApplicationContext            context;

    @Override
    public void processIncomingMessages() {
        do {
            try {
                ConsumerForRecordHbaseExif.LOGGER.info(
                    "[CONSUMER][{}] Start ConsumerForRecordHbaseExif.processIncomingMessages , subscribing to {} ",
                    this.getConsumer(),
                    this.topicExifImageDataToPersist);
                this.consumerToRecordExifDataOfImages.subscribe(Arrays.asList(this.topicExifImageDataToPersist));
                this.processMessagesFromTopic(
                    this.consumerToRecordExifDataOfImages,
                    "EXIF",
                    this.groupId + "-" + HbaseApplicationConfig.CONSUMER_EXIF);
            } catch (Throwable e) {
                ConsumerForRecordHbaseExif.LOGGER
                    .warn("[CONSUMER][{}] Error {}", this.getConsumer(), ExceptionUtils.getStackTrace(e));
            } finally {
                ConsumerForRecordHbaseExif.LOGGER
                    .warn("[CONSUMER][{}] Closing consummer for record hbase exif data", this.getConsumer());
                this.consumerToRecordExifDataOfImages.close();
            }
            try {
                TimeUnit.SECONDS.sleep(1);
                this.consumerToRecordExifDataOfImages = this.context
                    .getBean("consumerToRecordExifDataOfImages", Consumer.class);
            } catch (InterruptedException e) {
                ConsumerForRecordHbaseExif.LOGGER.warn("Interrupted, stopping", e);
                break;
            }
        } while (true);
    }

    @Override
    protected void postRecord(List<HbaseData> v) {
        v.stream()
            .collect(Collectors.groupingBy((hb) -> this.getGroupKey(hb), Collectors.counting()))
            .forEach(
                (k1, v1) -> ConsumerForRecordHbaseExif.LOGGER
                    .info("[CONSUMER][{}][{}] {} records were recorded", this.getConsumer(), k1, v1));
    }

    private String getGroupKey(HbaseData hb) {
        if (hb instanceof HbaseExifData) {
            return ((HbaseExifData) hb).getImageId();
        } else if (hb instanceof HbaseExifDataOfImages) {
            return ((HbaseExifDataOfImages) hb).getImageId();
        } else {
            throw new IllegalArgumentException("Unable to process class  " + hb.getClass());
        }
    }

    @Override
    protected @Nullable Optional<WfEvent> buildEvent(HbaseData x) {
        if (x instanceof HbaseExifData) { return this.buildEventForHbaseExifData((HbaseExifData) x); }
        return Optional.empty();
    }

    private Optional<WfEvent> buildEventForHbaseExifData(HbaseExifData hbd) {
        String hbdHashCode = HbaseExifDataKeyBuilder.build(hbd);
        return Optional.of(this.buildEvent(hbd.getImageId(), hbd.getDataId(), hbdHashCode));
    }

    private WfEvent buildEvent(String imageKey, String parentDataId, String dataId) {
        return WfEventRecorded.builder()
            .withDataId(dataId)
            .withParentDataId(parentDataId)
            .withImgId(imageKey)
            .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_RECORDED_IN_HBASE)
            .withRecordedEventType(RecordedEventType.EXIF)
            .build();
    }

    public ConsumerForRecordHbaseExif() {

    }

    @Override
    protected void doRecord(String key, HbaseData h) {
        try {
            if (h instanceof HbaseExifData) {
                this.HbaseExifDataDAO.put((HbaseExifData) h);
            }
        } catch (IOException e) {
            ConsumerForRecordHbaseExif.LOGGER.warn("Error when recording {}", h);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void flushAllDAO() throws IOException {
        ConsumerForRecordHbaseExif.LOGGER.info("[CONSUMER][{}] Flusing DAOs", this.getConsumer());
        try {
            this.HbaseExifDataDAO.flush();
        } finally {
            ConsumerForRecordHbaseExif.LOGGER.info("[CONSUMER][{}] End of flusing DAOs", this.getConsumer());
        }
    }

    @Override
    protected String getConsumer() { return "EXIF"; }

}
