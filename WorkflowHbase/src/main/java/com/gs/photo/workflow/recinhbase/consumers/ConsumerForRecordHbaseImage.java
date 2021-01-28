package com.gs.photo.workflow.recinhbase.consumers;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photo.workflow.recinhbase.HbaseApplicationConfig;
import com.gs.photo.workflow.recinhbase.dao.HbaseImageThumbnailDAO;
import com.gs.photo.workflow.recinhbase.dao.IHbaseImagesOfAlbumDAO;
import com.gs.photo.workflow.recinhbase.dao.IHbaseImagesOfKeyWordsDAO;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.builder.KeysBuilder.HbaseImageThumbnailKeyBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEventRecorded.RecordedEventType;
import com.workflow.model.events.WfEventStep;

@Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class ConsumerForRecordHbaseImage extends AbstractConsumerForRecordHbase<HbaseImageThumbnail>
    implements IConsumerForRecordHbaseImage {

    private static Logger                           LOGGER = LoggerFactory.getLogger(ConsumerForRecordHbaseImage.class);

    @Value("${topic.topicImageDataToPersist}")
    protected String                                topicImageDataToPersist;
    @Value("${group.id}")
    private String                                  groupId;
    @Autowired
    @Qualifier("consumerForRecordingImageFromTopic")
    protected Consumer<String, HbaseImageThumbnail> consumerForRecordingImageFromTopic;
    @Autowired
    protected HbaseImageThumbnailDAO                hbaseImageThumbnailDAO;
    @Autowired
    protected IHbaseImagesOfAlbumDAO                hbaseAlbumDAO;
    @Autowired
    protected IHbaseImagesOfKeyWordsDAO             hbaseKeyWordsDAO;
    @Autowired
    private ApplicationContext                      context;

    @Autowired
    private Connection                              hbaseConnection;

    @Override
    public void processIncomingMessages() {
        try (
            Admin admin = this.hbaseConnection.getAdmin()) {
        } catch (IOException e1) {

            e1.printStackTrace();
        }
        do {
            try {
                ConsumerForRecordHbaseImage.LOGGER
                    .info("Start ConsumerForRecordHbaseImage.processIncomingMessages, Ignite is ready");
                this.consumerForRecordingImageFromTopic.subscribe(Arrays.asList(this.topicImageDataToPersist));
                this.processMessagesFromTopic(
                    this.consumerForRecordingImageFromTopic,
                    "IMG",
                    this.groupId + "-" + HbaseApplicationConfig.CONSUMER_IMAGE);
            } catch (Throwable e) {
                ConsumerForRecordHbaseImage.LOGGER
                    .warn("[CONSUMER][{}] Error {}", this.getConsumer(), ExceptionUtils.getStackTrace(e));
            } finally {
                this.consumerForRecordingImageFromTopic.close();
                ConsumerForRecordHbaseImage.LOGGER
                    .warn("[CONSUMER][{}] Closing consummer for record hbase image data", this.getConsumer());
            }
            try {
                TimeUnit.SECONDS.sleep(1);
                this.consumerForRecordingImageFromTopic = this.context
                    .getBean("consumerForRecordingImageFromTopic", Consumer.class);
            } catch (InterruptedException e) {
                ConsumerForRecordHbaseImage.LOGGER.warn("Interrupted, stopping", e);
                break;
            }
        } while (true);
    }

    @Override
    protected void postRecord(List<HbaseImageThumbnail> v) {
        v.stream()
            .collect(Collectors.groupingBy((hb) -> this.getGroupKey(hb), Collectors.counting()))
            .forEach(
                (k1, v1) -> ConsumerForRecordHbaseImage.LOGGER
                    .info("[CONSUMER][{}][EVENT][{}] {} thumb records were recorded", this.getConsumer(), k1, v1));
    }

    private String getGroupKey(HbaseData hb) {
        if (hb instanceof HbaseImageThumbnail) { return ((HbaseImageThumbnail) hb).getImageId(); }
        throw new IllegalArgumentException("Unable to process class  " + hb.getClass());
    }

    @Override
    protected Optional<WfEvent> buildEvent(HbaseImageThumbnail x) {
        String hbdHashCode = HbaseImageThumbnailKeyBuilder.build(x);
        return Optional.of(
            WfEventRecorded.builder()
                .withImgId(x.getImageId())
                .withParentDataId(x.getDataId())
                .withDataId(hbdHashCode)
                .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_RECORDED_IN_HBASE)
                .withImageCreationDate(x.getCreationDate())
                .withRecordedEventType(RecordedEventType.THUMB)
                .build());
    }

    @Override
    protected void doRecord(String key, HbaseImageThumbnail k) {
        ConsumerForRecordHbaseImage.LOGGER.info(
            "[CONSUMER][{}][EVENT][{}] recording thumbs {}",
            this.getConsumer(),
            key,
            k.getThumbnail()
                .keySet());
        try {
            OffsetDateTime ldt = DateTimeHelper.toLocalDateTime(k.getCreationDate());
            k.setRegionSalt((short) ldt.getDayOfMonth());
            if (k.getThumbnail()
                .containsKey(1)) {
                this.hbaseImageThumbnailDAO.append(k);
            } else {
                this.hbaseImageThumbnailDAO.append(k, HbaseImageThumbnail.TABLE_FAMILY_THB);
            }
        } catch (IOException e) {
            ConsumerForRecordHbaseImage.LOGGER.warn("[CONSUMER][{}] unable to record {} ", this.getConsumer(), k);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void flushAllDAO() throws IOException {
        StopWatch stopWatch = new StopWatch();
        ConsumerForRecordHbaseImage.LOGGER.info("[CONSUMER][{}] Flusing DAOs", this.getConsumer());
        try {
            stopWatch.start();
            this.hbaseImageThumbnailDAO.flush();
        } catch (NotServingRegionException e) {
            ConsumerForRecordHbaseImage.LOGGER.warn(
                "[CONSUMER][{}] Error when flushing {} - exception is {} ",
                this.getConsumer(),
                ExceptionUtils.getStackTrace(e));
            throw e;
        } catch (RetriesExhaustedWithDetailsException e) {
            ConsumerForRecordHbaseImage.LOGGER.warn(
                "Error when flushing {} - exception is {} ",
                e.getExhaustiveDescription(),
                ExceptionUtils.getStackTrace(e));
            throw e;
        } catch (IOException e) {
            ConsumerForRecordHbaseImage.LOGGER.warn(
                "[CONSUMER][{}] Unknown Error when flushing {} ",
                this.getConsumer(),
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        } finally {
            stopWatch.stop();
            ConsumerForRecordHbaseImage.LOGGER.info(
                "[CONSUMER][{}] End of flusing DAOs - duration is {} ",
                this.getConsumer(),
                stopWatch.formatSplitTime());
        }

    }

    @Override
    protected String getConsumer() { return "IMAGE"; }

}
