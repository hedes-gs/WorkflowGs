package com.gs.photo.workflow.consumers;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.workflow.model.HbaseData;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseExifDataOfImages;
import com.workflow.model.builder.KeysBuilder.HbaseExifDataKeyBuilder;
import com.workflow.model.builder.KeysBuilder.HbaseExifDataOfImagesKeyBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventStep;

@Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class ConsumerForRecordHbaseExif extends AbstractConsumerForRecordHbase<HbaseData>
    implements IConsumerForRecordHbaseExif {

    private static Logger                 LOGGER = LogManager.getLogger(ConsumerForRecordHbaseExif.class);

    @Value("${topic.topicExifImageDataToPersist}")
    protected String                      topicExifImageDataToPersist;

    @Autowired
    protected Consumer<String, HbaseData> consumerForRecordingExifDataFromTopic;

    @Override
    public void recordIncomingMessageInHbase() {
        try {
            ConsumerForRecordHbaseExif.LOGGER.info("Start ConsumerForRecordHbaseExif.recordIncomingMessageInHbase");
            this.consumerForRecordingExifDataFromTopic.subscribe(Arrays.asList(this.topicExifImageDataToPersist));
            this.processMessagesFromTopic(this.consumerForRecordingExifDataFromTopic);
        } catch (WakeupException e) {
            ConsumerForRecordHbaseExif.LOGGER.warn("Error ", e);
        } finally {
            this.consumerForRecordingExifDataFromTopic.close();
        }
    }

    @Override
    protected void postRecord(List<HbaseData> v, Class<HbaseData> k) {}

    @Override
    protected @Nullable Optional<WfEvent> buildEvent(HbaseData x) {
        if (x instanceof HbaseExifData) {
            return this.buildEventForHbaseExifData((HbaseExifData) x);
        } else if (x instanceof HbaseExifDataOfImages) {
            return this.buildEventForHbaseExifData((HbaseExifDataOfImages) x);
        }
        return Optional.empty();
    }

    private Optional<WfEvent> buildEventForHbaseExifData(HbaseExifDataOfImages hbedoi) {
        String hbedoiHashCode = HbaseExifDataOfImagesKeyBuilder.build(hbedoi);
        return Optional.of(this.buildEvent(hbedoi.getImageId(), hbedoi.getDataId(), hbedoiHashCode));
    }

    private Optional<WfEvent> buildEventForHbaseExifData(HbaseExifData hbd) {
        String hbdHashCode = HbaseExifDataKeyBuilder.build(hbd);
        return Optional.of(this.buildEvent(hbd.getImageId(), hbd.getDataId(), hbdHashCode));
    }

    private WfEvent buildEvent(String imageKey, String parentDataId, String dataId) {
        return WfEvent.builder()
            .withDataId(dataId)
            .withParentDataId(parentDataId)
            .withImgId(imageKey)
            .withStep(
                WfEventStep.builder()
                    .withStep(WfEventStep.CREATED_FROM_STEP_RECORDED_IN_HBASE)
                    .build())
            .build();
    }

}
