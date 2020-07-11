package com.gs.photo.workflow.consumers;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.dao.HbaseExifDataDAO;
import com.gs.photo.workflow.dao.HbaseExifDataOfImagesDAO;
import com.gs.photo.workflow.hbase.dao.GenericDAO;
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
    @Qualifier("consumerToRecordExifDataOfImages")
    protected Consumer<String, HbaseData> consumerToRecordExifDataOfImages;

    @Autowired
    protected HbaseExifDataDAO            HbaseExifDataDAO;

    @Autowired
    protected HbaseExifDataOfImagesDAO    hbaseExifDataOfImagesDAO;

    @Override
    public void processIncomingMessages() {
        try {
            ConsumerForRecordHbaseExif.LOGGER.info(
                "Start ConsumerForRecordHbaseExif.processIncomingMessages , subscribing to {} ",
                this.topicExifImageDataToPersist);
            this.consumerToRecordExifDataOfImages.subscribe(Arrays.asList(this.topicExifImageDataToPersist));
            this.processMessagesFromTopic(this.consumerToRecordExifDataOfImages, "EXIF");
        } catch (WakeupException e) {
            ConsumerForRecordHbaseExif.LOGGER.warn("Error ", e);
        } finally {
            this.consumerToRecordExifDataOfImages.close();
        }
    }

    @Override
    protected void postRecord(List<HbaseData> v, Class<HbaseData> k) {
        v.stream()
            .collect(Collectors.groupingBy((hb) -> this.getGroupKey(hb), Collectors.counting()))
            .forEach((k1, v1) -> ConsumerForRecordHbaseExif.LOGGER.info("EVENT[{}] {} records were recorded", k1, v1));
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

    public ConsumerForRecordHbaseExif() {

    }

    @Override
    protected <X extends HbaseData> GenericDAO<X> getGenericDAO(Class<X> k) {
        if (k.equals(HbaseExifData.class)) {
            return (GenericDAO<X>) this.HbaseExifDataDAO;
        } else if (k.equals(HbaseExifDataOfImages.class)) { return (GenericDAO<X>) this.hbaseExifDataOfImagesDAO; }
        return null;
    }

    @Override
    protected void flushAllDAO() throws IOException {
        this.HbaseExifDataDAO.flush();
        this.hbaseExifDataOfImagesDAO.flush();
    }

}
