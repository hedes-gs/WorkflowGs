package com.gs.photo.workflow.consumers;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IIgniteDAO;
import com.gs.photo.workflow.dao.GenericDAO;
import com.gs.photo.workflow.dao.HbaseImageThumbnailDAO;
import com.workflow.model.HbaseData;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventStep;

@Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class ConsumerForRecordHbaseImage extends AbstractConsumerForRecordHbase<HbaseImageThumbnail>
    implements IConsumerForRecordHbaseImage {

    private static Logger                           LOGGER = LogManager.getLogger(ConsumerForRecordHbaseImage.class);

    @Value("${topic.topicImageDataToPersist}")
    protected String                                topicImageDataToPersist;

    @Autowired
    protected IIgniteDAO                            igniteDao;

    @Autowired
    @Qualifier("consumerForRecordingImageFromTopic")
    protected Consumer<String, HbaseImageThumbnail> consumerForRecordingImageFromTopic;

    @Autowired
    protected HbaseImageThumbnailDAO                hbaseImageThumbnailDAO;

    @Override
    public void processIncomingMessages() {
        boolean ready = true;
        do {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                ready = false;
                break;
            }
        } while (!this.igniteDao.isReady());
        if (ready) {
            try {
                ConsumerForRecordHbaseImage.LOGGER
                    .info("Start ConsumerForRecordHbaseImage.processIncomingMessages, Ignite is ready");
                this.consumerForRecordingImageFromTopic.subscribe(Arrays.asList(this.topicImageDataToPersist));
                this.processMessagesFromTopic(this.consumerForRecordingImageFromTopic, "IMG");
            } catch (WakeupException e) {
                ConsumerForRecordHbaseImage.LOGGER.warn("Error ", e);
            } finally {
                this.consumerForRecordingImageFromTopic.close();
            }
        }
    }

    @Override
    protected void postRecord(List<HbaseImageThumbnail> v, Class<HbaseImageThumbnail> k) {

        Stream<HbaseImageThumbnail> stream = v.stream()
            .filter((h) -> { return ((h.getVersion() == 0) || (h.getVersion() == 1)); });
        Map<String, HbaseImageThumbnail> convertedStream = stream
            .collect(Collectors.toMap(x -> x.getImageId() + "-" + x.getVersion(), x -> x));
        this.igniteDao.save(convertedStream, HbaseImageThumbnail.class);
        v.stream()
            .collect(Collectors.groupingBy((hb) -> this.getGroupKey(hb), Collectors.counting()))
            .forEach(
                (k1, v1) -> ConsumerForRecordHbaseImage.LOGGER
                    .info("EVENT[{}] {} thumb records were recorded", k1, v1));
    }

    private String getGroupKey(HbaseData hb) {
        if (hb instanceof HbaseImageThumbnail) { return ((HbaseImageThumbnail) hb).getImageId(); }
        throw new IllegalArgumentException("Unable to process class  " + hb.getClass());
    }

    @Override
    protected Optional<WfEvent> buildEvent(HbaseImageThumbnail x) {
        return Optional.of(
            WfEvent.builder()
                .withImgId(x.getImageId())
                .withParentDataId(x.getDataId())
                .withDataId(x.getDataId() + "-" + x.getVersion())
                .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_RECORDED_IN_HBASE)
                .build());
    }

    @Override
    protected <X extends HbaseImageThumbnail> GenericDAO<X> getGenericDAO(Class<X> k) {
        return (GenericDAO<X>) this.hbaseImageThumbnailDAO;
    }

}
