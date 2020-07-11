package com.gs.photo.workflow.consumers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.dao.HbaseStatsDAO;
import com.gs.photo.workflow.hbase.dao.GenericDAO;
import com.gs.photo.workflow.internal.KafkaManagedHbaseData;
import com.workflow.model.HbaseImageThumbnailKey;
import com.workflow.model.events.WfEvent;

@Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class ConsumerForRecordHbaseImageKey extends AbstractConsumerForRecordHbase<HbaseImageThumbnailKey> {

    private static Logger                              LOGGER = LogManager.getLogger(ConsumerForRecordHbaseExif.class);

    @Value("${topic.topicCountOfImagesPerDate}")
    protected String                                   topicCountOfImagesPerDate;

    @Autowired
    @Qualifier("consumerForRecordingImageFromTopicHbaseImageThumbnailKey")
    protected Consumer<String, HbaseImageThumbnailKey> consumerForRecordingImageFromTopicHbaseImageThumbnailKey;

    @Autowired
    protected IBeanTaskExecutor                        beanTaskExecutor;

    @Autowired
    protected HbaseStatsDAO                            hbaseStatsDAO;

    @Override
    protected void flushAllDAO() throws IOException { this.hbaseStatsDAO.flush(); }

    @Override
    protected Optional<WfEvent> buildEvent(HbaseImageThumbnailKey x) { return null; }

    @Override
    protected void postRecord(List<HbaseImageThumbnailKey> v, Class<HbaseImageThumbnailKey> k) {}

    @Override
    protected <X extends HbaseImageThumbnailKey> GenericDAO<X> getGenericDAO(Class<X> k) {
        return (GenericDAO<X>) this.hbaseStatsDAO;
    }

    @Override
    public void processIncomingMessages() {
        try {
            ConsumerForRecordHbaseImageKey.LOGGER.info(
                "Start ConsumerForRecordHbaseExif.processIncomingMessages , subscribing to {} ",
                this.topicCountOfImagesPerDate);
            this.consumerForRecordingImageFromTopicHbaseImageThumbnailKey
                .subscribe(Collections.singleton(this.topicCountOfImagesPerDate));
            this.processMessagesFromTopic(
                this.consumerForRecordingImageFromTopicHbaseImageThumbnailKey,
                "HBASETHUMBNAIL_KEY");
        } catch (WakeupException e) {
            ConsumerForRecordHbaseImageKey.LOGGER.warn("Error ", e);
        } finally {
            this.consumerForRecordingImageFromTopicHbaseImageThumbnailKey.close();
        }
    }

    @Override
    protected boolean eventsShouldBeProduced() { return false; }

    @Override
    protected KafkaManagedHbaseData record(ConsumerRecord<String, HbaseImageThumbnailKey> rec) {
        try {
            this.hbaseStatsDAO.incrementDateInterval(rec.key(), rec.value());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return KafkaManagedHbaseData.builder()
            .withKafkaOffset(rec.offset())
            .withPartition(rec.partition())
            .withTopic(rec.topic())
            .withValue(rec.value())
            .withImageKey(rec.key())
            .build();
    }

}
