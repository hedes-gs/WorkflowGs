package com.gs.photo.workflow.recinhbase.consumers;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

import com.gs.photo.common.workflow.IBeanTaskExecutor;
import com.gs.photo.workflow.recinhbase.dao.HbaseStatsDAO;
import com.workflow.model.HbaseImageThumbnailKey;
import com.workflow.model.events.WfEvent;

// @Component
@ConditionalOnProperty(name = "unit-test", havingValue = "false")
public class ConsumerForRecordHbaseImageKey extends AbstractConsumerForRecordHbase<HbaseImageThumbnailKey> {

    private static Logger                              LOGGER = LoggerFactory
        .getLogger(ConsumerForRecordHbaseExif.class);

    @Value("${topic.topicCountOfImagesPerDate}")
    protected String                                   topicCountOfImagesPerDate;

    @Autowired
    @Qualifier("consumerForRecordingImageFromTopicHbaseImageThumbnailKey")
    protected Consumer<String, HbaseImageThumbnailKey> consumerForRecordingImageFromTopicHbaseImageThumbnailKey;

    @Autowired
    protected IBeanTaskExecutor                        beanTaskExecutor;

    @Autowired
    protected HbaseStatsDAO                            hbaseStatsDAO;

    @Value("${group.id}")
    private String                                     groupId;

    @Override
    protected void flushAllDAO() throws IOException { this.hbaseStatsDAO.flush(); }

    @Override
    protected Optional<WfEvent> buildEvent(HbaseImageThumbnailKey x) { return null; }

    @Override
    protected void postRecord(List<HbaseImageThumbnailKey> v) {
        ConsumerForRecordHbaseImageKey.LOGGER.info(" Recording nb of date elements {} ", v.size());
    }

    @Override
    protected void doRecord(String key, HbaseImageThumbnailKey k) {}

    @Override
    public void processIncomingMessages() {
        try {
            ConsumerForRecordHbaseImageKey.LOGGER.info(
                "Start ConsumerForRecordHbaseExif.processIncomingMessages , subscribing to {} ",
                this.topicCountOfImagesPerDate);
            /*
             * this.consumerForRecordingImageFromTopicHbaseImageThumbnailKey
             * .subscribe(Collections.singleton(this.topicCountOfImagesPerDate));
             * this.processMessagesFromTopic(
             * this.consumerForRecordingImageFromTopicHbaseImageThumbnailKey,
             * "HBASETHUMBNAIL_KEY", );
             */
        } catch (WakeupException e) {
            ConsumerForRecordHbaseImageKey.LOGGER.warn("Error ", e);
        } finally {
            this.consumerForRecordingImageFromTopicHbaseImageThumbnailKey.close();
        }
    }

    @Override
    protected boolean eventsShouldBeProduced() { return false; }

    @Override
    protected String getConsumer() { // TODO Auto-generated method stub
        return null;
    }

}
