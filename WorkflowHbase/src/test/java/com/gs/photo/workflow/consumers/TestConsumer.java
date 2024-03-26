package com.gs.photo.workflow.consumers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.gs.photo.common.workflow.IKafkaProperties;
import com.gs.photo.workflow.recinhbase.WorkflowHbaseApplication;
import com.gs.photo.workflow.recinhbase.consumers.IGenericConsumerForRecording;
import com.gs.photo.workflow.recinhbase.consumers.config.SpecificApplicationProperties;
import com.gs.photos.serializers.WfEventsSerializer;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.SizeAndJpegContent;
import com.workflow.model.events.WfEvents;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@SpringBootTest(classes = { WorkflowHbaseApplication.class })
public class TestConsumer {

    protected static Logger                                   LOGGER = LoggerFactory.getLogger(TestConsumer.class);

    @MockBean
    public Void                                               startConsumers;

    @MockBean
    protected Supplier<Producer<String, WfEvents>>            producerSupplierForTransactionPublishingOnExifTopic;

    @MockBean
    protected Supplier<Consumer<String, HbaseImageThumbnail>> kafkaConsumerSupplierForRecordingImage;

    @MockBean
    protected Supplier<Consumer<String, HbaseExifData>>       kafkaConsumerSupplierForRecordingExifImages;

    @Autowired
    protected List<IGenericConsumerForRecording<?, ?>>        allBeans;

    @Autowired
    protected IKafkaProperties                                kafkaProperties;

    @TestConfiguration
    public static class TestConfig {
        @Mock
        protected Admin admin;
        protected Table table;

        @Bean
        @Primary
        public org.apache.hadoop.conf.Configuration hbaseConfiguration(
            SpecificApplicationProperties specificApplicationProperties
        ) {

            org.apache.hadoop.conf.Configuration config = Mockito.mock(org.apache.hadoop.conf.Configuration.class);
            return config;
        }

        @Bean
        @Primary
        public Connection hbaseConnection(
            @Autowired org.apache.hadoop.conf.Configuration hbaseConfiguration,
            SpecificApplicationProperties specificApplicationProperties
        ) {
            try {
                this.admin = Mockito.mock(Admin.class);
                this.table = Mockito.mock(Table.class);
                Connection connection = Mockito.mock(Connection.class);
                BufferedMutatorParams param = ArgumentMatchers.any();
                Mockito.when(connection.getBufferedMutator(param))
                    .thenReturn(Mockito.mock(BufferedMutator.class));
                Mockito.when(connection.getAdmin())
                    .thenReturn(this.admin);
                Mockito.when(connection.getTable(ArgumentMatchers.any()))
                    .thenReturn(this.table);
                return connection;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    protected static final String                   TEST                 = "";
    final MockConsumer<String, HbaseImageThumbnail> mockConsumerForImage = new MockConsumer<>(
        OffsetResetStrategy.EARLIEST);
    final MockConsumer<String, HbaseExifData>       mockConsumerForExif  = new MockConsumer<>(
        OffsetResetStrategy.EARLIEST);

    @BeforeEach
    protected void setUp() throws IOException, InterruptedException {
        Mockito.when(this.kafkaConsumerSupplierForRecordingImage.get())
            .thenReturn(this.mockConsumerForImage);
        Mockito.when(this.kafkaConsumerSupplierForRecordingExifImages.get())
            .thenReturn(this.mockConsumerForExif);
        Mockito.when(this.producerSupplierForTransactionPublishingOnExifTopic.get())
            .thenReturn(new MockProducer<>(false, new StringSerializer(), new WfEventsSerializer()))
            .thenReturn(new MockProducer<>(false, new StringSerializer(), new WfEventsSerializer()));
        this.allBeans.forEach(t -> t.start());
    }

    @Test
    public void test() {
        this.mockConsumerForImage.schedulePollTask(
            () -> this.mockConsumerForImage.updateBeginningOffsets(
                Collections.singletonMap(
                    new TopicPartition(this.kafkaProperties.getTopics()
                        .topicImageDataToPersist(), 0),
                    0L)));

        this.mockConsumerForImage.schedulePollTask(
            () -> this.mockConsumerForImage.rebalance(
                Collections.singleton(
                    new TopicPartition(this.kafkaProperties.getTopics()
                        .topicImageDataToPersist(), 0))));
        this.mockConsumerForImage.schedulePollTask(
            () -> this.mockConsumerForImage.addRecord(
                new ConsumerRecord<>(this.kafkaProperties.getTopics()
                    .topicImageDataToPersist(),
                    0,
                    0,
                    null,
                    HbaseImageThumbnail.builder()
                        .withImageId("TEST")
                        .withImageName("TEST")
                        .withPath("TEST")
                        .withThumbName(TestConsumer.TEST)
                        .withThumbnail(
                            new HashMap<>(Collections.singletonMap(
                                1,
                                SizeAndJpegContent.builder()
                                    .withJpegContent(new byte[0])
                                    .build())))

                        .build())));

    }

}
