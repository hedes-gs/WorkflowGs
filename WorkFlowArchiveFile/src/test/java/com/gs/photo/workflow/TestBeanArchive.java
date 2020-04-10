package com.gs.photo.workflow;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.impl.BeanArchive;

@RunWith(SpringRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@SpringBootTest(classes = { ApplicationConfig.class, BeanArchive.class })
public class TestBeanArchive {

    @Autowired
    @MockBean
    @Qualifier("consumerForTransactionalCopyForTopicWithStringKey")
    protected Consumer<String, String> consumerForTransactionalCopyForTopicWithStringKey;

    @MockBean
    protected IBeanTaskExecutor        beanTaskExecutor;

    @Autowired
    protected BeanArchive              beanArchive;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {}

    @Before
    public void setUp() throws Exception { MockitoAnnotations.initMocks(this); }

    @Test
    public void test() {
        Map<TopicPartition, List<ConsumerRecord<String, String>>> mapOfRecords = new HashMap<>();
        final List<ConsumerRecord<String, String>> asList = Arrays
            .asList(new ConsumerRecord<>("topic", 1, 0, "1", "1"));
        mapOfRecords.put(new TopicPartition("topic", 1), asList);
        ConsumerRecords<String, String> records = new ConsumerRecords<>(mapOfRecords);

        Mockito.doAnswer(invocation -> {
            Runnable arg = (Runnable) invocation.getArgument(0);
            arg.run();
            return null;
        })
            .when(this.beanTaskExecutor)
            .execute((Runnable) ArgumentMatchers.any());
        Mockito.when(this.consumerForTransactionalCopyForTopicWithStringKey.poll(ArgumentMatchers.any()))
            .thenReturn(records)
            .thenReturn(null);
        this.beanArchive.init();

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException ie) {
            Thread.currentThread()
                .interrupt();
        }

    }

}
