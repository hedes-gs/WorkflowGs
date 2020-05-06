package com.gs.workflow;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import com.gs.photo.workflow.impl.KafkaUtils;

public class TestKafkaUtils {

    @BeforeAll
    static void setUpBeforeClass() throws Exception {}

    @BeforeEach
    void setUp() throws Exception {}

    @Test
    void test() {
        Consumer<String, String> consumer = Mockito.mock(Consumer.class);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
        records.put(
            new TopicPartition("topic", 1),
            Arrays.asList(
                new ConsumerRecord<>("topic", 1, 0, "k1", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k3", "value")));
        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(records);
        Mockito.when(consumer.poll((Duration) ArgumentMatchers.any()))
            .thenReturn(consumerRecords)
            .thenReturn(null);
        Stream<ConsumerRecord<String, String>> stream = KafkaUtils.toStreamV2(
            200,
            consumer,
            15,
            true,
            (nbOfRecords) -> System.out.println("Start to process " + nbOfRecords),
            null);
        stream.forEach((a) -> System.out.println(Thread.currentThread() + ".... " + a));
        stream = KafkaUtils.toStreamV2(
            200,
            consumer,
            15,
            true,
            (nbOfRecords) -> System.out.println("Start to process " + nbOfRecords),
            null);
    }

    @Test
    void test001_shouldCount50ElementsWhenConsumerRecordsContains2TopicsWith25Elements() {
        Consumer<String, String> consumer = Mockito.mock(Consumer.class);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
        records.put(
            new TopicPartition("topic", 1),
            Arrays.asList(
                new ConsumerRecord<>("topic", 1, 0, "k1", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k3", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k4", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k5", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k6", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k7", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k8", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k9", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k10", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k11", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k12", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k13", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k14", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k15", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k16", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k17", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k18", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k19", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k20", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k21", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k22", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k23", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k24", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k25", "value")));
        records.put(
            new TopicPartition("topic", 2),
            Arrays.asList(
                new ConsumerRecord<>("topic", 2, 0, "k1", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k3", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k4", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k5", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k6", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k7", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k8", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k9", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k10", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k11", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k12", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k13", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k14", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k15", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k16", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k17", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k18", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k19", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k20", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k21", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k22", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k23", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k24", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k25", "value")));

        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(records);
        Mockito.when(consumer.poll((Duration) ArgumentMatchers.any()))
            .thenReturn(consumerRecords)
            .thenReturn(null);
        Stream<ConsumerRecord<String, String>> stream = KafkaUtils.toStreamV2(
            200,
            consumer,
            15,
            true,
            (nbOfRecords) -> System.out.println("Start to process " + nbOfRecords),
            null);
        stream = KafkaUtils.toStreamV2(
            200,
            consumer,
            15,
            true,
            (nbOfRecords) -> System.out.println("Start to process " + nbOfRecords),
            null);
        Assert.assertEquals(50, stream.count());
    }

    @Test
    void test001_shouldCount30ElementsWhenConsumerRecordsContains2TopicsWithBatchSizeElements() {
        Consumer<String, String> consumer = Mockito.mock(Consumer.class);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
        records.put(
            new TopicPartition("topic", 1),
            Arrays.asList(
                new ConsumerRecord<>("topic", 1, 0, "k1", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k3", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k4", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k5", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k6", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k7", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k8", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k9", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k10", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k11", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k12", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k13", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k14", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k15", "value")));
        records.put(
            new TopicPartition("topic", 2),
            Arrays.asList(
                new ConsumerRecord<>("topic", 2, 0, "k1", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k3", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k4", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k5", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k6", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k7", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k8", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k9", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k10", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k11", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k12", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k13", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k14", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k15", "value")));

        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(records);
        Mockito.when(consumer.poll((Duration) ArgumentMatchers.any()))
            .thenReturn(consumerRecords)
            .thenReturn(null);
        Stream<ConsumerRecord<String, String>> stream = KafkaUtils.toStreamV2(
            200,
            consumer,
            15,
            true,
            (nbOfRecords) -> System.out.println("Start to process " + nbOfRecords),
            null);
        stream = KafkaUtils.toStreamV2(
            200,
            consumer,
            15,
            true,
            (nbOfRecords) -> System.out.println("Start to process " + nbOfRecords),
            null);
        Assert.assertEquals(30, stream.count());
    }

    @Test
    void test001_shouldCount31ElementsWhenConsumerRecordsContains2TopicsWithBatchSizeElements() {
        Consumer<String, String> consumer = Mockito.mock(Consumer.class);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
        records.put(
            new TopicPartition("topic", 1),
            Arrays.asList(
                new ConsumerRecord<>("topic", 1, 0, "k1", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k3", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k4", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k5", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k6", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k7", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k8", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k9", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k10", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k11", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k12", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k13", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k14", "value"),
                new ConsumerRecord<>("topic", 1, 0, "k15", "value")));
        records.put(
            new TopicPartition("topic", 2),
            Arrays.asList(
                new ConsumerRecord<>("topic", 2, 0, "k1", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k2", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k3", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k4", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k5", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k6", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k7", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k8", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k9", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k10", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k11", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k12", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k13", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k14", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k15", "value"),
                new ConsumerRecord<>("topic", 2, 0, "k16", "value")));

        ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(records);
        Mockito.when(consumer.poll((Duration) ArgumentMatchers.any()))
            .thenReturn(consumerRecords)
            .thenReturn(null);
        Stream<ConsumerRecord<String, String>> stream = KafkaUtils.toStreamV2(
            200,
            consumer,
            15,
            true,
            (nbOfRecords) -> System.out.println("Start to process " + nbOfRecords),
            null);
        stream = KafkaUtils.toStreamV2(
            200,
            consumer,
            15,
            true,
            (nbOfRecords) -> System.out.println("Start to process " + nbOfRecords),
            null);
        Assert.assertEquals(31, stream.count());
    }

}
