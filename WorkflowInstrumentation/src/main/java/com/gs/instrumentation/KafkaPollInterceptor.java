package com.gs.instrumentation;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

public class KafkaPollInterceptor {

    ThreadLocal<Map<Thread, ChainedContext>> threadLocal;
    private static final Logger              LOGGER = LoggerFactory.getLogger(KafkaPollInterceptor.class);

    @RuntimeType
    public Object intercept(@Origin Method method, @SuperCall Callable<Object> callable) throws Exception {

        try {
            final ConsumerRecords<?, ?> consumerRecords = (ConsumerRecords<?, ?>) callable.call();
            if (consumerRecords.count() > 0) {
                String info = StreamSupport.stream(consumerRecords.spliterator(), false)
                    .collect(Collectors.groupingBy(k -> new TopicPartition(k.topic(), k.partition())))
                    .entrySet()
                    .stream()
                    .map(
                        e -> e.getKey() + " [ " + e.getValue()
                            .size() + " ] ")
                    .collect(Collectors.joining(","));
                KafkaPollInterceptor.LOGGER
                    .info("-> kafka consumer received {} records , creating kafka context", info);
                Data.createKafkaContext(
                    System.currentTimeMillis(),
                    info,
                    this.threadLocal.get()
                        .get(Thread.currentThread()));
            }
            return consumerRecords;
        } finally {

        }
    }

    public KafkaPollInterceptor(ThreadLocal<Map<Thread, ChainedContext>> threadLocal) {
        super();
        this.threadLocal = threadLocal;
    }

}