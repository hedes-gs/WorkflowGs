package com.gs.instrumentation;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.implementation.bind.annotation.Argument;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

public class KafkaSendInterceptor {

    private ThreadLocal<Map<Thread, ChainedContext>> threadLocal;
    private static final Logger                      LOGGER = LoggerFactory.getLogger(KafkaSendInterceptor.class);

    @RuntimeType
    public Object intercept(
        @Origin Method method,
        @Argument(0) ProducerRecord<?, ?> record,
        @SuperCall Callable<Object> callable
    ) throws Exception {

        try {
            return callable.call();
        } finally {
            ((AtomicInteger) Data.getKafkaContext(
                this.threadLocal.get()
                    .get(Thread.currentThread()))
                .computeIfAbsent(record.topic(), (k) -> new AtomicInteger(0))).incrementAndGet();
        }
    }

    public KafkaSendInterceptor(ThreadLocal<Map<Thread, ChainedContext>> threadLocal) {
        super();
        this.threadLocal = threadLocal;
    }

}