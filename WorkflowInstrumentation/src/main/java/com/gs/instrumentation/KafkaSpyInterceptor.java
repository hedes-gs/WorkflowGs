package com.gs.instrumentation;

import java.lang.reflect.Method;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

public class KafkaSpyInterceptor {
    private ThreadLocal<Map<Thread, ChainedContext>> threadLocal;
    private static final Logger                      LOGGER = LoggerFactory.getLogger(KafkaSpyInterceptor.class);

    @RuntimeType
    public Object intercept(@Origin Method method, @SuperCall Callable<Object> callable) throws Exception {

        try {
            return callable.call();
        } finally {
            final Map<String, Object> kafkaContext = Data.getKafkaContextWithPerformanceMetric(
                this.threadLocal.get()
                    .get(Thread.currentThread()));
            if (kafkaContext.size() > 0) {
                Gson gson = new GsonBuilder().setPrettyPrinting()
                    .create();
                final Instant timeMillis = Instant.now();

                final double duration = ((Instant) kafkaContext.get("start-process-time"))
                    .until(timeMillis, ChronoUnit.MICROS) / 1000.0;
                kafkaContext.put(
                    "start-process-time",
                    DateTimeFormatter.ISO_DATE_TIME
                        .format(((Instant) kafkaContext.get("start-process-time")).atZone(ZoneId.systemDefault())));
                kafkaContext.put(
                    "end-process-time",
                    DateTimeFormatter.ISO_DATE_TIME.format(timeMillis.atZone(ZoneId.systemDefault())));
                kafkaContext.put("total-duration-time", duration);
                KafkaSpyInterceptor.LOGGER
                    .info("-> After processing kafka records, stats are: \n{}", gson.toJson(kafkaContext));
                Data.deleteKafkaContext(
                    this.threadLocal.get()
                        .get(Thread.currentThread()));
            }
        }
    }

    public KafkaSpyInterceptor(ThreadLocal<Map<Thread, ChainedContext>> threadLocal) {
        super();
        this.threadLocal = threadLocal;
    }

}