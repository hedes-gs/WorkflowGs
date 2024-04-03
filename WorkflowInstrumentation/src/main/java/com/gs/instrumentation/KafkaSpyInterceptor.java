package com.gs.instrumentation;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            final Map<String, Object> kafkaContext = Data.getKafkaContext(
                this.threadLocal.get()
                    .get(Thread.currentThread()));
            if (kafkaContext.size() > 0) {
                final long timeMillis = System.currentTimeMillis();
                kafkaContext.put("END_PROCESS_TIME", timeMillis);
                final double duration = (double)(timeMillis - (long) kafkaContext.get("START_PROCESS_TIME"));
                kafkaContext
                    .put("DURATION_TIME", duration / 1000.0f);
                KafkaSpyInterceptor.LOGGER.info("-> After processing kafka records, stats are {}", kafkaContext);
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