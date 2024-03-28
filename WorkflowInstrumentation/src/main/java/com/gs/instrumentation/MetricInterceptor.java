package com.gs.instrumentation;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

public class MetricInterceptor {
    InheritableThreadLocal<MeterRegistry> threadLocal;
    private static final Logger           LOGGER = LoggerFactory.getLogger(TimingInterceptor.class);

    @RuntimeType
    public Object intercept(@Origin Method method, @SuperCall Callable<Object> callable) throws Exception {

        MetricInterceptor.LOGGER.info("Getting metrics");
        try {
            final Object call = callable.call();
            this.threadLocal.set((MeterRegistry) call);
            return call;
        } finally {

        }
    }

    public MetricInterceptor(InheritableThreadLocal<MeterRegistry> threadLocal) {
        super();
        this.threadLocal = threadLocal;
    }

}