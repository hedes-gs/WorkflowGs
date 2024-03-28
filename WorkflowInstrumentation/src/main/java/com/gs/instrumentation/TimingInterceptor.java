package com.gs.instrumentation;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

public class TimingInterceptor {

    private static final Logger                   LOGGER = LoggerFactory.getLogger(TimingInterceptor.class);
    private TypeDescription                       type;
    private InheritableThreadLocal<MeterRegistry> threadLocal;

    @RuntimeType
    public Object intercept(@Origin Method method, @SuperCall Callable<Object> callable) throws Exception {

        TimingInterceptor.LOGGER.info(
            "class is {} - {} - method is {}  ",
            method.getDeclaringClass(),
            method.getDeclaringClass()
                .getEnclosingClass(),
            method);
        try {
            return this.threadLocal.get()
                .timer("agent-buddy-" + method.getName())
                .record(() -> {
                    try {
                        return callable.call();
                    } catch (RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        TimingInterceptor.LOGGER.error("Unexpected error ", e);
                        throw new RuntimeException(e);
                    }
                });
        } finally {
            TimingInterceptor.LOGGER.info(
                "End of class is {} - {} - method is {}  ",
                method.getDeclaringClass(),
                method.getDeclaringClass()
                    .getEnclosingClass(),
                method);

        }
    }

    public TimingInterceptor(
        InheritableThreadLocal<MeterRegistry> threadLocal,
        TypeDescription type
    ) {
        super();
        this.threadLocal = threadLocal;
        this.type = type;
    }

}