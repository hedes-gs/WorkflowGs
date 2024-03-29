package com.gs.instrumentation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;

public class TimingInterceptor {

    record Data(
        Set<Timer> currentTimers,
        List<Object> children
    ) {}

    private static final Logger                      LOGGER = LoggerFactory.getLogger(TimingInterceptor.class);
    private TypeDescription                          type;
    private InheritableThreadLocal<MeterRegistry>    threadLocal;
    private ThreadLocal<Map<Thread, ChainedContext>> threadLocalForTree;

    @RuntimeType
    public Object intercept(@Origin Method method, @SuperCall Callable<Object> callable) throws Exception {
        final Data context = this.toContext(
            this.threadLocalForTree.get()
                .get(Thread.currentThread()));
        final Timer timer = this.threadLocal.get()
            .timer(
                "agent-buddy-" + method.getName(),
                "thread",
                Thread.currentThread()
                    .toString());

        context.currentTimers()
            .add(timer);
        return timer.record(() -> {
            try {
                return callable.call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                TimingInterceptor.LOGGER.error("Unexpected error ", e);
                throw new RuntimeException(e);
            }
        });

    }

    private Data toContext(Object chainedContext) { // TODO Auto-generated method stub
        try {
            return new Data((Set<Timer>) chainedContext.getClass()
                .getMethod("getCurrentTimers")
                .invoke(chainedContext),
                (List<Object>) chainedContext.getClass()
                    .getMethod("getChildren")
                    .invoke(chainedContext));
        } catch (
            IllegalAccessException |
            InvocationTargetException |
            NoSuchMethodException |
            SecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public TimingInterceptor(
        ThreadLocal<Map<Thread, ChainedContext>> threadLocalForTree,
        InheritableThreadLocal<MeterRegistry> threadLocal,
        TypeDescription type
    ) {
        super();
        this.threadLocal = threadLocal;
        this.threadLocalForTree = threadLocalForTree;
        this.type = type;
    }

}