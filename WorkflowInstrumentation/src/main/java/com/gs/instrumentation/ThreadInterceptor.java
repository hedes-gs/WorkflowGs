package com.gs.instrumentation;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import net.bytebuddy.asm.Advice;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;

public class ThreadInterceptor {
    public static ThreadLocal<Map<Thread, ChainedContext>> threadLocal = new InheritableThreadLocal<>();
    public static Thread                                   initialThread;

    static {
        if (ThreadInterceptor.threadLocal.get() == null) {
            ThreadInterceptor.initialThread = Thread.currentThread();
            ThreadInterceptor.threadLocal.set(new ConcurrentHashMap<>());
            ThreadInterceptor.threadLocal.get()
                .put(Thread.currentThread(), new ChainedContext(Thread.currentThread()));

        }
    }

    @Advice.OnMethodEnter
    public static void enter(@Advice.Origin Method method, @Advice.This Thread createdThread) {

        try {
            ChainedContext chainedContext = new ChainedContext(createdThread);
            /*
             * if (ThreadInterceptor.DEBUG) { System.out
             * .println("Creating context for thread " + createdThread + " as child of " +
             * Thread.currentThread()); Thread.dumpStack(); }
             */
            final ChainedContext chainedContext2 = ThreadInterceptor.threadLocal.get()
                .get(Thread.currentThread());
            if (chainedContext2 != null) {
                chainedContext2.getChildren()
                    .add(chainedContext);
                ThreadInterceptor.threadLocal.get()
                    .put(createdThread, chainedContext);
            } else {
                System.err.println(
                    "End of starting thread --> " + createdThread + " Context was not created !! PArent thread  "
                        + Thread.currentThread());
            }

        } finally {

        }
    }

    @Advice.OnMethodExit
    public static void exit(@Advice.Origin Method method, @Advice.This Thread createdThread) {}

    @RuntimeType
    public Object intercept(@This Thread newThread, @Origin Method method, @SuperCall Callable<Object> callable)
        throws Exception {

        try {
            ChainedContext chainedContext = new ChainedContext(newThread);
            chainedContext.currentThread = newThread;
            final ChainedContext chainedContext2 = ThreadInterceptor.threadLocal.get()
                .get(Thread.currentThread());
            if (chainedContext2 != null) {
                chainedContext2.children.add(chainedContext);
                ThreadInterceptor.threadLocal.get()
                    .put(newThread, chainedContext);
            }
            final Object call = callable.call();
            return call;
        } finally {

        }
    }

    public ThreadInterceptor(ThreadLocal<Map<Thread, ChainedContext>> threadLocal) {
        super();
        ThreadInterceptor.threadLocal = threadLocal;
    }

}