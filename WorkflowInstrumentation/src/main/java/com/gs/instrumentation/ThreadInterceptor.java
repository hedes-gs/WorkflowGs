package com.gs.instrumentation;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.bytebuddy.asm.Advice;

public class ThreadInterceptor {
    public static ThreadLocal<Map<Thread, ChainedContext>> threadLocal = new InheritableThreadLocal<>();
    public static Thread                                   initialThread;

    static {
        if (ThreadInterceptor.threadLocal.get() == null) {
            ThreadInterceptor.initialThread = Thread.currentThread();
            ThreadInterceptor.threadLocal.set(new ConcurrentHashMap<>());
            ThreadInterceptor.threadLocal.get()
                .put(
                    Thread.currentThread(),
                    new ChainedContext(Thread.currentThread(), new ConcurrentHashMap<>(), null));

        }
    }

    @Advice.OnMethodEnter
    public static void enter(@Advice.Origin Method method, @Advice.This Thread createdThread) {

        try {
            /*
             * if (ThreadInterceptor.DEBUG) { System.out
             * .println("Creating context for thread " + createdThread + " as child of " +
             * Thread.currentThread()); Thread.dumpStack(); }
             */
            final ChainedContext chainedContext2 = ThreadInterceptor.threadLocal.get()
                .get(Thread.currentThread());
            ChainedContext chainedContext = new ChainedContext(createdThread,
                chainedContext2.getGlobalContext(),
                chainedContext2);
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

    public ThreadInterceptor(ThreadLocal<Map<Thread, ChainedContext>> threadLocal) {
        super();
        ThreadInterceptor.threadLocal = threadLocal;
    }

}