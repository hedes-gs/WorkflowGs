package com.gs.instrumentation;

import java.io.File;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.AgentBuilder.Listener;
import net.bytebuddy.agent.builder.AgentBuilder.RawMatcher;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.Ownership;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.loading.ClassInjector;
import net.bytebuddy.implementation.LoadedTypeInitializer;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.matcher.StringMatcher;

public class TimerAgent {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimerAgent.class);

    public static Map<String, Class<?>> installThreadInterceptor(Instrumentation instrumentation) throws IOException {
        Listener.Filtering debuggingListener = new Listener.Filtering(
            new StringMatcher("java.lang.Thread", StringMatcher.Mode.CONTAINS),
            Listener.StreamWriting.toSystemOut());
        String interceptor = ThreadInterceptor.class.getName();
        String chainedContext = ChainedContext.class.getName();
        String simpleSet = SimpleSet.class.getName();

        ClassFileLocator locator = ClassFileLocator.ForClassLoader.ofSystemLoader();

        File temp = Files.createTempDirectory("tmp")
            .toFile();
        Map<String, Class<?>> returnMap = ClassInjector.UsingInstrumentation
            .of(temp, ClassInjector.UsingInstrumentation.Target.BOOTSTRAP, instrumentation)
            .injectRaw(
                Map.of(
                    interceptor,
                    locator.locate(interceptor)
                        .resolve(),
                    chainedContext,
                    locator.locate(chainedContext)
                        .resolve(),
                    simpleSet,
                    locator.locate(simpleSet)
                        .resolve()));

        new AgentBuilder.Default().ignore(ElementMatchers.none())
            // .with(debuggingListener)
            .disableClassFormatChanges()
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
            .with(AgentBuilder.TypeStrategy.Default.REBASE)
            .type(
                ElementMatchers.named("java.lang.Thread")
                    .or(ElementMatchers.named("java.lang.VirtualThread")))
            .transform(
                (builder, typeDescription, classLoader, javaModule, protectionDomain) -> builder.visit(
                    Advice.to(ThreadInterceptor.class)
                        .on(ElementMatchers.named("start"))))
            .installOn(instrumentation);
        return returnMap;
    }

    public static void premain(String arguments, Instrumentation instrumentation) {
        final InheritableThreadLocal<MeterRegistry> threadLocal = new InheritableThreadLocal<MeterRegistry>();
        final ThreadLocal<Map<Thread, ChainedContext>> threadLocalForTree = ThreadInterceptor.threadLocal;

        RawMatcher ignoreMatcher = new RawMatcher.ForElementMatchers(ElementMatchers.nameStartsWith("net.bytebuddy.")
            .or(ElementMatchers.nameStartsWith("org"))
            .or(ElementMatchers.nameContainsIgnoreCase("test")));

        final ElementMatcher<? super MethodDescription> anyOf = ElementMatchers.isAnnotatedWith(Timed.class);
        final ElementMatcher<? super MethodDescription> returnMetricObject = ElementMatchers
            .nameContains("meterRegistry");
        final ElementMatcher<? super MethodDescription> startMetricObject = ElementMatchers.nameContains("start");
        threadLocalForTree.set(new HashMap<>());
        Map<String, Class<?>> mapOfLoadedClasses;
        try {
            mapOfLoadedClasses = TimerAgent.installThreadInterceptor(instrumentation);
            ThreadLocal<Map<Thread, ChainedContext>> threadLocalOFChainedContext = (ThreadLocal<Map<Thread, ChainedContext>>) mapOfLoadedClasses
                .get(ThreadInterceptor.class.getName())
                .getDeclaredField("threadLocal")
                .get(null);

            TimerAgent.installMetricInterceptor(instrumentation, threadLocal, ignoreMatcher);
            TimerAgent.installKafkaPollInterceptor(instrumentation, threadLocalOFChainedContext);
            TimerAgent.installKafkaSendInterceptor(instrumentation, threadLocalOFChainedContext);
            TimerAgent.installKafkaSpyInterceptor(instrumentation, threadLocalOFChainedContext);
            TimerAgent.installtimingInterceptor(instrumentation, threadLocal, ignoreMatcher, anyOf, mapOfLoadedClasses);
        } catch (
            IllegalAccessException |
            NoSuchFieldException |
            IOException e) {
            e.printStackTrace();
        }

    }

    private static void installKafkaPollInterceptor(
        Instrumentation instrumentation,
        final ThreadLocal<Map<Thread, ChainedContext>> threadLocal
    ) {
        RawMatcher ignoreMatcher = new RawMatcher.ForElementMatchers(ElementMatchers.nameStartsWith("net.bytebuddy."));
        Listener.Filtering debuggingListener = new Listener.Filtering(
            new StringMatcher("Kafka", StringMatcher.Mode.CONTAINS_IGNORE_CASE),
            Listener.StreamWriting.toSystemOut());
        final ElementMatcher<? super MethodDescription> pollMethod = ElementMatchers.nameContains("poll");
        new AgentBuilder.Default().ignore(ignoreMatcher)
            // .with(debuggingListener)
            .type(
                ElementMatchers.nameContains("KafkaConsumer")
                    .or(ElementMatchers.nameContains("MockConsumer")))
            .transform((builder, type, classLoader, module, protectionDomain) ->

            {
                return builder.method(pollMethod)
                    .intercept(MethodDelegation.to(new KafkaPollInterceptor(threadLocal)));
            })
            .installOn(instrumentation);
    }

    private static void installKafkaSendInterceptor(
        Instrumentation instrumentation,
        final ThreadLocal<Map<Thread, ChainedContext>> threadLocal
    ) {
        RawMatcher ignoreMatcher = new RawMatcher.ForElementMatchers(ElementMatchers.nameStartsWith("net.bytebuddy."));
        Listener.Filtering debuggingListener = new Listener.Filtering(
            new StringMatcher("Kafka", StringMatcher.Mode.CONTAINS_IGNORE_CASE),
            Listener.StreamWriting.toSystemOut());
        final ElementMatcher<? super MethodDescription> pollMethod = ElementMatchers.nameContains("send");
        new AgentBuilder.Default().ignore(ignoreMatcher)
            // .with(debuggingListener)
            .type(
                ElementMatchers.nameContains("KafkaProducer")
                    .or(ElementMatchers.nameContains("MockProducer")))
            .transform((builder, type, classLoader, module, protectionDomain) ->

            {
                return builder.method(pollMethod)
                    .intercept(MethodDelegation.to(new KafkaSendInterceptor(threadLocal)));
            })
            .installOn(instrumentation);
    }

    private static void installKafkaSpyInterceptor(
        Instrumentation instrumentation,
        final ThreadLocal<Map<Thread, ChainedContext>> threadLocal
    ) {

        RawMatcher ignoreMatcher = new RawMatcher.ForElementMatchers(ElementMatchers.nameStartsWith("net.bytebuddy."));
        Listener.Filtering debuggingListener = new Listener.Filtering(
            new StringMatcher("Kafka", StringMatcher.Mode.CONTAINS_IGNORE_CASE),
            Listener.StreamWriting.toSystemOut());
        final ElementMatcher<? super MethodDescription> kafkaSpyMethod = ElementMatchers
            .isAnnotatedWith(KafkaSpy.class);
        new AgentBuilder.Default().ignore(ignoreMatcher)
            .type(ElementMatchers.nameContains("com.gs."))
            .transform((builder, type, classLoader, module, protectionDomain) -> {
                return builder.method(kafkaSpyMethod)
                    .intercept(MethodDelegation.to(new KafkaSpyInterceptor(threadLocal)));
            })
            .installOn(instrumentation);
    }

    private static void installMetricInterceptor(
        Instrumentation instrumentation,
        final InheritableThreadLocal<MeterRegistry> threadLocal,
        RawMatcher ignoreMatcher
    ) {
        final ElementMatcher<? super MethodDescription> returnMetricObject = ElementMatchers
            .nameContains("meterRegistry");
        new AgentBuilder.Default().ignore(ignoreMatcher)
            // .with(debuggingListener)
            .type(ElementMatchers.nameContains("ObservationConfiguration"))
            .transform((builder, type, classLoader, module, protectionDomain) ->

            {
                return builder.method(returnMetricObject)
                    .intercept(MethodDelegation.to(new MetricInterceptor(threadLocal)));
            })
            .installOn(instrumentation);
    }

    private static void installtimingInterceptor(
        Instrumentation instrumentation,
        final InheritableThreadLocal<MeterRegistry> threadLocal,
        RawMatcher ignoreMatcher,
        final ElementMatcher<? super MethodDescription> anyOf,
        Map<String, Class<?>> mapOfLoadedClasses
    ) throws IllegalAccessException, NoSuchFieldException {
        ThreadLocal<Map<Thread, ChainedContext>> threadLocalOFChainedContext = (ThreadLocal<Map<Thread, ChainedContext>>) mapOfLoadedClasses
            .get(ThreadInterceptor.class.getName())
            .getDeclaredField("threadLocal")
            .get(null);
        Thread initialThread = (Thread) mapOfLoadedClasses.get(ThreadInterceptor.class.getName())
            .getDeclaredField("initialThread")
            .get(null);
        new AgentBuilder.Default().ignore(ignoreMatcher)
            .type(ElementMatchers.nameContains("com.gs.photo.workflow.extimginfo.impl.BeanProcessIncomingFile"))
            .transform((builder, type, classLoader, module, protectionDomain) -> {
                return builder.initializer(new LoadedTypeInitializer.ForStaticField("THREAD_LOCAL", threadLocal))
                    .defineField("THREAD_LOCAL", java.lang.InheritableThreadLocal.class, Ownership.STATIC);
            })
            .transform((builder, type, classLoader, module, protectionDomain) -> {
                return builder.method(anyOf)
                    .intercept(MethodDelegation.to(new TimingInterceptor(threadLocalOFChainedContext, threadLocal)));
            })
            .installOn(instrumentation);
        TimerAgent.startShutdownHook(TimerAgent.class.getClassLoader(), threadLocalOFChainedContext, initialThread);
    }

    private static void startShutdownHook(
        ClassLoader classLoader,
        ThreadLocal<Map<Thread, ChainedContext>> threadLocalOFChainedContext,
        Thread initialThread
    ) {
        Thread printingHook = new Thread(() -> {

            StringBuilder strBuilder = new StringBuilder();
            try {
                Thread.sleep(Duration.ofMillis(500));
                TimerAgent.dump(
                    threadLocalOFChainedContext.get()
                        .get(initialThread),
                    strBuilder,
                    "");
            } catch (
                IllegalAccessException |
                InvocationTargetException |
                NoSuchMethodException |
                SecurityException |
                InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println(strBuilder);
        });

        Runtime.getRuntime()
            .addShutdownHook(printingHook);
    }

    private static void dump(Object chainedContext, StringBuilder strBuilder, String gap)
        throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException {
        final Object currentThread = chainedContext.getClass()
            .getMethod("getCurrentThread")
            .invoke(chainedContext);
        final String PREFIX = "> ";
        strBuilder.append(gap)
            .append(PREFIX)
            .append(currentThread)
            .append(" ")
            .append(
                TimerAgent.toString(
                    gap.length() + currentThread.toString()
                        .length() + PREFIX.length(),
                    chainedContext))
            .append("\n");

        final List listOfChildren = (List) chainedContext.getClass()
            .getMethod("getChildren")
            .invoke(chainedContext);
        for (Object t : listOfChildren) {
            TimerAgent.dump(t, strBuilder, gap + "--");
        }

    }

    private static String toString(int gap, Object chainedContext)
        throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException {
        final Set<Timer> listOfChildren = (Set) chainedContext.getClass()
            .getMethod("getCurrentTimers")
            .invoke(chainedContext);
        final String chainedContextAsString = listOfChildren.stream()
            .map(t -> " ".repeat(gap) + t.getId() + ":" + TimerAgent.toStringSnapshot(t.takeSnapshot()))
            .collect(Collectors.joining(",\n"));
        return chainedContextAsString.length() > 0 ? "\n" + chainedContextAsString : "";
    }

    private static String toStringSnapshot(HistogramSnapshot t) {
        return "count : " + t.count() + ", mean: " + t.mean(TimeUnit.MILLISECONDS) + " millis";
    }
}