package com.gs.instrumentation;

import java.lang.instrument.Instrumentation;

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.agent.builder.AgentBuilder.Listener;
import net.bytebuddy.agent.builder.AgentBuilder.RawMatcher;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.Ownership;
import net.bytebuddy.implementation.LoadedTypeInitializer;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.matcher.StringMatcher;

public class TimerAgent {
    public static void premain(String arguments, Instrumentation instrumentation) {
        final InheritableThreadLocal<MeterRegistry> threadLocal = new InheritableThreadLocal<MeterRegistry>();

        RawMatcher ignoreMatcher = new RawMatcher.ForElementMatchers(ElementMatchers.nameStartsWith("net.bytebuddy.")
            .or(ElementMatchers.nameStartsWith("org"))
            .or(ElementMatchers.nameContainsIgnoreCase("test")));
        Listener.Filtering debuggingListener = new Listener.Filtering(
            new StringMatcher(".ObservationTextPublisherConfiguration", StringMatcher.Mode.CONTAINS),
            Listener.StreamWriting.toSystemOut());
        final ElementMatcher<? super MethodDescription> anyOf = ElementMatchers.isAnnotatedWith(Timed.class);
        final ElementMatcher<? super MethodDescription> returnMetricObject = ElementMatchers
            .nameContains("meterRegistry");

        new AgentBuilder.Default().ignore(ignoreMatcher)
            // .with(debuggingListener)
            .type(ElementMatchers.nameContains("ObservationConfiguration"))
            .transform((builder, type, classLoader, module, protectionDomain) -> {
                return builder.method(returnMetricObject)
                    .intercept(MethodDelegation.to(new MetricInterceptor(threadLocal)));
            })
            .installOn(instrumentation);

        new AgentBuilder.Default().ignore(ignoreMatcher)
            .type(ElementMatchers.nameContains("com.gs.photo.workflow.extimginfo.impl.BeanProcessIncomingFile"))
            .transform((builder, type, classLoader, module, protectionDomain) -> {
                return builder.initializer(new LoadedTypeInitializer.ForStaticField("THREAD_LOCAL", threadLocal))
                    .defineField("THREAD_LOCAL", java.lang.InheritableThreadLocal.class, Ownership.STATIC);
            })
            .transform((builder, type, classLoader, module, protectionDomain) -> {
                return builder.method(anyOf)
                    .intercept(MethodDelegation.to(new TimingInterceptor(threadLocal, type)));
            })
            .installOn(instrumentation);

    }
}