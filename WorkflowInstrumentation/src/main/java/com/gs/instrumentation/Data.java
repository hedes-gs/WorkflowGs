package com.gs.instrumentation;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;

record Data(
    Set<Timer> currentTimers,
    List<Object> children
) {
    @SuppressWarnings("unchecked")
    public static Data toContext(Object chainedContext) {
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

    public static Map<String, Object> getKafkaContext(Object chainedContext) {
        try {
            return (Map<String, Object>) chainedContext.getClass()
                .getMethod("getKafkaContext")
                .invoke(chainedContext);
        } catch (
            IllegalAccessException |
            InvocationTargetException |
            NoSuchMethodException |
            SecurityException e) {
            throw new RuntimeException(e);
        }

    }

    public static Map<String, Object> getKafkaContextWithPerformanceMetric(Object chainedContext) {
        try {
            Map<String, Object> returnValue = (Map<String, Object>) chainedContext.getClass()
                .getMethod("getKafkaContext")
                .invoke(chainedContext);
            if (returnValue.size() > 0) {
                Data.dumpMetrics(
                    chainedContext,
                    (Map<String, Object>) returnValue
                        .computeIfAbsent("metrics", (k) -> new ConcurrentHashMap<String, Object>()));
            }
            return returnValue;
        } catch (
            IllegalAccessException |
            InvocationTargetException |
            NoSuchMethodException |
            SecurityException e) {
            throw new RuntimeException(e);
        }

    }

    private static void dumpMetrics(Object chainedContext, Map<String, Object> returnValue)
        throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException {
        final Set<Timer> listOfTimer = (Set) chainedContext.getClass()
            .getMethod("getCurrentTimers")
            .invoke(chainedContext);

        for (Timer t : listOfTimer) {
            HistogramSnapshot histogram = t.takeSnapshot();
            returnValue.put(
                t.getId()
                    .getName(),
                Map.of(
                    "tags",
                    t.getId()
                        .getTags(),
                    "count",
                    histogram.count(),
                    "mean",
                    histogram.mean(TimeUnit.MILLISECONDS)));
        }

        final List listOfChildren = (List) chainedContext.getClass()
            .getMethod("getChildren")
            .invoke(chainedContext);
        final Object currentThread = chainedContext.getClass()
            .getMethod("getCurrentThread")
            .invoke(chainedContext);
        for (Object t : listOfChildren) {
            Data.dumpMetrics(
                t,
                (Map<String, Object>) returnValue
                    .computeIfAbsent(currentThread.toString(), (e) -> new ConcurrentHashMap<>()));
        }
    }

    public static void createKafkaContext(Instant creationDate, Object consumersInfo, Object chainedContext) {
        try {
            Map<String, Object> kafkaContext = new ConcurrentHashMap<>();
            kafkaContext.put("records-per-topic", consumersInfo);
            kafkaContext.put("start-process-time", creationDate);
            chainedContext.getClass()
                .getMethod("addKafkaContext", Map.class)
                .invoke(chainedContext, kafkaContext);
        } catch (
            IllegalAccessException |
            InvocationTargetException |
            NoSuchMethodException |
            SecurityException e) {
            throw new RuntimeException(e);
        }

    }

    public static void deleteKafkaContext(Object chainedContext) {
        try {
            chainedContext.getClass()
                .getMethod("deleteKafkaContext")
                .invoke(chainedContext);
        } catch (
            IllegalAccessException |
            InvocationTargetException |
            NoSuchMethodException |
            SecurityException e) {
            throw new RuntimeException(e);
        }

    }

}