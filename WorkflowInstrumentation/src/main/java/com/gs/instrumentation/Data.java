package com.gs.instrumentation;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.Timer;

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

    public static void createKafkaContext(long creationDate, Object consumersInfo, Object chainedContext) {
        try {
            Map<String, Object> kafkaContext = new ConcurrentHashMap<>();
            kafkaContext.put("RECORDS_INPUT", consumersInfo);
            kafkaContext.put("START_PROCESS_TIME", creationDate);
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