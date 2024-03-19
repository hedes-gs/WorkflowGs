package com.gs.photo.common.ks.transformers;

import java.time.Duration;
import java.util.Collection;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

public class MapCollector<K extends Comparable<K>, V extends Comparable<V>, R>
    implements Transformer<K, V, KeyValue<K, R>> {

    protected static Logger LOGGER = LoggerFactory.getLogger(MapCollector.class);

    public static interface Transform<R, K extends Comparable<K>, V1 extends Comparable<V1>> {
        R to(K key, Collection<V1> values);
    }

    public static interface HandlerPublishEvent<K extends Comparable<K>, V1 extends Comparable<V1>> {
        void apply(K key, Collection<V1> values);
    }

    protected Multimap<K, V>            currentMultimap;
    protected Transform<R, K, V>        transformer;
    protected HandlerPublishEvent<K, V> handlerPublishEvent;
    protected int                       size;
    protected long                      duration;

    @Override
    public void init(ProcessorContext context) {
        MapCollector.LOGGER
            .info("initializing ProcessorContext, window time is {}, size is {}", this.duration, this.size);
        this.currentMultimap = ArrayListMultimap.create();
        context.schedule(Duration.ofMillis(this.duration), PunctuationType.WALL_CLOCK_TIME, (t) -> {
            this.currentMultimap.keySet()
                .forEach((k) -> {
                    final Collection<V> values = this.currentMultimap.get(k);
                    this.handlerPublishEvent.apply(k, values);
                    context.forward(k, this.transformer.to(k, values));
                });
            this.currentMultimap.clear();
        });
    }

    @Override
    public KeyValue<K, R> transform(K key, V value) {
        KeyValue<K, R> returnValue = null;
        this.currentMultimap.put(key, value);
        if (this.currentMultimap.get(key)
            .size() >= this.size) {
            final Collection<V> values = this.currentMultimap.get(key);
            this.handlerPublishEvent.apply(key, values);
            returnValue = new KeyValue<>(key, this.transformer.to(key, values));
            this.currentMultimap.removeAll(key);
        }
        return returnValue;
    }

    @Override
    public void close() {}

    private MapCollector(
        Transform<R, K, V> transformer,
        HandlerPublishEvent<K, V> handlerPublishEvent,
        int size,
        long duration
    ) {
        super();
        this.transformer = transformer;
        this.size = size;
        this.duration = duration;
        this.handlerPublishEvent = handlerPublishEvent;
    }

    public static <K extends Comparable<K>, V extends Comparable<V>, R> Transformer<K, V, KeyValue<K, R>> of(
        Transform<R, K, V> transformer,
        HandlerPublishEvent<K, V> handlerPublishEvent,
        int size,
        long duration
    ) {
        return new MapCollector<K, V, R>(transformer, handlerPublishEvent, size, duration);
    }

}
