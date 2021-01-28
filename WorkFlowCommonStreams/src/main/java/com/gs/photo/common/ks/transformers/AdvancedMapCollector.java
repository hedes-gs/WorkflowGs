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
import com.google.common.collect.Multimaps;

public class AdvancedMapCollector<K extends Comparable<K>, V, R> implements Transformer<K, V, KeyValue<K, R>> {

    protected static Logger LOGGER = LoggerFactory.getLogger(AdvancedMapCollector.class);

    public static interface Transform<R, K extends Comparable<K>, V1> { R to(K key, Collection<V1> values); }

    public static interface HandlerPublishEvent<K extends Comparable<K>, V1> {
        void apply(K key, Collection<V1> values);
    }

    public static interface HandlerPublishShouldBeDone<K extends Comparable<K>, V1> {
        boolean apply(K key, Collection<V1> values);
    }

    protected Multimap<K, V>                   currentMultimap;
    protected Transform<R, K, V>               transformer;
    protected HandlerPublishEvent<K, V>        handlerPublishEvent;
    protected HandlerPublishShouldBeDone<K, V> handlerPublishShouldBeDone;
    protected long                             duration;

    @Override
    public void init(ProcessorContext context) {
        AdvancedMapCollector.LOGGER.info("initializing ProcessorContext, window time is {}", this.duration);
        this.currentMultimap = Multimaps.synchronizedMultimap(ArrayListMultimap.create());
        if (this.duration > 0) {
            context.schedule(Duration.ofMillis(this.duration), PunctuationType.WALL_CLOCK_TIME, (t) -> {
                this.currentMultimap.keySet()
                    .forEach((k) -> {
                        final Collection<V> values = this.currentMultimap.get(k);
                        this.handlerPublishEvent.apply(k, values);
                        if (this.handlerPublishShouldBeDone.apply(k, this.currentMultimap.get(k))) {
                            context.forward(k, this.transformer.to(k, values));
                        }
                    });
                this.currentMultimap.clear();
            });
        }
    }

    @Override
    public KeyValue<K, R> transform(K key, V value) {
        KeyValue<K, R> returnValue = null;
        this.currentMultimap.put(key, value);
        if (this.handlerPublishShouldBeDone.apply(key, this.currentMultimap.get(key))) {
            final Collection<V> values = this.currentMultimap.get(key);
            this.handlerPublishEvent.apply(key, values);
            returnValue = new KeyValue<>(key, this.transformer.to(key, values));
            this.currentMultimap.removeAll(key);
        }
        return returnValue;
    }

    @Override
    public void close() {}

    private AdvancedMapCollector(
        Transform<R, K, V> transformer,
        HandlerPublishEvent<K, V> handlerPublishEvent,
        HandlerPublishShouldBeDone<K, V> handlerPublishShouldBeDone,
        long duration
    ) {
        super();
        this.transformer = transformer;
        this.duration = duration;
        this.handlerPublishEvent = handlerPublishEvent;
        this.handlerPublishShouldBeDone = handlerPublishShouldBeDone;
    }

    public static <K extends Comparable<K>, V extends Comparable<V>, R> Transformer<K, V, KeyValue<K, R>> of(
        Transform<R, K, V> transformer,
        HandlerPublishEvent<K, V> handlerPublishEvent,
        HandlerPublishShouldBeDone<K, V> handlerPublishShouldBeDone,
        long duration
    ) {
        return new AdvancedMapCollector<K, V, R>(transformer,
            handlerPublishEvent,
            handlerPublishShouldBeDone,
            duration);
    }

    public static <K extends Comparable<K>, V, R> Transformer<K, V, KeyValue<K, R>> of(
        Transform<R, K, V> transformer,
        HandlerPublishEvent<K, V> handlerPublishEvent,
        HandlerPublishShouldBeDone<K, V> handlerPublishShouldBeDone
    ) {
        return new AdvancedMapCollector<K, V, R>(transformer, handlerPublishEvent, handlerPublishShouldBeDone, 0);
    }

}