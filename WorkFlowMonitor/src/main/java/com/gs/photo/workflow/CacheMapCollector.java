package com.gs.photo.workflow;

import java.time.Instant;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.LoggerFactory;

import com.gs.photo.workflow.daos.ICacheNodeDAO;
import com.gs.photo.workflow.daos.impl.CacheNodeDAOV2;
import com.workflow.model.events.WfEvents;

public class CacheMapCollector implements Transformer<String, WfEvents, KeyValue<String, String>> {

    protected static final org.slf4j.Logger   LOGGER            = LoggerFactory.getLogger(CacheMapCollector.class);
    private static final long                 WINDOW_SIZE_IN_MS = 86400;
    protected ICacheNodeDAO<String, WfEvents> icacheNodeDAO;
    private String                            storeName;
    private WindowStore<String, WfEvents>     eventIdStore;
    private ProcessorContext                  context;

    public CacheMapCollector(String storeName) { this.storeName = storeName; }

    @Override
    public void init(ProcessorContext context) {
        this.icacheNodeDAO = new CacheNodeDAOV2();
        this.icacheNodeDAO.init();
        this.eventIdStore = (WindowStore<String, WfEvents>) context.getStateStore(this.storeName);
        this.context = context;

        this.eventIdStore.fetchAll(
            Instant.now()
                .minusSeconds(CacheMapCollector.WINDOW_SIZE_IN_MS),
            Instant.now())
            .forEachRemaining((t) -> this.rebuildCache(t));

    }

    private void rebuildCache(KeyValue<Windowed<String>, WfEvents> t) {
        this.icacheNodeDAO.addOrCreate(t.key.key(), t.value);
    }

    @Override
    public KeyValue<String, String> transform(String key, WfEvents value) {
        this.eventIdStore.put(key, value, this.context.timestamp());
        return this.doTransform(key, value);
    }

    protected KeyValue<String, String> doTransform(String key, WfEvents value) {
        this.icacheNodeDAO.addOrCreate(key, value);
        if (this.icacheNodeDAO.allEventsAreReceivedForAnImage(key)) {
            CacheMapCollector.LOGGER.info("[CACHE_MAP_COLLECTOR][{}] image is fully processed ", key);
            return new KeyValue<>(key, key);
        }
        return null;
    }

    @Override
    public void close() {}

    public static Transformer<String, WfEvents, KeyValue<String, String>> of(String storeName) {
        return new CacheMapCollector(storeName);
    }

}
