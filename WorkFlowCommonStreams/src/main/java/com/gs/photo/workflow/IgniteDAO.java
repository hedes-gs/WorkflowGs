package com.gs.photo.workflow;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "ignite.is.used", havingValue = "true")
public class IgniteDAO implements IIgniteDAO {

    protected final Logger        LOGGER = LoggerFactory.getLogger(IgniteDAO.class);

    @Autowired
    protected IIgniteCacheFactory igniteCacheFactory;

    @Override
    public boolean save(String key, byte[] rawFile) {
        final IgniteCache<String, byte[]> igniteCache2 = this.igniteCacheFactory.getIgniteCache(byte[].class);
        boolean saved = igniteCache2.putIfAbsent(key, rawFile);
        if (!saved) {
            this.LOGGER.warn("Warning : file with key {} already exist", key);
        }
        return saved;
    }

    @Override
    public void delete(Set<String> keys) {
        final IgniteCache<String, byte[]> igniteCache2 = this.igniteCacheFactory.getIgniteCache(byte[].class);
        igniteCache2.clearAll(keys);
    }

    @Override
    public <T extends Serializable> void save(Map<String, T> data, Class<T> cl) {
        this.igniteCacheFactory.getIgniteCache(cl)
            .putAll(data);
    }

    @Override
    public <T extends Serializable> T get(String key, Class<T> cl) {
        return this.igniteCacheFactory.getIgniteCache(cl)
            .get(key);
    }

    @Override
    public Optional<byte[]> get(String key) {
        final IgniteCache<String, byte[]> igniteCache2 = this.igniteCacheFactory.getIgniteCache(byte[].class);
        return Optional.ofNullable(igniteCache2.get(key));
    }

    @Override
    public boolean isReady() { return this.igniteCacheFactory.isReady(); }

}