package com.gs.photo.workflow;

import java.io.Serializable;
import java.util.Map;

import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "ignite.is.used", havingValue = "true")
public class IgniteDAO implements IIgniteDAO {

    protected final Logger                LOGGER = LoggerFactory.getLogger(IgniteDAO.class);

    @Autowired
    protected IgniteCache<String, byte[]> igniteCache;

    @Autowired
    protected IIgniteCacheFactory         igniteCacheFactory;

    @Override
    public void save(String key, byte[] rawFile) {
        boolean saved = this.igniteCache.putIfAbsent(key,
            rawFile);
        if (!saved) {
            this.LOGGER.warn("Warning : file with key {} already exist",
                key);
        }
    }

    @Override
    public <T extends Serializable> void save(Map<String, T> data, Class<T> cl) {
        this.igniteCacheFactory.getIgniteCache(cl).putAll(data);
    }

    public <T extends Serializable> T get(String key, Class<T> cl) {
        return this.igniteCacheFactory.getIgniteCache(cl).get(key);
    }

    @Override
    public byte[] get(String key) {
        return this.igniteCache.get(key);
    }

}
