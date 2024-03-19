package com.gs.photo.common.workflow;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteCacheFactory implements IIgniteCacheFactory {

    private static final Logger LOGGER             = LoggerFactory.getLogger(IIgniteCacheFactory.class);

    public static final String  IGNITE_SPRING_BEAN = "igniteSpringBean";

    protected Ignite            beanIgnite;
    protected IIgniteProperties igniteProperties;

    @Override
    public boolean isReady() {
        try {
            this.beanIgnite.getOrCreateCache(this.igniteProperties.getDefaultCache());
            return true;
        } catch (Exception e) {
            IgniteCacheFactory.LOGGER.info("Ignite not ready...");
        }
        return false;
    }

    @Override
    public <V> IgniteCache<String, V> getIgniteCache(Class<V> cl) {
        final String cacheName = this.igniteProperties.getCachesPerclasses()
            .get(cl.getName());
        if (cacheName != null) { return this.beanIgnite.getOrCreateCache(cacheName); }
        return this.beanIgnite.getOrCreateCache(this.igniteProperties.getDefaultCache());
    }

    @Override
    public <V> IgniteCache<String, V> getIgniteCacheBinary(Class<V> cl) {
        return this.beanIgnite.getOrCreateCache(this.igniteProperties.getDefaultCache())
            .withKeepBinary();
    }

    public IgniteCacheFactory(
        Ignite beanIgnite,
        IIgniteProperties igniteProperties
    ) {
        super();
        this.beanIgnite = beanIgnite;
        this.igniteProperties = igniteProperties;
    }

}
