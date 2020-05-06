package com.gs.photo.workflow;

import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(name = "ignite.is.used", havingValue = "true")
public class IgniteCacheFactory implements IIgniteCacheFactory {

    private static final Logger   LOGGER             = LoggerFactory.getLogger(IIgniteCacheFactory.class);

    private static final String   IGNITE_SPRING_BEAN = "igniteSpringBean";

    @Qualifier(IgniteCacheFactory.IGNITE_SPRING_BEAN)
    @Autowired
    protected Ignite              beanIgnite;

    @Value("${ignite.defaultCache}")
    protected String              defaultCache;

    @Value("#{${ignite.caches}}")
    protected Map<String, String> cachesPerclasses;

    @Override
    public boolean isReady() {
        try {
            this.beanIgnite.getOrCreateCache(this.defaultCache);
            return true;
        } catch (Exception e) {
            IgniteCacheFactory.LOGGER.info("Ignite not ready...");
        }
        return false;
    }

    @Override
    public <V> IgniteCache<String, V> getIgniteCache(Class<V> cl) {
        final String cacheName = this.cachesPerclasses.get(cl.getName());
        if (cacheName != null) { return this.beanIgnite.getOrCreateCache(cacheName); }
        return this.beanIgnite.getOrCreateCache(this.defaultCache);
    }

}
