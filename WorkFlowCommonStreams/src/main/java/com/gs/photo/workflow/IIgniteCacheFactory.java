package com.gs.photo.workflow;

import org.apache.ignite.IgniteCache;

public interface IIgniteCacheFactory {

    public <V> IgniteCache<String, V> getIgniteCache(Class<V> cl);

    public boolean isReady();

    <V> IgniteCache<String, V> getIgniteCacheBinary(Class<V> cl);

}