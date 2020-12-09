package com.gs.photo.workflow.daos;

public interface ICacheNodeDAO<K, V> {
    public V addOrCreate(K key, V events);

    public boolean allEventsAreReceivedForAnImage(K imgId);

    public void init();
}
