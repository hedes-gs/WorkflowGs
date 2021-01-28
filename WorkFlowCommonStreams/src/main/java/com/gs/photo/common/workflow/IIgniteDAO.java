package com.gs.photo.common.workflow;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface IIgniteDAO {

    public boolean save(String key, byte[] rawFile);

    public void delete(Set<String> keys);

    public Optional<byte[]> get(String key);

    public <T extends Serializable> void save(Map<String, T> data, Class<T> cl);

    public <T extends Serializable> T get(String key, Class<T> cl);

    public boolean isReady();

}
