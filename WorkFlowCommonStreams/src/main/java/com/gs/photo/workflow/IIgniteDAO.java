package com.gs.photo.workflow;

import java.io.Serializable;
import java.util.Map;

public interface IIgniteDAO {

    public void save(String key, byte[] rawFile);

    public byte[] get(String key);

    public <T extends Serializable> void save(Map<String, T> data, Class<T> cl);

    public <T extends Serializable> T get(String key, Class<T> cl);
}
