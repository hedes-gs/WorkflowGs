package com.gs.photo.workflow;

public interface IIgniteDAO {

	public void save(String key, byte[] rawFile);

	public byte[] get(String key);
}
